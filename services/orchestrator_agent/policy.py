"""
MetalLedger — Policy gate enforcement for the orchestrator agent.

NON-NEGOTIABLE GUARDRAILS:
1. Agent cannot create/modify journal entries.
2. Agent cannot change source configs without HUMAN-signed APPROVAL.
3. All external network calls must go through egress.py.
4. Everything writes to audit_log.

This module exposes:
  check_action(action, actor, pool) → raises PolicyViolation or returns "ALLOWED"
  require_approval(config_key, pool) → raises PolicyViolation if not approved
  log_policy_event(action, actor, result, reason, pool) → writes to policy_events
"""

from __future__ import annotations

import os
import sys
from typing import Any, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from common.audit import write_audit_entry_async, new_request_id
from common.egress import EgressViolation, ALLOWLIST
from common.logging_util import get_logger

log = get_logger(__name__)


# ── Exceptions ────────────────────────────────────────────────────────────────

class PolicyViolation(Exception):
    """Raised when an action violates a guardrail."""
    def __init__(self, action: str, actor: str, reason: str):
        self.action = action
        self.actor  = actor
        self.reason = reason
        super().__init__(
            f"PolicyViolation: actor='{actor}' action='{action}' reason='{reason}'"
        )


# ── Blocked actions (Guardrail #1) ────────────────────────────────────────────

# Actions that agents are NEVER allowed to perform
_AGENT_BLOCKED_ACTIONS = {
    "POST /journal_entries",
    "PATCH /journal_entries",
    "PUT /journal_entries",
    "DELETE /journal_entries",
    "create_journal_entry",
    "modify_journal_entry",
}

# Actions that require HUMAN approval (Guardrail #2)
_APPROVAL_REQUIRED_ACTIONS = {
    "UPDATE source_configs",
    "INSERT source_configs",
    "DELETE source_configs",
    "mutate_source_config",
}


# ── Core policy check ─────────────────────────────────────────────────────────

async def check_action(
    action: str,
    actor:  str,
    pool:   Any,
    payload: Optional[dict] = None,
) -> str:
    """
    Enforce policy guardrails for a given action/actor pair.

    Args:
        action:  The action being attempted (e.g. "POST /journal_entries").
        actor:   Who is attempting it ("agent", "HUMAN", role string).
        pool:    asyncpg pool for DB writes.
        payload: Optional payload dict for audit hashing.

    Returns:
        "ALLOWED" if the action passes all guardrails.

    Raises:
        PolicyViolation: if the action is blocked.
    """
    request_id = new_request_id()
    payload    = payload or {"action": action, "actor": actor}

    # Guardrail #1: Agent cannot create/modify journal entries
    if actor == "agent" and action in _AGENT_BLOCKED_ACTIONS:
        reason = "Guardrail #1: agent is not permitted to create or modify journal entries"
        await log_policy_event(action, actor, "DENIED", reason, pool)
        await write_audit_entry_async(
            pool,
            request_id = request_id,
            actor      = actor,
            action     = action,
            payload    = {**payload, "result": "DENIED", "reason": reason},
        )
        raise PolicyViolation(action, actor, reason)

    # Guardrail #2: Source config mutations require approval
    if action in _APPROVAL_REQUIRED_ACTIONS:
        approved = await _has_active_approval(pool, action)
        if not approved:
            reason = "Guardrail #2: source config mutations require HUMAN-signed approval"
            await log_policy_event(action, actor, "DENIED", reason, pool)
            await write_audit_entry_async(
                pool,
                request_id = request_id,
                actor      = actor,
                action     = action,
                payload    = {**payload, "result": "DENIED", "reason": reason},
            )
            raise PolicyViolation(action, actor, reason)

    # Guardrail #4: Audit log (allowed path)
    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = actor,
        action     = action,
        payload    = {**payload, "result": "ALLOWED"},
    )
    await log_policy_event(action, actor, "ALLOWED", None, pool)
    return "ALLOWED"


# ── Approval check (Guardrail #2) ─────────────────────────────────────────────

async def require_approval(config_key: str, pool: Any) -> None:
    """
    Raise PolicyViolation if no active HUMAN-signed approval exists for config_key.
    """
    approved = await _has_active_approval_for_key(pool, config_key)
    if not approved:
        raise PolicyViolation(
            action = f"mutate_source_config:{config_key}",
            actor  = "agent",
            reason = (
                f"No active HUMAN-signed approval for config_key='{config_key}'. "
                "Create an approval record in the approvals table first."
            ),
        )


async def _has_active_approval(pool: Any, action: str) -> bool:
    """Check if any non-expired, non-revoked approval exists (generic)."""
    row = await pool.fetchrow(
        """
        SELECT id FROM approvals
        WHERE NOT revoked
          AND (expires_at IS NULL OR expires_at > NOW())
        LIMIT 1
        """
    )
    return row is not None


async def _has_active_approval_for_key(pool: Any, config_key: str) -> bool:
    """Check for a HUMAN-signed, non-expired approval for a specific config key."""
    row = await pool.fetchrow(
        """
        SELECT id FROM approvals
        WHERE config_key = $1
          AND NOT revoked
          AND (expires_at IS NULL OR expires_at > NOW())
        LIMIT 1
        """,
        config_key,
    )
    return row is not None


# ── Egress guard helper (Guardrail #3) ────────────────────────────────────────

def check_egress(url: str) -> None:
    """
    Raise EgressViolation if `url` domain is not in the allowlist.
    Delegates to common.egress for consistent enforcement.
    """
    import urllib.parse
    parsed = urllib.parse.urlparse(url)
    host   = parsed.netloc.split(":")[0].lower()
    if host.startswith("www."):
        host = host[4:]

    for allowed in ALLOWLIST:
        if host == allowed or host.endswith("." + allowed):
            return

    raise EgressViolation(url, host)


# ── Event logger ──────────────────────────────────────────────────────────────

async def log_policy_event(
    action: str,
    actor:  str,
    result: str,
    reason: Optional[str],
    pool:   Any,
) -> None:
    """Write an entry to policy_events table."""
    try:
        await pool.execute(
            """
            INSERT INTO policy_events (action, actor, result, reason)
            VALUES ($1, $2, $3, $4)
            """,
            action, actor, result, reason,
        )
    except Exception as exc:
        # Never let logging failures break orchestrator flow
        log.error("Failed to log policy event: %s", exc)
