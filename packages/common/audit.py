"""
MetalLedger — Immutable audit log writer.

Every API action and agent action MUST call `write_audit_entry()`.
Entries are stored in the audit_log table which has UPDATE/DELETE triggers
that deny all mutations (append-only enforcement at DB level).

Each entry contains:
  - request_id: UUID (v4), correlates all actions in one request
  - actor:      role string ("HUMAN", "agent", API key role, etc.)
  - action:     short verb/noun describing what happened
  - payload_hash: SHA256 hex of the serialised payload
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


def _sha256(payload: Any) -> str:
    """Return SHA-256 hex digest of the JSON-serialised payload."""
    raw = json.dumps(payload, default=str, sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


def new_request_id() -> uuid.UUID:
    """Generate a fresh request UUID."""
    return uuid.uuid4()


# ─── Sync DB writer (for services that use psycopg2 / synchronous pools) ─────

def write_audit_entry_sync(
    conn: Any,
    *,
    request_id: uuid.UUID,
    actor:       str,
    action:      str,
    payload:     Any,
) -> None:
    """
    Write an audit entry using a synchronous psycopg2-compatible connection.

    Args:
        conn:       A psycopg2 connection (or compatible); caller manages lifecycle.
        request_id: UUID identifying the current request.
        actor:      Who is performing the action ("HUMAN", "agent", role name).
        action:     Short description, e.g. "POST /journal_entries".
        payload:    Any JSON-serialisable dict/object.  Its hash is stored.
    """
    payload_hash = _sha256(payload)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit_log (request_id, actor, action, payload_hash)
            VALUES (%s, %s, %s, %s)
            """,
            (str(request_id), actor, action, payload_hash),
        )
    conn.commit()


# ─── Async DB writer (for FastAPI services that use asyncpg) ──────────────────

async def write_audit_entry_async(
    pool: Any,
    *,
    request_id: uuid.UUID,
    actor:       str,
    action:      str,
    payload:     Any,
) -> None:
    """
    Write an audit entry using an asyncpg pool.

    Args:
        pool:       asyncpg connection pool.
        request_id: UUID identifying the current request.
        actor:      Who is performing the action.
        action:     Short description.
        payload:    Any JSON-serialisable object.  Its hash is stored.
    """
    payload_hash = _sha256(payload)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO audit_log (request_id, actor, action, payload_hash)
            VALUES ($1, $2, $3, $4)
            """,
            str(request_id), actor, action, payload_hash,
        )


# ─── In-memory fallback (for tests / services without DB) ────────────────────

_IN_MEMORY_LOG: list[dict] = []


def write_audit_entry_memory(
    *,
    request_id: uuid.UUID,
    actor:       str,
    action:      str,
    payload:     Any,
) -> None:
    """Write to an in-process list.  Useful in unit tests."""
    _IN_MEMORY_LOG.append(
        {
            "request_id":   str(request_id),
            "actor":        actor,
            "action":       action,
            "payload_hash": _sha256(payload),
            "created_at":   datetime.now(tz=timezone.utc).isoformat(),
        }
    )


def get_memory_log() -> list[dict]:
    """Return a copy of the in-memory audit log."""
    return list(_IN_MEMORY_LOG)


def clear_memory_log() -> None:
    """Clear in-memory log (test teardown only)."""
    _IN_MEMORY_LOG.clear()
