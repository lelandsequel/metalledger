"""
Tests for MetalLedger orchestrator policy gates.

Verifies:
1. Agent cannot POST /journal_entries (raises PolicyViolation).
2. EgressViolation raised for non-allowlisted domain.
3. Agent CAN call POST /forecast/run (allowed action).
4. Source config mutation without approval raises PolicyViolation.
5. check_egress allows metals-api.com, blocks example.com.
"""

from __future__ import annotations

import os
import sys
import asyncio

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.egress import EgressViolation, ALLOWLIST
from policy import PolicyViolation, check_action, check_egress, require_approval


# ── In-memory pool mock ───────────────────────────────────────────────────────

class _FakeConn:
    """Fake asyncpg connection."""

    def __init__(self, pool: "FakePool"):
        self._pool = pool

    async def execute(self, query: str, *args):
        if "INSERT INTO policy_events" in query:
            self._pool.policy_events.append(args)
        if "INSERT INTO audit_log" in query:
            self._pool.audit_log.append(args)

    async def fetchrow(self, query: str, *args):
        if "approvals" in query and self._pool._has_approval:
            return {"id": 1}
        return None

    async def fetchval(self, query: str, *args):
        return None


class _FakeAcquire:
    def __init__(self, pool: "FakePool"):
        self._pool = pool

    async def __aenter__(self):
        return _FakeConn(self._pool)

    async def __aexit__(self, *args):
        pass


class FakePool:
    """Asyncpg pool mock for policy tests."""

    def __init__(self, has_approval: bool = False):
        self._has_approval = has_approval
        self.policy_events: list = []
        self.audit_log:     list = []

    def acquire(self):
        return _FakeAcquire(self)

    async def execute(self, query: str, *args):
        if "INSERT INTO policy_events" in query:
            self.policy_events.append(args)

    async def fetchrow(self, query: str, *args):
        if "approvals" in query and self._has_approval:
            return {"id": 1}
        return None

    async def fetchval(self, query: str, *args):
        return None


# ── Helper to run async tests ─────────────────────────────────────────────────

def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ── Test: Guardrail #1 — agent cannot journal ─────────────────────────────────

class TestGuardrail1_JournalBlock:
    def test_agent_cannot_post_journal_entries(self):
        """Agent attempting POST /journal_entries must raise PolicyViolation."""
        pool = FakePool()
        with pytest.raises(PolicyViolation) as exc_info:
            run(check_action("POST /journal_entries", "agent", pool))

        assert "journal" in str(exc_info.value).lower()
        assert exc_info.value.actor  == "agent"
        assert exc_info.value.action == "POST /journal_entries"

    def test_agent_cannot_patch_journal_entries(self):
        """Agent cannot PATCH journal entries either."""
        pool = FakePool()
        with pytest.raises(PolicyViolation):
            run(check_action("PATCH /journal_entries", "agent", pool))

    def test_agent_cannot_create_journal_entry_alias(self):
        """Internal create_journal_entry alias also blocked."""
        pool = FakePool()
        with pytest.raises(PolicyViolation):
            run(check_action("create_journal_entry", "agent", pool))

    def test_human_can_post_journal_entries(self):
        """HUMAN role should NOT be blocked by Guardrail #1."""
        pool = FakePool()
        # HUMAN is not "agent", so this should be allowed
        result = run(check_action("POST /journal_entries", "HUMAN", pool))
        assert result == "ALLOWED"

    def test_policy_event_logged_on_denial(self):
        """A policy_events row is written when agent is denied."""
        pool = FakePool()
        try:
            run(check_action("POST /journal_entries", "agent", pool))
        except PolicyViolation:
            pass
        assert len(pool.policy_events) > 0


# ── Test: Guardrail #2 — source config approval ───────────────────────────────

class TestGuardrail2_SourceConfigApproval:
    def test_source_config_mutation_without_approval_denied(self):
        """Mutating source_configs without approval raises PolicyViolation."""
        pool = FakePool(has_approval=False)
        with pytest.raises(PolicyViolation) as exc_info:
            run(check_action("UPDATE source_configs", "agent", pool))
        assert "approval" in str(exc_info.value).lower()

    def test_source_config_mutation_with_approval_allowed(self):
        """Mutating source_configs WITH active approval is allowed."""
        pool = FakePool(has_approval=True)
        result = run(check_action("UPDATE source_configs", "agent", pool))
        assert result == "ALLOWED"

    def test_require_approval_without_approval_raises(self):
        """require_approval() raises PolicyViolation if no approval row."""
        pool = FakePool(has_approval=False)
        with pytest.raises(PolicyViolation) as exc_info:
            run(require_approval("metals_api", pool))
        assert "approval" in str(exc_info.value).lower()

    def test_require_approval_with_approval_passes(self):
        """require_approval() returns None (no exception) when approved."""
        pool = FakePool(has_approval=True)
        # Should not raise
        run(require_approval("metals_api", pool))


# ── Test: Guardrail #3 — Egress allowlist ────────────────────────────────────

class TestGuardrail3_EgressAllowlist:
    def test_allowlist_contains_expected_domains(self):
        """Verify allowlist includes required domains."""
        assert "metals-api.com"   in ALLOWLIST
        assert "api.lbma.org.uk"  in ALLOWLIST

    def test_metals_api_allowed(self):
        """metals-api.com passes egress check."""
        # Should not raise
        check_egress("https://metals-api.com/api/latest")

    def test_lbma_allowed(self):
        """api.lbma.org.uk passes egress check."""
        check_egress("https://api.lbma.org.uk/gold/price/json")

    def test_subdomain_of_allowed_domain_passes(self):
        """Subdomains of allowed domains pass (e.g. data.metals-api.com)."""
        check_egress("https://data.metals-api.com/endpoint")

    def test_unlisted_domain_raises_egress_violation(self):
        """A domain not on the allowlist raises EgressViolation."""
        with pytest.raises(EgressViolation) as exc_info:
            check_egress("https://example.com/data")
        assert "example.com" in str(exc_info.value)

    def test_google_raises_egress_violation(self):
        """google.com is not allowed."""
        with pytest.raises(EgressViolation):
            check_egress("https://www.google.com/search")

    def test_similar_domain_not_allowed(self):
        """A similar-but-different domain is blocked (metals-api.org vs metals-api.com)."""
        with pytest.raises(EgressViolation):
            check_egress("https://metals-api.org/api/latest")

    def test_empty_domain_raises(self):
        """Malformed URL with no recognised domain raises EgressViolation."""
        with pytest.raises(EgressViolation):
            check_egress("https://evil.example.net/api")


# ── Test: Guardrail #4 — Audit log ───────────────────────────────────────────

class TestGuardrail4_AuditLog:
    def test_allowed_action_is_audited(self):
        """
        check_action writes to audit_log on ALLOWED.
        We patch at the policy module level (where the name is bound after import).
        """
        from common.audit import get_memory_log, clear_memory_log, write_audit_entry_memory
        import policy as policy_mod

        audit_entries: list = []

        async def capturing_write(pool, *, request_id, actor, action, payload):
            """Capture calls in our local list AND the in-memory log."""
            write_audit_entry_memory(
                request_id=request_id,
                actor=actor,
                action=action,
                payload=payload,
            )
            audit_entries.append({"actor": actor, "action": action})

        original = policy_mod.write_audit_entry_async
        policy_mod.write_audit_entry_async = capturing_write

        clear_memory_log()
        pool = FakePool()
        try:
            run(check_action("POST /forecast/run", "agent", pool))
        finally:
            policy_mod.write_audit_entry_async = original

        assert len(audit_entries) > 0, "No audit entries written"
        assert any(e["actor"] == "agent" for e in audit_entries)
        clear_memory_log()


# ── Test: Agent CAN trigger forecast ─────────────────────────────────────────

class TestAgentAllowedActions:
    def test_agent_can_trigger_forecast_run(self):
        """Agent can call POST /forecast/run (allowed action)."""
        pool = FakePool()
        result = run(check_action("POST /forecast/run", "agent", pool))
        assert result == "ALLOWED"

    def test_agent_can_get_forecasts(self):
        """Agent can read forecast results."""
        pool = FakePool()
        result = run(check_action("GET /forecast/latest", "agent", pool))
        assert result == "ALLOWED"
