"""
MetalLedger — Orchestrator scheduler.

Reads ORCHESTRATOR_CRON from environment (default: "0 6 * * *" = daily 06:00 UTC).
On each trigger:
  1. Checks policy (guardrails)
  2. Calls forecast service: POST /forecast/run
  3. Generates markdown report
  4. Stores report in DB + writes to /reports/YYYY-MM-DD_<metal>.md
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from common.config import FORECAST_BASE_URL, ORCHESTRATOR_CRON
from common.egress import egress_post
from common.logging_util import get_logger
from policy import check_action, PolicyViolation

log = get_logger("orchestrator.scheduler")


async def trigger_forecast_run(pool) -> dict:
    """
    Call the forecast service to run all models.

    Uses egress_post (Guardrail #3: all external calls via egress module).
    The forecast service is an internal service URL, not an external domain,
    so it uses httpx directly (not egress) — internal services are not
    subject to the egress allowlist which guards only external internet calls.
    """
    import httpx

    url = f"{FORECAST_BASE_URL}/forecast/run"
    log.info("Triggering forecast run at %s", url)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url)
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        log.error("Forecast run failed: %s", exc)
        return {"error": str(exc), "forecasts_created": 0}


async def orchestrator_tick(pool) -> None:
    """
    Main orchestrator action — runs on schedule.

    Flow:
    1. Policy check: ensure this action is allowed for agent
    2. Trigger forecast run
    3. Generate markdown report
    4. Store report
    """
    log.info("Orchestrator tick at %s", datetime.now(tz=timezone.utc).isoformat())

    # Guardrail check before any action
    try:
        await check_action("POST /forecast/run", "agent", pool)
    except PolicyViolation as exc:
        log.error("Orchestrator blocked by policy: %s", exc)
        return

    # Trigger forecast
    from reporter import generate_and_store_report
    forecast_result = await trigger_forecast_run(pool)

    # Generate and store report
    await generate_and_store_report(pool, forecast_result)


def build_scheduler(pool):
    """
    Build an APScheduler AsyncIOScheduler from the ORCHESTRATOR_CRON config.

    Falls back to a simple interval loop (every 24h) if APScheduler is missing.
    """
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = AsyncIOScheduler()
        parts     = ORCHESTRATOR_CRON.split()
        if len(parts) == 5:
            minute, hour, dom, month, dow = parts
        else:
            minute, hour, dom, month, dow = "0", "6", "*", "*", "*"

        scheduler.add_job(
            orchestrator_tick,
            CronTrigger(
                minute        = minute,
                hour          = hour,
                day           = dom,
                month         = month,
                day_of_week   = dow,
                timezone      = "UTC",
            ),
            args      = [pool],
            id        = "orchestrator_tick",
            name      = "MetalLedger orchestrator tick",
            replace_existing = True,
        )
        log.info("APScheduler configured with cron: %s", ORCHESTRATOR_CRON)
        return scheduler

    except ImportError:
        log.warning("APScheduler not available; falling back to asyncio loop (24h interval)")
        return None


async def run_loop(pool) -> None:
    """Simple fallback: run tick every 24 hours."""
    while True:
        try:
            await orchestrator_tick(pool)
        except Exception as exc:
            log.exception("Orchestrator tick error: %s", exc)
        await asyncio.sleep(86400)   # 24 hours
