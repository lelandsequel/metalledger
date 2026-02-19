"""
MetalLedger — Orchestrator Agent (FastAPI + scheduler).
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from common.config import DATABASE_URL
from common.logging_util import get_logger
from scheduler import build_scheduler, run_loop, orchestrator_tick

log = get_logger("orchestrator")

app = FastAPI(
    title       = "MetalLedger — Orchestrator Agent",
    description = "Guardrailed orchestrator: forecast scheduling, report generation.",
    version     = "0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)


@app.on_event("startup")
async def startup():
    # Connect DB
    for attempt in range(10):
        try:
            app.state.pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=1, max_size=5
            )
            log.info("Orchestrator connected to database")
            break
        except Exception as exc:
            log.warning("DB connect attempt %d: %s", attempt + 1, exc)
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Could not connect to database")

    # Start scheduler
    scheduler = build_scheduler(app.state.pool)
    if scheduler:
        scheduler.start()
        app.state.scheduler = scheduler
        log.info("APScheduler started")
    else:
        # Fallback: run in background task
        app.state.bg_task = asyncio.create_task(run_loop(app.state.pool))
        log.info("Fallback loop task started")


@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, "scheduler"):
        app.state.scheduler.shutdown(wait=False)
    if hasattr(app.state, "bg_task"):
        app.state.bg_task.cancel()
    if hasattr(app.state, "pool"):
        await app.state.pool.close()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "orchestrator_agent"}


@app.post("/trigger", summary="Manually trigger an orchestrator tick (for testing)")
async def trigger():
    """
    Manually trigger the orchestrator tick.
    Still subject to all policy guardrails.
    """
    try:
        await orchestrator_tick(app.state.pool)
        return {"status": "triggered"}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
