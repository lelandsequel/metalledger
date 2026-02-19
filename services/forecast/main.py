"""
MetalLedger — Forecast service (FastAPI app).
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from common.config import DATABASE_URL
from common.logging_util import get_logger
from endpoints import router

log = get_logger("forecast")

app = FastAPI(
    title       = "MetalLedger — Forecast Service",
    description = "Agentic price forecasting for precious metals.",
    version     = "0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)

app.include_router(router)


@app.on_event("startup")
async def startup():
    for attempt in range(10):
        try:
            app.state.pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=1, max_size=10
            )
            log.info("Forecast service connected to database")
            return
        except Exception as exc:
            import asyncio
            log.warning("DB connect attempt %d: %s", attempt + 1, exc)
            await asyncio.sleep(3)
    raise RuntimeError("Could not connect to database")


@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, "pool"):
        await app.state.pool.close()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "forecast"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
