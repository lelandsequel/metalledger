"""
MetalLedger — Forecast service API endpoints.

Routes:
  POST /forecast/run           — Runs all models for all metals, stores results
  GET  /forecast/latest        — Latest P10/P50/P90 by metal and horizon
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from fastapi import APIRouter, Depends, HTTPException, Request, status

from common.audit import write_audit_entry_async, new_request_id
from common.logging_util import get_logger
from common.types import ForecastOut

log    = get_logger(__name__)
router = APIRouter()

# Scrap metal slugs — ferrous + non-ferrous
METALS = [
    # Ferrous
    "HMS1", "HMS2", "SHRED", "CAST",
    # Non-ferrous
    "CU_BARE", "CU_1", "CU_2",
    "AL_CAST", "AL_EXTRUSION",
    "BRASS", "SS_304", "LEAD", "ZORBA",
]
HORIZONS = [1, 5, 20]


def get_pool(request: Request):
    return request.app.state.pool


async def _load_prices(pool, metal: str) -> tuple[list[float], list]:
    """Load canonical prices from DB for a metal, ascending by date."""
    rows = await pool.fetch(
        """
        SELECT price_ts::date AS price_date, value
        FROM prices_canonical
        WHERE metal = $1
        ORDER BY price_ts ASC
        """,
        metal,
    )
    prices = [float(r["value"]) for r in rows]
    dates  = [r["price_date"] for r in rows]
    return prices, dates


async def _run_model_and_store(
    pool,
    model_name: str,
    model_fn,
    metal: str,
    prices: list[float],
    request_id,
) -> List[dict]:
    """Run a single model for all horizons and store forecast rows."""
    if not prices:
        return []

    forecasts = model_fn(prices, horizons=HORIZONS)
    stored    = []

    for horizon, pq in forecasts.items():
        if pq["p50"] is None:
            continue

        row_id = await pool.fetchval(
            """
            INSERT INTO forecasts (model, metal, horizon, p10, p50, p90)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            model_name, metal, horizon,
            pq.get("p10"), pq["p50"], pq.get("p90"),
        )
        stored.append({
            "id":      row_id,
            "model":   model_name,
            "metal":   metal,
            "horizon": horizon,
            **pq,
        })

    log.info("Stored %d forecast rows for %s/%s", len(stored), model_name, metal)
    return stored


# ── POST /forecast/run ────────────────────────────────────────────────────────

@router.post(
    "/forecast/run",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Run all forecast models for all metals",
)
async def run_forecast(
    request: Request,
    pool=Depends(get_pool),
):
    """
    Trigger a forecast run across all metals (XAU, XAG, CU) and all models
    (naive, ARIMA, gradient_boost).

    Returns a summary of forecast rows created.
    Allowed for agent actor.
    """
    request_id = new_request_id()
    log.info("Forecast run triggered request_id=%s", request_id)

    # Import models here to keep startup fast
    from models import naive, arima_model, gradient_boost

    model_registry = [
        ("naive",          naive.run),
        ("arima",          arima_model.run),
        ("gradient_boost", gradient_boost.run),
    ]

    all_results = []

    for metal in METALS:
        prices, dates = await _load_prices(pool, metal)
        if len(prices) < 5:
            log.warning("Insufficient data for %s (%d rows)", metal, len(prices))
            continue

        for model_name, model_fn in model_registry:
            try:
                results = await _run_model_and_store(
                    pool, model_name, model_fn, metal, prices, request_id
                )
                all_results.extend(results)
            except Exception as exc:
                log.error("Model %s/%s failed: %s", model_name, metal, exc)

    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = "agent",
        action     = "POST /forecast/run",
        payload    = {"forecasts_stored": len(all_results)},
    )

    return {
        "status":            "ok",
        "forecasts_created": len(all_results),
        "request_id":        str(request_id),
    }


# ── GET /forecast/latest ──────────────────────────────────────────────────────

@router.get(
    "/forecast/latest",
    response_model=List[ForecastOut],
    summary="Latest P10/P50/P90 for a metal across all horizons",
)
async def get_latest_forecast(
    metal:   str,
    request: Request,
    pool=Depends(get_pool),
):
    """
    Return the most recent forecast rows for a metal across all horizons
    and all models.

    Query param: `metal` (XAU, XAG, CU)
    """
    request_id = new_request_id()

    rows = await pool.fetch(
        """
        SELECT DISTINCT ON (model, horizon)
            id, model, metal, horizon, run_at, p10, p50, p90
        FROM forecasts
        WHERE metal = $1
        ORDER BY model, horizon, run_at DESC
        """,
        metal.upper(),
    )

    if not rows:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail      = f"No forecasts found for {metal}",
        )

    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = "api",
        action     = f"GET /forecast/latest metal={metal}",
        payload    = {"count": len(rows)},
    )

    return [ForecastOut(**dict(r)) for r in rows]
