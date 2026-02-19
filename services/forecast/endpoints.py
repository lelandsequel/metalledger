"""
MetalLedger — Forecast service API endpoints.

Routes:
  POST /forecast/run           — Runs all models for all metals, stores results
  GET  /forecast/latest        — Latest P10/P50/P90 by metal and horizon
  GET  /forecast/live          — Real-time price (yfinance) + latest forecast
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

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


# ── GET /forecast/live ────────────────────────────────────────────────────────

class LiveForecastOut(BaseModel):
    """Response schema for /forecast/live."""
    metal_slug:        str
    # Live price (15-min delay via yfinance)
    scrap_price:       Optional[float]
    raw_futures_price: Optional[float]
    ticker:            Optional[str]
    spread_factor:     Optional[float]
    fetched_at:        Optional[str]
    # Latest stored forecast — P10/P50/P90 at 30, 90, 180 trading days
    p10_30d:           Optional[float]
    p50_30d:           Optional[float]
    p90_30d:           Optional[float]
    p10_90d:           Optional[float]
    p50_90d:           Optional[float]
    p90_90d:           Optional[float]
    p10_180d:          Optional[float]
    p50_180d:          Optional[float]
    p90_180d:          Optional[float]
    # Source of price data
    price_source:      str  # "live" | "synthetic"
    generated_at:      str


_SYNTHETIC_PRICES = {
    "CU_BARE": (4.02, 4.1443, "HG=F", 0.97),
    "CU_1":    (3.71, 4.1443, "HG=F", 0.91),
    "CU_2":    (3.18, 4.1443, "HG=F", 0.82),
    "HMS1":    (0.099, 0.1053, "HR=F", 0.94),
    "HMS2":    (0.0905, 0.1040, "HR=F", 0.87),
    "SHRED":   (0.096, 0.1055, "HR=F", 0.91),
    "CAST":    (0.072, 0.1059, "HR=F", 0.68),
    "AL_CAST": (0.48, 0.9231, "ALI=F", 0.52),
    "AL_EXTRUSION": (0.57, 0.9344, "ALI=F", 0.61),
}


@router.get(
    "/forecast/live",
    response_model=LiveForecastOut,
    summary="Real-time price + latest forecast for a scrap metal",
)
async def get_live_forecast(
    metal:   str,
    request: Request,
    pool=Depends(get_pool),
):
    """
    Returns current scrap price estimate (15-min delayed via yfinance futures)
    plus the latest stored P10/P50/P90 forecast at 30, 90, and 180 trading days.

    Falls back to synthetic/seed price if yfinance is unavailable.

    Query param: `metal` (e.g. CU_BARE, HMS1, AL_CAST)
    """
    from datetime import timezone
    request_id = new_request_id()
    metal_upper = metal.upper()

    # ── 1. Fetch live price ───────────────────────────────────────────────────
    live_meta  = None
    price_source = "synthetic"

    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from data.commodity_feed import get_latest_price_with_meta
        live_meta = get_latest_price_with_meta(metal_upper)
        if live_meta:
            price_source = "live"
    except Exception as exc:
        log.warning("live price fetch failed for %s: %s", metal_upper, exc)

    # Synthetic fallback
    if live_meta is None:
        synth = _SYNTHETIC_PRICES.get(metal_upper)
        if synth:
            scrap_p, raw_p, ticker, spread = synth
            live_meta = {
                "metal_slug":        metal_upper,
                "scrap_price":       scrap_p,
                "raw_futures_price": raw_p,
                "ticker":            ticker,
                "spread_factor":     spread,
                "fetched_at":        datetime.now(timezone.utc).isoformat(),
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Unknown metal: {metal}",
            )

    # ── 2. Pull latest forecast rows from DB ──────────────────────────────────
    # We want the ensemble P50 at horizons 30, 90, 180 days.
    # Horizons stored are 1, 5, 20 — we extrapolate from the closest stored horizon
    # for now (20-day P50/P10/P90 scaled by √(horizon/20) for spread).
    rows = await pool.fetch(
        """
        SELECT DISTINCT ON (model, horizon)
            model, horizon, p10, p50, p90
        FROM forecasts
        WHERE metal = $1
        ORDER BY model, horizon, run_at DESC
        """,
        metal_upper,
    )

    def _get_pq(target_horizon: int) -> dict:
        """Get median ensemble P10/P50/P90 for a given horizon, or extrapolate."""
        # Exact match
        exact = [r for r in rows if r["horizon"] == target_horizon]
        if exact:
            p50s = [float(r["p50"]) for r in exact if r["p50"] is not None]
            p10s = [float(r["p10"]) for r in exact if r["p10"] is not None]
            p90s = [float(r["p90"]) for r in exact if r["p90"] is not None]
            return {
                "p10": round(sum(p10s)/len(p10s), 6) if p10s else None,
                "p50": round(sum(p50s)/len(p50s), 6) if p50s else None,
                "p90": round(sum(p90s)/len(p90s), 6) if p90s else None,
            }

        # Extrapolate from nearest stored horizon (closest to target)
        if not rows:
            return {"p10": None, "p50": None, "p90": None}

        stored_horizons = sorted({r["horizon"] for r in rows})
        nearest = min(stored_horizons, key=lambda h: abs(h - target_horizon))
        base_rows = [r for r in rows if r["horizon"] == nearest]
        if not base_rows:
            return {"p10": None, "p50": None, "p90": None}

        p50s = [float(r["p50"]) for r in base_rows if r["p50"] is not None]
        p10s = [float(r["p10"]) for r in base_rows if r["p10"] is not None]
        p90s = [float(r["p90"]) for r in base_rows if r["p90"] is not None]
        if not p50s:
            return {"p10": None, "p50": None, "p90": None}

        base_p50 = sum(p50s) / len(p50s)
        scale = (target_horizon / nearest) ** 0.5  # spread grows with √time

        base_p10 = sum(p10s)/len(p10s) if p10s else base_p50 * 0.95
        base_p90 = sum(p90s)/len(p90s) if p90s else base_p50 * 1.05
        half_spread = (base_p90 - base_p10) / 2

        return {
            "p10": round(base_p50 - half_spread * scale, 6),
            "p50": round(base_p50, 6),
            "p90": round(base_p50 + half_spread * scale, 6),
        }

    pq_30  = _get_pq(30)
    pq_90  = _get_pq(90)
    pq_180 = _get_pq(180)

    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = "api",
        action     = f"GET /forecast/live metal={metal}",
        payload    = {"price_source": price_source},
    )

    return LiveForecastOut(
        metal_slug        = live_meta["metal_slug"],
        scrap_price       = live_meta["scrap_price"],
        raw_futures_price = live_meta["raw_futures_price"],
        ticker            = live_meta["ticker"],
        spread_factor     = live_meta["spread_factor"],
        fetched_at        = live_meta["fetched_at"],
        p10_30d           = pq_30["p10"],
        p50_30d           = pq_30["p50"],
        p90_30d           = pq_30["p90"],
        p10_90d           = pq_90["p10"],
        p50_90d           = pq_90["p50"],
        p90_90d           = pq_90["p90"],
        p10_180d          = pq_180["p10"],
        p50_180d          = pq_180["p50"],
        p90_180d          = pq_180["p90"],
        price_source      = price_source,
        generated_at      = datetime.now(timezone.utc).isoformat(),
    )
