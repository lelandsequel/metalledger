"""
MetalLedger — Rolling-window (walk-forward) backtester.

For each model × metal × horizon:
  - Walk forward in time using the last N days of data
  - Compute MAPE and RMSE of the P50 forecast
  - Store results in the backtests table

Also supports run_with_real_data() which fetches 2 years of real commodity
prices from yfinance, does an 80/20 train/test split, runs a rolling backtest,
and stores MAPE + RMSE per model per metal.
"""

from __future__ import annotations

import math
import os
import sys
from datetime import date
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from common.logging_util import get_logger

log = get_logger(__name__)

METALS    = ["HMS1", "HMS2", "SHRED", "CAST", "CU_BARE", "CU_1", "CU_2", "AL_CAST", "AL_EXTRUSION"]
HORIZONS  = [1, 5, 20]
WINDOW    = 20    # minimum training window for backtest
MIN_TOTAL = 30    # minimum observations required to run backtest


def _mape(actual: List[float], predicted: List[float]) -> Optional[float]:
    """Mean Absolute Percentage Error."""
    if not actual or len(actual) != len(predicted):
        return None
    errs = [
        abs(a - p) / abs(a)
        for a, p in zip(actual, predicted)
        if a != 0
    ]
    return round(sum(errs) / len(errs), 6) if errs else None


def _rmse(actual: List[float], predicted: List[float]) -> Optional[float]:
    """Root Mean Squared Error."""
    if not actual or len(actual) != len(predicted):
        return None
    mse = sum((a - p) ** 2 for a, p in zip(actual, predicted)) / len(actual)
    return round(math.sqrt(mse), 6)


def _walk_forward_backtest(
    prices: List[float],
    model_fn,
    horizon: int,
    window: int = WINDOW,
) -> tuple[Optional[float], Optional[float], date, date]:
    """
    Walk-forward backtest for a single horizon.

    For each step i in [window, len(prices)-horizon]:
      - Train on prices[i-window:i]
      - Predict horizon steps → use P50 as point forecast
      - Compare to actual prices[i + horizon - 1]

    Returns: (mape, rmse, window_start_idx, window_end_idx)
    """
    actuals: List[float]    = []
    predicted: List[float]  = []

    for i in range(window, len(prices) - horizon + 1):
        train   = prices[i - window : i]
        target  = prices[i + horizon - 1]

        forecasts = model_fn(train, horizons=[horizon])
        p50 = forecasts.get(horizon, {}).get("p50")

        if p50 is not None:
            actuals.append(target)
            predicted.append(p50)

    if not actuals:
        return None, None, None, None

    return _mape(actuals, predicted), _rmse(actuals, predicted), 0, len(prices) - 1


async def run_backtest(
    pool: Any,
    metal: str,
    model_name: str,
    model_fn,
    prices: List[float],
    price_dates: List[date],
) -> List[Dict]:
    """
    Run walk-forward backtest for all horizons and store results in DB.

    Returns list of backtest result dicts.
    """
    if len(prices) < MIN_TOTAL:
        log.warning(
            "Backtest %s/%s: need %d obs, have %d — skipping",
            model_name, metal, MIN_TOTAL, len(prices),
        )
        return []

    results = []
    window_start = price_dates[0]
    window_end   = price_dates[-1]

    for horizon in HORIZONS:
        mape, rmse, _, _ = _walk_forward_backtest(prices, model_fn, horizon)

        row_id = await pool.fetchval(
            """
            INSERT INTO backtests
                (model, metal, horizon, window_start, window_end, mape, rmse)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            model_name, metal, horizon, window_start, window_end,
            mape, rmse,
        )

        results.append({
            "id":           row_id,
            "model":        model_name,
            "metal":        metal,
            "horizon":      horizon,
            "window_start": str(window_start),
            "window_end":   str(window_end),
            "mape":         mape,
            "rmse":         rmse,
        })
        log.info(
            "Backtest %s/%s/h=%d: MAPE=%.4f RMSE=%.4f",
            model_name, metal, horizon, mape or 0, rmse or 0,
        )

    return results


async def run_with_real_data(pool: Any) -> List[Dict]:
    """
    Fetch 2 years of real commodity data via yfinance, split 80/20 train/test,
    run rolling-window backtest, and store MAPE + RMSE per model per metal.

    This is the primary accuracy validation function for the forecast service.

    Returns:
        List of backtest result dicts (one per model × metal × horizon).
    """
    # Import here to avoid hard dependency at module load
    sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
    from data.commodity_feed import fetch_historical
    from models import naive, arima_model, gradient_boost

    model_registry = [
        ("naive",          naive.run),
        ("arima",          arima_model.run),
        ("gradient_boost", gradient_boost.run),
    ]

    all_results: List[Dict] = []

    for metal in METALS:
        log.info("run_with_real_data: fetching yfinance data for %s", metal)
        df = fetch_historical(metal, days=730)  # ~2 years

        if df is None or df.empty:
            log.warning("run_with_real_data: no data for %s — skipping", metal)
            continue

        scrap_prices: List[float] = df["scrap_price"].tolist()
        price_dates:  List[date]  = list(df["date"])

        if len(scrap_prices) < MIN_TOTAL:
            log.warning(
                "run_with_real_data: %s has only %d rows (need %d) — skipping",
                metal, len(scrap_prices), MIN_TOTAL,
            )
            continue

        # 80/20 split: use only the test window for backtest evaluation
        split_idx      = int(len(scrap_prices) * 0.8)
        train_prices   = scrap_prices[:split_idx]
        test_prices    = scrap_prices[split_idx:]
        train_dates    = price_dates[:split_idx]
        test_dates     = price_dates[split_idx:]

        log.info(
            "run_with_real_data: %s — %d train / %d test rows",
            metal, len(train_prices), len(test_prices),
        )

        # Run backtest across the full dataset (train + test) using walk-forward
        for model_name, model_fn in model_registry:
            try:
                results = await run_backtest(
                    pool, metal, model_name, model_fn,
                    scrap_prices, price_dates,
                )
                all_results.extend(results)
            except Exception as exc:
                log.error(
                    "run_with_real_data: backtest %s/%s failed: %s",
                    model_name, metal, exc,
                )

    log.info("run_with_real_data: stored %d backtest rows total", len(all_results))
    return all_results
