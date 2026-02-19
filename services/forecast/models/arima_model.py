"""
MetalLedger — ARIMA forecast model.

Uses statsmodels ARIMA(5,1,0) as default.
If pmdarima is installed, uses auto_arima for automatic order selection.

Confidence intervals from the model fit are used to derive P10/P50/P90.

Data priority:
  1. Real commodity futures via yfinance (commodity_feed.fetch_historical)
  2. DB prices_canonical (passed in as prices arg)
  3. Synthetic / naive fallback
"""

from __future__ import annotations

import math
import os
import sys
from typing import List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))

from common.logging_util import get_logger

log = get_logger(__name__)

# Minimum data points required for ARIMA fit
MIN_OBSERVATIONS = 15


def _arima_statsmodels(
    prices: List[float],
    horizon: int,
    order: Tuple[int, int, int] = (5, 1, 0),
) -> dict:
    """Fit ARIMA with statsmodels and forecast `horizon` steps ahead."""
    from statsmodels.tsa.arima.model import ARIMA
    import numpy as np

    try:
        model  = ARIMA(prices, order=order)
        result = model.fit()

        forecast   = result.get_forecast(steps=horizon)
        fc_mean    = forecast.predicted_mean
        fc_conf    = forecast.conf_int(alpha=0.2)   # 80% CI → ~P10/P90

        p50 = float(fc_mean.iloc[-1])
        p10 = float(fc_conf.iloc[-1, 0])
        p90 = float(fc_conf.iloc[-1, 1])

        # Guard against negative prices
        p10 = max(p10, 0.0)

        return {"p10": round(p10, 6), "p50": round(p50, 6), "p90": round(p90, 6)}

    except Exception as exc:
        log.error("statsmodels ARIMA failed: %s", exc)
        return {"p10": None, "p50": None, "p90": None}


def _arima_pmdarima(prices: List[float], horizon: int) -> dict:
    """Use pmdarima auto_arima for automatic order selection."""
    import pmdarima as pm
    import numpy as np

    try:
        model = pm.auto_arima(
            prices,
            start_p=1, start_q=0, max_p=5, max_q=3, d=1,
            seasonal=False, stepwise=True, suppress_warnings=True,
            error_action="ignore",
        )
        fc, conf = model.predict(n_periods=horizon, return_conf_int=True, alpha=0.2)
        p50 = float(fc[-1])
        p10 = max(float(conf[-1, 0]), 0.0)
        p90 = float(conf[-1, 1])
        return {"p10": round(p10, 6), "p50": round(p50, 6), "p90": round(p90, 6)}
    except Exception as exc:
        log.error("pmdarima auto_arima failed: %s", exc)
        return {"p10": None, "p50": None, "p90": None}


def _naive_fallback(prices: List[float], horizon: int) -> dict:
    """
    Fallback when statsmodels is not available or ARIMA fit fails.
    Uses a simple exponential smoothing approximation.
    """
    if not prices:
        return {"p10": None, "p50": None, "p90": None}

    last  = prices[-1]
    alpha = 0.3
    smoothed = prices[0]
    for p in prices[1:]:
        smoothed = alpha * p + (1 - alpha) * smoothed

    # Estimate daily drift and volatility
    if len(prices) > 1:
        diffs   = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        mu      = sum(diffs) / len(diffs)
        sigma   = math.sqrt(sum((d - mu) ** 2 for d in diffs) / len(diffs)) if len(diffs) > 1 else abs(mu)
    else:
        mu, sigma = 0.0, last * 0.01

    p50 = last + mu * horizon
    p10 = max(p50 - 1.28 * sigma * math.sqrt(horizon), 0.0)
    p90 = p50 + 1.28 * sigma * math.sqrt(horizon)

    return {"p10": round(p10, 6), "p50": round(p50, 6), "p90": round(p90, 6)}


def _fetch_real_prices(metal_slug: str) -> Optional[List[float]]:
    """
    Attempt to fetch real commodity prices from yfinance.

    Returns list of scrap_price floats (ascending by date), or None on failure.
    """
    try:
        # Import inside function to avoid hard dependency at module load
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from data.commodity_feed import fetch_historical

        df = fetch_historical(metal_slug, days=730)
        if df is None or df.empty:
            return None

        prices = df["scrap_price"].tolist()
        log.info(
            "ARIMA: loaded %d real price points for %s via yfinance",
            len(prices), metal_slug,
        )
        return prices
    except Exception as exc:
        log.warning("ARIMA: yfinance fetch failed for %s: %s", metal_slug, exc)
        return None


def run(
    prices: List[float],
    horizons: List[int] = [1, 5, 20],
    metal_slug: Optional[str] = None,
) -> dict:
    """
    Run ARIMA forecast on a sorted list of historical prices.

    Data priority:
      1. Real commodity futures via yfinance (if metal_slug provided)
      2. DB prices_canonical (the prices arg passed in)
      3. Synthetic / naive fallback

    Args:
        prices:     Historical closing prices from DB, ascending by date.
        horizons:   List of forecast horizons in trading days.
        metal_slug: Optional scrap metal slug (e.g. "CU_BARE") for yfinance lookup.

    Returns:
        Dict mapping horizon → {"p10": float, "p50": float, "p90": float}
    """
    # 1. Try real data from yfinance
    if metal_slug:
        real_prices = _fetch_real_prices(metal_slug)
        if real_prices and len(real_prices) >= MIN_OBSERVATIONS:
            log.info(
                "ARIMA: using real yfinance data for %s (%d points)",
                metal_slug, len(real_prices),
            )
            prices = real_prices

    # 2. Fall back to DB prices (already set as default), or synthetic fallback
    if len(prices) < MIN_OBSERVATIONS:
        log.warning(
            "ARIMA requires >= %d observations, got %d — using fallback",
            MIN_OBSERVATIONS, len(prices),
        )
        return {h: _naive_fallback(prices, h) for h in horizons}

    # Choose backend: pmdarima > statsmodels > fallback
    results = {}
    for h in horizons:
        try:
            import pmdarima  # noqa: F401
            res = _arima_pmdarima(prices, h)
        except ImportError:
            try:
                import statsmodels  # noqa: F401
                res = _arima_statsmodels(prices, h)
            except ImportError:
                log.warning("Neither pmdarima nor statsmodels available; using fallback")
                res = _naive_fallback(prices, h)

        if res["p50"] is None:
            res = _naive_fallback(prices, h)

        results[h] = res
        log.debug("ARIMA h=%d → P10=%.4f P50=%.4f P90=%.4f",
                  h, res["p10"] or 0, res["p50"] or 0, res["p90"] or 0)

    return results
