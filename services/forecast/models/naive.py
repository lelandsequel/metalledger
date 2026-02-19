"""
MetalLedger — Naive forecast model.

Baseline: "yesterday's price == tomorrow's price" (random-walk assumption).

Despite its simplicity, the naive model is a strong baseline for commodity
prices, especially at short horizons, and is used as a benchmark in backtesting.

P10/P50/P90 are estimated by computing the historical distribution of
day-to-day percentage changes and applying them to the last price.
"""

from __future__ import annotations

import math
from statistics import mean, median, stdev
from typing import List, Optional, Tuple

from common.logging_util import get_logger

log = get_logger(__name__)


def _percentiles_from_changes(
    last_price: float,
    pct_changes: List[float],
    horizon: int,
) -> Tuple[float, float, float]:
    """
    Estimate P10, P50, P90 by projecting the distribution of daily pct changes
    forward `horizon` steps.

    Uses the central-limit approximation: over H steps, the cumulative change
    is approximately normal with:
        mean  = H × μ
        stdev = sqrt(H) × σ

    where μ and σ are the mean and stdev of daily pct changes.
    """
    if len(pct_changes) < 2:
        return last_price, last_price, last_price

    mu    = mean(pct_changes)
    sigma = stdev(pct_changes)

    cumulative_mean  = mu    * horizon
    cumulative_sigma = sigma * math.sqrt(horizon)

    # 10th and 90th percentile of normal distribution (z ≈ ±1.282)
    z10, z90 = -1.2816, 1.2816

    p50 = last_price * (1 + cumulative_mean)
    p10 = last_price * (1 + cumulative_mean + z10 * cumulative_sigma)
    p90 = last_price * (1 + cumulative_mean + z90 * cumulative_sigma)

    # Guard against negative prices
    p10 = max(p10, 0.0)
    return round(p10, 6), round(p50, 6), round(p90, 6)


def run(
    prices: List[float],
    horizons: List[int] = [1, 5, 20],
) -> dict:
    """
    Run the naive forecast on a sorted (ascending by date) list of prices.

    Args:
        prices:   Historical prices, most recent last.
        horizons: Forecast horizons in days.

    Returns:
        Dict mapping horizon → {"p10": float, "p50": float, "p90": float}
    """
    if not prices:
        log.warning("Naive model received empty price series")
        return {h: {"p10": None, "p50": None, "p90": None} for h in horizons}

    last_price = prices[-1]

    # Compute daily percentage changes
    pct_changes: List[float] = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            pct_changes.append((prices[i] - prices[i - 1]) / prices[i - 1])

    results = {}
    for h in horizons:
        p10, p50, p90 = _percentiles_from_changes(last_price, pct_changes, h)
        results[h] = {"p10": p10, "p50": p50, "p90": p90}
        log.debug("Naive h=%d → P10=%.4f P50=%.4f P90=%.4f", h, p10, p50, p90)

    return results
