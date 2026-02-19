"""
MetalLedger — Price normalizer.

Responsibilities:
  1. Receive PricePoint objects from one or more adapters.
  2. Apply source priority (metals_api > lbma > seed).
  3. Reject outliers: price > OUTLIER_MULTIPLIER × rolling 7-day median.
  4. Promote valid, highest-priority price for each (metal, timestamp) to
     prices_canonical via DB writer.

The rolling median is computed over prices_raw for the past ROLLING_MEDIAN_DAYS.
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal
from statistics import median
from typing import List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from common.config import (
    METAL_SOURCE_PREFERENCE,
    OUTLIER_MULTIPLIER,
    ROLLING_MEDIAN_DAYS,
    SOURCE_PRIORITY,
)
from common.logging_util import get_logger
from common.types import PricePoint

log = get_logger(__name__)


# ── Rolling-median outlier detection ─────────────────────────────────────────

def compute_rolling_median(historical_values: Sequence[float]) -> Optional[float]:
    """Return the median of `historical_values`, or None if empty."""
    if not historical_values:
        return None
    return float(median(historical_values))


def is_outlier(
    value: Decimal,
    historical_values: Sequence[float],
    multiplier: float = OUTLIER_MULTIPLIER,
) -> bool:
    """
    Return True if `value` is more than `multiplier` × rolling median.

    A price is rejected (outlier) when:
        price > multiplier * rolling_median(last N days)

    If there is no historical data (first ingestion), the price is accepted.
    """
    med = compute_rolling_median(historical_values)
    if med is None or med == 0:
        return False
    return float(value) > multiplier * med


# ── Source priority resolution ────────────────────────────────────────────────

def sort_by_priority(prices: List[PricePoint]) -> List[PricePoint]:
    """Sort a list of PricePoint by source priority (ascending = higher priority)."""
    return sorted(prices, key=lambda p: SOURCE_PRIORITY.get(p.source, 999))


def select_best_price(
    prices: List[PricePoint],
    metal: str,
) -> Optional[PricePoint]:
    """
    Given multiple PricePoint objects for the same metal/timestamp, pick
    the one from the highest-priority source per METAL_SOURCE_PREFERENCE.
    Falls back to global SOURCE_PRIORITY ranking if metal not in preference map.
    """
    if not prices:
        return None

    preference = METAL_SOURCE_PREFERENCE.get(metal, [])
    for preferred_source in preference:
        for p in prices:
            if p.source == preferred_source:
                return p

    # Fall back to global priority sort
    return sort_by_priority(prices)[0]


# ── Main normalizer ───────────────────────────────────────────────────────────

class NormalizationResult:
    """Container for the outcome of a normalisation run."""

    def __init__(self) -> None:
        self.accepted:    List[PricePoint] = []
        self.rejected:    List[Tuple[PricePoint, str]] = []  # (price, reason)

    def accept(self, price: PricePoint) -> None:
        self.accepted.append(price)
        log.debug(
            "ACCEPTED %s %s from %s value=%s",
            price.metal, price.price_ts, price.source, price.value,
        )

    def reject(self, price: PricePoint, reason: str) -> None:
        self.rejected.append((price, reason))
        log.warning(
            "REJECTED %s %s from %s value=%s reason=%s",
            price.metal, price.price_ts, price.source, price.value, reason,
        )

    @property
    def summary(self) -> str:
        return (
            f"Normalisation complete: "
            f"{len(self.accepted)} accepted, {len(self.rejected)} rejected"
        )


def normalize(
    incoming: List[PricePoint],
    historical_lookup: dict,          # {metal: [float, ...]} — recent values
    multiplier: float = OUTLIER_MULTIPLIER,
) -> NormalizationResult:
    """
    Normalise a batch of incoming PricePoints.

    Args:
        incoming:           New prices from adapters.
        historical_lookup:  Dict mapping metal → list of recent float values
                            (from prices_raw, last ROLLING_MEDIAN_DAYS days).
        multiplier:         Outlier threshold multiplier (default: 3.0).

    Returns:
        NormalizationResult with .accepted and .rejected lists.
        Only .accepted prices should be written to prices_canonical.
    """
    result = NormalizationResult()

    # Group by metal so we can apply priority selection per metal
    by_metal: dict[str, List[PricePoint]] = {}
    for p in incoming:
        by_metal.setdefault(p.metal, []).append(p)

    for metal, candidates in by_metal.items():
        history = historical_lookup.get(metal, [])

        for price in candidates:
            # 1. Outlier check
            if is_outlier(price.value, history, multiplier):
                result.reject(price, f"outlier: value={price.value} > {multiplier}×median={compute_rolling_median(history):.4f}")
                continue

            # 2. Accept
            result.accept(price)

    log.info(result.summary)
    return result
