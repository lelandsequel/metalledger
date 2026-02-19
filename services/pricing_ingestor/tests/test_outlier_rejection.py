"""
Tests for MetalLedger normalizer outlier rejection.

Requirements verified:
  - Prices > 3× rolling 7-day median are rejected.
  - Valid prices are accepted.
  - Empty history → price accepted (no false rejects on first ingestion).
  - Exactly-at-threshold (= 3×) is accepted; strictly-above (> 3×) is rejected.
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal
from datetime import datetime, timezone

import pytest

# Add packages to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from normalizer import is_outlier, normalize, compute_rolling_median
from common.types import PricePoint


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_price(
    value: float,
    metal: str = "XAU",
    source: str = "metals_api",
) -> PricePoint:
    return PricePoint(
        source    = source,
        metal     = metal,
        venue     = "SPOT",
        price_ts  = datetime(2024, 1, 15, 20, 0, 0, tzinfo=timezone.utc),
        value     = Decimal(str(value)),
        currency  = "USD",
        source_id = f"test_{metal}_{value}",
    )


# ── Unit tests: is_outlier() ──────────────────────────────────────────────────

class TestIsOutlier:
    def test_normal_price_not_outlier(self):
        """Price within 3× median is not an outlier."""
        history = [2000.0, 2010.0, 2020.0, 2015.0, 2005.0, 2025.0, 2018.0]
        med = compute_rolling_median(history)  # ~2015
        # 2050 is well within 3 × 2015 = 6045
        assert not is_outlier(Decimal("2050.0"), history)

    def test_large_spike_is_outlier(self):
        """Price > 3× median is rejected as outlier."""
        history = [2000.0, 2010.0, 2020.0, 2015.0, 2005.0, 2025.0, 2018.0]
        med = compute_rolling_median(history)  # ~2015
        spike = Decimal(str(med * 3.1))        # 10% above threshold
        assert is_outlier(spike, history)

    def test_exactly_at_threshold_not_outlier(self):
        """Price exactly equal to 3× median is NOT rejected (> not >=)."""
        history = [100.0, 100.0, 100.0, 100.0, 100.0]
        # median = 100; threshold = 300; value = 300 → NOT > 300 → accepted
        assert not is_outlier(Decimal("300.0"), history)

    def test_one_above_threshold_is_outlier(self):
        """Price of 300.001 with median=100 should be rejected."""
        history = [100.0, 100.0, 100.0, 100.0, 100.0]
        assert is_outlier(Decimal("300.001"), history)

    def test_empty_history_accepts_any_price(self):
        """No history → accept all prices (first ingestion case)."""
        assert not is_outlier(Decimal("999999.0"), [])

    def test_zero_median_accepts_any_price(self):
        """Edge case: if median is zero, we can't reject."""
        assert not is_outlier(Decimal("1000.0"), [0.0, 0.0, 0.0])

    def test_silver_outlier(self):
        """Silver prices with outlier detection."""
        history = [23.0, 23.5, 24.0, 23.8, 23.2, 23.6, 23.9]
        # Normal silver price
        assert not is_outlier(Decimal("25.0"), history)
        # Wild spike to $200 → outlier (median ~23.6, threshold ~70.8)
        assert is_outlier(Decimal("200.0"), history)

    def test_custom_multiplier(self):
        """Custom multiplier (2×) rejects sooner."""
        history = [100.0, 100.0, 100.0]
        # With multiplier=2: threshold = 200
        assert not is_outlier(Decimal("199.0"), history, multiplier=2.0)
        assert is_outlier(Decimal("201.0"), history, multiplier=2.0)


# ── Integration tests: normalize() ───────────────────────────────────────────

class TestNormalize:
    def test_valid_prices_all_accepted(self):
        """All prices within threshold should be accepted."""
        prices = [
            make_price(2040.0, "XAU"),
            make_price(2035.0, "XAU", "lbma"),
            make_price(23.5,   "XAG"),
        ]
        history = {
            "XAU": [2000.0, 2010.0, 2020.0, 2015.0, 2005.0, 2025.0, 2018.0],
            "XAG": [23.0, 23.5, 24.0, 23.8, 23.2, 23.6, 23.9],
        }
        result = normalize(prices, history)
        assert len(result.accepted) == 3
        assert len(result.rejected) == 0

    def test_outlier_prices_rejected(self):
        """Prices above 3× median should be in rejected."""
        history = {
            "XAU": [2000.0, 2010.0, 2020.0, 2015.0, 2005.0, 2025.0, 2018.0],
        }
        # Normal price
        normal = make_price(2050.0, "XAU")
        # Outlier: median ~2015, threshold ~6045; use 9000
        outlier = make_price(9000.0, "XAU", "lbma")

        result = normalize([normal, outlier], history)
        assert len(result.accepted) == 1
        assert result.accepted[0].value == Decimal("2050.0")
        assert len(result.rejected) == 1
        assert result.rejected[0][0].value == Decimal("9000.0")

    def test_rejection_reason_included(self):
        """Rejected prices include a reason string."""
        history = {"XAU": [100.0, 100.0, 100.0, 100.0]}
        outlier = make_price(9999.0, "XAU")
        result  = normalize([outlier], history)
        assert len(result.rejected) == 1
        price, reason = result.rejected[0]
        assert "outlier" in reason.lower()

    def test_no_history_accepts_all(self):
        """With empty history, all prices should be accepted."""
        prices = [make_price(2050.0, "XAU"), make_price(23.5, "XAG")]
        result = normalize(prices, {})
        assert len(result.accepted) == 2
        assert len(result.rejected) == 0

    def test_multiple_metals_independent_medians(self):
        """Outlier detection is per-metal, not cross-metal."""
        history = {
            "XAU": [2000.0] * 7,   # threshold = 6000
            "XAG": [24.0] * 7,     # threshold = 72
        }
        prices = [
            make_price(5999.0, "XAU"),  # accepted (< 6000)
            make_price(73.0,   "XAG"),  # rejected (> 72)
        ]
        result = normalize(prices, history)
        accepted_metals = [p.metal for p in result.accepted]
        rejected_metals = [p.metal for p, _ in result.rejected]
        assert "XAU" in accepted_metals
        assert "XAG" in rejected_metals

    def test_empty_incoming_produces_empty_result(self):
        """No incoming prices → no accepted or rejected."""
        result = normalize([], {})
        assert result.accepted  == []
        assert result.rejected  == []
