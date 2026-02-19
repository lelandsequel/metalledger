"""
Tests for MetalLedger normalizer outlier rejection.

Requirements verified:
  - Prices > 3× rolling 7-day median are rejected.
  - Valid prices are accepted.
  - Empty history → price accepted (no false rejects on first ingestion).
  - Exactly-at-threshold (= 3×) is accepted; strictly-above (> 3×) is rejected.

Updated: Uses scrap metal slugs (CU_BARE, HMS1, etc.) instead of precious metals.
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
    metal: str = "CU_BARE",
    source: str = "dealer_manual",
) -> PricePoint:
    return PricePoint(
        source    = source,
        metal     = metal,
        venue     = "DEALER:dealer_001:77001",
        price_ts  = datetime(2024, 1, 15, 20, 0, 0, tzinfo=timezone.utc),
        value     = Decimal(str(value)),
        currency  = "USD",
        source_id = f"test_{metal}_{value}",
    )


# ── Unit tests: is_outlier() ──────────────────────────────────────────────────

class TestIsOutlier:
    def test_normal_copper_price_not_outlier(self):
        """Bare bright copper price within 3× median is not an outlier."""
        # CU_BARE typical range ~$3.50-4.20/lb
        history = [3.80, 3.85, 3.90, 3.82, 3.78, 3.88, 3.84]
        # 4.00 is well within 3 × 3.84 = 11.52
        assert not is_outlier(Decimal("4.00"), history)

    def test_large_spike_copper_is_outlier(self):
        """Copper price > 3× median is rejected as outlier."""
        history = [3.80, 3.85, 3.90, 3.82, 3.78, 3.88, 3.84]
        med = compute_rolling_median(history)  # ~3.84
        spike = Decimal(str(med * 3.1))        # 10% above threshold
        assert is_outlier(spike, history)

    def test_exactly_at_threshold_not_outlier(self):
        """Price exactly equal to 3× median is NOT rejected (> not >=)."""
        # Use values that give a clean median to avoid floating point edge cases
        history = [3.00, 3.00, 3.00, 3.00, 3.00]
        # median = 3.00; threshold = 9.00; value = 9.00 → NOT > 9.00 → accepted
        assert not is_outlier(Decimal("9.00"), history)

    def test_one_above_threshold_is_outlier(self):
        """Price of 11.401 with median=3.80 should be rejected."""
        history = [3.80, 3.80, 3.80, 3.80, 3.80]
        assert is_outlier(Decimal("11.401"), history)

    def test_empty_history_accepts_any_price(self):
        """No history → accept all prices (first ingestion case)."""
        assert not is_outlier(Decimal("999999.0"), [])

    def test_zero_median_accepts_any_price(self):
        """Edge case: if median is zero, we can't reject."""
        assert not is_outlier(Decimal("1000.0"), [0.0, 0.0, 0.0])

    def test_hms1_outlier(self):
        """HMS1 (Heavy Melting Steel #1) prices with outlier detection."""
        # HMS1 ~$0.090-0.110/lb
        history = [0.098, 0.100, 0.102, 0.099, 0.101, 0.100, 0.103]
        # Normal HMS1 price
        assert not is_outlier(Decimal("0.105"), history)
        # Wild spike to $5/lb → outlier (median ~0.100, threshold ~0.300)
        assert is_outlier(Decimal("5.00"), history)

    def test_zorba_outlier(self):
        """Zorba (mixed non-ferrous) prices with outlier detection."""
        # ZORBA ~$0.55-0.80/lb
        history = [0.63, 0.65, 0.67, 0.64, 0.66, 0.65, 0.68]
        # Normal zorba price
        assert not is_outlier(Decimal("0.70"), history)
        # Spike to $5/lb → outlier (median ~0.65, threshold ~1.95)
        assert is_outlier(Decimal("5.00"), history)

    def test_custom_multiplier(self):
        """Custom multiplier (2×) rejects sooner."""
        history = [3.80, 3.80, 3.80]
        # With multiplier=2: threshold = 7.60
        assert not is_outlier(Decimal("7.59"), history, multiplier=2.0)
        assert is_outlier(Decimal("7.61"), history, multiplier=2.0)


# ── Integration tests: normalize() ───────────────────────────────────────────

class TestNormalize:
    def test_valid_scrap_prices_all_accepted(self):
        """All scrap prices within threshold should be accepted."""
        prices = [
            make_price(3.85, "CU_BARE", "dealer_manual"),
            make_price(3.55, "CU_1",    "iscrap"),
            make_price(0.65, "ZORBA",   "scrap_register"),
        ]
        history = {
            "CU_BARE": [3.80, 3.82, 3.84, 3.86, 3.83, 3.81, 3.85],
            "CU_1":    [3.50, 3.52, 3.54, 3.51, 3.53, 3.50, 3.55],
            "ZORBA":   [0.63, 0.64, 0.65, 0.64, 0.63, 0.65, 0.66],
        }
        result = normalize(prices, history)
        assert len(result.accepted) == 3
        assert len(result.rejected) == 0

    def test_outlier_copper_price_rejected(self):
        """Copper price above 3× median should be in rejected."""
        history = {
            "CU_BARE": [3.80, 3.82, 3.84, 3.86, 3.83, 3.81, 3.85],
        }
        # Normal price
        normal = make_price(3.90, "CU_BARE")
        # Outlier: median ~3.83, threshold ~11.49; use 15.00
        outlier = make_price(15.00, "CU_BARE", "iscrap")

        result = normalize([normal, outlier], history)
        assert len(result.accepted) == 1
        assert result.accepted[0].value == Decimal("3.90")
        assert len(result.rejected) == 1
        assert result.rejected[0][0].value == Decimal("15.00")

    def test_outlier_ferrous_price_rejected(self):
        """HMS1 price spike rejected correctly."""
        history = {"HMS1": [0.098, 0.100, 0.101, 0.099, 0.100, 0.102, 0.100]}
        normal  = make_price(0.105, "HMS1")
        outlier = make_price(5.000, "HMS1", "iscrap")  # $5/lb is absurd for steel

        result = normalize([normal, outlier], history)
        assert len(result.accepted) == 1
        assert result.accepted[0].metal == "HMS1"
        assert len(result.rejected) == 1

    def test_rejection_reason_included(self):
        """Rejected prices include a reason string."""
        history = {"CU_BARE": [3.80, 3.80, 3.80, 3.80]}
        outlier = make_price(99.99, "CU_BARE")
        result  = normalize([outlier], history)
        assert len(result.rejected) == 1
        price, reason = result.rejected[0]
        assert "outlier" in reason.lower()

    def test_no_history_accepts_all(self):
        """With empty history, all prices should be accepted."""
        prices = [
            make_price(3.85, "CU_BARE"),
            make_price(0.65, "ZORBA"),
            make_price(0.100, "HMS1"),
        ]
        result = normalize(prices, {})
        assert len(result.accepted) == 3
        assert len(result.rejected) == 0

    def test_multiple_scrap_metals_independent_medians(self):
        """Outlier detection is per-metal, not cross-metal."""
        history = {
            "CU_BARE": [3.80] * 7,   # threshold = 11.40
            "HMS1":    [0.100] * 7,  # threshold = 0.300
        }
        prices = [
            make_price(11.39, "CU_BARE"),  # accepted (< 11.40)
            make_price(0.310, "HMS1"),     # rejected (> 0.300)
        ]
        result = normalize(prices, history)
        accepted_metals = [p.metal for p in result.accepted]
        rejected_metals = [p.metal for p, _ in result.rejected]
        assert "CU_BARE" in accepted_metals
        assert "HMS1"    in rejected_metals

    def test_empty_incoming_produces_empty_result(self):
        """No incoming prices → no accepted or rejected."""
        result = normalize([], {})
        assert result.accepted  == []
        assert result.rejected  == []

    def test_all_scrap_slugs_pass_reasonable_prices(self):
        """Typical scrap prices for all metal slugs pass outlier check with no history."""
        typical_prices = {
            "HMS1":         0.100,
            "HMS2":         0.088,
            "SHRED":        0.095,
            "CAST":         0.072,
            "CU_BARE":      3.85,
            "CU_1":         3.55,
            "CU_2":         3.10,
            "AL_CAST":      0.42,
            "AL_EXTRUSION": 0.53,
            "BRASS":        1.65,
            "SS_304":       0.58,
            "LEAD":         0.42,
            "ZORBA":        0.65,
        }
        prices = [make_price(v, metal=k) for k, v in typical_prices.items()]
        result = normalize(prices, {})
        assert len(result.accepted) == len(typical_prices)
        assert len(result.rejected) == 0
