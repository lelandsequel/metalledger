"""
Tests for MetalLedger commodity_feed module.

Strategy: mock yfinance to avoid real network calls.
Tests verify:
  1. fetch_historical returns DataFrame with correct columns
  2. Spread adjustment is applied correctly (CU_BARE = futures * 0.97)
  3. Graceful None return when ticker not found in TICKER_MAP
  4. Graceful None return when yfinance returns empty DataFrame
  5. get_latest_price returns most recent scrap price
  6. get_latest_price_with_meta returns all metadata fields
  7. fetch_all_metals returns dict with multiple slugs
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from typing import Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Make sure the forecast service modules are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))

from data.commodity_feed import (
    TICKER_MAP,
    fetch_historical,
    fetch_all_metals,
    get_latest_price,
    get_latest_price_with_meta,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_yf_history(close_prices: list, ticker: str = "HG=F") -> pd.DataFrame:
    """Build a fake yfinance history DataFrame."""
    dates = pd.date_range(
        start="2025-01-01", periods=len(close_prices), freq="B", tz="UTC"
    )
    df = pd.DataFrame({"Close": close_prices}, index=dates)
    return df


def _mock_ticker(close_prices: list):
    """Return a mock yf.Ticker whose .history() returns fake data."""
    mock = MagicMock()
    mock.history.return_value = _make_yf_history(close_prices)
    return mock


def _empty_ticker():
    """Return a mock yf.Ticker whose .history() returns empty DataFrame."""
    mock = MagicMock()
    mock.history.return_value = pd.DataFrame()
    return mock


# ── Tests — fetch_historical ──────────────────────────────────────────────────

class TestFetchHistorical:

    def test_returns_none_for_unknown_slug(self):
        result = fetch_historical("UNKNOWN_METAL")
        assert result is None

    def test_returns_none_for_empty_data(self):
        with patch("data.commodity_feed.yf.Ticker", return_value=_empty_ticker()):
            result = fetch_historical("CU_BARE")
        assert result is None

    def test_returns_dataframe_with_correct_columns(self):
        fake_prices = [4.15, 4.18, 4.12, 4.20, 4.17]
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker(fake_prices)):
            df = fetch_historical("CU_BARE", days=10)

        assert df is not None
        assert isinstance(df, pd.DataFrame)
        required_cols = {"date", "raw_price", "scrap_price", "ticker", "spread_factor", "metal_slug"}
        assert required_cols.issubset(set(df.columns)), \
            f"Missing columns: {required_cols - set(df.columns)}"

    def test_spread_adjustment_cu_bare(self):
        """CU_BARE scrap_price = futures_close * 0.97"""
        futures_price = 4.20
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([futures_price])):
            df = fetch_historical("CU_BARE", days=5)

        assert df is not None and not df.empty
        expected = round(futures_price * 0.97, 10)
        actual   = round(float(df["scrap_price"].iloc[-1]), 10)
        assert abs(actual - expected) < 1e-6, f"Expected {expected}, got {actual}"

    def test_spread_adjustment_cu_1(self):
        """CU_1 scrap_price = futures_close * 0.91"""
        futures_price = 4.10
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([futures_price])):
            df = fetch_historical("CU_1", days=5)

        assert df is not None and not df.empty
        expected = round(futures_price * 0.91, 10)
        actual   = round(float(df["scrap_price"].iloc[-1]), 10)
        assert abs(actual - expected) < 1e-6

    def test_spread_adjustment_hms1(self):
        """HMS1 scrap_price = HR=F futures * 0.94"""
        futures_price = 800.0  # $/ton typical hot-rolled coil
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([futures_price])):
            df = fetch_historical("HMS1", days=5)

        assert df is not None and not df.empty
        expected = round(futures_price * 0.94, 10)
        actual   = round(float(df["scrap_price"].iloc[-1]), 10)
        assert abs(actual - expected) < 1e-6

    def test_spread_adjustment_al_cast(self):
        """AL_CAST scrap_price = ALI=F futures * 0.52"""
        futures_price = 0.92
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([futures_price])):
            df = fetch_historical("AL_CAST", days=5)

        assert df is not None and not df.empty
        expected = round(futures_price * 0.52, 10)
        actual   = round(float(df["scrap_price"].iloc[-1]), 10)
        assert abs(actual - expected) < 1e-6

    def test_ticker_metadata_stored_in_dataframe(self):
        """ticker column should match the TICKER_MAP value."""
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([4.15, 4.20])):
            df = fetch_historical("CU_BARE", days=10)

        assert df is not None
        assert all(df["ticker"] == "HG=F"), "Unexpected ticker symbol in DataFrame"
        assert all(df["spread_factor"] == 0.97), "Unexpected spread_factor in DataFrame"
        assert all(df["metal_slug"] == "CU_BARE"), "Unexpected metal_slug in DataFrame"

    def test_returns_correct_row_count(self):
        """Should return same number of rows as fake price list."""
        n = 30
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([4.15] * n)):
            df = fetch_historical("CU_BARE", days=60)

        assert df is not None and len(df) == n

    def test_handles_yfinance_exception_gracefully(self):
        """If yfinance raises, should return None (not crash)."""
        mock = MagicMock()
        mock.history.side_effect = RuntimeError("network timeout")
        with patch("data.commodity_feed.yf.Ticker", return_value=mock):
            result = fetch_historical("CU_BARE", days=10)

        assert result is None

    def test_no_negative_scrap_prices(self):
        """Scrap prices must always be positive."""
        prices = [4.15, 4.10, 4.22, 4.08, 4.18]
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker(prices)):
            df = fetch_historical("CU_BARE", days=10)

        assert df is not None
        assert (df["scrap_price"] > 0).all()


# ── Tests — get_latest_price ──────────────────────────────────────────────────

class TestGetLatestPrice:

    def test_returns_float(self):
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([4.15, 4.20])):
            price = get_latest_price("CU_BARE")

        assert price is not None
        assert isinstance(price, float)

    def test_returns_last_scrap_price(self):
        """Should return the spread-adjusted price of the LAST row."""
        closing = [4.10, 4.20, 4.25]
        expected = round(4.25 * 0.97, 6)
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker(closing)):
            price = get_latest_price("CU_BARE")

        assert price is not None
        assert abs(price - expected) < 1e-5

    def test_returns_none_for_unknown_metal(self):
        result = get_latest_price("NOT_A_METAL")
        assert result is None

    def test_returns_none_when_empty(self):
        with patch("data.commodity_feed.yf.Ticker", return_value=_empty_ticker()):
            result = get_latest_price("CU_BARE")
        assert result is None


# ── Tests — get_latest_price_with_meta ────────────────────────────────────────

class TestGetLatestPriceWithMeta:

    def test_returns_all_metadata_fields(self):
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([4.15, 4.20])):
            meta = get_latest_price_with_meta("CU_BARE")

        assert meta is not None
        required_keys = {"metal_slug", "scrap_price", "raw_futures_price",
                         "ticker", "spread_factor", "fetched_at"}
        assert required_keys.issubset(set(meta.keys())), \
            f"Missing keys: {required_keys - set(meta.keys())}"

    def test_scrap_price_matches_spread(self):
        futures = 4.18
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([futures])):
            meta = get_latest_price_with_meta("CU_BARE")

        assert meta is not None
        expected = round(futures * 0.97, 6)
        assert abs(meta["scrap_price"] - expected) < 1e-5

    def test_raw_price_is_unadjusted_futures(self):
        futures = 4.18
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([futures])):
            meta = get_latest_price_with_meta("CU_BARE")

        assert meta is not None
        assert abs(meta["raw_futures_price"] - futures) < 1e-5

    def test_returns_none_for_unknown_metal(self):
        result = get_latest_price_with_meta("XYZZY")
        assert result is None


# ── Tests — fetch_all_metals ──────────────────────────────────────────────────

class TestFetchAllMetals:

    def test_returns_dict(self):
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([4.15, 4.20, 4.18])):
            result = fetch_all_metals(days=10)

        assert isinstance(result, dict)

    def test_all_slugs_present_when_data_available(self):
        with patch("data.commodity_feed.yf.Ticker", return_value=_mock_ticker([4.15, 4.20, 4.18])):
            result = fetch_all_metals(days=10)

        for slug in TICKER_MAP:
            assert slug in result, f"Missing slug: {slug}"

    def test_slugs_omitted_when_data_unavailable(self):
        """Metals with no yfinance data should be omitted, not crash."""
        with patch("data.commodity_feed.yf.Ticker", return_value=_empty_ticker()):
            result = fetch_all_metals(days=10)

        assert result == {}


# ── Tests — TICKER_MAP completeness ──────────────────────────────────────────

class TestTickerMap:

    def test_all_required_metals_in_map(self):
        required = ["CU_BARE", "CU_1", "CU_2", "HMS1", "HMS2", "SHRED", "CAST",
                    "AL_CAST", "AL_EXTRUSION"]
        for metal in required:
            assert metal in TICKER_MAP, f"{metal} missing from TICKER_MAP"

    def test_spreads_between_0_and_1(self):
        for slug, (ticker, spread) in TICKER_MAP.items():
            assert 0 < spread <= 1.0, \
                f"{slug} spread {spread} is out of range (0, 1]"

    def test_tickers_are_valid_yfinance_format(self):
        valid_tickers = {"HG=F", "HR=F", "ALI=F"}
        for slug, (ticker, _) in TICKER_MAP.items():
            assert ticker in valid_tickers, \
                f"{slug} uses unknown ticker {ticker}"
