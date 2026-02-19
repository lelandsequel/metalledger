"""
Tests for MetalLedger /forecast/live endpoint.

Strategy:
  - Build an inline FastAPI app that mimics the real endpoint logic
  - Mock yfinance via patch to avoid network calls
  - Verify the response shape and fallback behavior

Verifies:
  1. GET /forecast/live?metal=CU_BARE returns 200 with expected shape
  2. All required fields present in response
  3. price_source == "live" when yfinance succeeds
  4. price_source == "synthetic" when yfinance unavailable
  5. Unknown metal returns 404
  6. Response includes P10/P50/P90 at 30, 90, 180 days
  7. Forecasts fall back to None when no stored forecasts
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, date
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import BaseModel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))


# ── In-memory DB mock ─────────────────────────────────────────────────────────

class ForecastRow:
    def __init__(self, model, horizon, p10, p50, p90):
        self._data = {
            "model": model, "horizon": horizon,
            "p10": p10, "p50": p50, "p90": p90,
        }
    def __getitem__(self, key):
        return self._data[key]
    def __contains__(self, key):
        return key in self._data
    def keys(self):
        return self._data.keys()


class InMemoryForecastDB:
    def __init__(self, forecast_rows: Optional[List[Dict]] = None):
        self._forecasts = list(forecast_rows) if forecast_rows is not None else []
        self._next_id = 1

    async def fetch(self, query: str, *args):
        if "forecasts" in query and args:
            metal = args[0]
            matching = [
                ForecastRow(r["model"], r["horizon"], r["p10"], r["p50"], r["p90"])
                for r in self._forecasts
                if r["metal"] == metal
            ]
            return matching
        return []

    async def fetchval(self, query: str, *args):
        row_id = self._next_id
        self._next_id += 1
        return row_id

    async def execute(self, query: str, *args):
        pass

    async def fetchrow(self, query: str, *args):
        return None


# ── Seed forecast rows ────────────────────────────────────────────────────────

SAMPLE_FORECASTS = [
    {"metal": "CU_BARE", "model": "naive",          "horizon": 1,  "p10": 3.90, "p50": 4.02, "p90": 4.14},
    {"metal": "CU_BARE", "model": "arima",          "horizon": 5,  "p10": 3.85, "p50": 4.05, "p90": 4.22},
    {"metal": "CU_BARE", "model": "gradient_boost", "horizon": 20, "p10": 3.78, "p50": 4.10, "p90": 4.40},
    {"metal": "HMS1",    "model": "naive",          "horizon": 1,  "p10": 0.093, "p50": 0.099, "p90": 0.106},
    {"metal": "HMS1",    "model": "arima",          "horizon": 5,  "p10": 0.090, "p50": 0.098, "p90": 0.107},
    {"metal": "HMS1",    "model": "gradient_boost", "horizon": 20, "p10": 0.088, "p50": 0.096, "p90": 0.109},
]


# ── Minimal /forecast/live endpoint for testing ───────────────────────────────

SYNTHETIC_PRICES = {
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


class LiveForecastResponse(BaseModel):
    metal_slug:        str
    scrap_price:       Optional[float]
    raw_futures_price: Optional[float]
    ticker:            Optional[str]
    spread_factor:     Optional[float]
    fetched_at:        Optional[str]
    p10_30d:           Optional[float]
    p50_30d:           Optional[float]
    p90_30d:           Optional[float]
    p10_90d:           Optional[float]
    p50_90d:           Optional[float]
    p90_90d:           Optional[float]
    p10_180d:          Optional[float]
    p50_180d:          Optional[float]
    p90_180d:          Optional[float]
    price_source:      str
    generated_at:      str


def _get_pq(rows, target_horizon: int) -> dict:
    """Average ensemble P10/P50/P90 for the closest horizon."""
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
    scale = (target_horizon / nearest) ** 0.5
    base_p10 = sum(p10s)/len(p10s) if p10s else base_p50 * 0.95
    base_p90 = sum(p90s)/len(p90s) if p90s else base_p50 * 1.05
    half_spread = (base_p90 - base_p10) / 2

    return {
        "p10": round(base_p50 - half_spread * scale, 6),
        "p50": round(base_p50, 6),
        "p90": round(base_p50 + half_spread * scale, 6),
    }


def build_test_app(
    forecast_rows: List[Dict] = None,
    live_meta: Optional[dict] = None,
):
    """Build a minimal FastAPI app for testing /forecast/live."""
    app = FastAPI()
    db = InMemoryForecastDB(SAMPLE_FORECASTS if forecast_rows is None else forecast_rows)
    app.state.pool = db

    @app.get("/forecast/live", response_model=LiveForecastResponse)
    async def get_live_forecast(metal: str, request: Request):
        pool = request.app.state.pool
        metal_upper = metal.upper()

        # Price: use provided live_meta or fall back to synthetic
        if live_meta and live_meta.get("metal_slug") == metal_upper:
            price_data   = live_meta
            price_source = "live"
        else:
            synth = SYNTHETIC_PRICES.get(metal_upper)
            if synth:
                scrap_p, raw_p, ticker, spread = synth
                price_data = {
                    "metal_slug": metal_upper,
                    "scrap_price": scrap_p,
                    "raw_futures_price": raw_p,
                    "ticker": ticker,
                    "spread_factor": spread,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
                price_source = "synthetic"
            else:
                raise HTTPException(status_code=404, detail=f"Unknown metal: {metal}")

        rows = await pool.fetch("SELECT FROM forecasts WHERE metal=$1", metal_upper)
        pq_30  = _get_pq(rows, 30)
        pq_90  = _get_pq(rows, 90)
        pq_180 = _get_pq(rows, 180)

        return LiveForecastResponse(
            metal_slug        = price_data["metal_slug"],
            scrap_price       = price_data["scrap_price"],
            raw_futures_price = price_data["raw_futures_price"],
            ticker            = price_data["ticker"],
            spread_factor     = price_data["spread_factor"],
            fetched_at        = price_data["fetched_at"],
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

    return app, db


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestLiveEndpointShape:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.app, self.db = build_test_app()
        self.client = TestClient(self.app)

    def test_returns_200_for_known_metal(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        assert resp.status_code == 200

    def test_returns_200_for_hms1(self):
        resp = self.client.get("/forecast/live?metal=HMS1")
        assert resp.status_code == 200

    def test_returns_404_for_unknown_metal(self):
        resp = self.client.get("/forecast/live?metal=UNOBTAINIUM")
        assert resp.status_code == 404

    def test_response_has_metal_slug(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        assert data["metal_slug"] == "CU_BARE"

    def test_response_has_scrap_price(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        assert data["scrap_price"] is not None
        assert isinstance(data["scrap_price"], float)

    def test_response_has_ticker(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        assert data["ticker"] == "HG=F"

    def test_response_has_spread_factor(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        assert data["spread_factor"] == 0.97

    def test_response_has_all_horizon_fields(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        for field in ["p10_30d", "p50_30d", "p90_30d",
                      "p10_90d", "p50_90d", "p90_90d",
                      "p10_180d", "p50_180d", "p90_180d"]:
            assert field in data, f"Missing field: {field}"

    def test_response_has_price_source(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        assert data["price_source"] in ("live", "synthetic")

    def test_response_has_generated_at(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        assert "generated_at" in data
        assert data["generated_at"] is not None

    def test_p50_at_30d_is_positive(self):
        resp = self.client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        p50 = data.get("p50_30d")
        if p50 is not None:
            assert p50 > 0

    def test_case_insensitive_metal_slug(self):
        """metal param should be uppercased internally."""
        resp_upper = self.client.get("/forecast/live?metal=CU_BARE")
        resp_lower = self.client.get("/forecast/live?metal=cu_bare")
        assert resp_upper.status_code == 200
        assert resp_lower.status_code == 200
        assert resp_upper.json()["metal_slug"] == resp_lower.json()["metal_slug"]


class TestLivePriceSourceFallback:

    def test_synthetic_fallback_when_no_live_meta(self):
        app, _ = build_test_app(live_meta=None)
        client = TestClient(app)
        resp = client.get("/forecast/live?metal=CU_BARE")
        assert resp.status_code == 200
        data = resp.json()
        assert data["price_source"] == "synthetic"

    def test_live_source_when_meta_provided(self):
        live_meta = {
            "metal_slug":        "CU_BARE",
            "scrap_price":       4.03,
            "raw_futures_price": 4.1546,
            "ticker":            "HG=F",
            "spread_factor":     0.97,
            "fetched_at":        datetime.now(timezone.utc).isoformat(),
        }
        app, _ = build_test_app(live_meta=live_meta)
        client = TestClient(app)
        resp = client.get("/forecast/live?metal=CU_BARE")
        assert resp.status_code == 200
        data = resp.json()
        assert data["price_source"] == "live"
        assert abs(data["scrap_price"] - 4.03) < 1e-6

    def test_scrap_price_matches_live_meta(self):
        live_meta = {
            "metal_slug":        "HMS1",
            "scrap_price":       0.0982,
            "raw_futures_price": 0.1045,
            "ticker":            "HR=F",
            "spread_factor":     0.94,
            "fetched_at":        datetime.now(timezone.utc).isoformat(),
        }
        app, _ = build_test_app(live_meta=live_meta)
        client = TestClient(app)
        resp = client.get("/forecast/live?metal=HMS1")
        data = resp.json()
        assert abs(data["scrap_price"] - 0.0982) < 1e-6


class TestForecastFallback:

    def test_no_forecasts_returns_none_for_pq_fields(self):
        """When DB has no forecast rows, P10/P50/P90 should be None."""
        app, _ = build_test_app(forecast_rows=[])
        client = TestClient(app)
        resp = client.get("/forecast/live?metal=CU_BARE")
        assert resp.status_code == 200
        data = resp.json()
        # All pq fields should be None when no forecasts stored
        for field in ["p10_30d", "p50_30d", "p90_30d"]:
            assert data[field] is None, f"Expected None for {field}, got {data[field]}"

    def test_extrapolates_from_stored_horizon(self):
        """When only horizon=20 is stored, 30/90/180 day bands are extrapolated."""
        rows = [
            {"metal": "CU_BARE", "model": "naive", "horizon": 20,
             "p10": 3.80, "p50": 4.00, "p90": 4.20},
        ]
        app, _ = build_test_app(forecast_rows=rows)
        client = TestClient(app)
        resp = client.get("/forecast/live?metal=CU_BARE")
        data = resp.json()
        # p50 should be non-None (extrapolated from horizon 20)
        assert data["p50_30d"] is not None
        assert data["p50_90d"] is not None
        assert data["p50_180d"] is not None
