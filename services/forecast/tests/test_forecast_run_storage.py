"""
Tests for MetalLedger Forecast service — forecast run and DB storage.

Strategy: build an inline FastAPI app that embeds the forecast logic
but mocks the heavy ML model calls (naive, ARIMA, GBM) with fast stubs.
This avoids statsmodels/sklearn import overhead in CI while still exercising
the storage, routing, and horizon logic.

Verifies:
1. POST /forecast/run returns 202.
2. Forecast run creates rows in forecasts table with correct horizons (1, 5, 20).
3. Each row has model, metal, horizon, p50 fields populated.
4. GET /forecast/latest returns stored data.
5. Insufficient data returns graceful response (not a crash).

Updated: Uses scrap metal slugs (CU_BARE, HMS1, ZORBA, etc.) instead of XAU/XAG/CU.
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional
from datetime import datetime, timezone, date

import pytest
from fastapi import FastAPI, Request, status
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.types import ForecastOut


# ── Fast model stubs ──────────────────────────────────────────────────────────

def _stub_model(prices: List[float], horizons: List[int]) -> dict:
    """A fast stub that returns realistic-looking forecast values."""
    if not prices or len(prices) < 5:
        return {h: {"p10": None, "p50": None, "p90": None} for h in horizons}
    last = prices[-1]
    return {
        h: {
            "p10": round(last * 0.98, 6),
            "p50": round(last * 1.00, 6),
            "p90": round(last * 1.02, 6),
        }
        for h in horizons
    }


# ── In-memory DB mock ─────────────────────────────────────────────────────────

class InMemoryDB:
    def __init__(self, seed_prices: Dict[str, List[float]] = None):
        self.forecasts: List[Dict] = []
        self._next_id  = 1
        self._prices   = seed_prices or {}

    async def fetchval(self, query: str, *args):
        row_id = self._next_id
        self._next_id += 1
        if "INSERT INTO forecasts" in query:
            model, metal, horizon, p10, p50, p90 = args
            self.forecasts.append({
                "id":      row_id,
                "model":   model,
                "metal":   metal,
                "horizon": horizon,
                "p10":     p10,
                "p50":     p50,
                "p90":     p90,
                "run_at":  datetime.now(tz=timezone.utc),
            })
        return row_id

    async def fetch(self, query: str, *args):
        if "prices_canonical" in query and args:
            metal  = args[0]
            prices = self._prices.get(metal, [])
            return [
                {"price_date": date(2024, 1, i + 1), "value": v}
                for i, v in enumerate(prices)
            ]
        if "forecasts" in query and args:
            metal   = args[0]
            matching = [r for r in self.forecasts if r["metal"] == metal]
            seen, unique = set(), []
            for r in reversed(matching):
                key = (r["model"], r["horizon"])
                if key not in seen:
                    seen.add(key)
                    unique.append(r)
            return unique
        return []

    async def execute(self, query: str, *args):
        pass

    async def fetchrow(self, query: str, *args):
        return None

    def acquire(self):
        return _MockAcquire(self)


class _MockAcquire:
    def __init__(self, db):
        self._db = db

    async def __aenter__(self):
        return self._db

    async def __aexit__(self, *args):
        pass


# ── Seed prices (scrap metals) ────────────────────────────────────────────────

# CU_BARE — Bare Bright Copper (~$3.50-4.20/lb)
CU_BARE_PRICES = [
    3.85, 3.80, 3.88, 3.82, 3.90,
    3.87, 3.84, 3.79, 3.83, 3.91,
    3.86, 3.78, 3.85, 3.89, 3.84,
    3.81, 3.92, 3.88, 3.85, 3.83,
    3.87, 3.90, 3.84, 3.88, 3.91,
    3.86, 3.83, 3.89, 3.85, 3.82,
]

# HMS1 — Heavy Melting Steel #1 (~$0.090-0.110/lb)
HMS1_PRICES = [
    0.1005, 0.0995, 0.1010, 0.0988, 0.1015,
    0.0998, 0.1002, 0.0990, 0.1008, 0.1020,
    0.0985, 0.0993, 0.1005, 0.1012, 0.0997,
    0.1000, 0.1018, 0.0995, 0.1003, 0.0988,
    0.1010, 0.1022, 0.0992, 0.1005, 0.1015,
    0.0998, 0.1008, 0.1000, 0.0995, 0.1005,
]


# ── Inline forecast app (no real models) ─────────────────────────────────────

# Use scrap metal slugs for testing
METALS_TEST   = ["CU_BARE", "HMS1", "ZORBA"]
HORIZONS_TEST = [1, 5, 20]

MODEL_REGISTRY_STUB = [
    ("naive",          _stub_model),
    ("arima",          _stub_model),
    ("gradient_boost", _stub_model),
]


def build_test_app(seed_prices: Dict[str, List[float]] = None):
    test_app = FastAPI()
    db       = InMemoryDB(seed_prices or {"CU_BARE": CU_BARE_PRICES, "HMS1": HMS1_PRICES})

    async def _load_prices(metal):
        rows   = await db.fetch("SELECT FROM prices_canonical WHERE metal=$1", metal)
        prices = [float(r["value"]) for r in rows]
        return prices

    async def _run_and_store(model_name, model_fn, metal, prices):
        if not prices or len(prices) < 5:
            return []
        forecasts = model_fn(prices, horizons=HORIZONS_TEST)
        stored = []
        for horizon, pq in forecasts.items():
            if pq.get("p50") is None:
                continue
            row_id = await db.fetchval(
                "INSERT INTO forecasts (model, metal, horizon, p10, p50, p90) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
                model_name, metal, horizon, pq.get("p10"), pq["p50"], pq.get("p90"),
            )
            stored.append({"id": row_id, "model": model_name, "metal": metal,
                           "horizon": horizon, **pq})
        return stored

    @test_app.post("/forecast/run", status_code=202)
    async def run_forecast(request: Request):
        all_results = []
        for metal in METALS_TEST:
            prices = await _load_prices(metal)
            for model_name, model_fn in MODEL_REGISTRY_STUB:
                results = await _run_and_store(model_name, model_fn, metal, prices)
                all_results.extend(results)
        return {
            "status":            "ok",
            "forecasts_created": len(all_results),
            "request_id":        "test-id",
        }

    @test_app.get("/forecast/latest")
    async def get_latest(metal: str):
        rows = await db.fetch("SELECT FROM forecasts WHERE metal=$1", metal.upper())
        if not rows:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"No forecasts for {metal}")
        return [ForecastOut(**r) for r in rows]

    test_app.state.pool = db
    return test_app, db


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestForecastRunStorage:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.app, self.db = build_test_app()
        self.client       = TestClient(self.app)

    def test_forecast_run_returns_202(self):
        resp = self.client.post("/forecast/run")
        assert resp.status_code == status.HTTP_202_ACCEPTED

    def test_forecast_run_creates_rows(self):
        self.client.post("/forecast/run")
        assert len(self.db.forecasts) > 0

    def test_all_horizons_stored(self):
        self.client.post("/forecast/run")
        horizons_found = {row["horizon"] for row in self.db.forecasts}
        assert 1  in horizons_found, "Horizon 1 missing"
        assert 5  in horizons_found, "Horizon 5 missing"
        assert 20 in horizons_found, "Horizon 20 missing"

    def test_forecast_rows_have_p50(self):
        self.client.post("/forecast/run")
        for row in self.db.forecasts:
            assert row["p50"] is not None, f"Row missing p50: {row}"

    def test_forecast_rows_have_correct_fields(self):
        self.client.post("/forecast/run")
        assert len(self.db.forecasts) > 0
        for row in self.db.forecasts:
            assert "model"   in row and row["model"]   is not None
            assert "metal"   in row and row["metal"]   is not None
            assert "horizon" in row and row["horizon"] in [1, 5, 20]
            assert "p50"     in row

    def test_naive_model_in_stored_forecasts(self):
        self.client.post("/forecast/run")
        models = {row["model"] for row in self.db.forecasts}
        assert "naive" in models

    def test_arima_model_in_stored_forecasts(self):
        self.client.post("/forecast/run")
        models = {row["model"] for row in self.db.forecasts}
        assert "arima" in models

    def test_gradient_boost_in_stored_forecasts(self):
        self.client.post("/forecast/run")
        models = {row["model"] for row in self.db.forecasts}
        assert "gradient_boost" in models

    def test_response_includes_forecast_count(self):
        resp = self.client.post("/forecast/run")
        data = resp.json()
        assert "forecasts_created" in data
        assert data["forecasts_created"] > 0

    def test_get_latest_forecast_after_run(self):
        self.client.post("/forecast/run")
        resp = self.client.get("/forecast/latest?metal=CU_BARE")
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_get_latest_missing_metal_404(self):
        # ZORBA has no seed prices in this fixture
        resp = self.client.get("/forecast/latest?metal=ZORBA")
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_forecast_run_with_insufficient_data(self):
        """Forecast run with < 5 rows does not crash — returns 202 with 0 forecasts."""
        app_short, db_short = build_test_app(
            seed_prices={"CU_BARE": [3.85, 3.80]}
        )
        client_short = TestClient(app_short)
        resp = client_short.post("/forecast/run")
        assert resp.status_code == status.HTTP_202_ACCEPTED
        # CU_BARE with 2 rows → 0 forecasts (< 5 minimum)
        assert db_short.forecasts == [] or all(
            r["metal"] != "CU_BARE" for r in db_short.forecasts
        )

    def test_scrap_metals_covered(self):
        """With seed data for CU_BARE and HMS1, both get forecast rows."""
        self.client.post("/forecast/run")
        metals = {row["metal"] for row in self.db.forecasts}
        assert "CU_BARE" in metals
        assert "HMS1"    in metals

    def test_horizons_per_model(self):
        """Each model produces forecasts for all 3 horizons."""
        self.client.post("/forecast/run")
        from collections import defaultdict
        model_horizons = defaultdict(set)
        for row in self.db.forecasts:
            model_horizons[row["model"]].add(row["horizon"])
        for model in ["naive", "arima", "gradient_boost"]:
            assert {1, 5, 20}.issubset(model_horizons[model]), \
                f"{model} missing horizons: {model_horizons[model]}"
