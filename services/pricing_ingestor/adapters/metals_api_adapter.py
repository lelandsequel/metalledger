"""
MetalLedger — Metals-API.com adapter.

Live path:
  Set METALS_API_KEY env var.  The adapter calls
  https://metals-api.com/api/latest?base=USD&symbols=XAU,XAG,CU
  and returns a list of PricePoint objects.

Stub path (METALS_API_KEY missing / empty):
  Returns a realistic synthetic payload so downstream code (normalizer,
  DB writes, tests) all work without a live key.  The stub response
  mirrors the exact shape of the real API.

Licensing note:
  Metals-API.com offers free dev tiers (limited calls/month).
  Commercial use may require a paid plan.  See https://metals-api.com/pricing
"""

from __future__ import annotations

import sys
import os

# Allow running from service root without installing packages
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

from common.config import METALS_API_KEY
from common.egress import egress_get
from common.logging_util import get_logger
from common.types import PricePoint

log = get_logger(__name__)

# Metals-API.com endpoint — see https://metals-api.com/documentation
_BASE_URL = "https://metals-api.com/api/latest"
_SYMBOLS  = "XAU,XAG,CU"

# Synthetic fallback data — updated weekly during dev; mirrors API shape
_SYNTHETIC_RATES: Dict[str, float] = {
    "XAU": 2041.50,
    "XAG": 23.85,
    "CU":  3.912,
}


def _parse_response(data: Dict[str, Any], fetched_at: datetime) -> List[PricePoint]:
    """
    Parse the metals-api.com JSON response into PricePoint objects.

    Expected response shape::

        {
          "success": true,
          "base": "USD",
          "rates": {
            "XAU": 0.000489,   # units per USD → invert to get price in USD
            "XAG": 0.04195,
            "CU":  0.2556
          },
          "timestamp": 1705435200
        }

    The API expresses rates as [metal] / [base], i.e. how many troy-oz of gold
    equal 1 USD.  We invert to get USD/oz (the industry-standard quote).
    """
    rates: Dict[str, float] = data.get("rates", {})
    base:  str              = data.get("base", "USD")
    points: List[PricePoint] = []

    for symbol in ["XAU", "XAG", "CU"]:
        rate = rates.get(symbol)
        if rate is None or rate <= 0:
            log.warning("Missing or zero rate for %s in metals-api response", symbol)
            continue

        # Invert: API gives oz-per-USD, we store USD-per-oz
        usd_price = Decimal(str(round(1.0 / rate, 6)))

        points.append(
            PricePoint(
                source    = "metals_api",
                metal     = symbol,
                venue     = "SPOT",
                price_ts  = fetched_at,
                value     = usd_price,
                currency  = base,
                source_id = f"metals_api_{symbol}_{int(fetched_at.timestamp())}",
            )
        )

    return points


def _synthetic_prices(fetched_at: datetime) -> List[PricePoint]:
    """Return deterministic synthetic prices when no API key is configured."""
    log.info("METALS_API_KEY not set — using synthetic price data")
    return [
        PricePoint(
            source    = "metals_api",
            metal     = symbol,
            venue     = "SPOT",
            price_ts  = fetched_at,
            value     = Decimal(str(price)),
            currency  = "USD",
            source_id = f"synthetic_{symbol}_{int(fetched_at.timestamp())}",
        )
        for symbol, price in _SYNTHETIC_RATES.items()
    ]


async def fetch_prices() -> List[PricePoint]:
    """
    Fetch latest spot prices from Metals-API.com.

    Returns:
        List of PricePoint (one per supported metal).

    Raises:
        common.egress.EgressViolation: if domain is off allowlist (should never
            happen for metals-api.com, but guards against config drift).
        httpx.HTTPError: on network failure.
    """
    fetched_at = datetime.now(tz=timezone.utc)

    if not METALS_API_KEY:
        return _synthetic_prices(fetched_at)

    params = {
        "access_key": METALS_API_KEY,
        "base":       "USD",
        "symbols":    _SYMBOLS,
    }

    log.info("Fetching prices from metals-api.com (symbols=%s)", _SYMBOLS)
    response = await egress_get(_BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    if not data.get("success", True):
        # API returns {"success": false, "error": {...}} on quota/auth errors
        err = data.get("error", {})
        log.error("Metals-API error: code=%s info=%s", err.get("code"), err.get("info"))
        log.warning("Falling back to synthetic data")
        return _synthetic_prices(fetched_at)

    prices = _parse_response(data, fetched_at)
    log.info("Fetched %d price points from metals-api.com", len(prices))
    return prices
