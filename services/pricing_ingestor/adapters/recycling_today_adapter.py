"""
MetalLedger — RecyclingToday / Fastmarkets commodity benchmark adapter (stub).

────────────────────────────────────────────────────────────────────────────
RecyclingToday (https://www.recyclingtoday.com) and Fastmarkets
(https://www.fastmarkets.com) publish commodity-grade benchmark prices for
recycled metals including HMS 1&2, shredded steel, zorba, and other grades.

LICENSING NOTE:
  Fastmarkets requires a subscription for real-time data. Their benchmark
  prices (e.g., HMS 1&2 US export, shredded scrap, zorba) are used by
  traders, mills, and recyclers worldwide as contract reference prices.
  Commercial use of Fastmarkets data requires a signed license agreement.
  See https://www.fastmarkets.com/about-us/licensing for details.

  RecyclingToday editorial content and market reports are available under
  their standard publishing terms. Systematic data extraction may require
  a separate licensing arrangement.

HOW TO WIRE THE REAL FEED:
1. Subscribe to Fastmarkets at https://www.fastmarkets.com
2. Obtain API credentials (Bearer token or API key).
3. Set FASTMARKETS_API_KEY and FASTMARKETS_BASE_URL env vars.
4. Replace _synthetic_prices() with _fetch_live() — stubbed below.
5. Update EGRESS_ALLOWLIST in config.py to include "api.fastmarkets.com".
6. Map Fastmarkets price codes to MetalLedger metal slugs.

Supported commodity grades (Fastmarkets codes → MetalLedger slugs):
  - HMS 1 US export  → HMS1
  - HMS 2 US export  → HMS2
  - Shredded scrap   → SHRED
  - Cast iron        → CAST
  - Zorba 95/2       → ZORBA
  - (Copper and aluminum grades are covered by iscrap/dealer adapters)

Stub behavior:
   Returns synthetic commodity benchmark prices per ton (USD) for ferrous
   grades and per pound for zorba, mirroring Fastmarkets price structure.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))

from common.logging_util import get_logger
from common.types import PricePoint

log = get_logger(__name__)

_FASTMARKETS_API_KEY:  str = os.getenv("FASTMARKETS_API_KEY",  "")
_FASTMARKETS_BASE_URL: str = os.getenv("FASTMARKETS_BASE_URL", "https://api.fastmarkets.com")

# Commodity benchmark prices — unit noted per metal
# Ferrous grades: USD/ton (long ton for HMS export, short ton for domestic)
# Non-ferrous: USD/lb
_SYNTHETIC_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "HMS1":  {"price": 205.00, "unit": "ton",  "venue": "US_EXPORT",   "description": "Heavy Melting Steel #1 — US export benchmark"},
    "HMS2":  {"price": 180.00, "unit": "ton",  "venue": "US_EXPORT",   "description": "Heavy Melting Steel #2 — US export benchmark"},
    "SHRED": {"price": 192.00, "unit": "ton",  "venue": "US_DOMESTIC", "description": "Shredded Steel — US domestic benchmark"},
    "CAST":  {"price": 142.00, "unit": "ton",  "venue": "US_DOMESTIC", "description": "Cast Iron — US domestic benchmark"},
    "ZORBA": {"price": 0.67,   "unit": "lb",   "venue": "US_DOMESTIC", "description": "Zorba 95/2 (mixed non-ferrous shredded) — US benchmark"},
}

# Fastmarkets price codes (for reference when wiring real API)
_FASTMARKETS_CODES: Dict[str, str] = {
    "HMS1":  "MB-FE-0003",   # HMS 1&2 (80:20) US export - $/gross ton
    "HMS2":  "MB-FE-0004",   # HMS 2 US export
    "SHRED": "MB-FE-0005",   # Shredded scrap - US Midwest
    "CAST":  "MB-FE-0009",   # Cast iron borings - US Midwest
    "ZORBA": "MB-AL-0032",   # Zorba 95/2 - US
}


def _synthetic_prices(fetched_at: datetime) -> List[PricePoint]:
    """Return synthetic commodity benchmark prices mimicking Fastmarkets data."""
    log.info("Fastmarkets/RecyclingToday: no live feed — returning synthetic benchmark data")
    points: List[PricePoint] = []
    for metal, info in _SYNTHETIC_BENCHMARKS.items():
        # Store price as price_per_lb for consistency; include unit in source_id
        price = info["price"]
        unit  = info["unit"]
        # Normalize to $/lb for internal storage (ferrous: $/ton ÷ 2000)
        if unit == "ton":
            price_per_lb = price / 2000.0
        else:
            price_per_lb = price

        points.append(
            PricePoint(
                source    = "recycling_today",
                metal     = metal,
                venue     = info["venue"],
                price_ts  = fetched_at,
                value     = Decimal(str(round(price_per_lb, 6))),
                currency  = "USD",
                source_id = f"fastmarkets_synthetic_{_FASTMARKETS_CODES.get(metal, metal)}_{int(fetched_at.timestamp())}",
            )
        )
    return points


async def _fetch_live(fetched_at: datetime) -> List[PricePoint]:
    """
    Call the live Fastmarkets API to fetch commodity benchmark prices.

    ── WIRING INSTRUCTIONS ──────────────────────────────────────────────────
    Fastmarkets API (hypothetical endpoint — verify with your subscription):
        GET https://api.fastmarkets.com/v2/prices?codes=MB-FE-0003,MB-FE-0004,...
        Headers:
            Authorization: Bearer {FASTMARKETS_API_KEY}
            Accept: application/json

    Example response shape (verify against actual Fastmarkets API docs):
        {
          "data": [
            {
              "code": "MB-FE-0003",
              "name": "Steel scrap, HMS 1&2 (80:20 mix), US export",
              "price": 205.00,
              "unit": "gross ton",
              "currency": "USD",
              "date": "2024-01-15"
            },
            ...
          ]
        }

    Steps:
    1. Map each code back to MetalLedger slug using _FASTMARKETS_CODES (inverted).
    2. Normalize unit (gross ton → lb: divide by 2240; short ton → lb: divide by 2000).
    3. Build PricePoint objects and return.
    ─────────────────────────────────────────────────────────────────────────
    """
    # from common.egress import egress_get
    # codes = ",".join(_FASTMARKETS_CODES.values())
    # url = f"{_FASTMARKETS_BASE_URL}/v2/prices"
    # headers = {"Authorization": f"Bearer {_FASTMARKETS_API_KEY}", "Accept": "application/json"}
    # resp = await egress_get(url, params={"codes": codes}, headers=headers)
    # resp.raise_for_status()
    # data = resp.json()
    # return _parse_response(data["data"], fetched_at)
    raise NotImplementedError(
        "Fastmarkets live feed not implemented. "
        "Add api.fastmarkets.com to EGRESS_ALLOWLIST and implement _parse_response()."
    )


async def fetch_prices() -> List[PricePoint]:
    """
    Fetch commodity benchmark scrap prices from RecyclingToday / Fastmarkets.

    Supported grades: HMS1, HMS2, SHRED, CAST, ZORBA.

    Returns:
        List of PricePoint with benchmark commodity prices.

    Note:
        Falls back to synthetic data until Fastmarkets subscription is configured.
        Fastmarkets requires subscription for real-time data.
    """
    fetched_at = datetime.now(tz=timezone.utc)

    if _FASTMARKETS_API_KEY:
        try:
            log.info("Fetching live Fastmarkets benchmark prices")
            return await _fetch_live(fetched_at)
        except NotImplementedError:
            log.warning("Fastmarkets live fetch not yet implemented — using synthetic data")
        except Exception as exc:
            log.warning("Fastmarkets live fetch failed (%s) — using synthetic data", exc)

    return _synthetic_prices(fetched_at)
