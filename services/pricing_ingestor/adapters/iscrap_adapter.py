"""
MetalLedger — iScrap App adapter (scraper stub).

────────────────────────────────────────────────────────────────────────────
IMPORTANT: iScrap App does not have a public API. This adapter scrapes their
public price listings. For production, consider iScrap's dealer portal or a
licensed data feed. See https://www.iscrapapp.com for more information.

The iScrap App (https://www.iscrapapp.com) is the most popular scrap metal
price aggregator in North America. It aggregates dealer-posted prices for
ferrous and non-ferrous metals across thousands of scrap yards.

HOW TO WIRE THE REAL FEED:
1. iScrap does not expose a public REST API. Their mobile app communicates
   with internal endpoints that may change without notice.
2. For a robust implementation, either:
   a) Contact iScrap directly for a data partnership or dealer portal API.
   b) Use their public-facing price pages (HTML scraping — may violate ToS).
   c) Subscribe to a licensed scrap price data aggregator (e.g., Fastmarkets,
      ScrapMonster, or RecyclingToday/Fastmarkets commodity feed).
3. Set ISCRAP_USER_AGENT and ISCRAP_ZIP env vars for localized results.

Stub behavior:
   Returns synthetic scrap prices per pound/ton for supported metals at
   3 synthetic yards, mimicking the shape of iScrap price responses.
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

_BASE_URL    = "https://www.iscrapapp.com"
_USER_AGENT  = os.getenv("ISCRAP_USER_AGENT", "MetalLedger/0.1 (scrap price research)")
_SEARCH_ZIP  = os.getenv("ISCRAP_ZIP", "77001")

# Synthetic prices matching real iScrap yard listings (price_per_lb, USD)
# Shape mirrors what iScrap's internal price API returns per yard per metal.
_SYNTHETIC_YARDS: List[Dict[str, Any]] = [
    {
        "yard_id":   "yard_iscrap_001",
        "yard_name": "Houston Metals Inc",
        "zip":       "77001",
        "prices": {
            "CU_BARE":     3.85,
            "CU_1":        3.55,
            "CU_2":        3.10,
            "AL_CAST":     0.42,
            "AL_EXTRUSION": 0.52,
            "BRASS":       1.65,
            "SS_304":      0.58,
            "LEAD":        0.42,
            "ZORBA":       0.65,
            "HMS1":        0.10,   # ~$200/ton ÷ 2000 lb/ton
            "HMS2":        0.089,
            "SHRED":       0.095,
            "CAST":        0.072,
        },
    },
    {
        "yard_id":   "yard_iscrap_002",
        "yard_name": "Gulf Coast Scrap",
        "zip":       "77002",
        "prices": {
            "CU_BARE":     3.78,
            "CU_1":        3.48,
            "CU_2":        3.05,
            "AL_CAST":     0.40,
            "AL_EXTRUSION": 0.50,
            "BRASS":       1.60,
            "SS_304":      0.55,
            "LEAD":        0.40,
            "ZORBA":       0.63,
            "HMS1":        0.098,
            "HMS2":        0.085,
            "SHRED":       0.091,
            "CAST":        0.069,
        },
    },
]


def _synthetic_prices(fetched_at: datetime) -> List[PricePoint]:
    """Return synthetic scrap prices mimicking iScrap yard listings."""
    log.info("iScrap: no live scraper configured — returning synthetic yard price data")
    points: List[PricePoint] = []
    for yard in _SYNTHETIC_YARDS:
        for metal, price_per_lb in yard["prices"].items():
            points.append(
                PricePoint(
                    source    = "iscrap",
                    metal     = metal,
                    venue     = yard["yard_name"],
                    price_ts  = fetched_at,
                    value     = Decimal(str(price_per_lb)),
                    currency  = "USD",
                    source_id = f"iscrap_synthetic_{yard['yard_id']}_{metal}_{int(fetched_at.timestamp())}",
                )
            )
    return points


async def _scrape_yard_prices(zip_code: str, fetched_at: datetime) -> List[PricePoint]:
    """
    Scrape iScrap App price listings for a given ZIP code.

    ── WIRING INSTRUCTIONS ──────────────────────────────────────────────────
    iScrap's mobile app calls an internal JSON endpoint to retrieve nearby
    yard prices. The URL pattern (subject to change without notice):

        GET https://www.iscrapapp.com/api/v1/prices?zip={zip}&radius=50
        Headers:
            User-Agent: {_USER_AGENT}
            Accept: application/json

    The response shape (hypothetical — verify against actual traffic):
        {
          "yards": [
            {
              "yard_id": "abc123",
              "name": "Some Scrap Yard",
              "zip": "77001",
              "metals": [
                {"slug": "CU_BARE", "price": 3.85, "unit": "lb"},
                {"slug": "HMS1",    "price": 200.0, "unit": "ton"},
                ...
              ]
            }
          ]
        }

    For HTML scraping (fallback), parse the price table at:
        https://www.iscrapapp.com/prices?zip={zip}

    NOTE: Scraping may violate iScrap's Terms of Service. Use only with
    permission or through an official data partnership.
    ─────────────────────────────────────────────────────────────────────────
    """
    # Egress note: iscrapapp.com must be added to EGRESS_ALLOWLIST in config.py
    # before enabling this code path.
    #
    # from common.egress import egress_get
    # url = f"{_BASE_URL}/api/v1/prices"
    # headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    # resp = await egress_get(url, params={"zip": zip_code, "radius": "50"}, headers=headers)
    # resp.raise_for_status()
    # data = resp.json()
    # return _parse_response(data, fetched_at)
    raise NotImplementedError(
        "iScrap live scraper not implemented. "
        "Add iscrapapp.com to EGRESS_ALLOWLIST and implement _parse_response()."
    )


async def fetch_prices(zip_code: Optional[str] = None) -> List[PricePoint]:
    """
    Fetch scrap metal prices from iScrap App for a given ZIP code.

    Args:
        zip_code: 5-digit ZIP for geographic price lookup. Defaults to ISCRAP_ZIP env var.

    Returns:
        List of PricePoint (one per metal per yard).

    Note:
        Falls back to synthetic data until live scraper is wired.
    """
    fetched_at = datetime.now(tz=timezone.utc)
    zip_code   = zip_code or _SEARCH_ZIP

    log.info("iScrap fetch for ZIP=%s (stub — returning synthetic data)", zip_code)
    return _synthetic_prices(fetched_at)
