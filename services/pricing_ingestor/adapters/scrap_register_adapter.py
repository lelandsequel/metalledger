"""
MetalLedger — ScrapRegister.com adapter (scraper stub).

────────────────────────────────────────────────────────────────────────────
ScrapRegister.com (https://www.scrapregister.com) publishes regional average
scrap metal price listings aggregated from dealers across the US. Prices are
organized by metal category and updated regularly.

LICENSING NOTE:
  ScrapRegister's price data is published for informational purposes. Commercial
  use or systematic scraping may require a license or data partnership agreement.
  Contact ScrapRegister at https://www.scrapregister.com/contact before building
  any production system that relies on their data. Redistribution of their data
  without written permission is prohibited.

HOW TO WIRE THE REAL FEED:
1. ScrapRegister does not provide a public API.
2. Their price pages (e.g., https://www.scrapregister.com/prices/copper) are
   HTML-rendered and require scraping (BeautifulSoup or Playwright).
3. Alternatively, contact them for a data licensing arrangement.
4. Set SCRAP_REGISTER_USER_AGENT and SCRAP_REGISTER_REGION env vars.

Stub behavior:
   Returns synthetic regional average prices per pound for all supported metals,
   organized by US region (South, Midwest, Northeast, West).
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

_BASE_URL    = "https://www.scrapregister.com"
_USER_AGENT  = os.getenv("SCRAP_REGISTER_USER_AGENT", "MetalLedger/0.1 (price research)")
_REGION      = os.getenv("SCRAP_REGISTER_REGION", "South")

# Synthetic regional average prices (price_per_lb, USD)
# ScrapRegister typically organizes by region: South, Midwest, Northeast, West
_SYNTHETIC_REGIONAL: Dict[str, Dict[str, float]] = {
    "South": {
        "CU_BARE":      3.82,
        "CU_1":         3.52,
        "CU_2":         3.08,
        "AL_CAST":      0.41,
        "AL_EXTRUSION": 0.51,
        "BRASS":        1.63,
        "SS_304":       0.56,
        "LEAD":         0.41,
        "ZORBA":        0.64,
        "HMS1":         0.099,   # ~$198/ton
        "HMS2":         0.086,   # ~$172/ton
        "SHRED":        0.093,   # ~$186/ton
        "CAST":         0.071,   # ~$142/ton
    },
    "Midwest": {
        "CU_BARE":      3.79,
        "CU_1":         3.49,
        "CU_2":         3.05,
        "AL_CAST":      0.39,
        "AL_EXTRUSION": 0.49,
        "BRASS":        1.60,
        "SS_304":       0.54,
        "LEAD":         0.39,
        "ZORBA":        0.62,
        "HMS1":         0.101,
        "HMS2":         0.088,
        "SHRED":        0.096,
        "CAST":         0.073,
    },
    "Northeast": {
        "CU_BARE":      3.91,
        "CU_1":         3.60,
        "CU_2":         3.15,
        "AL_CAST":      0.43,
        "AL_EXTRUSION": 0.54,
        "BRASS":        1.68,
        "SS_304":       0.59,
        "LEAD":         0.43,
        "ZORBA":        0.67,
        "HMS1":         0.103,
        "HMS2":         0.090,
        "SHRED":        0.098,
        "CAST":         0.075,
    },
    "West": {
        "CU_BARE":      3.88,
        "CU_1":         3.57,
        "CU_2":         3.12,
        "AL_CAST":      0.44,
        "AL_EXTRUSION": 0.55,
        "BRASS":        1.66,
        "SS_304":       0.58,
        "LEAD":         0.44,
        "ZORBA":        0.66,
        "HMS1":         0.100,
        "HMS2":         0.087,
        "SHRED":        0.094,
        "CAST":         0.072,
    },
}


def _synthetic_prices(region: str, fetched_at: datetime) -> List[PricePoint]:
    """Return synthetic regional average prices mimicking ScrapRegister listings."""
    log.info("ScrapRegister: no live scraper configured — returning synthetic regional data for %s", region)
    regional_data = _SYNTHETIC_REGIONAL.get(region, _SYNTHETIC_REGIONAL["South"])
    points: List[PricePoint] = []
    for metal, price_per_lb in regional_data.items():
        points.append(
            PricePoint(
                source    = "scrap_register",
                metal     = metal,
                venue     = f"REGIONAL_{region.upper()}",
                price_ts  = fetched_at,
                value     = Decimal(str(price_per_lb)),
                currency  = "USD",
                source_id = f"scrapreg_synthetic_{region.lower()}_{metal}_{int(fetched_at.timestamp())}",
            )
        )
    return points


async def _scrape_prices(region: str, fetched_at: datetime) -> List[PricePoint]:
    """
    Scrape ScrapRegister.com regional price listings.

    ── WIRING INSTRUCTIONS ──────────────────────────────────────────────────
    ScrapRegister publishes prices at URLs like:
        https://www.scrapregister.com/prices/copper
        https://www.scrapregister.com/prices/aluminum
        https://www.scrapregister.com/prices/steel

    HTML scraping approach (requires BeautifulSoup or lxml):
        1. GET the metal-category page with appropriate User-Agent.
        2. Parse the price table (CSS selector: table.price-table or similar).
        3. Filter by region if regional tabs are available.
        4. Map scraped metal names to MetalLedger slugs (CU_BARE, HMS1, etc.)

    NOTE: Scraping may violate ScrapRegister's Terms of Service.
    Always check robots.txt and obtain permission before scraping.

    Example (illustrative — verify against actual HTML):

        from bs4 import BeautifulSoup
        from common.egress import egress_get

        headers = {"User-Agent": _USER_AGENT}
        resp = await egress_get(f"{_BASE_URL}/prices/copper", headers=headers)
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("table.price-table tr")
        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            metal_name = cells[0].text.strip()
            price_str  = cells[1].text.strip().lstrip("$")
            # Map metal_name → slug, parse price → PricePoint
    ─────────────────────────────────────────────────────────────────────────
    """
    raise NotImplementedError(
        "ScrapRegister live scraper not implemented. "
        "Add scrapregister.com to EGRESS_ALLOWLIST and implement HTML parser."
    )


async def fetch_prices(region: Optional[str] = None) -> List[PricePoint]:
    """
    Fetch regional average scrap prices from ScrapRegister.com.

    Args:
        region: US region string (South, Midwest, Northeast, West).
                Defaults to SCRAP_REGISTER_REGION env var.

    Returns:
        List of PricePoint (one per metal, for the selected region).

    Note:
        Falls back to synthetic data until live scraper is wired.
    """
    fetched_at = datetime.now(tz=timezone.utc)
    region     = region or _REGION

    log.info("ScrapRegister fetch for region=%s (stub — returning synthetic data)", region)
    return _synthetic_prices(region, fetched_at)
