"""
MetalLedger — LBMA (London Bullion Market Association) adapter.

STATUS: Functional stub — returns synthetic or CSV data.

────────────────────────────────────────────────────────────────────────────
HOW TO WIRE THE REAL LBMA FEED (when licensed):

1. Obtain institutional access from LBMA: https://www.lbma.org.uk/prices-and-data
   LBMA price data (AM/PM fixes) requires a licensing agreement.
   Commercial redistribution is prohibited without a signed data contract.

2. Set environment variables:
       LBMA_API_KEY=<your_key>
       LBMA_BASE_URL=https://api.lbma.org.uk/   (or confirmed endpoint)

3. Replace `_synthetic_prices()` call in `fetch_prices()` with
   `await _fetch_live(fetched_at)` which calls `egress_get(...)` — already
   stubbed below.  Uncomment the block and fill in the real URL path.

4. Adjust `_parse_response()` for the actual JSON/CSV schema from LBMA.

5. Update source_configs in the DB:
       UPDATE source_configs SET config_val = '{"enabled": true, ...}'
       WHERE config_key = 'lbma';
   (Requires a HUMAN-signed approval record per guardrail #2.)

Licensing notes:
   - LME, CME, and LBMA data require paid licensing for commercial use.
   - Spot access via LBMA is free for personal/research; redistribution is not.
   - Users are solely responsible for data licensing compliance.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import csv
import io
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "packages"))

from common.egress import egress_get
from common.logging_util import get_logger
from common.types import PricePoint

log = get_logger(__name__)

_LBMA_API_KEY:  str = os.getenv("LBMA_API_KEY",  "")
_LBMA_BASE_URL: str = os.getenv("LBMA_BASE_URL",  "https://api.lbma.org.uk/")
_LBMA_CSV_PATH: str = os.getenv("LBMA_CSV_PATH",  "")   # optional local CSV override

# Synthetic fallback matching LBMA AM fix conventions (USD/troy oz)
_SYNTHETIC_RATES: Dict[str, float] = {
    "XAU": 2038.75,   # LBMA Gold AM Fix
    "XAG": 23.72,     # LBMA Silver Fix
}


# ── Private helpers ───────────────────────────────────────────────────────────

def _synthetic_prices(fetched_at: datetime) -> List[PricePoint]:
    log.info("LBMA: no live feed configured — returning synthetic AM-fix data")
    return [
        PricePoint(
            source    = "lbma",
            metal     = symbol,
            venue     = "LBMA_AM",
            price_ts  = fetched_at.replace(hour=10, minute=30, second=0, microsecond=0),
            value     = Decimal(str(price)),
            currency  = "USD",
            source_id = f"lbma_synthetic_{symbol}_{fetched_at.date()}",
        )
        for symbol, price in _SYNTHETIC_RATES.items()
    ]


def _parse_csv(csv_text: str, fetched_at: datetime) -> List[PricePoint]:
    """
    Parse a LBMA-style CSV with columns: Date,USD AM,USD PM (gold example).

    Expected format::
        Date,USD AM,USD PM
        2024-01-02,2063.10,2066.50
        ...

    Returns only AM fix prices.  Extend to PM as needed.
    """
    reader = csv.DictReader(io.StringIO(csv_text.strip()))
    points: List[PricePoint] = []
    for row in reader:
        try:
            fix_date = datetime.strptime(row["Date"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc, hour=10, minute=30
            )
            am_val = row.get("USD AM") or row.get("XAU AM") or row.get("AM")
            if am_val:
                points.append(
                    PricePoint(
                        source    = "lbma",
                        metal     = "XAU",
                        venue     = "LBMA_AM",
                        price_ts  = fix_date,
                        value     = Decimal(am_val.replace(",", "")),
                        currency  = "USD",
                        source_id = f"lbma_xau_am_{fix_date.date()}",
                    )
                )
        except (ValueError, KeyError) as exc:
            log.warning("LBMA CSV parse error on row %s: %s", row, exc)
    return points


async def _fetch_live(fetched_at: datetime) -> List[PricePoint]:
    """
    Call the live LBMA API endpoint.  Uncomment + adapt when licensed.

    ── WIRING INSTRUCTIONS ──────────────────────────────────────────────────
    Replace the URL path below with the correct LBMA endpoint once you have
    institutional access.  The egress.egress_get() call is already allowlisted
    for "api.lbma.org.uk".

    Expected response: JSON or CSV — adjust _parse_response accordingly.
    ─────────────────────────────────────────────────────────────────────────
    """
    url = f"{_LBMA_BASE_URL}gold/price/json"   # <- adjust to real path
    headers = {"Authorization": f"Bearer {_LBMA_API_KEY}"}
    log.info("Fetching LBMA prices from %s", url)
    response = await egress_get(url, headers=headers)
    response.raise_for_status()

    # Adapt parsing to actual LBMA response schema
    data = response.json()
    points: List[PricePoint] = []
    # Example shape (hypothetical):
    # {"date": "2024-01-02", "xauUSD": {"am": 2063.10, "pm": 2066.50}}
    for metal, key in [("XAU", "xauUSD"), ("XAG", "xagUSD")]:
        metal_data = data.get(key, {})
        am_val = metal_data.get("am")
        if am_val:
            points.append(
                PricePoint(
                    source    = "lbma",
                    metal     = metal,
                    venue     = "LBMA_AM",
                    price_ts  = fetched_at.replace(hour=10, minute=30, second=0),
                    value     = Decimal(str(am_val)),
                    currency  = "USD",
                    source_id = f"lbma_{metal.lower()}_am_{fetched_at.date()}",
                )
            )
    return points


# ── Public API ────────────────────────────────────────────────────────────────

async def fetch_prices() -> List[PricePoint]:
    """
    Fetch LBMA official daily fix prices.

    Routing logic:
    1. If LBMA_CSV_PATH is set → read local CSV file (useful for backfill).
    2. Elif LBMA_API_KEY is set → call live API (institutional access required).
    3. Else → return synthetic data so the pipeline stays functional.
    """
    fetched_at = datetime.now(tz=timezone.utc)

    if _LBMA_CSV_PATH and os.path.isfile(_LBMA_CSV_PATH):
        log.info("LBMA: reading from local CSV: %s", _LBMA_CSV_PATH)
        with open(_LBMA_CSV_PATH, "r") as f:
            csv_text = f.read()
        return _parse_csv(csv_text, fetched_at)

    if _LBMA_API_KEY:
        try:
            return await _fetch_live(fetched_at)
        except Exception as exc:
            log.warning("LBMA live fetch failed (%s) — falling back to synthetic", exc)

    return _synthetic_prices(fetched_at)
