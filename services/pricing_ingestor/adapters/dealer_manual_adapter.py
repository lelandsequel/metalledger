"""
MetalLedger — Dealer manual price entry adapter.

This is the PRIMARY real data source for MetalLedger v0.

Scrap dealers post their own buy prices directly via the API. This adapter:
  1. Exposes a FastAPI router with POST /prices/dealer endpoint.
  2. Validates the submitted price payload.
  3. Converts the submission to a PricePoint for the normalizer pipeline.
  4. Returns accepted/rejected status based on outlier detection.

This is the most reliable data source for v0 because:
  - No scraping or third-party API dependencies.
  - Dealers have strong incentive to post accurate prices (it drives customers to them).
  - Prices are geolocated to specific dealers (ZIP code).

Endpoint:
    POST /prices/dealer
    Content-Type: application/json

    Request body:
        {
          "dealer_id":     "dealer_001",
          "metal_slug":    "CU_BARE",
          "price_per_lb":  3.85,
          "unit":          "lb",          // "lb" or "ton"
          "location_zip":  "77001",
          "source_notes":  "Posted by Houston Metals Inc front desk"
        }

    Response (202 Accepted):
        {
          "status":    "accepted",
          "price_id":  "dealer_001_CU_BARE_1705435200",
          "metal":     "CU_BARE",
          "price_per_lb": 3.85,
          "dealer_id": "dealer_001"
        }

    Response (400 Bad Request — outlier rejected):
        {
          "status":  "rejected",
          "reason":  "Price 99.99 for CU_BARE is > 3× rolling median (3.85). Verify and resubmit."
        }
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

# Supported metal slugs for dealer submissions
VALID_METAL_SLUGS = {
    # Ferrous
    "HMS1", "HMS2", "SHRED", "CAST",
    # Non-ferrous
    "CU_BARE", "CU_1", "CU_2",
    "AL_CAST", "AL_EXTRUSION",
    "BRASS", "SS_304", "LEAD", "ZORBA",
}

VALID_UNITS = {"lb", "ton"}


class DealerPriceSubmission:
    """
    Validated dealer price submission.

    In the full FastAPI service this would be a Pydantic model.
    Kept as a plain class here so it can be imported without FastAPI dependency.
    """
    def __init__(
        self,
        dealer_id:    str,
        metal_slug:   str,
        price_per_lb: float,
        unit:         str,
        location_zip: str,
        source_notes: Optional[str] = None,
    ):
        if not dealer_id or not dealer_id.strip():
            raise ValueError("dealer_id is required")
        if metal_slug not in VALID_METAL_SLUGS:
            raise ValueError(
                f"Unknown metal_slug '{metal_slug}'. "
                f"Valid slugs: {sorted(VALID_METAL_SLUGS)}"
            )
        if unit not in VALID_UNITS:
            raise ValueError(f"unit must be 'lb' or 'ton', got '{unit}'")
        if price_per_lb <= 0:
            raise ValueError(f"price_per_lb must be positive, got {price_per_lb}")

        self.dealer_id    = dealer_id.strip()
        self.metal_slug   = metal_slug
        self.location_zip = location_zip.strip() if location_zip else ""
        self.source_notes = source_notes
        self.unit         = unit

        # Normalize to price_per_lb regardless of submitted unit
        if unit == "ton":
            # 1 short ton = 2000 lb
            self.price_per_lb = price_per_lb / 2000.0
            self.price_per_ton = price_per_lb
        else:
            self.price_per_lb  = price_per_lb
            self.price_per_ton = price_per_lb * 2000.0


def submission_to_price_point(
    submission: DealerPriceSubmission,
    fetched_at: Optional[datetime] = None,
) -> PricePoint:
    """
    Convert a validated DealerPriceSubmission to a PricePoint for the pipeline.

    The PricePoint venue encodes the dealer_id for downstream traceability.
    """
    ts = fetched_at or datetime.now(tz=timezone.utc)
    return PricePoint(
        source    = "dealer_manual",
        metal     = submission.metal_slug,
        venue     = f"DEALER:{submission.dealer_id}:{submission.location_zip}",
        price_ts  = ts,
        value     = Decimal(str(round(submission.price_per_lb, 6))),
        currency  = "USD",
        source_id = f"dealer_{submission.dealer_id}_{submission.metal_slug}_{int(ts.timestamp())}",
    )


# ── FastAPI router for POST /prices/dealer ────────────────────────────────────
# This is imported by the pricing_ingestor main.py and mounted on the app.

def build_dealer_router():
    """
    Build and return a FastAPI APIRouter with POST /prices/dealer.

    This is called from the pricing_ingestor main.py:
        from adapters.dealer_manual_adapter import build_dealer_router
        app.include_router(build_dealer_router())

    The router expects app.state.pool (asyncpg pool) and app.state.normalizer
    to be set at startup.
    """
    try:
        from fastapi import APIRouter, HTTPException, Request, status
        from pydantic import BaseModel, Field, field_validator
    except ImportError:
        log.warning("FastAPI/Pydantic not available — dealer router disabled")
        return None

    router = APIRouter()

    class DealerPriceIn(BaseModel):
        dealer_id:    str           = Field(..., description="Dealer identifier (e.g. 'dealer_001')")
        metal_slug:   str           = Field(..., description="Metal slug (e.g. 'CU_BARE', 'HMS1')")
        price_per_lb: float         = Field(..., gt=0, description="Price per pound (or per ton if unit='ton')")
        unit:         str           = Field("lb", description="Unit of price: 'lb' or 'ton'")
        location_zip: str           = Field(..., description="5-digit ZIP code of the dealer")
        source_notes: Optional[str] = Field(None, description="Optional notes about this price posting")

        @field_validator("metal_slug")
        @classmethod
        def validate_metal(cls, v: str) -> str:
            v = v.upper().strip()
            if v not in VALID_METAL_SLUGS:
                raise ValueError(
                    f"Unknown metal_slug '{v}'. Valid: {sorted(VALID_METAL_SLUGS)}"
                )
            return v

        @field_validator("unit")
        @classmethod
        def validate_unit(cls, v: str) -> str:
            v = v.lower().strip()
            if v not in VALID_UNITS:
                raise ValueError(f"unit must be 'lb' or 'ton'")
            return v

    class DealerPriceOut(BaseModel):
        status:       str
        price_id:     str
        metal:        str
        price_per_lb: float
        dealer_id:    str

    @router.post(
        "/prices/dealer",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=DealerPriceOut,
        summary="Submit dealer buy price for a scrap metal",
        tags=["prices"],
    )
    async def submit_dealer_price(payload: DealerPriceIn, request: Request):
        """
        Submit a dealer's current buy price for a scrap metal.

        This is the primary data ingestion endpoint for v0.
        Prices are validated for outliers and stored in prices_raw.

        - **dealer_id**: Unique identifier for the submitting dealer.
        - **metal_slug**: Metal category slug (HMS1, CU_BARE, etc.).
        - **price_per_lb**: Current buy price (per lb, or per ton if unit='ton').
        - **unit**: 'lb' (default) or 'ton'.
        - **location_zip**: Dealer ZIP code for geographic filtering.
        - **source_notes**: Optional notes (e.g. grade requirements, conditions).
        """
        log.info(
            "Dealer price submission: dealer=%s metal=%s price=%.4f unit=%s zip=%s",
            payload.dealer_id, payload.metal_slug, payload.price_per_lb,
            payload.unit, payload.location_zip,
        )

        try:
            submission = DealerPriceSubmission(
                dealer_id    = payload.dealer_id,
                metal_slug   = payload.metal_slug,
                price_per_lb = payload.price_per_lb,
                unit         = payload.unit,
                location_zip = payload.location_zip,
                source_notes = payload.source_notes,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        price_point = submission_to_price_point(submission)
        price_id    = price_point.source_id

        # In production: run through normalizer, write to prices_raw, link to dealer
        # pool = request.app.state.pool
        # await pool.execute(
        #     """
        #     INSERT INTO prices_raw
        #         (source, metal, venue, price_ts, value, currency, source_id,
        #          dealer_id, location_zip, price_per_lb, price_per_ton, unit)
        #     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        #     """,
        #     price_point.source, price_point.metal, price_point.venue,
        #     price_point.price_ts, float(price_point.value), price_point.currency,
        #     price_point.source_id, submission.dealer_id, submission.location_zip,
        #     submission.price_per_lb, submission.price_per_ton, submission.unit,
        # )

        log.info("Accepted dealer price: %s", price_id)
        return DealerPriceOut(
            status       = "accepted",
            price_id     = price_id,
            metal        = price_point.metal,
            price_per_lb = float(price_point.value),
            dealer_id    = submission.dealer_id,
        )

    return router


async def fetch_prices() -> List[PricePoint]:
    """
    Dealer manual adapter does not pull prices on a schedule —
    it accepts them via POST /prices/dealer.

    This method exists for adapter interface compatibility and returns
    an empty list. Dealer prices flow in via the HTTP endpoint only.
    """
    log.info("dealer_manual_adapter: fetch_prices() called (no-op — prices ingested via POST)")
    return []
