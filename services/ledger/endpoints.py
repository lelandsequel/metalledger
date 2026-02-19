"""
MetalLedger — Ledger service API endpoints.

Routes:
  POST /journal_entries     — Requires X-API-Role: HUMAN header
  GET  /valuations          — Mark-to-market lookup
  GET  /accounts            — List chart of accounts
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import date
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

from common.audit import write_audit_entry_async, new_request_id
from common.logging_util import get_logger
from common.types import AccountOut, JournalEntryIn, JournalEntryOut, ValuationOut

from models import (
    create_journal_entry,
    get_journal_entry,
    get_valuation,
    list_accounts,
    get_price_comparison,
)

log    = get_logger(__name__)
router = APIRouter()


def get_pool(request: Request):
    return request.app.state.pool


# ── POST /journal_entries ─────────────────────────────────────────────────────

@router.post(
    "/journal_entries",
    status_code=status.HTTP_201_CREATED,
    response_model=JournalEntryOut,
    summary="Create a balanced journal entry",
)
async def create_entry(
    payload:    JournalEntryIn,
    request:    Request,
    x_api_role: Optional[str] = Header(None, alias="X-API-Role"),
    pool=Depends(get_pool),
):
    """
    Create a double-entry journal entry.

    - Requires header `X-API-Role: HUMAN`.
    - Returns 403 if role is not HUMAN (orchestrator agent is explicitly blocked).
    - Returns 400 if debits ≠ credits (also enforced by Pydantic model).
    - Returns 201 with the created entry on success.
    """
    request_id = new_request_id()

    # Guardrail #1: only HUMAN role may create journal entries
    if x_api_role != "HUMAN":
        await write_audit_entry_async(
            pool,
            request_id = request_id,
            actor      = x_api_role or "anonymous",
            action     = "POST /journal_entries",
            payload    = {"status": "DENIED", "reason": "non-HUMAN role"},
        )
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail      = "Only HUMAN role may create journal entries. Agent access denied.",
        )

    # Balance is already validated by Pydantic; catching DB-level recheck
    try:
        entry_id = await create_journal_entry(
            pool,
            entry_date = payload.entry_date,
            memo       = payload.memo,
            created_by = x_api_role,
            lines      = payload.lines,
        )
    except ValueError as exc:
        # Should not reach here if Pydantic validation worked, but belt-and-suspenders
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail      = str(exc),
        )

    entry = await get_journal_entry(pool, entry_id)

    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = x_api_role,
        action     = "POST /journal_entries",
        payload    = {"entry_id": entry_id, "status": "CREATED"},
    )

    log.info("Journal entry %d created by %s", entry_id, x_api_role)
    return JournalEntryOut(**entry)


# ── GET /valuations ───────────────────────────────────────────────────────────

@router.get(
    "/valuations",
    response_model=Optional[ValuationOut],
    summary="Mark-to-market valuation for a metal on a date",
)
async def get_valuation_endpoint(
    metal:   str,
    date:    date,
    request: Request,
    pool=Depends(get_pool),
):
    """
    Return the mark-to-market value of inventory for a metal on a given date.

    Uses prices_canonical for price lookup and inventory_lots for quantity.
    Returns 404 if no canonical price exists on or before the requested date.
    """
    request_id = new_request_id()

    result = await get_valuation(pool, metal=metal.upper(), valuation_date=date)

    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = "api",
        action     = f"GET /valuations metal={metal} date={date}",
        payload    = {"found": result is not None},
    )

    if result is None:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail      = f"No canonical price found for {metal} on or before {date}",
        )
    return result


# ── GET /accounts ─────────────────────────────────────────────────────────────

@router.get(
    "/accounts",
    response_model=list[AccountOut],
    summary="List chart of accounts",
)
async def get_accounts(
    request: Request,
    pool=Depends(get_pool),
):
    """Return all active accounts in the chart of accounts."""
    request_id = new_request_id()
    accounts   = await list_accounts(pool)

    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = "api",
        action     = "GET /accounts",
        payload    = {"count": len(accounts)},
    )

    return accounts


# ── GET /prices/compare ───────────────────────────────────────────────────────

@router.get(
    "/prices/compare",
    summary="Compare dealer prices for a scrap metal by ZIP code",
    tags=["prices"],
)
async def compare_prices(
    metal:        str,
    zip:          str,
    radius_miles: int     = 50,
    request:      Request = None,
    pool=Depends(get_pool),
):
    """
    Return list of dealers with their current prices for a scrap metal,
    sorted by price descending (best payer first).

    This is the core value proposition for scrap resellers: find the dealer
    paying the most for your metal today.

    Query params:
    - **metal**: Metal slug (e.g. CU_BARE, HMS1, ZORBA)
    - **zip**: 5-digit ZIP code to search from
    - **radius_miles**: Search radius in miles (default 50)

    Returns:
        List of dealer price records sorted by price_per_lb descending.
        Includes dealer name, location, price, and price age.

    Notes:
    - Uses most recent price per dealer (last 30 days).
    - ZIP-based radius is approximate (same-prefix ZIP matching for v0;
      full geo-distance requires PostGIS or a geocoding service).
    - Dealers with no prices in the last 30 days are excluded.
    """
    request_id = new_request_id()

    results = await get_price_comparison(
        pool,
        metal        = metal.upper().strip(),
        zip_code     = zip.strip(),
        radius_miles = radius_miles,
    )

    await write_audit_entry_async(
        pool,
        request_id = request_id,
        actor      = "api",
        action     = f"GET /prices/compare metal={metal} zip={zip} radius={radius_miles}",
        payload    = {"result_count": len(results)},
    )

    if not results:
        from fastapi import HTTPException
        raise HTTPException(
            status_code = 404,
            detail      = (
                f"No dealer prices found for {metal.upper()} near ZIP {zip} "
                f"within {radius_miles} miles. "
                f"Dealers can submit prices via POST /prices/dealer."
            ),
        )

    return results
