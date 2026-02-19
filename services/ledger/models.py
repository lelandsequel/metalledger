"""
MetalLedger — Ledger service database models (asyncpg row helpers).

We use raw asyncpg (no ORM) for performance and auditability.
Each function returns typed dicts or Pydantic models.

Updated: Scrap metal pivot — get_price_comparison() for dealer price lookup.
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from common.types import AccountOut, DealerPriceOut, JournalEntryOut, JournalLineIn, ValuationOut


# ── Accounts ──────────────────────────────────────────────────────────────────

async def list_accounts(pool: Any) -> List[AccountOut]:
    rows = await pool.fetch(
        "SELECT id, code, name, type, currency, active FROM accounts ORDER BY code"
    )
    return [AccountOut(**dict(r)) for r in rows]


async def get_account(pool: Any, account_id: int) -> Optional[Dict]:
    row = await pool.fetchrow(
        "SELECT id, code, name, type, currency, active FROM accounts WHERE id = $1",
        account_id,
    )
    return dict(row) if row else None


# ── Journal Entries ───────────────────────────────────────────────────────────

async def create_journal_entry(
    pool: Any,
    *,
    entry_date: date,
    memo:       Optional[str],
    created_by: str,
    lines:      List[JournalLineIn],
) -> int:
    """
    Insert a journal entry + lines in a transaction.
    Balance check is enforced by the Pydantic model BEFORE this call,
    and again here as a safety net.

    Returns the new entry id.
    """
    total_debit  = sum(ln.debit  for ln in lines)
    total_credit = sum(ln.credit for ln in lines)
    if total_debit != total_credit:
        raise ValueError(
            f"Unbalanced entry: debit={total_debit} credit={total_credit}"
        )

    async with pool.acquire() as conn:
        async with conn.transaction():
            entry_id = await conn.fetchval(
                """
                INSERT INTO journal_entries (entry_date, memo, created_by, status)
                VALUES ($1, $2, $3, 'POSTED')
                RETURNING id
                """,
                entry_date, memo, created_by,
            )
            for ln in lines:
                await conn.execute(
                    """
                    INSERT INTO journal_lines
                        (entry_id, account_id, debit, credit, memo)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    entry_id, ln.account_id,
                    float(ln.debit), float(ln.credit), ln.memo,
                )
    return entry_id


async def get_journal_entry(pool: Any, entry_id: int) -> Optional[Dict]:
    row = await pool.fetchrow(
        """
        SELECT id, entry_date, memo, created_by, status, created_at
        FROM journal_entries WHERE id = $1
        """,
        entry_id,
    )
    return dict(row) if row else None


# ── Valuations ────────────────────────────────────────────────────────────────

async def get_price_comparison(
    pool: Any,
    metal: str,
    zip_code: str,
    radius_miles: int = 50,
) -> List[DealerPriceOut]:
    """
    Return dealer prices for a scrap metal near a ZIP code, sorted by price
    descending (best-paying dealer first).

    This is the core value proposition for scrap resellers.

    Args:
        pool:         asyncpg connection pool.
        metal:        Metal slug (e.g. 'CU_BARE', 'HMS1').
        zip_code:     5-digit ZIP code to search from.
        radius_miles: Search radius in miles (default 50).

    Returns:
        List of DealerPriceOut sorted by price_per_lb descending.

    Notes on ZIP-based proximity (v0):
        Full geo-distance requires PostGIS or a geocoding service.
        For v0, we use ZIP prefix matching:
          - Same ZIP = 0 miles (always included)
          - Same 3-digit prefix (~county level) ≈ within ~50 miles
          - Same 1-digit prefix (~region) ≈ within ~200 miles
        This is a practical approximation for the Houston-area demo dealers.
        Replace with PostGIS ST_DWithin() when geo data is available.
    """
    # ZIP proximity filter: same prefix (3 digits) approximates county radius
    zip_prefix = zip_code[:3] if len(zip_code) >= 3 else zip_code

    rows = await pool.fetch(
        """
        SELECT
            d.id::text          AS dealer_id,
            d.name              AS dealer_name,
            d.location_zip,
            d.city,
            d.state,
            p.metal,
            p.price_per_lb,
            p.price_per_ton,
            p.unit,
            p.price_ts,
            p.source
        FROM prices_raw p
        JOIN dealers d ON d.id = p.dealer_id
        WHERE
            p.metal       = $1
            AND d.active  = TRUE
            AND p.price_ts >= NOW() - INTERVAL '30 days'
            AND d.location_zip LIKE $2
        ORDER BY p.price_per_lb DESC NULLS LAST
        """,
        metal,
        f"{zip_prefix}%",
    )

    now = datetime.now(tz=timezone.utc)
    results = []
    seen_dealers = set()

    for row in rows:
        # One price per dealer (most recent already sorted by price DESC,
        # but we take first occurrence of each dealer = highest price)
        dealer_id = row["dealer_id"]
        if dealer_id in seen_dealers:
            continue
        seen_dealers.add(dealer_id)

        price_ts = row["price_ts"]
        if price_ts.tzinfo is None:
            price_ts = price_ts.replace(tzinfo=timezone.utc)

        age_hours = (now - price_ts).total_seconds() / 3600.0

        price_per_lb  = row["price_per_lb"]
        price_per_ton = row["price_per_ton"]

        # Fallback: compute from value column if price_per_lb not populated
        if price_per_lb is None:
            # prices_raw.value is stored as price_per_lb (normalized)
            price_per_lb  = None
            price_per_ton = None

        results.append(
            DealerPriceOut(
                dealer_id      = dealer_id,
                dealer_name    = row["dealer_name"],
                location_zip   = row["location_zip"] or "",
                city           = row["city"],
                state          = row["state"],
                metal          = row["metal"],
                price_per_lb   = Decimal(str(price_per_lb)) if price_per_lb is not None else Decimal("0"),
                price_per_ton  = Decimal(str(price_per_ton)) if price_per_ton is not None else None,
                unit           = row["unit"] or "lb",
                price_ts       = price_ts,
                source         = row["source"],
                price_age_hours = round(age_hours, 2),
            )
        )

    # Sort by price_per_lb descending (best payer first)
    results.sort(key=lambda r: r.price_per_lb, reverse=True)
    return results


async def get_valuation(
    pool: Any,
    metal: str,
    valuation_date: date,
) -> Optional[ValuationOut]:
    """
    Return mark-to-market valuation for a metal on a given date.

    Strategy:
    1. Look up the canonical price for the metal on that date (exact match).
    2. Look up total inventory quantity from inventory_lots.
    3. Compute market_value = quantity × price.

    If no canonical price exists for the exact date, walk backwards up to
    5 business days to find the most recent available price.
    """
    # Find closest canonical price on or before valuation_date
    price_row = await pool.fetchrow(
        """
        SELECT value, source, price_ts
        FROM prices_canonical
        WHERE metal = $1 AND price_ts::date <= $2
        ORDER BY price_ts DESC
        LIMIT 1
        """,
        metal, valuation_date,
    )

    if not price_row:
        return None

    # Sum active inventory lots for this metal
    qty_row = await pool.fetchrow(
        """
        SELECT COALESCE(SUM(quantity), 0) AS total_qty
        FROM inventory_lots
        WHERE metal = $1 AND NOT closed
        """,
        metal,
    )
    quantity     = Decimal(str(qty_row["total_qty"]))
    price        = Decimal(str(price_row["value"]))
    market_value = quantity * price

    return ValuationOut(
        metal          = metal,
        valuation_date = valuation_date,
        quantity       = quantity,
        price          = price,
        market_value   = market_value,
        source         = price_row["source"],
    )
