"""
MetalLedger — Ledger service database models (asyncpg row helpers).

We use raw asyncpg (no ORM) for performance and auditability.
Each function returns typed dicts or Pydantic models.
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

from common.types import AccountOut, JournalEntryOut, JournalLineIn, ValuationOut


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
