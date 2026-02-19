"""
MetalLedger — Shared Pydantic models used across all services.
"""

from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


# ── Enums ────────────────────────────────────────────────────────────────────

class Metal(str, enum.Enum):
    XAU = "XAU"
    XAG = "XAG"
    CU  = "CU"


class AccountType(str, enum.Enum):
    ASSET     = "ASSET"
    LIABILITY = "LIABILITY"
    EQUITY    = "EQUITY"
    REVENUE   = "REVENUE"
    EXPENSE   = "EXPENSE"


class JournalStatus(str, enum.Enum):
    DRAFT  = "DRAFT"
    POSTED = "POSTED"
    VOID   = "VOID"


class ForecastModel(str, enum.Enum):
    NAIVE          = "naive"
    ARIMA          = "arima"
    GRADIENT_BOOST = "gradient_boost"


# ── Pricing ───────────────────────────────────────────────────────────────────

class PricePoint(BaseModel):
    source:   str
    metal:    str
    venue:    str     = ""
    price_ts: datetime
    value:    Decimal
    currency: str     = "USD"
    source_id: Optional[str] = None


class CanonicalPrice(BaseModel):
    metal:       str
    price_ts:    datetime
    value:       Decimal
    currency:    str = "USD"
    source:      str
    raw_id:      Optional[int] = None
    promoted_at: Optional[datetime] = None


# ── Ledger ────────────────────────────────────────────────────────────────────

class AccountOut(BaseModel):
    id:       int
    code:     str
    name:     str
    type:     AccountType
    currency: str
    active:   bool

    model_config = {"from_attributes": True}


class JournalLineIn(BaseModel):
    account_id: int
    debit:      Decimal = Decimal("0")
    credit:     Decimal = Decimal("0")
    memo:       Optional[str] = None

    @model_validator(mode="after")
    def check_exclusive(self) -> "JournalLineIn":
        if self.debit > 0 and self.credit > 0:
            raise ValueError("A journal line cannot have both debit and credit > 0")
        return self


class JournalEntryIn(BaseModel):
    entry_date: date
    memo:       Optional[str] = None
    lines:      list[JournalLineIn] = Field(..., min_length=2)

    @model_validator(mode="after")
    def check_balance(self) -> "JournalEntryIn":
        total_debit  = sum(ln.debit  for ln in self.lines)
        total_credit = sum(ln.credit for ln in self.lines)
        if total_debit != total_credit:
            raise ValueError(
                f"Journal entry is unbalanced: debit={total_debit} credit={total_credit}"
            )
        return self


class JournalEntryOut(BaseModel):
    id:         int
    entry_date: date
    memo:       Optional[str]
    created_by: str
    status:     JournalStatus
    created_at: datetime

    model_config = {"from_attributes": True}


class ValuationOut(BaseModel):
    metal:        str
    valuation_date: date
    quantity:     Decimal
    price:        Decimal
    market_value: Decimal
    source:       str


# ── Forecast ──────────────────────────────────────────────────────────────────

class ForecastOut(BaseModel):
    id:      int
    model:   str
    metal:   str
    horizon: int
    run_at:  datetime
    p10:     Optional[Decimal]
    p50:     Optional[Decimal]
    p90:     Optional[Decimal]

    model_config = {"from_attributes": True}


class BacktestOut(BaseModel):
    id:           int
    model:        str
    metal:        str
    horizon:      int
    window_start: date
    window_end:   date
    mape:         Optional[Decimal]
    rmse:         Optional[Decimal]
    run_at:       datetime

    model_config = {"from_attributes": True}


# ── Audit ─────────────────────────────────────────────────────────────────────

class AuditEntry(BaseModel):
    request_id:   UUID
    actor:        str
    action:       str
    payload_hash: str
    created_at:   Optional[datetime] = None
