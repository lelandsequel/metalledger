"""
MetalLedger — Pricing Ingestor service entry point.

Runs on a configurable interval (INGEST_INTERVAL_SECONDS, default 300s).
On each tick:
  1. Fetch prices from metals_api and lbma adapters.
  2. Store raw prices in prices_raw.
  3. Run normalizer (outlier rejection + source priority).
  4. Promote valid prices to prices_canonical.
  5. Write audit log entry.

Uses APScheduler for scheduling; degrades gracefully to a simple loop if
APScheduler is not installed (shouldn't happen with requirements.txt).
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "packages"))

import asyncpg

from common.audit import write_audit_entry_async
from common.config import DATABASE_URL, INGEST_INTERVAL_SECONDS, ROLLING_MEDIAN_DAYS
from common.logging_util import get_logger
from common.types import PricePoint

from adapters import metals_api_adapter, lbma_adapter
from normalizer import normalize

log = get_logger("pricing_ingestor")


# ── DB helpers ────────────────────────────────────────────────────────────────

async def insert_raw_price(pool: asyncpg.Pool, p: PricePoint) -> int:
    """Insert a raw price and return the new row id."""
    row = await pool.fetchrow(
        """
        INSERT INTO prices_raw (source, metal, venue, price_ts, value, currency, source_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        p.source, p.metal, p.venue, p.price_ts,
        float(p.value), p.currency, p.source_id,
    )
    return row["id"] if row else -1


async def promote_to_canonical(pool: asyncpg.Pool, p: PricePoint, raw_id: int) -> None:
    """Insert into prices_canonical (upsert by metal+price_ts)."""
    await pool.execute(
        """
        INSERT INTO prices_canonical (metal, price_ts, value, currency, source, raw_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (metal, price_ts) DO UPDATE
            SET value  = EXCLUDED.value,
                source = EXCLUDED.source,
                raw_id = EXCLUDED.raw_id,
                promoted_at = NOW()
        """,
        p.metal, p.price_ts, float(p.value), p.currency, p.source, raw_id,
    )


async def get_historical_values(
    pool: asyncpg.Pool, metal: str, days: int = ROLLING_MEDIAN_DAYS
) -> list[float]:
    """Fetch recent canonical prices for outlier detection."""
    since = datetime.now(tz=timezone.utc) - timedelta(days=days)
    rows = await pool.fetch(
        """
        SELECT value FROM prices_canonical
        WHERE metal = $1 AND price_ts >= $2
        ORDER BY price_ts
        """,
        metal, since,
    )
    return [float(r["value"]) for r in rows]


# ── Core ingest tick ──────────────────────────────────────────────────────────

async def ingest_tick(pool: asyncpg.Pool) -> None:
    request_id = uuid.uuid4()
    log.info("Ingest tick started request_id=%s", request_id)

    # 1. Fetch from all adapters
    all_prices: list[PricePoint] = []
    for adapter_name, fetch_fn in [
        ("metals_api", metals_api_adapter.fetch_prices),
        ("lbma",       lbma_adapter.fetch_prices),
    ]:
        try:
            prices = await fetch_fn()
            all_prices.extend(prices)
            log.info("Adapter %s returned %d prices", adapter_name, len(prices))
        except Exception as exc:
            log.error("Adapter %s failed: %s", adapter_name, exc)

    if not all_prices:
        log.warning("No prices fetched this tick")
        return

    # 2. Build historical lookup for outlier detection
    metals = {p.metal for p in all_prices}
    historical: dict[str, list[float]] = {}
    for metal in metals:
        historical[metal] = await get_historical_values(pool, metal)

    # 3. Normalize
    result = normalize(all_prices, historical)

    # 4. Store raw + promote canonical
    promoted = 0
    for price in all_prices:
        raw_id = await insert_raw_price(pool, price)

        if any(p is price for p in result.accepted):
            if raw_id > 0:
                await promote_to_canonical(pool, price, raw_id)
                promoted += 1

    log.info(
        "Tick complete: %d raw stored, %d promoted, %d rejected",
        len(all_prices), promoted, len(result.rejected),
    )

    # 5. Audit log
    await write_audit_entry_async(
        pool,
        request_id=request_id,
        actor="pricing_ingestor",
        action="ingest_tick",
        payload={
            "fetched": len(all_prices),
            "accepted": len(result.accepted),
            "rejected": len(result.rejected),
            "promoted": promoted,
        },
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info(
        "Pricing ingestor starting — interval=%ds", INGEST_INTERVAL_SECONDS
    )

    # Retry loop for DB connection (container startup race)
    for attempt in range(10):
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            log.info("Connected to database")
            break
        except Exception as exc:
            log.warning("DB connection attempt %d failed: %s", attempt + 1, exc)
            await asyncio.sleep(3)
    else:
        log.error("Could not connect to database after 10 attempts. Exiting.")
        sys.exit(1)

    try:
        while True:
            try:
                await ingest_tick(pool)
            except Exception as exc:
                log.exception("Ingest tick error: %s", exc)
            await asyncio.sleep(INGEST_INTERVAL_SECONDS)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
