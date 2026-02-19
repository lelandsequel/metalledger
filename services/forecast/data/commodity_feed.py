"""
Free commodity price feed using yfinance.
Pulls futures data as proxies for scrap metal prices.

Mapping:
  - CU_BARE, CU_1, CU_2  → HG=F  (COMEX Copper futures, $/lb)
  - HMS1, HMS2, SHRED, CAST → HR=F (Hot Rolled Coil Steel futures, $/ton)
  - AL_CAST, AL_EXTRUSION  → ALI=F (Aluminum futures, $/lb)

Scrap spread adjustments (calibrated to typical dealer spreads):
  - Bare Bright Copper: futures * 0.97 (3% below COMEX)
  - Copper #1: futures * 0.91
  - Copper #2: futures * 0.82
  - HMS1: futures * 0.94
  - HMS2: futures * 0.87
  - Shredded Steel: futures * 0.91
  - Cast Iron: futures * 0.68
  - Cast Aluminum: futures * 0.52 (aluminum scrap is ~52% of primary)
  - Aluminum Extrusion: futures * 0.61
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

TICKER_MAP = {
    "CU_BARE": ("HG=F", 0.97),
    "CU_1":    ("HG=F", 0.91),
    "CU_2":    ("HG=F", 0.82),
    "HMS1":    ("HR=F", 0.94),
    "HMS2":    ("HR=F", 0.87),
    "SHRED":   ("HR=F", 0.91),
    "CAST":    ("HR=F", 0.68),
    "AL_CAST": ("ALI=F", 0.52),
    "AL_EXTRUSION": ("ALI=F", 0.61),
}


def fetch_historical(metal_slug: str, days: int = 730) -> Optional[pd.DataFrame]:
    """
    Fetch historical closing prices for a scrap metal slug.
    Returns DataFrame with columns: date, raw_price, scrap_price,
    ticker, spread_factor, metal_slug.

    Args:
        metal_slug: One of the TICKER_MAP keys (e.g. "CU_BARE", "HMS1").
        days:       How many calendar days of history to fetch.

    Returns:
        DataFrame or None if the slug is unknown or data unavailable.
    """
    if metal_slug not in TICKER_MAP:
        logger.warning(f"No ticker mapping for {metal_slug}")
        return None

    ticker_symbol, spread = TICKER_MAP[metal_slug]
    end = datetime.now()
    start = end - timedelta(days=days)

    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )

        if hist.empty:
            logger.warning(f"No data returned for {ticker_symbol}")
            return None

        df = pd.DataFrame({
            "date":          hist.index.date,
            "raw_price":     hist["Close"].values,
            "scrap_price":   hist["Close"].values * spread,
            "ticker":        ticker_symbol,
            "spread_factor": spread,
            "metal_slug":    metal_slug,
        })

        df = df.dropna().reset_index(drop=True)
        logger.info(f"Fetched {len(df)} rows for {metal_slug} ({ticker_symbol})")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch {ticker_symbol}: {e}")
        return None


def fetch_all_metals(days: int = 730) -> dict[str, pd.DataFrame]:
    """Fetch historical data for all mapped metals.

    Returns a dict mapping metal_slug → DataFrame.
    Metals for which data is unavailable are omitted.
    """
    results = {}
    for slug in TICKER_MAP:
        df = fetch_historical(slug, days)
        if df is not None:
            results[slug] = df
    return results


def get_latest_price(metal_slug: str) -> Optional[float]:
    """Get the most recent scrap price estimate for a metal.

    Uses the last closing price (may be up to 15 minutes delayed for futures).

    Returns:
        Most recent scrap_price as float, or None if unavailable.
    """
    df = fetch_historical(metal_slug, days=5)
    if df is None or df.empty:
        return None
    return float(df["scrap_price"].iloc[-1])


def get_latest_price_with_meta(metal_slug: str) -> Optional[dict]:
    """Get the most recent price + metadata for the /forecast/live endpoint.

    Returns:
        Dict with keys: metal_slug, scrap_price, raw_futures_price,
        ticker, spread_factor, fetched_at — or None if unavailable.
    """
    df = fetch_historical(metal_slug, days=5)
    if df is None or df.empty:
        return None

    last = df.iloc[-1]
    return {
        "metal_slug":        str(last["metal_slug"]),
        "scrap_price":       float(last["scrap_price"]),
        "raw_futures_price": float(last["raw_price"]),
        "ticker":            str(last["ticker"]),
        "spread_factor":     float(last["spread_factor"]),
        "fetched_at":        datetime.now().isoformat() + "Z",
    }
