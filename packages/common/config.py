"""
MetalLedger — Environment-based configuration and source priority rules.

All settings are read from environment variables (12-factor style).
No secrets are hard-coded; defaults are safe for local dev only.
"""

import os
from typing import Dict, List

# ── Database ────────────────────────────────────────────────────────────────
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql://metalledger:secret@localhost:5432/metalledger",
)

# ── API Keys ─────────────────────────────────────────────────────────────────
METALS_API_KEY: str = os.getenv("METALS_API_KEY", "")

# ── Ingestor ─────────────────────────────────────────────────────────────────
INGEST_INTERVAL_SECONDS: int = int(os.getenv("INGEST_INTERVAL_SECONDS", "300"))

# ── Orchestrator schedule (cron expression) ──────────────────────────────────
ORCHESTRATOR_CRON: str = os.getenv("ORCHESTRATOR_CRON", "0 6 * * *")   # daily 06:00 UTC
REPORTS_DIR: str = os.getenv("REPORTS_DIR", "/app/reports")

# ── Source priority rules ────────────────────────────────────────────────────
# Lower number = higher priority.  Used by normalizer to resolve conflicts.
SOURCE_PRIORITY: Dict[str, int] = {
    "dealer_manual":   1,    # Primary: real dealer submissions via API
    "iscrap":          2,    # iScrap App scraper stub
    "scrap_register":  3,    # ScrapRegister.com regional averages
    "recycling_today": 4,    # Fastmarkets/RecyclingToday commodity benchmarks
    "seed":            99,   # Synthetic seed data (dev only)
}

# Which sources are canonical for each metal (first matching wins)
# Ferrous metals
METAL_SOURCE_PREFERENCE: Dict[str, List[str]] = {
    "HMS1":         ["dealer_manual", "iscrap", "recycling_today"],
    "HMS2":         ["dealer_manual", "iscrap", "recycling_today"],
    "SHRED":        ["dealer_manual", "iscrap", "recycling_today"],
    "CAST":         ["dealer_manual", "iscrap", "recycling_today"],
    # Non-ferrous metals
    "CU_BARE":      ["dealer_manual", "iscrap", "scrap_register"],
    "CU_1":         ["dealer_manual", "iscrap", "scrap_register"],
    "CU_2":         ["dealer_manual", "iscrap", "scrap_register"],
    "AL_CAST":      ["dealer_manual", "iscrap", "scrap_register"],
    "AL_EXTRUSION": ["dealer_manual", "iscrap", "scrap_register"],
    "BRASS":        ["dealer_manual", "iscrap", "scrap_register"],
    "SS_304":       ["dealer_manual", "iscrap", "scrap_register"],
    "LEAD":         ["dealer_manual", "iscrap", "scrap_register"],
    "ZORBA":        ["dealer_manual", "iscrap", "recycling_today"],
}

# ── Outlier rejection ────────────────────────────────────────────────────────
OUTLIER_MULTIPLIER: float = float(os.getenv("OUTLIER_MULTIPLIER", "3.0"))
ROLLING_MEDIAN_DAYS: int  = int(os.getenv("ROLLING_MEDIAN_DAYS", "7"))

# ── Egress allowlist ─────────────────────────────────────────────────────────
# NOTE: iscrapapp.com and scrapregister.com are listed here but their adapters
# are stubs. Enable only after verifying scraping is permitted by each site's ToS.
EGRESS_ALLOWLIST: List[str] = [
    # Scrap price sources (stub adapters — enable carefully)
    "iscrapapp.com",          # iScrap App — no public API; scraping may violate ToS
    "scrapregister.com",      # ScrapRegister — scraping requires licensing
    "api.fastmarkets.com",    # Fastmarkets — requires paid subscription
    "www.recyclingtoday.com", # RecyclingToday editorial (not a price API)
]

# ── Service URLs (used by orchestrator to call sibling services) ──────────────
LEDGER_BASE_URL:   str = os.getenv("LEDGER_BASE_URL",   "http://ledger:8000")
FORECAST_BASE_URL: str = os.getenv("FORECAST_BASE_URL", "http://forecast:8000")
