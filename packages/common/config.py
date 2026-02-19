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
    "metals_api": 1,
    "lbma":       2,
    "seed":       99,
}

# Which sources are canonical for each metal (first matching wins)
METAL_SOURCE_PREFERENCE: Dict[str, List[str]] = {
    "XAU": ["metals_api", "lbma"],
    "XAG": ["metals_api", "lbma"],
    "CU":  ["metals_api"],
}

# ── Outlier rejection ────────────────────────────────────────────────────────
OUTLIER_MULTIPLIER: float = float(os.getenv("OUTLIER_MULTIPLIER", "3.0"))
ROLLING_MEDIAN_DAYS: int  = int(os.getenv("ROLLING_MEDIAN_DAYS", "7"))

# ── Egress allowlist ─────────────────────────────────────────────────────────
EGRESS_ALLOWLIST: List[str] = [
    "metals-api.com",
    "api.lbma.org.uk",
]

# ── Service URLs (used by orchestrator to call sibling services) ──────────────
LEDGER_BASE_URL:   str = os.getenv("LEDGER_BASE_URL",   "http://ledger:8000")
FORECAST_BASE_URL: str = os.getenv("FORECAST_BASE_URL", "http://forecast:8000")
