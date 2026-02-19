# MetalLedger — Scrap Metal Price Aggregator & Ledger

**MetalLedger** is a Python/FastAPI monorepo for scrap metal price aggregation,
dealer price comparison, ferrous + non-ferrous price forecasting, and a double-entry
accounting ledger — built for **scrap metal resellers**.

**Who is this for?**
- Scrap collectors who want to know which local dealer is paying the best price *today*
- Scrap yards who want to publish their prices and attract more sellers
- Commodity traders tracking ferrous and non-ferrous price trends
- Recycling operations that need auditable price records and inventory valuation

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     MetalLedger v0 — Scrap Metal                        │
│                                                                          │
│  ┌─────────────────────┐  ┌──────────────────┐   ┌────────────────────┐ │
│  │  Pricing Ingestor   │  │  Ledger Service  │   │  Forecast Service  │ │
│  │                     │  │                  │   │                    │ │
│  │  dealer_manual ───► │  │  POST            │   │  POST /forecast    │ │
│  │  iscrap_adapter     │  │  /prices/dealer  │   │  /run              │ │
│  │  scrap_register     │  │  GET /prices     │   │  GET /forecast     │ │
│  │  recycling_today    │  │  /compare        │   │  /latest           │ │
│  │  normalizer         │  │  POST            │   │                    │ │
│  │  outlier reject     │  │  /journal_entries│   │  naive model       │ │
│  │  → prices_raw       │  │  (HUMAN only)    │   │  ARIMA model       │ │
│  │  → prices_canonical │  │  GET /valuations │   │  GBM model         │ │
│  └──────────┬──────────┘  └────────┬─────────┘   └──────────┬─────────┘ │
│             │                      │                         │            │
│             └──────────────────────┴─────────────────────────┘            │
│                                    │                                      │
│                  ┌─────────────────▼────────────────────────────────┐    │
│                  │                PostgreSQL 15                      │    │
│                  │  dealers │ prices_raw │ prices_canonical          │    │
│                  │  accounts │ journal_entries │ journal_lines       │    │
│                  │  forecasts │ audit_log (append-only)              │    │
│                  └──────────────────────────────────────────────────┘    │
│                                        ▲                                 │
│  ┌─────────────────────────────────────┴────────────────────────────┐    │
│  │             Orchestrator Agent (GUARDRAILED)                     │    │
│  │  policy.py   — 4 guardrails enforced before every action         │    │
│  │  scheduler.py— cron-triggered forecast runs                      │    │
│  │  reporter.py — markdown reports → /reports/ + audit log          │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Supported Metals

### Ferrous

| Slug    | Description              | Typical Price (USD) |
|---------|--------------------------|---------------------|
| `HMS1`  | Heavy Melting Steel #1   | $180–220/ton        |
| `HMS2`  | Heavy Melting Steel #2   | $160–195/ton        |
| `SHRED` | Shredded Steel           | $170–210/ton        |
| `CAST`  | Cast Iron                | $120–160/ton        |

### Non-Ferrous

| Slug           | Description              | Typical Price (USD) |
|----------------|--------------------------|---------------------|
| `CU_BARE`      | Bare Bright Copper       | $3.50–4.20/lb       |
| `CU_1`         | #1 Copper                | $3.20–3.90/lb       |
| `CU_2`         | #2 Copper                | $2.80–3.40/lb       |
| `AL_CAST`      | Cast Aluminum            | $0.35–0.55/lb       |
| `AL_EXTRUSION` | Aluminum Extrusion       | $0.45–0.65/lb       |
| `BRASS`        | Yellow Brass             | $1.50–1.90/lb       |
| `SS_304`       | Stainless Steel 304      | $0.45–0.75/lb       |
| `LEAD`         | Lead                     | $0.35–0.55/lb       |
| `ZORBA`        | Zorba (mixed non-ferrous)| $0.55–0.80/lb       |

All prices stored as USD/lb (ferrous prices normalized from $/ton by dividing by 2000).

---

## Data Sources

MetalLedger aggregates from 4 adapters with different reliability levels:

### 1. `dealer_manual_adapter.py` — Manual Dealer Price Entry ✅ (Primary / v0)
The **primary real data source** for MetalLedger v0. Scrap dealers POST their current
buy prices directly via the API. No scraping, no external dependencies.

```
POST /prices/dealer
{
  "dealer_id":    "dealer_001",
  "metal_slug":   "CU_BARE",
  "price_per_lb": 3.85,
  "unit":         "lb",
  "location_zip": "77001",
  "source_notes": "Bare bright only, 16ga+"
}
```

Why this works: dealers are motivated to post competitive prices because it drives
scrap sellers to their yard. Network effects build the data flywheel.

### 2. `iscrap_adapter.py` — iScrap App 🔶 (Stub)
**iScrap App does not have a public API.** The adapter scrapes their public price
listings. For production, consider iScrap's dealer portal or a licensed data feed.
See https://www.iscrapapp.com for more information.

- Status: Stub — returns synthetic data
- Enable: Set `ISCRAP_ZIP` env var; add scraping logic and iScrap ToS consent

### 3. `scrap_register_adapter.py` — ScrapRegister.com 🔶 (Stub)
ScrapRegister publishes regional average prices. Commercial use requires licensing.
Contact https://www.scrapregister.com for data partnership arrangements.

- Status: Stub — returns synthetic regional averages
- Regions: South, Midwest, Northeast, West

### 4. `recycling_today_adapter.py` — RecyclingToday / Fastmarkets 🔶 (Stub)
**Fastmarkets requires subscription for real-time data.** Their commodity benchmark
prices (HMS 1&2, shredded, zorba) are industry-standard contract reference prices.
See https://www.fastmarkets.com for licensing.

- Status: Stub — returns synthetic benchmarks
- Enable: Set `FASTMARKETS_API_KEY` env var

---

## Quick Start

### Prerequisites
- Docker & Docker Compose v2
- No external API keys required for v0 (all adapters fall back to synthetic data)

### Run

```bash
cd metalledger

# Copy environment file
cp .env.example .env

# Build and start all services
docker-compose up --build

# Ledger API:       http://localhost:8001/docs
# Forecast API:     http://localhost:8002/docs
# Orchestrator API: http://localhost:8003/docs
```

### Submit a Dealer Price (the core workflow)

```bash
# A dealer posts their current CU_BARE buy price
curl -X POST http://localhost:8001/prices/dealer \
  -H "Content-Type: application/json" \
  -d '{
    "dealer_id":    "dealer_001",
    "metal_slug":   "CU_BARE",
    "price_per_lb": 3.85,
    "unit":         "lb",
    "location_zip": "77001",
    "source_notes": "Bare bright, 16ga+, no romex"
  }'
```

### Find the Best Price Near You

```bash
# Scrap seller looks for best CU_BARE price near ZIP 77001
curl "http://localhost:8001/prices/compare?metal=CU_BARE&zip=77001&radius_miles=50"
```

Response (sorted best-paying dealer first):
```json
[
  {
    "dealer_id": "dealer_003",
    "dealer_name": "Lone Star Recycling",
    "location_zip": "77003",
    "metal": "CU_BARE",
    "price_per_lb": 3.92,
    "price_per_ton": 7840.00,
    "price_age_hours": 2.3
  },
  {
    "dealer_id": "dealer_001",
    "dealer_name": "Houston Metals Inc",
    "location_zip": "77001",
    "metal": "CU_BARE",
    "price_per_lb": 3.85,
    "price_age_hours": 1.8
  }
]
```

---

## Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest services/*/tests/ -v

# Run specific test suites
pytest services/pricing_ingestor/tests/test_outlier_rejection.py -v
pytest services/ledger/tests/test_price_comparison.py -v
pytest services/forecast/tests/test_forecast_run_storage.py -v
```

---

## API Reference

### Ledger Service (port 8001)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET`  | `/prices/compare?metal=CU_BARE&zip=77001&radius_miles=50` | — | **Best-paying dealers near ZIP (core feature)** |
| `POST` | `/prices/dealer` | — | Submit dealer buy price |
| `POST` | `/journal_entries` | `X-API-Role: HUMAN` | Create balanced journal entry |
| `GET`  | `/valuations?metal=CU_BARE&date=2024-01-15` | — | Mark-to-market valuation |
| `GET`  | `/accounts` | — | List chart of accounts |
| `GET`  | `/health` | — | Service health check |

**Example — Best Prices for HMS1:**
```bash
curl "http://localhost:8001/prices/compare?metal=HMS1&zip=77001&radius_miles=50"
```

### Forecast Service (port 8002)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/forecast/run` | — | Run all models for all scrap metals |
| `GET`  | `/forecast/latest?metal=CU_BARE` | — | Latest P10/P50/P90 forecasts |
| `GET`  | `/health` | — | Service health check |

### Orchestrator Agent (port 8003)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/trigger` | — | Manually trigger orchestrator tick |
| `GET`  | `/health` | — | Service health check |

---

## Guardrails

MetalLedger enforces **4 non-negotiable guardrails**:

### 1. Agent Cannot Create/Modify Journal Entries
The orchestrator agent is permanently blocked from calling `POST /journal_entries`.
`policy.py:check_action()` raises `PolicyViolation` if actor == "agent" and the action
is any journal write. The ledger endpoint also enforces `X-API-Role: HUMAN`.

### 2. Source Config Changes Require HUMAN Approval
Any mutation of `source_configs` (which data feeds are active) requires an active,
non-expired, non-revoked approval record in the `approvals` table signed by a HUMAN.

### 3. All External Calls Through egress.py
Every adapter uses `common/egress.py:egress_get()`. Functions check URLs against
the allowlist: `["iscrapapp.com", "scrapregister.com", "api.fastmarkets.com", ...]`.
Any unlisted domain raises `EgressViolation` immediately.

### 4. Everything Writes to audit_log
Every API call writes an entry to `audit_log` with `request_id`, `actor`, `action`,
and `payload_hash` (SHA256). The table has database-level triggers denying UPDATE/DELETE —
making it append-only and tamper-resistant.

---

## Forecast Models

Forecasts run for all 13 scrap metal slugs across horizons **1d, 5d, 20d**:

| Model | Description | Min Observations |
|-------|-------------|-----------------|
| `naive` | Yesterday's price = tomorrow's price. P10/P90 from historical change distribution. | 1 |
| `arima` | ARIMA(5,1,0) via statsmodels. | 15 |
| `gradient_boost` | Quantile GBM with lag features (P10/P50/P90). Uses LightGBM if available. | 12 |

---

## Project Structure

```
metalledger/
├── services/
│   ├── pricing_ingestor/          # Price fetching + normalization
│   │   └── adapters/
│   │       ├── dealer_manual_adapter.py   # ✅ Primary: dealer-posted prices
│   │       ├── iscrap_adapter.py          # 🔶 Stub: iScrap App scraper
│   │       ├── scrap_register_adapter.py  # 🔶 Stub: ScrapRegister.com
│   │       └── recycling_today_adapter.py # 🔶 Stub: Fastmarkets benchmarks
│   ├── ledger/                    # Double-entry accounting + price compare API
│   ├── forecast/                  # ML forecast models + API
│   └── orchestrator_agent/        # Guardrailed scheduler + reporter
├── packages/
│   └── common/                    # Shared: config, egress, audit, types
├── db/
│   ├── migrations/001_schema.sql  # dealers table + updated prices_raw schema
│   └── seed.sql                   # 50+ rows of synthetic dealer prices (3 dealers, 13 metals)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ⚠️ Licensing Notes

Users are **solely responsible** for compliance with data licensing terms.

| Data Source | License Requirements |
|-------------|---------------------|
| **iScrap App** | No public API. Scraping their price listings may violate Terms of Service. For production use, contact iScrap directly for dealer portal or data partnership access. See https://www.iscrapapp.com |
| **ScrapRegister.com** | Regional price data published for informational use. Commercial extraction or redistribution requires a licensing arrangement. Contact https://www.scrapregister.com |
| **Fastmarkets** (RecyclingToday) | Commodity benchmark prices (HMS 1&2, shredded, zorba) require a paid subscription. Commercial use requires a signed license agreement. See https://www.fastmarkets.com/about-us/licensing |
| **Dealer Manual Data** | Prices submitted directly by dealers. Dealers are responsible for accuracy and any applicable pricing laws (price fixing, antitrust). |

> MetalLedger ships with **synthetic seed data** for development. This synthetic data
> is not real market data and carries no licensing restrictions.

---

## Development Notes

- All services use `asyncpg` for async PostgreSQL (no ORM).
- Pydantic v2 models enforce data integrity at the boundary.
- `common/egress.py` is the single point of truth for external HTTP.
- ZIP-based radius in `/prices/compare` uses 3-digit prefix matching for v0.
  Upgrade to PostGIS `ST_DWithin()` for production geo-distance queries.
- Reports are plain markdown files written to `./reports/`.

---

*MetalLedger v0 — Scrap Metal Price Aggregator. Built with FastAPI, asyncpg, statsmodels.*
