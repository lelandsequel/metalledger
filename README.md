# MetalLedger v0

**MetalLedger** is a Python/FastAPI monorepo for live precious-metals pricing ingestion,
double-entry accounting, agentic price forecasting, and a guardrailed orchestrator agent.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          MetalLedger v0                              │
│                                                                      │
│  ┌──────────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │  Pricing         │   │  Ledger          │   │  Forecast      │  │
│  │  Ingestor        │   │  Service         │   │  Service       │  │
│  │                  │   │                  │   │                │  │
│  │  metals_api  ──► │   │  POST            │   │  POST          │  │
│  │  lbma_adapter    │   │  /journal_entries│   │  /forecast/run │  │
│  │  normalizer      │   │  (HUMAN only)    │   │  GET /forecast │  │
│  │  outlier reject  │   │  GET /valuations │   │  /latest       │  │
│  │  → prices_raw    │   │  GET /accounts   │   │                │  │
│  │  → prices_canon  │   │                  │   │  naive model   │  │
│  └─────────┬────────┘   └────────┬─────────┘   │  ARIMA model   │  │
│            │                     │             │  GBM model     │  │
│            │                     │             │  backtester    │  │
│            └──────────┬──────────┘             └───────┬────────┘  │
│                       │                                │            │
│                  ┌────▼────────────────────────────────▼────────┐  │
│                  │              PostgreSQL 15                    │  │
│                  │  prices_raw │ prices_canonical │ accounts     │  │
│                  │  journal_entries │ journal_lines │ forecasts  │  │
│                  │  audit_log (append-only) │ policy_events      │  │
│                  └──────────────────────────────────────────────┘  │
│                                        ▲                            │
│  ┌─────────────────────────────────────┴──────────────────────────┐ │
│  │            Orchestrator Agent (GUARDRAILED)                    │ │
│  │                                                                │ │
│  │  policy.py   — 4 guardrails enforced before every action       │ │
│  │  scheduler.py— cron-triggered forecast runs                    │ │
│  │  reporter.py — markdown reports → /reports/ + audit log        │ │
│  │  egress.py   — allowlist-enforced external HTTP                │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites

- Docker & Docker Compose v2
- (Optional) `METALS_API_KEY` for live prices

### Run

```bash
# Clone / navigate to metalledger/
cd metalledger

# Copy and edit environment variables
cp .env.example .env
# Set METALS_API_KEY=<your_key> if you have one

# Build and start all services
docker-compose up --build

# Ledger API:       http://localhost:8001/docs
# Forecast API:     http://localhost:8002/docs
# Orchestrator API: http://localhost:8003/docs
```

### API Keys (.env file)

Create a `.env` file in the `metalledger/` root:

```env
# Metals-API.com key (free tier available at https://metals-api.com)
METALS_API_KEY=your_key_here

# Optional LBMA key (requires institutional access)
LBMA_API_KEY=

# Orchestrator cron schedule (default: daily 06:00 UTC)
ORCHESTRATOR_CRON=0 6 * * *

# Log level
LOG_LEVEL=INFO
```

> **Note:** If `METALS_API_KEY` is empty, the ingestor returns realistic synthetic data
> so all downstream services work correctly in development.

---

## Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest services/*/tests/ -v

# Run a specific test file
pytest services/pricing_ingestor/tests/test_outlier_rejection.py -v
```

---

## Guardrails

MetalLedger enforces **4 non-negotiable guardrails**:

### 1. Agent Cannot Create/Modify Journal Entries

The orchestrator agent is permanently blocked from calling `POST /journal_entries`
(or any journal mutation endpoint). `policy.py:check_action()` raises `PolicyViolation`
if actor == "agent" and the action is any journal write. The ledger endpoint itself
also enforces `X-API-Role: HUMAN` header and returns `403` otherwise.

### 2. Source Config Changes Require HUMAN Approval

Any mutation of the `source_configs` table (which controls which data feeds are active)
requires an active, non-expired, non-revoked approval record in the `approvals` table
signed by a `HUMAN` actor. Without this record, `policy.py:require_approval()` raises
`PolicyViolation`.

### 3. All External Calls Through egress.py

Every adapter and service that makes HTTP requests to external domains **must** use
`common/egress.py:egress_get()` or `egress_post()`. These functions check the URL
against the **allowlist**: `["metals-api.com", "api.lbma.org.uk"]`. Any call to an
unlisted domain raises `EgressViolation` immediately, before the request is sent.

### 4. Everything Writes to audit_log

Every API call and every agent action writes an entry to the `audit_log` table with:
- `request_id`: UUID v4 (correlates all sub-actions of one request)
- `actor`: role string ("HUMAN", "agent", API key role)
- `action`: short verb/noun (e.g. "POST /journal_entries")
- `payload_hash`: SHA256 of the JSON-serialised payload

The `audit_log` table has database-level triggers that **deny all UPDATE and DELETE**
operations, making it append-only and tamper-resistant.

---

## API Reference

### Ledger Service (port 8001)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/journal_entries` | `X-API-Role: HUMAN` | Create balanced journal entry |
| `GET`  | `/valuations?metal=XAU&date=2024-01-15` | — | Mark-to-market valuation |
| `GET`  | `/accounts` | — | List chart of accounts |
| `GET`  | `/health` | — | Service health check |

**Example — Create Journal Entry:**
```bash
curl -X POST http://localhost:8001/journal_entries \
  -H "X-API-Role: HUMAN" \
  -H "Content-Type: application/json" \
  -d '{
    "entry_date": "2024-01-15",
    "memo": "Gold purchase",
    "lines": [
      {"account_id": 2, "debit": 204150.00},
      {"account_id": 1, "credit": 204150.00}
    ]
  }'
```

### Forecast Service (port 8002)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/forecast/run` | — | Run all models for all metals |
| `GET`  | `/forecast/latest?metal=XAU` | — | Latest P10/P50/P90 forecasts |
| `GET`  | `/health` | — | Service health check |

### Orchestrator Agent (port 8003)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/trigger` | — | Manually trigger orchestrator tick |
| `GET`  | `/health` | — | Service health check |

---

## Forecast Models

| Model | Description | Min Observations |
|-------|-------------|-----------------|
| `naive` | Yesterday's price = tomorrow's price. P10/P90 from historical change distribution. | 1 |
| `arima` | ARIMA(5,1,0) via statsmodels. auto_arima if pmdarima installed. | 15 |
| `gradient_boost` | Quantile GBM with lag features (P10/P50/P90). Uses LightGBM if available. | 12 |

Forecasts are generated for horizons **1d, 5d, 20d** (≈1 day, 1 week, 1 month).

---

## Project Structure

```
metalledger/
├── services/
│   ├── pricing_ingestor/      # Price fetching + normalization
│   ├── ledger/                # Double-entry accounting API
│   ├── forecast/              # ML forecast models + API
│   └── orchestrator_agent/    # Guardrailed scheduler + reporter
├── packages/
│   └── common/                # Shared: config, egress, audit, types
├── db/
│   ├── migrations/001_schema.sql
│   └── seed.sql               # Synthetic XAU/XAG/CU prices (32 rows each)
├── reports/                   # Generated markdown reports
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ⚠️ Licensing Notes

Users are **solely responsible** for compliance with data licensing terms.

| Data Source | License Requirements |
|-------------|---------------------|
| **Metals-API.com** | Free dev tier (limited calls/month). Paid plans for production. See https://metals-api.com/pricing |
| **LBMA** (London Bullion Market Association) | Official AM/PM fix data requires **institutional access** and a signed data license. Personal/research use may be permitted; commercial redistribution is **prohibited** without a contract. Contact https://www.lbma.org.uk |
| **LME** (London Metal Exchange) | Market data (including copper) requires a paid data license from LME. See https://www.lme.com/en/market-data |
| **CME Group** | Precious metals futures data requires a CME data subscription. See https://www.cmegroup.com/market-data.html |

> MetalLedger ships with **synthetic seed data** for development. This synthetic data
> is not real market data and carries no licensing restrictions.
> When you connect a live data feed, ensure you have the appropriate license.

---

## Development Notes

- All services use `asyncpg` for async PostgreSQL access (no ORM).
- Pydantic v2 models enforce data integrity at the boundary.
- The `common/egress.py` module is the **single point of truth** for external HTTP.
- Reports are plain markdown files written to `./reports/`.
- The DB schema runs on startup via Docker's `initdb.d` mechanism.

---

*MetalLedger v0 — Built with FastAPI, asyncpg, statsmodels, scikit-learn*
