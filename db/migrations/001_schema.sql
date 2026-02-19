-- MetalLedger v0 — Full Database Schema
-- PostgreSQL 15+
-- Updated: Scrap metal pivot — dealer/location model, ferrous+non-ferrous catalog

-- ============================================================
-- DEALERS
-- ============================================================

CREATE TABLE IF NOT EXISTS dealers (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT        NOT NULL,
    location_zip TEXT,
    city         TEXT,
    state        TEXT,
    phone        TEXT,
    website      TEXT,
    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dealers_zip ON dealers (location_zip);

-- ============================================================
-- PRICING
-- ============================================================

CREATE TABLE IF NOT EXISTS prices_raw (
    id            SERIAL PRIMARY KEY,
    source        TEXT        NOT NULL,           -- e.g. 'dealer_manual', 'iscrap', 'scrap_register'
    metal         TEXT        NOT NULL,           -- e.g. 'CU_BARE', 'HMS1', 'SHRED'
    venue         TEXT        NOT NULL DEFAULT '', -- e.g. 'DEALER:dealer_001:77001', 'REGIONAL_SOUTH'
    price_ts      TIMESTAMPTZ NOT NULL,           -- timestamp of the price observation
    value         NUMERIC(18,6) NOT NULL,          -- price per lb (normalized)
    currency      TEXT        NOT NULL DEFAULT 'USD',
    source_id     TEXT,                           -- upstream provider's own ID
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Dealer/location fields (populated for dealer_manual source)
    dealer_id     UUID        REFERENCES dealers(id),
    location_zip  TEXT,
    price_per_lb  NUMERIC(10,4),                 -- price per pound
    price_per_ton NUMERIC(10,2),                 -- price per short ton (2000 lb)
    unit          TEXT        NOT NULL DEFAULT 'lb'  -- 'lb' or 'ton'
);

CREATE INDEX IF NOT EXISTS idx_prices_raw_metal_ts ON prices_raw (metal, price_ts DESC);

CREATE TABLE IF NOT EXISTS prices_canonical (
    id          SERIAL PRIMARY KEY,
    metal       TEXT        NOT NULL,
    price_ts    TIMESTAMPTZ NOT NULL,
    value       NUMERIC(18,6) NOT NULL,
    currency    TEXT        NOT NULL DEFAULT 'USD',
    source      TEXT        NOT NULL,
    raw_id      INTEGER     REFERENCES prices_raw(id),
    promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_canonical_metal_ts ON prices_canonical (metal, price_ts);
CREATE INDEX IF NOT EXISTS idx_canonical_metal_ts ON prices_canonical (metal, price_ts DESC);

CREATE TABLE IF NOT EXISTS source_configs (
    id          SERIAL PRIMARY KEY,
    config_key  TEXT        NOT NULL UNIQUE,
    config_val  JSONB       NOT NULL,
    approved_by TEXT,                           -- must reference approvals.id before mutation
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- LEDGER
-- ============================================================

CREATE TABLE IF NOT EXISTS accounts (
    id          SERIAL PRIMARY KEY,
    code        TEXT        NOT NULL UNIQUE,    -- e.g. '1100'
    name        TEXT        NOT NULL,
    type        TEXT        NOT NULL,           -- ASSET | LIABILITY | EQUITY | REVENUE | EXPENSE
    currency    TEXT        NOT NULL DEFAULT 'USD',
    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS journal_entries (
    id          SERIAL PRIMARY KEY,
    entry_date  DATE        NOT NULL,
    memo        TEXT,
    created_by  TEXT        NOT NULL,           -- role or user name; must be 'HUMAN'
    status      TEXT        NOT NULL DEFAULT 'POSTED',  -- DRAFT | POSTED | VOID
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS journal_lines (
    id          SERIAL PRIMARY KEY,
    entry_id    INTEGER     NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
    account_id  INTEGER     NOT NULL REFERENCES accounts(id),
    debit       NUMERIC(18,6) NOT NULL DEFAULT 0,
    credit      NUMERIC(18,6) NOT NULL DEFAULT 0,
    memo        TEXT,
    CONSTRAINT chk_debit_credit CHECK (debit >= 0 AND credit >= 0),
    CONSTRAINT chk_not_both CHECK (NOT (debit > 0 AND credit > 0))
);

CREATE INDEX IF NOT EXISTS idx_journal_lines_entry ON journal_lines (entry_id);

-- Enforce balance: sum(debit) == sum(credit) per entry_id
-- This is enforced at the application layer (FastAPI) for flexibility,
-- but can also be enforced via a deferred constraint trigger.

CREATE OR REPLACE FUNCTION check_journal_balance()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_debit  NUMERIC;
    v_credit NUMERIC;
BEGIN
    SELECT COALESCE(SUM(debit),0), COALESCE(SUM(credit),0)
    INTO v_debit, v_credit
    FROM journal_lines
    WHERE entry_id = NEW.entry_id;

    -- Only enforce once the entry is POSTED (not DRAFT)
    IF (SELECT status FROM journal_entries WHERE id = NEW.entry_id) = 'POSTED' THEN
        IF v_debit <> v_credit THEN
            RAISE EXCEPTION 'Journal entry % is unbalanced: debit=% credit=%',
                NEW.entry_id, v_debit, v_credit;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS inventory_lots (
    id          SERIAL PRIMARY KEY,
    metal       TEXT        NOT NULL,
    quantity    NUMERIC(18,6) NOT NULL,
    unit        TEXT        NOT NULL DEFAULT 'oz',
    cost_basis  NUMERIC(18,6) NOT NULL,         -- cost per unit at acquisition
    acquired_at DATE        NOT NULL,
    account_id  INTEGER     REFERENCES accounts(id),
    closed      BOOLEAN     NOT NULL DEFAULT FALSE,
    closed_at   DATE
);

CREATE TABLE IF NOT EXISTS valuations (
    id          SERIAL PRIMARY KEY,
    metal       TEXT        NOT NULL,
    valuation_date DATE     NOT NULL,
    quantity    NUMERIC(18,6) NOT NULL,
    price       NUMERIC(18,6) NOT NULL,
    market_value NUMERIC(18,6) NOT NULL,
    source      TEXT        NOT NULL DEFAULT 'prices_canonical',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_valuations_metal_date ON valuations (metal, valuation_date);

-- ============================================================
-- FORECAST
-- ============================================================

CREATE TABLE IF NOT EXISTS forecasts (
    id          SERIAL PRIMARY KEY,
    model       TEXT        NOT NULL,           -- 'naive', 'arima', 'gradient_boost'
    metal       TEXT        NOT NULL,
    horizon     INTEGER     NOT NULL,           -- days: 1, 5, 20
    run_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    p10         NUMERIC(18,6),
    p50         NUMERIC(18,6),
    p90         NUMERIC(18,6),
    metadata    JSONB
);

CREATE INDEX IF NOT EXISTS idx_forecasts_metal_model ON forecasts (metal, model, run_at DESC);

CREATE TABLE IF NOT EXISTS backtests (
    id          SERIAL PRIMARY KEY,
    model       TEXT        NOT NULL,
    metal       TEXT        NOT NULL,
    horizon     INTEGER     NOT NULL,
    window_start DATE       NOT NULL,
    window_end   DATE       NOT NULL,
    mape        NUMERIC(10,6),                  -- Mean Absolute Percentage Error
    rmse        NUMERIC(18,6),                  -- Root Mean Squared Error
    run_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- ORCHESTRATOR
-- ============================================================

CREATE TABLE IF NOT EXISTS approvals (
    id          SERIAL PRIMARY KEY,
    config_key  TEXT        NOT NULL,
    approved_by TEXT        NOT NULL,           -- must be a HUMAN role actor
    signature   TEXT,                           -- optional cryptographic signature
    approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ,
    revoked     BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_approvals_config_key ON approvals (config_key);

CREATE TABLE IF NOT EXISTS policy_events (
    id          SERIAL PRIMARY KEY,
    action      TEXT        NOT NULL,
    actor       TEXT        NOT NULL,
    result      TEXT        NOT NULL,           -- 'ALLOWED' | 'DENIED' | 'ERROR'
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- AUDIT LOG (append-only)
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id          SERIAL PRIMARY KEY,
    request_id  UUID        NOT NULL,
    actor       TEXT        NOT NULL,
    action      TEXT        NOT NULL,
    payload_hash TEXT       NOT NULL,           -- SHA256 of the payload
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_request ON audit_log (request_id);

-- Prevent UPDATE and DELETE on audit_log (immutability enforcement)
CREATE OR REPLACE FUNCTION deny_audit_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only: % operations are not permitted', TG_OP;
END;
$$;

CREATE TRIGGER trg_audit_no_update
    BEFORE UPDATE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION deny_audit_mutation();

CREATE TRIGGER trg_audit_no_delete
    BEFORE DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION deny_audit_mutation();

-- ============================================================
-- SEED: Chart of Accounts (Scrap Metal)
-- ============================================================

INSERT INTO accounts (code, name, type) VALUES
    ('1000', 'Cash and Cash Equivalents',            'ASSET'),
    -- Ferrous inventory
    ('1110', 'HMS #1 Inventory',                     'ASSET'),
    ('1111', 'HMS #2 Inventory',                     'ASSET'),
    ('1112', 'Shredded Steel Inventory',             'ASSET'),
    ('1113', 'Cast Iron Inventory',                  'ASSET'),
    -- Non-ferrous inventory
    ('1120', 'Bare Bright Copper Inventory',         'ASSET'),
    ('1121', '#1 Copper Inventory',                  'ASSET'),
    ('1122', '#2 Copper Inventory',                  'ASSET'),
    ('1130', 'Cast Aluminum Inventory',              'ASSET'),
    ('1131', 'Aluminum Extrusion Inventory',         'ASSET'),
    ('1132', 'Yellow Brass Inventory',               'ASSET'),
    ('1133', 'Stainless Steel 304 Inventory',        'ASSET'),
    ('1134', 'Lead Inventory',                       'ASSET'),
    ('1135', 'Zorba Inventory',                      'ASSET'),
    -- Other
    ('1200', 'Accounts Receivable',                  'ASSET'),
    ('2000', 'Accounts Payable',                     'LIABILITY'),
    ('2100', 'Accrued Liabilities',                  'LIABILITY'),
    ('3000', 'Retained Earnings',                    'EQUITY'),
    ('3100', 'Common Stock',                         'EQUITY'),
    ('4000', 'Revenue — Scrap Metal Sales',          'REVENUE'),
    ('5000', 'Cost of Goods Sold',                   'EXPENSE'),
    ('5100', 'Operating Expenses',                   'EXPENSE')
ON CONFLICT (code) DO NOTHING;

-- ============================================================
-- SEED: Dealers
-- ============================================================

INSERT INTO dealers (id, name, location_zip, city, state, active) VALUES
    ('00000000-0001-0000-0000-000000000001', 'Houston Metals Inc',    '77001', 'Houston', 'TX', TRUE),
    ('00000000-0002-0000-0000-000000000002', 'Gulf Coast Scrap',      '77002', 'Houston', 'TX', TRUE),
    ('00000000-0003-0000-0000-000000000003', 'Lone Star Recycling',   '77003', 'Houston', 'TX', TRUE)
ON CONFLICT (id) DO NOTHING;

-- ============================================================
-- SOURCE CONFIGS
-- ============================================================

INSERT INTO source_configs (config_key, config_val) VALUES
    ('dealer_manual',    '{"priority": 1, "enabled": true,  "description": "Manual dealer price submissions via POST /prices/dealer"}'),
    ('iscrap',           '{"url": "https://www.iscrapapp.com", "priority": 2, "enabled": false, "description": "iScrap App scraper stub — requires licensing or scraping consent"}'),
    ('scrap_register',   '{"url": "https://www.scrapregister.com", "priority": 3, "enabled": false, "description": "ScrapRegister.com regional averages — requires licensing"}'),
    ('recycling_today',  '{"url": "https://api.fastmarkets.com", "priority": 4, "enabled": false, "description": "Fastmarkets commodity benchmarks — requires paid subscription"}')
ON CONFLICT (config_key) DO NOTHING;
