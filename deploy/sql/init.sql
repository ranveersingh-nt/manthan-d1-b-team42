-- StreamGuard schema (PostgreSQL 18).
-- Runs on Postgres container start. Safe to re-run: every object uses IF NOT EXISTS.
-- Field names and types follow docs/data-contract.md.
-- Must run through psql (the Postgres image's /docker-entrypoint-initdb.d does this): it uses \gexec and \connect.
-- Format validation (card token, currency list, country, category) happens in the processor;
-- bad events go to the dead-letter topic, so the database only enforces basic safety checks.

-- Create team42_db and superset_db if they do not exist yet (CREATE DATABASE has no IF NOT EXISTS).
SELECT 'CREATE DATABASE team42_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'team42_db')\gexec

SELECT 'CREATE DATABASE superset_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset_db')\gexec

-- Switch to team42_db so every table below is created inside it.
\connect team42_db

-- Every valid payment event, plus the extra facts the processor looked up for it.
CREATE TABLE IF NOT EXISTS enriched_txn (
    event_id          UUID           PRIMARY KEY,
    card_token        TEXT           NOT NULL,
    merchant_id       TEXT           NOT NULL,
    merchant_category TEXT           NOT NULL,
    amount            NUMERIC(12,2)  NOT NULL CHECK (amount > 0),
    currency          CHAR(3)        NOT NULL,
    country           CHAR(2)        NOT NULL,
    channel           TEXT           NOT NULL CHECK (channel IN ('POS', 'ECOM', 'ATM')),
    event_time        TIMESTAMPTZ    NOT NULL,
    status            TEXT           NOT NULL CHECK (status IN ('APPROVED', 'DECLINED', 'PENDING')),
    card_avg_30d      NUMERIC(12,2),
    is_blacklisted    BOOLEAN        NOT NULL DEFAULT FALSE,
    processed_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS payment_events_validated (
    event_id          UUID           PRIMARY KEY,
    card_token        TEXT           NOT NULL,
    merchant_id       TEXT           NOT NULL,
    merchant_category TEXT           NOT NULL,
    amount            NUMERIC(12,2)  NOT NULL CHECK (amount > 0),
    currency          CHAR(3)        NOT NULL,
    country           CHAR(2)        NOT NULL,
    channel           TEXT           NOT NULL CHECK (channel IN ('POS', 'ECOM', 'ATM')),
    event_time        TIMESTAMPTZ    NOT NULL,
    status            TEXT           NOT NULL CHECK (status IN ('APPROVED', 'DECLINED', 'PENDING')),
    card_avg_30d      NUMERIC(12,2),
    is_blacklisted    BOOLEAN        NOT NULL DEFAULT FALSE,
    processed_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS payment_events_invalidated (
    src_topic      TEXT        NOT NULL,
    src_partition  INTEGER     NOT NULL,
    src_offset     BIGINT      NOT NULL,
    src_kafka_ts   TIMESTAMPTZ,
    event_id       TEXT,                 -- as sent (may be missing or invalid), masked
    card_token     TEXT,                 -- as sent, masked
    dq_rule        TEXT        NOT NULL, -- first failed rule
    reason         TEXT        NOT NULL, -- all failure reasons, '; '-separated
    dq_failures    JSONB       NOT NULL, -- [{rule, field, reason}, ...]
    raw_payload    TEXT,                 -- original message, card numbers masked
    rejected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_payment_events_invalidated PRIMARY KEY (src_topic, src_partition, src_offset)
);
CREATE INDEX IF NOT EXISTS ix_pei_rule_time ON payment_events_invalidated (dq_rule, rejected_at);


CREATE INDEX IF NOT EXISTS idx_enriched_txn_event_time ON enriched_txn (event_time);

-- Per-minute totals by merchant category and country, for the live dashboard.
CREATE TABLE IF NOT EXISTS agg_1min (
    window_start      TIMESTAMPTZ    NOT NULL,
    merchant_category TEXT           NOT NULL,
    country           CHAR(2)        NOT NULL,
    txn_count         INTEGER        NOT NULL DEFAULT 0 CHECK (txn_count >= 0),
    total_amount      NUMERIC(14,2)  NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    alert_count       INTEGER        NOT NULL DEFAULT 0 CHECK (alert_count >= 0),
    PRIMARY KEY (window_start, merchant_category, country)
);

-- One row each time a fraud rule fires on an event; alert_id is a hash of rule_id + event_id so replays never duplicate it.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id          TEXT           PRIMARY KEY,
    rule_id           TEXT           NOT NULL,
    event_id          UUID           NOT NULL,
    card_token        TEXT           NOT NULL,
    reason            TEXT           NOT NULL,
    severity          TEXT           NOT NULL,
    amount            NUMERIC(12,2)  NOT NULL CHECK (amount > 0),
    event_time        TIMESTAMPTZ    NOT NULL,
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT now(),
    UNIQUE (rule_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_id    ON alerts (rule_id);

-- An analyst's verdict on an alert (real fraud or not); feeds the daily precision report.
CREATE TABLE IF NOT EXISTS alert_feedback (
    alert_id          TEXT           PRIMARY KEY REFERENCES alerts (alert_id),
    label             TEXT           NOT NULL CHECK (label IN ('TRUE_POSITIVE', 'FALSE_POSITIVE')),
    analyst           TEXT           NOT NULL,
    marked_at         TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Merchants we never trust; any payment to them raises an alert.
CREATE TABLE IF NOT EXISTS merchant_blacklist (
    merchant_id       TEXT           PRIMARY KEY,
    reason            TEXT           NOT NULL,
    added_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Each card's normal spending over the last 30 days, used by the amount-spike rule.
CREATE TABLE IF NOT EXISTS card_baseline (
    card_token        TEXT           PRIMARY KEY,
    avg_amount_30d    NUMERIC(12,2)  NOT NULL CHECK (avg_amount_30d >= 0),
    txn_count_30d     INTEGER        NOT NULL CHECK (txn_count_30d >= 0),
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Health numbers about the pipeline itself (lag, latency, dead-letter rate, alerts per minute).
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_time       TIMESTAMPTZ      NOT NULL,
    metric_name       TEXT             NOT NULL,
    metric_value      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (metric_time, metric_name)
);
