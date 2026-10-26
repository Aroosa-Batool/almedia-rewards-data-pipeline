-- Operational + warehouse schema.
-- SQLite is used so the demo runs anywhere (and on Vercel). The same DDL runs on
-- PostgreSQL with small changes noted inline (see GUIDE.md, "Moving to Postgres").

-- RAW LAYER ---------------------------------------------------------------
-- One row per event, exactly as received (after validation).
-- event_id is the idempotency key: producers retry, so duplicates WILL arrive.
CREATE TABLE IF NOT EXISTS raw_events (
    event_id      TEXT PRIMARY KEY,              -- Postgres: UUID PRIMARY KEY
    event_type    TEXT NOT NULL CHECK (event_type IN ('offer_click','offer_complete','reward_paid')),
    user_id       TEXT NOT NULL,
    offer_id      TEXT NOT NULL,
    advertiser    TEXT NOT NULL,
    country       TEXT NOT NULL,
    reward_cents  INTEGER NOT NULL DEFAULT 0 CHECK (reward_cents >= 0),
    occurred_at   TEXT NOT NULL,                 -- Postgres: TIMESTAMPTZ
    ingested_at   TEXT NOT NULL,
    partition_key INTEGER NOT NULL               -- which "Kafka partition" it came from
);

-- Indexes chosen for the queries we actually run (see GUIDE.md, "Indexes"):
-- incremental models scan by ingested_at; per-user lookups power the API.
CREATE INDEX IF NOT EXISTS ix_raw_events_ingested_at ON raw_events (ingested_at);
CREATE INDEX IF NOT EXISTS ix_raw_events_user_time  ON raw_events (user_id, occurred_at);

-- Records that failed validation. Never drop bad data silently.
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    payload     TEXT NOT NULL,
    error       TEXT NOT NULL,
    received_at TEXT NOT NULL
);

-- Stream consumer bookkeeping: last committed offset per partition.
-- This is what Kafka stores in __consumer_offsets.
CREATE TABLE IF NOT EXISTS consumer_offsets (
    consumer_group TEXT NOT NULL,
    partition_key  INTEGER NOT NULL,
    committed_offset INTEGER NOT NULL,
    PRIMARY KEY (consumer_group, partition_key)
);

-- The "topic": an append-only log. Offsets only grow.
CREATE TABLE IF NOT EXISTS event_log (
    offset_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    partition_key INTEGER NOT NULL,
    payload       TEXT NOT NULL,
    produced_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_event_log_partition ON event_log (partition_key, offset_id);

-- Watermarks for incremental batch models (like dbt's is_incremental()).
CREATE TABLE IF NOT EXISTS model_state (
    model_name   TEXT PRIMARY KEY,
    high_watermark TEXT,
    last_run_at  TEXT,
    rows_written INTEGER
);

-- Pipeline run history = the basis for monitoring.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    stage       TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL,
    rows        INTEGER DEFAULT 0,
    detail      TEXT
);
