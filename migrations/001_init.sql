-- Producers authenticate with API keys. Only a SHA-256 hash is stored.
CREATE TABLE producers (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    key_hash    TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ
);

-- The topic: append-only, partitioned by key. offset_id only grows.
CREATE TABLE event_log (
    offset_id     BIGSERIAL PRIMARY KEY,
    partition_key SMALLINT NOT NULL,
    producer_id   BIGINT REFERENCES producers(id),
    payload       JSONB NOT NULL,
    produced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_event_log_partition_offset ON event_log (partition_key, offset_id);

CREATE TABLE consumer_offsets (
    consumer_group   TEXT NOT NULL,
    partition_key    SMALLINT NOT NULL,
    committed_offset BIGINT NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, partition_key)
);

-- Raw layer. event_id is the idempotency key: producers retry, duplicates WILL arrive.
CREATE TABLE raw_events (
    event_id      UUID PRIMARY KEY,
    event_type    TEXT NOT NULL CHECK (event_type IN ('offer_click','offer_complete','reward_paid')),
    user_id       TEXT NOT NULL,
    offer_id      TEXT NOT NULL,
    advertiser    TEXT NOT NULL,
    country       CHAR(2) NOT NULL,
    reward_cents  INTEGER NOT NULL DEFAULT 0 CHECK (reward_cents >= 0),
    occurred_at   TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    partition_key SMALLINT NOT NULL,
    source_offset BIGINT NOT NULL
);
-- ingested_at grows with insertion order -> BRIN is ~1000x smaller than a B-tree and
-- perfect for "everything since the watermark" scans.
CREATE INDEX ix_raw_events_ingested_brin ON raw_events USING BRIN (ingested_at);
-- Point lookups for the user API.
CREATE INDEX ix_raw_events_user_time ON raw_events (user_id, occurred_at DESC);
-- Partial index: only completions, used by the orphan-payment check.
CREATE INDEX ix_raw_events_completes ON raw_events (user_id, offer_id) WHERE event_type = 'offer_complete';

CREATE TABLE dead_letter_events (
    id           BIGSERIAL PRIMARY KEY,
    source_offset BIGINT,
    payload      JSONB NOT NULL,
    error        TEXT NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE model_state (
    model_name     TEXT PRIMARY KEY,
    high_watermark TIMESTAMPTZ,
    last_run_at    TIMESTAMPTZ,
    rows_written   INTEGER
);

CREATE TABLE pipeline_runs (
    id          BIGSERIAL PRIMARY KEY,
    stage       TEXT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status      TEXT NOT NULL,
    rows        INTEGER NOT NULL DEFAULT 0,
    detail      JSONB
);
CREATE INDEX ix_pipeline_runs_stage_time ON pipeline_runs (stage, finished_at DESC);
