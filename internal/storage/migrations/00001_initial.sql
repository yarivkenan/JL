-- +goose Up

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- metrics holds one row per unique metric name.
-- It is a stable definition table: rows are upserted on first encounter
-- and shared across all services that emit the same metric name.
CREATE TABLE metrics (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    unit        TEXT NOT NULL DEFAULT '',
    type        TEXT NOT NULL, -- 'gauge' | 'sum'
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- data_points stores individual measurements. Converted to a TimescaleDB
-- hypertable partitioned by timestamp so rule-engine queries on recent
-- windows (e.g. last 5m) only scan the current chunk.
CREATE TABLE data_points (
    id                  UUID        NOT NULL DEFAULT gen_random_uuid(),
    metric_id           UUID        NOT NULL REFERENCES metrics(id),
    service_name        TEXT        NOT NULL DEFAULT '',
    value               DOUBLE PRECISION NOT NULL,
    timestamp           TIMESTAMPTZ NOT NULL,      -- OTel timeUnixNano, hypertable dimension
    start_timestamp     TIMESTAMPTZ,               -- sum metrics only
    ingestion_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resource_attributes JSONB       NOT NULL DEFAULT '{}',
    attributes          JSONB       NOT NULL DEFAULT '{}',
    is_monotonic        BOOLEAN                    -- sum metrics only
);

SELECT create_hypertable('data_points', 'timestamp');

-- Composite indexes match the two dominant query shapes:
--   WHERE metric_id = ? AND timestamp >= ? ORDER BY timestamp DESC
--   WHERE service_name = ? AND timestamp >= ? ORDER BY timestamp DESC
CREATE INDEX idx_dp_metric_id    ON data_points (metric_id,    timestamp DESC);
CREATE INDEX idx_dp_service_name ON data_points (service_name, timestamp DESC);

-- GIN indexes support JSONB containment queries: attributes @> '{"method":"GET"}'
CREATE INDEX idx_dp_attributes          ON data_points USING gin (attributes);
CREATE INDEX idx_dp_resource_attributes ON data_points USING gin (resource_attributes);

-- dead_letter_queue holds messages that exceeded the consumer retry limit.
-- Preserved for manual inspection and replay.
CREATE TABLE dead_letter_queue (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  TEXT NOT NULL,
    payload     TEXT NOT NULL,
    error       TEXT NOT NULL,
    attempts    INT  NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down

DROP TABLE IF EXISTS dead_letter_queue;
DROP TABLE IF EXISTS data_points;
DROP TABLE IF EXISTS metrics;
