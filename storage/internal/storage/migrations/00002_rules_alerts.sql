-- +goose Up

-- rules defines alerting rules that evaluate metric conditions.
CREATE TABLE rules (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT NOT NULL UNIQUE,
    metric_name  TEXT NOT NULL,
    service_name TEXT NOT NULL DEFAULT '',
    condition    JSONB NOT NULL DEFAULT '{}',   -- {"aggregation":"avg","operator":">","threshold":500}
    window       JSONB NOT NULL DEFAULT '{}',   -- {"type":"time","duration":"5m"}
    action       JSONB NOT NULL DEFAULT '{}',   -- {"type":"alert","severity":"warning","message":"..."}
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- alerts stores rule evaluation results.
CREATE TABLE alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_name       TEXT NOT NULL,
    metric_name     TEXT NOT NULL,
    service_name    TEXT NOT NULL DEFAULT '',
    evaluated_value DOUBLE PRECISION NOT NULL,
    threshold       DOUBLE PRECISION NOT NULL,
    operator        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'firing',   -- 'firing' | 'resolved'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX idx_alerts_rule_name ON alerts (rule_name, created_at DESC);
CREATE INDEX idx_alerts_status    ON alerts (status, created_at DESC);

-- +goose Down

DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS rules;
