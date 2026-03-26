-- +goose Up

-- alerts stores one record per rule evaluation that is firing or resolved.
-- Written by the rules engine evaluator after each threshold check.
CREATE TABLE alerts (
    id              UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_name       TEXT             NOT NULL,
    metric_name     TEXT             NOT NULL,
    evaluated_value DOUBLE PRECISION NOT NULL,
    threshold       DOUBLE PRECISION NOT NULL,
    fired_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    status          TEXT             NOT NULL  -- 'firing' | 'resolved'
);

-- Support looking up recent alerts by rule and filtering by status.
CREATE INDEX idx_alerts_rule_name ON alerts (rule_name, fired_at DESC);
CREATE INDEX idx_alerts_status    ON alerts (status,    fired_at DESC);

-- +goose Down

DROP TABLE IF EXISTS alerts;
