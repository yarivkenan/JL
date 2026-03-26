package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AlertDataPoint represents an alert evaluation written as a timeseries data point.
// Value is 1.0 when firing, 0.0 when resolved.
type AlertDataPoint struct {
	RuleName       string
	MetricName     string
	ServiceName    string
	EvaluatedValue float64
	Threshold      float64
	Severity       string
	Timestamp      time.Time
	Firing         bool
}

// AlertRepository writes alert evaluations as timeseries data points.
type AlertRepository struct {
	pool *pgxpool.Pool
}

// NewAlertRepository creates an AlertRepository backed by the given pool.
func NewAlertRepository(pool *pgxpool.Pool) *AlertRepository {
	return &AlertRepository{pool: pool}
}

// WriteAlertDataPoint upserts an "alert.status" metric and inserts a data point
// with value 1.0 (firing) or 0.0 (resolved). Alert metadata is stored in the
// data point's attributes so it can be queried like any other timeseries.
func (r *AlertRepository) WriteAlertDataPoint(ctx context.Context, dp AlertDataPoint) error {
	// Upsert the alert metric definition.
	const upsertMetric = `
		INSERT INTO metrics (name, description, unit, type)
		VALUES ('alert.status', 'Alert evaluation status (1=firing, 0=resolved)', '', 'gauge')
		ON CONFLICT (name) DO UPDATE SET name = metrics.name
		RETURNING id`

	var metricID string
	if err := r.pool.QueryRow(ctx, upsertMetric).Scan(&metricID); err != nil {
		return fmt.Errorf("upsert alert metric: %w", err)
	}

	var value float64
	if dp.Firing {
		value = 1.0
	}

	attrs := map[string]any{
		"rule_name":       dp.RuleName,
		"source_metric":   dp.MetricName,
		"evaluated_value": dp.EvaluatedValue,
		"threshold":       dp.Threshold,
	}
	if dp.Severity != "" {
		attrs["severity"] = dp.Severity
	}

	attrsJSON, err := json.Marshal(attrs)
	if err != nil {
		return fmt.Errorf("marshal alert attributes: %w", err)
	}

	const insertDP = `
		INSERT INTO data_points
			(metric_id, service_name, value, timestamp, ingestion_timestamp, resource_attributes, attributes)
		VALUES ($1, $2, $3, $4, NOW(), '{}'::jsonb, $5::jsonb)`

	if _, err := r.pool.Exec(ctx, insertDP,
		metricID, dp.ServiceName, value, dp.Timestamp, attrsJSON,
	); err != nil {
		return fmt.Errorf("insert alert data point for rule %q: %w", dp.RuleName, err)
	}
	return nil
}
