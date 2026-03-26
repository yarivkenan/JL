package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AlertRecord represents a single alert written when a rule fires or resolves.
type AlertRecord struct {
	RuleName       string
	MetricName     string
	EvaluatedValue float64
	Threshold      float64
	FiredAt        time.Time
	Status         string // "firing" | "resolved"
}

// AlertRepository handles alert persistence.
type AlertRepository struct {
	pool *pgxpool.Pool
}

// NewAlertRepository creates an AlertRepository backed by the given pool.
func NewAlertRepository(pool *pgxpool.Pool) *AlertRepository {
	return &AlertRepository{pool: pool}
}

// InsertAlert persists an alert record.
func (r *AlertRepository) InsertAlert(ctx context.Context, a AlertRecord) error {
	const q = `
		INSERT INTO alerts (rule_name, metric_name, evaluated_value, threshold, fired_at, status)
		VALUES ($1, $2, $3, $4, $5, $6)`
	if _, err := r.pool.Exec(ctx, q,
		a.RuleName, a.MetricName, a.EvaluatedValue, a.Threshold, a.FiredAt, a.Status,
	); err != nil {
		return fmt.Errorf("insert alert %q: %w", a.RuleName, err)
	}
	return nil
}
