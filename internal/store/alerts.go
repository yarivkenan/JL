package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

// AggregateQuery describes a window aggregation over data_points.
type AggregateQuery struct {
	MetricName  string
	ServiceName string
	Attributes  map[string]any // JSONB containment filter on dp.attributes
	Aggregation string         // "avg" | "p95" | "count" | "value"
	Since       time.Time
	Until       time.Time
}

// AlertRepository handles alert persistence and metric aggregation queries.
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

// QueryAggregate computes the aggregated metric value over the given window.
// Returns (value, hasData, error). hasData is false when no rows match —
// the caller should skip alert evaluation rather than treat zero as a real value.
func (r *AlertRepository) QueryAggregate(ctx context.Context, q AggregateQuery) (float64, bool, error) {
	aggExpr, err := aggregationExpr(q.Aggregation)
	if err != nil {
		return 0, false, err
	}

	var args []any
	var clauses []string

	add := func(clause string, val any) {
		args = append(args, val)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}

	if q.MetricName != "" {
		add("m.name = $%d", q.MetricName)
	}
	if q.ServiceName != "" {
		add("dp.service_name = $%d", q.ServiceName)
	}
	if !q.Since.IsZero() {
		add("dp.timestamp >= $%d", q.Since)
	}
	if !q.Until.IsZero() {
		add("dp.timestamp <= $%d", q.Until)
	}
	if len(q.Attributes) > 0 {
		attrJSON, err := json.Marshal(q.Attributes)
		if err != nil {
			return 0, false, fmt.Errorf("marshal attributes filter: %w", err)
		}
		add("dp.attributes @> $%d::jsonb", string(attrJSON))
	}

	where := "1=1"
	if len(clauses) > 0 {
		where = strings.Join(clauses, " AND ")
	}

	sql := fmt.Sprintf(`
		SELECT %s
		FROM data_points dp
		JOIN metrics m ON m.id = dp.metric_id
		WHERE %s`, aggExpr, where)

	var result *float64
	if err := r.pool.QueryRow(ctx, sql, args...).Scan(&result); err != nil {
		return 0, false, fmt.Errorf("query aggregate (%s) for %q: %w", q.Aggregation, q.MetricName, err)
	}
	if result == nil {
		return 0, false, nil
	}
	return *result, true, nil
}

// aggregationExpr returns the SQL expression for the requested aggregation.
// "value" uses MAX so that any single point breaching the threshold is caught.
func aggregationExpr(agg string) (string, error) {
	switch agg {
	case "avg":
		return "AVG(dp.value)", nil
	case "count":
		return "COUNT(dp.value)::double precision", nil
	case "p95":
		return "percentile_cont(0.95) WITHIN GROUP (ORDER BY dp.value)", nil
	case "value":
		return "MAX(dp.value)", nil
	default:
		return "", fmt.Errorf("unknown aggregation %q: must be avg, p95, count, or value", agg)
	}
}
