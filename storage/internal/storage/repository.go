package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MetricDef is the stored representation of a metric definition.
type MetricDef struct {
	ID          uuid.UUID
	Name        string
	Description string
	Unit        string
	Type        string
	CreatedAt   time.Time
}

// DataPoint is the stored representation of a single measurement.
type DataPoint struct {
	ID                 uuid.UUID      `json:"id"`
	MetricID           uuid.UUID      `json:"metric_id"`
	MetricName         string         `json:"metric_name"`
	ServiceName        string         `json:"service_name"`
	Value              float64        `json:"value"`
	Timestamp          time.Time      `json:"timestamp"`
	StartTimestamp     *time.Time     `json:"start_timestamp,omitempty"`
	IngestionTimestamp time.Time      `json:"ingestion_timestamp"`
	ResourceAttributes map[string]any `json:"resource_attributes"`
	Attributes         map[string]any `json:"attributes"`
	IsMonotonic        *bool          `json:"is_monotonic,omitempty"`
}

// Rule defines an alerting rule.
type Rule struct {
	ID          uuid.UUID      `json:"id"`
	Name        string         `json:"name"`
	MetricName  string         `json:"metric_name"`
	ServiceName string         `json:"service_name,omitempty"`
	Condition   map[string]any `json:"condition"`
	Window      map[string]any `json:"window"`
	Action      map[string]any `json:"action"`
	CreatedAt   time.Time      `json:"created_at"`
}

// Alert stores a rule evaluation result.
type Alert struct {
	ID             uuid.UUID  `json:"id"`
	RuleName       string     `json:"rule_name"`
	MetricName     string     `json:"metric_name"`
	ServiceName    string     `json:"service_name,omitempty"`
	EvaluatedValue float64    `json:"evaluated_value"`
	Threshold      float64    `json:"threshold"`
	Operator       string     `json:"operator"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"created_at"`
	ResolvedAt     *time.Time `json:"resolved_at,omitempty"`
}

// AlertFilter limits results from ListAlerts.
type AlertFilter struct {
	RuleName string
	Status   string
	Since    time.Time
	Limit    int
}

// DeadLetter represents a Kafka message that exceeded its retry limit.
type DeadLetter struct {
	ID        uuid.UUID `json:"id"`
	MessageID string    `json:"message_id"`
	Payload   string    `json:"payload"`
	Error     string    `json:"error"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"created_at"`
}

// MetricFilter limits results from ListMetrics.
type MetricFilter struct {
	Name string
}

// DataPointFilter limits results from QueryDataPoints.
type DataPointFilter struct {
	MetricName  string
	ServiceName string
	Since       time.Time
	Attributes  map[string]any // JSONB containment filter: attributes @> {...}
	Limit       int
}

// Repository is the interface for all metric persistence operations.
// Both the rules engine (Phase 4) and CLI (Phase 5) depend on this interface,
// not on the concrete postgres implementation.
type Repository interface {
	// UpsertMetric inserts the metric definition or returns the existing row on
	// name conflict. Description and unit are updated if the new values are non-empty.
	UpsertMetric(ctx context.Context, def *MetricDef) (*MetricDef, error)

	// InsertDataPoints persists a batch of data points in a single round trip.
	InsertDataPoints(ctx context.Context, metricID uuid.UUID, points []DataPoint) error

	// ListMetrics returns metric definitions matching the filter.
	ListMetrics(ctx context.Context, filter MetricFilter) ([]*MetricDef, error)

	// QueryDataPoints returns data points matching the filter, ordered by timestamp DESC.
	QueryDataPoints(ctx context.Context, filter DataPointFilter) ([]*DataPoint, error)

	// InsertDeadLetter stores a message that exceeded its retry limit.
	InsertDeadLetter(ctx context.Context, messageID, payload, errMsg string, attempts int) error

	// ListDeadLetters returns the most recent dead-letter entries, newest first.
	ListDeadLetters(ctx context.Context, limit int) ([]*DeadLetter, error)

	// CreateRule inserts a new alerting rule.
	CreateRule(ctx context.Context, rule *Rule) (*Rule, error)

	// ListRules returns all defined rules.
	ListRules(ctx context.Context) ([]*Rule, error)

	// DeleteRule removes a rule by name. Returns true if a row was deleted.
	DeleteRule(ctx context.Context, name string) (bool, error)

	// ListAlerts returns alerts matching the filter.
	ListAlerts(ctx context.Context, filter AlertFilter) ([]*Alert, error)

	// GetAlert returns a single alert by ID.
	GetAlert(ctx context.Context, id uuid.UUID) (*Alert, error)
}

type postgresRepository struct {
	pool *pgxpool.Pool
}

// NewRepository returns a postgres-backed Repository.
func NewRepository(pool *pgxpool.Pool) Repository {
	return &postgresRepository{pool: pool}
}

func (r *postgresRepository) UpsertMetric(ctx context.Context, def *MetricDef) (*MetricDef, error) {
	// ON CONFLICT DO UPDATE ensures we always get a RETURNING row.
	// COALESCE preserves existing non-empty values when the incoming ones are blank.
	const q = `
		INSERT INTO metrics (name, description, unit, type)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO UPDATE SET
			description = COALESCE(NULLIF(EXCLUDED.description, ''), metrics.description),
			unit        = COALESCE(NULLIF(EXCLUDED.unit, ''),        metrics.unit)
		RETURNING id, name, description, unit, type, created_at`

	result := &MetricDef{}
	err := r.pool.QueryRow(ctx, q, def.Name, def.Description, def.Unit, def.Type).
		Scan(&result.ID, &result.Name, &result.Description, &result.Unit, &result.Type, &result.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("upsert metric %q: %w", def.Name, err)
	}
	return result, nil
}

func (r *postgresRepository) InsertDataPoints(ctx context.Context, metricID uuid.UUID, points []DataPoint) error {
	if len(points) == 0 {
		return nil
	}

	const q = `
		INSERT INTO data_points
			(metric_id, service_name, value, timestamp, start_timestamp,
			 ingestion_timestamp, resource_attributes, attributes, is_monotonic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	batch := &pgx.Batch{}
	for _, p := range points {
		resAttrs, err := json.Marshal(p.ResourceAttributes)
		if err != nil {
			return fmt.Errorf("marshal resource_attributes: %w", err)
		}
		attrs, err := json.Marshal(p.Attributes)
		if err != nil {
			return fmt.Errorf("marshal attributes: %w", err)
		}
		batch.Queue(q,
			metricID,
			p.ServiceName,
			p.Value,
			p.Timestamp,
			p.StartTimestamp,
			p.IngestionTimestamp,
			resAttrs,
			attrs,
			p.IsMonotonic,
		)
	}

	br := r.pool.SendBatch(ctx, batch)
	for i := range points {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("insert data point %d: %w", i, err)
		}
	}
	return br.Close()
}

func (r *postgresRepository) ListMetrics(ctx context.Context, filter MetricFilter) ([]*MetricDef, error) {
	q := `SELECT id, name, description, unit, type, created_at FROM metrics WHERE 1=1`
	var args []any

	if filter.Name != "" {
		args = append(args, filter.Name)
		q += fmt.Sprintf(" AND name = $%d", len(args))
	}

	q += " ORDER BY name"

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list metrics: %w", err)
	}
	defer rows.Close()

	var metrics []*MetricDef
	for rows.Next() {
		m := &MetricDef{}
		if err := rows.Scan(&m.ID, &m.Name, &m.Description, &m.Unit, &m.Type, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan metric: %w", err)
		}
		metrics = append(metrics, m)
	}
	return metrics, rows.Err()
}

func (r *postgresRepository) QueryDataPoints(ctx context.Context, filter DataPointFilter) ([]*DataPoint, error) {
	var (
		args    []any
		clauses []string
	)

	addClause := func(clause string, val any) {
		args = append(args, val)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}

	q := `
		SELECT dp.id, dp.metric_id, m.name, dp.service_name, dp.value, dp.timestamp,
		       dp.start_timestamp, dp.ingestion_timestamp,
		       dp.resource_attributes, dp.attributes, dp.is_monotonic
		FROM data_points dp
		JOIN metrics m ON m.id = dp.metric_id
		WHERE 1=1`

	if filter.MetricName != "" {
		addClause(" AND m.name = $%d", filter.MetricName)
	}
	if filter.ServiceName != "" {
		addClause(" AND dp.service_name = $%d", filter.ServiceName)
	}
	if !filter.Since.IsZero() {
		addClause(" AND dp.timestamp >= $%d", filter.Since)
	}
	if len(filter.Attributes) > 0 {
		attrJSON, err := json.Marshal(filter.Attributes)
		if err != nil {
			return nil, fmt.Errorf("marshal attribute filter: %w", err)
		}
		addClause(" AND dp.attributes @> $%d::jsonb", string(attrJSON))
	}

	q += strings.Join(clauses, "")
	q += " ORDER BY dp.timestamp DESC"

	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query data points: %w", err)
	}
	defer rows.Close()

	var points []*DataPoint
	for rows.Next() {
		p := &DataPoint{}
		var resAttrsRaw, attrsRaw []byte
		if err := rows.Scan(
			&p.ID, &p.MetricID, &p.MetricName, &p.ServiceName, &p.Value, &p.Timestamp,
			&p.StartTimestamp, &p.IngestionTimestamp,
			&resAttrsRaw, &attrsRaw, &p.IsMonotonic,
		); err != nil {
			return nil, fmt.Errorf("scan data point: %w", err)
		}
		if err := json.Unmarshal(resAttrsRaw, &p.ResourceAttributes); err != nil {
			return nil, fmt.Errorf("unmarshal resource_attributes: %w", err)
		}
		if err := json.Unmarshal(attrsRaw, &p.Attributes); err != nil {
			return nil, fmt.Errorf("unmarshal attributes: %w", err)
		}
		points = append(points, p)
	}
	return points, rows.Err()
}

func (r *postgresRepository) InsertDeadLetter(ctx context.Context, messageID, payload, errMsg string, attempts int) error {
	const q = `
		INSERT INTO dead_letter_queue (message_id, payload, error, attempts)
		VALUES ($1, $2, $3, $4)`
	if _, err := r.pool.Exec(ctx, q, messageID, payload, errMsg, attempts); err != nil {
		return fmt.Errorf("insert dead letter (message %s): %w", messageID, err)
	}
	return nil
}

func (r *postgresRepository) ListDeadLetters(ctx context.Context, limit int) ([]*DeadLetter, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
		SELECT id, message_id, payload, error, attempts, created_at
		FROM dead_letter_queue
		ORDER BY created_at DESC
		LIMIT $1`
	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()

	var letters []*DeadLetter
	for rows.Next() {
		dl := &DeadLetter{}
		if err := rows.Scan(&dl.ID, &dl.MessageID, &dl.Payload, &dl.Error, &dl.Attempts, &dl.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		letters = append(letters, dl)
	}
	return letters, rows.Err()
}

func (r *postgresRepository) CreateRule(ctx context.Context, rule *Rule) (*Rule, error) {
	condJSON, err := json.Marshal(rule.Condition)
	if err != nil {
		return nil, fmt.Errorf("marshal condition: %w", err)
	}
	windowJSON, err := json.Marshal(rule.Window)
	if err != nil {
		return nil, fmt.Errorf("marshal window: %w", err)
	}
	actionJSON, err := json.Marshal(rule.Action)
	if err != nil {
		return nil, fmt.Errorf("marshal action: %w", err)
	}

	const q = `
		INSERT INTO rules (name, metric_name, service_name, condition, window, action)
		VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb)
		RETURNING id, name, metric_name, service_name, condition, window, action, created_at`

	result := &Rule{}
	var condRaw, windowRaw, actionRaw []byte
	err = r.pool.QueryRow(ctx, q,
		rule.Name, rule.MetricName, rule.ServiceName, condJSON, windowJSON, actionJSON,
	).Scan(&result.ID, &result.Name, &result.MetricName, &result.ServiceName,
		&condRaw, &windowRaw, &actionRaw, &result.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("create rule %q: %w", rule.Name, err)
	}
	json.Unmarshal(condRaw, &result.Condition)
	json.Unmarshal(windowRaw, &result.Window)
	json.Unmarshal(actionRaw, &result.Action)
	return result, nil
}

func (r *postgresRepository) ListRules(ctx context.Context) ([]*Rule, error) {
	const q = `SELECT id, name, metric_name, service_name, condition, window, action, created_at
		FROM rules ORDER BY name`
	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	defer rows.Close()

	var rules []*Rule
	for rows.Next() {
		ru := &Rule{}
		var condRaw, windowRaw, actionRaw []byte
		if err := rows.Scan(&ru.ID, &ru.Name, &ru.MetricName, &ru.ServiceName,
			&condRaw, &windowRaw, &actionRaw, &ru.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan rule: %w", err)
		}
		json.Unmarshal(condRaw, &ru.Condition)
		json.Unmarshal(windowRaw, &ru.Window)
		json.Unmarshal(actionRaw, &ru.Action)
		rules = append(rules, ru)
	}
	return rules, rows.Err()
}

func (r *postgresRepository) DeleteRule(ctx context.Context, name string) (bool, error) {
	const q = `DELETE FROM rules WHERE name = $1`
	tag, err := r.pool.Exec(ctx, q, name)
	if err != nil {
		return false, fmt.Errorf("delete rule %q: %w", name, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (r *postgresRepository) ListAlerts(ctx context.Context, filter AlertFilter) ([]*Alert, error) {
	q := `SELECT id, rule_name, metric_name, service_name, evaluated_value, threshold, operator, status, created_at, resolved_at
		FROM alerts WHERE 1=1`
	var args []any

	if filter.RuleName != "" {
		args = append(args, filter.RuleName)
		q += fmt.Sprintf(" AND rule_name = $%d", len(args))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if !filter.Since.IsZero() {
		args = append(args, filter.Since)
		q += fmt.Sprintf(" AND created_at >= $%d", len(args))
	}

	q += " ORDER BY created_at DESC"

	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list alerts: %w", err)
	}
	defer rows.Close()

	var alerts []*Alert
	for rows.Next() {
		a := &Alert{}
		if err := rows.Scan(&a.ID, &a.RuleName, &a.MetricName, &a.ServiceName,
			&a.EvaluatedValue, &a.Threshold, &a.Operator, &a.Status,
			&a.CreatedAt, &a.ResolvedAt); err != nil {
			return nil, fmt.Errorf("scan alert: %w", err)
		}
		alerts = append(alerts, a)
	}
	return alerts, rows.Err()
}

func (r *postgresRepository) GetAlert(ctx context.Context, id uuid.UUID) (*Alert, error) {
	const q = `SELECT id, rule_name, metric_name, service_name, evaluated_value, threshold, operator, status, created_at, resolved_at
		FROM alerts WHERE id = $1`
	a := &Alert{}
	err := r.pool.QueryRow(ctx, q, id).Scan(&a.ID, &a.RuleName, &a.MetricName, &a.ServiceName,
		&a.EvaluatedValue, &a.Threshold, &a.Operator, &a.Status, &a.CreatedAt, &a.ResolvedAt)
	if err != nil {
		return nil, fmt.Errorf("get alert %s: %w", id, err)
	}
	return a, nil
}
