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
	ID                 uuid.UUID
	MetricID           uuid.UUID
	ServiceName        string
	Value              float64
	Timestamp          time.Time
	StartTimestamp     *time.Time
	IngestionTimestamp time.Time
	ResourceAttributes map[string]any
	Attributes         map[string]any
	IsMonotonic        *bool
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
		SELECT dp.id, dp.metric_id, dp.service_name, dp.value, dp.timestamp,
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
			&p.ID, &p.MetricID, &p.ServiceName, &p.Value, &p.Timestamp,
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
