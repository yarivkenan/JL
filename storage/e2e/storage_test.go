//go:build e2e

package e2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/judgment-labs/ingest/storage/internal/storage"
)

var dbURL = func() string {
	if u := os.Getenv("DATABASE_URL"); u != "" {
		return u
	}
	return "postgres://otel:otel@localhost:5432/otel_metrics"
}()

// TestMain runs migrations once before all tests so each test starts with a
// fully-provisioned schema.
func TestMain(m *testing.M) {
	if err := storage.RunMigrations(dbURL); err != nil {
		panic("migrations failed: " + err.Error())
	}
	os.Exit(m.Run())
}

// TestUpsertMetricCreates verifies that a new metric definition is persisted.
func TestUpsertMetricCreates(t *testing.T) {
	repo, cleanup := newRepo(t)
	defer cleanup()

	def := &storage.MetricDef{
		Name:        uniqueName(t, "http.request.duration"),
		Description: "HTTP request latency",
		Unit:        "ms",
		Type:        "gauge",
	}

	got, err := repo.UpsertMetric(context.Background(), def)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got.ID.String() == "" {
		t.Fatal("expected a non-zero ID")
	}
	if got.Name != def.Name {
		t.Errorf("name: got %q, want %q", got.Name, def.Name)
	}
}

// TestUpsertMetricIdempotent verifies that upserting the same metric name
// twice returns the same row ID.
func TestUpsertMetricIdempotent(t *testing.T) {
	repo, cleanup := newRepo(t)
	defer cleanup()

	def := &storage.MetricDef{
		Name: uniqueName(t, "requests.total"),
		Type: "sum",
	}

	first, err := repo.UpsertMetric(context.Background(), def)
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	second, err := repo.UpsertMetric(context.Background(), def)
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	if first.ID != second.ID {
		t.Errorf("expected same ID on re-upsert: first=%v second=%v", first.ID, second.ID)
	}
}

// TestInsertAndQueryDataPoints verifies round-trip persistence of data points
// and that basic service/metric filters work.
func TestInsertAndQueryDataPoints(t *testing.T) {
	ctx := context.Background()
	repo, cleanup := newRepo(t)
	defer cleanup()

	metric, err := repo.UpsertMetric(ctx, &storage.MetricDef{
		Name: uniqueName(t, "http.request.duration"),
		Type: "gauge",
	})
	if err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	now := time.Now().UTC()
	points := []storage.DataPoint{
		{
			ServiceName:        "checkout-service",
			Value:              123.45,
			Timestamp:          now,
			IngestionTimestamp: now,
			ResourceAttributes: map[string]any{"service.name": "checkout-service"},
			Attributes:         map[string]any{"method": "GET", "status": "200"},
		},
		{
			ServiceName:        "checkout-service",
			Value:              456.78,
			Timestamp:          now.Add(-30 * time.Second),
			IngestionTimestamp: now,
			ResourceAttributes: map[string]any{"service.name": "checkout-service"},
			Attributes:         map[string]any{"method": "POST", "status": "201"},
		},
	}

	if err := repo.InsertDataPoints(ctx, metric.ID, points); err != nil {
		t.Fatalf("insert data points: %v", err)
	}

	got, err := repo.QueryDataPoints(ctx, storage.DataPointFilter{
		MetricName:  metric.Name,
		ServiceName: "checkout-service",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("query data points: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 data points, got %d", len(got))
	}
}

// TestQueryDataPointsSinceFilter verifies the time-window filter returns only
// recent data points — the core predicate used by the rules engine.
func TestQueryDataPointsSinceFilter(t *testing.T) {
	ctx := context.Background()
	repo, cleanup := newRepo(t)
	defer cleanup()

	metric, err := repo.UpsertMetric(ctx, &storage.MetricDef{
		Name: uniqueName(t, "http.request.duration"),
		Type: "gauge",
	})
	if err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	now := time.Now().UTC()
	points := []storage.DataPoint{
		{
			ServiceName:        "svc",
			Value:              1.0,
			Timestamp:          now,                    // recent
			IngestionTimestamp: now,
			ResourceAttributes: map[string]any{},
			Attributes:         map[string]any{},
		},
		{
			ServiceName:        "svc",
			Value:              2.0,
			Timestamp:          now.Add(-10 * time.Minute), // outside 5m window
			IngestionTimestamp: now,
			ResourceAttributes: map[string]any{},
			Attributes:         map[string]any{},
		},
	}

	if err := repo.InsertDataPoints(ctx, metric.ID, points); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := repo.QueryDataPoints(ctx, storage.DataPointFilter{
		MetricName: metric.Name,
		Since:      now.Add(-5 * time.Minute),
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("since filter: expected 1 point, got %d", len(got))
	}
	if got[0].Value != 1.0 {
		t.Errorf("expected value 1.0, got %v", got[0].Value)
	}
}

// TestQueryDataPointsAttributeFilter verifies JSONB containment queries —
// used by the rules engine to filter on resource/data-point attributes.
func TestQueryDataPointsAttributeFilter(t *testing.T) {
	ctx := context.Background()
	repo, cleanup := newRepo(t)
	defer cleanup()

	metric, err := repo.UpsertMetric(ctx, &storage.MetricDef{
		Name: uniqueName(t, "http.request.duration"),
		Type: "gauge",
	})
	if err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	now := time.Now().UTC()
	points := []storage.DataPoint{
		{
			ServiceName:        "svc",
			Value:              100,
			Timestamp:          now,
			IngestionTimestamp: now,
			ResourceAttributes: map[string]any{},
			Attributes:         map[string]any{"method": "GET"},
		},
		{
			ServiceName:        "svc",
			Value:              200,
			Timestamp:          now.Add(-1 * time.Second),
			IngestionTimestamp: now,
			ResourceAttributes: map[string]any{},
			Attributes:         map[string]any{"method": "POST"},
		},
	}

	if err := repo.InsertDataPoints(ctx, metric.ID, points); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := repo.QueryDataPoints(ctx, storage.DataPointFilter{
		MetricName: metric.Name,
		Attributes: map[string]any{"method": "GET"},
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("attribute filter: expected 1 point, got %d", len(got))
	}
	if got[0].Value != 100 {
		t.Errorf("expected value 100, got %v", got[0].Value)
	}
}

// TestListMetrics verifies that upserted metrics appear in ListMetrics.
func TestListMetrics(t *testing.T) {
	ctx := context.Background()
	repo, cleanup := newRepo(t)
	defer cleanup()

	name := uniqueName(t, "cpu.usage")
	if _, err := repo.UpsertMetric(ctx, &storage.MetricDef{Name: name, Type: "gauge"}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	metrics, err := repo.ListMetrics(ctx, storage.MetricFilter{Name: name})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metrics) != 1 {
		t.Errorf("expected 1 metric, got %d", len(metrics))
	}
}

// TestDeadLetterInsert verifies that failed messages can be persisted for
// later inspection.
func TestDeadLetterInsert(t *testing.T) {
	ctx := context.Background()
	repo, cleanup := newRepo(t)
	defer cleanup()

	err := repo.InsertDeadLetter(ctx,
		"msg-001",
		`{"resourceMetrics":[]}`,
		"parse error: unexpected EOF",
		3,
	)
	if err != nil {
		t.Fatalf("insert dead letter: %v", err)
	}
}

// --- helpers ----------------------------------------------------------------

func newRepo(t *testing.T) (storage.Repository, func()) {
	t.Helper()
	ctx := context.Background()
	pool, err := storage.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to db: %v", err)
	}
	return storage.NewRepository(pool), func() { pool.Close() }
}

// uniqueName returns a metric name scoped to this test to prevent cross-test
// interference when running against a shared database.
func uniqueName(t *testing.T, base string) string {
	t.Helper()
	return t.Name() + "/" + base
}
