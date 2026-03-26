//go:build e2e

package e2e_test

import (
	"context"
	"encoding/base64"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	collectorv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/judgment-labs/ingest/storage/internal/consumer"
	"github.com/judgment-labs/ingest/storage/internal/storage"
)

var (
	dbURL = func() string {
		if u := os.Getenv("DATABASE_URL"); u != "" {
			return u
		}
		return "postgres://otel:otel@localhost:5432/otel_metrics"
	}()

	kafkaBrokers = func() []string {
		if b := os.Getenv("KAFKA_BROKERS"); b != "" {
			return []string{b}
		}
		return []string{"localhost:19092"}
	}()

	kafkaTopic = func() string {
		if t := os.Getenv("KAFKA_TOPIC"); t != "" {
			return t
		}
		return "otel.metrics"
	}()
)

// TestMain runs migrations once before all tests so each test starts with a
// fully-provisioned schema.
func TestMain(m *testing.M) {
	if err := storage.RunMigrations(dbURL); err != nil {
		panic("migrations failed: " + err.Error())
	}
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// Repository tests
// ---------------------------------------------------------------------------

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
			Timestamp:          now,                     // recent
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

// ---------------------------------------------------------------------------
// Consumer tests
// ---------------------------------------------------------------------------

// TestConsumerPersistsFromKafka is the full end-to-end flow test:
// produce an OTLP message → consumer reads from Kafka → data lands in DB.
func TestConsumerPersistsFromKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	metricName := uniqueName(t, "http.request.duration")
	publishOTLPMessage(t, metricName, "checkout-service", 250.0)

	repo, cleanup := newRepo(t)
	defer cleanup()

	// Use a test-scoped consumer group so offset tracking is isolated.
	c := newConsumer(t, "e2e-"+t.Name(), repo)

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	go c.Run(consumerCtx) //nolint:errcheck

	// Poll the DB until the data point appears or the deadline is exceeded.
	if !pollUntil(ctx, 200*time.Millisecond, func() bool {
		points, err := repo.QueryDataPoints(context.Background(), storage.DataPointFilter{
			MetricName: metricName,
			Limit:      1,
		})
		return err == nil && len(points) == 1
	}) {
		t.Fatal("data point never appeared in database after consumer processed Kafka message")
	}

	// Verify the data point fields were decoded correctly.
	points, _ := repo.QueryDataPoints(context.Background(), storage.DataPointFilter{
		MetricName: metricName,
		Limit:      1,
	})
	if points[0].ServiceName != "checkout-service" {
		t.Errorf("service_name: got %q, want %q", points[0].ServiceName, "checkout-service")
	}
	if points[0].Value != 250.0 {
		t.Errorf("value: got %v, want 250.0", points[0].Value)
	}
}

// TestConsumerDeadLettersInvalidMessage verifies that a message which cannot
// be decoded is retried then written to the dead-letter table, and that the
// consumer continues processing subsequent valid messages without stalling.
func TestConsumerDeadLettersInvalidMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce one poisoned message followed by one valid one.
	publishRawMessage(t, "not-valid-base64!!!")
	validMetricName := uniqueName(t, "healthy.metric")
	publishOTLPMessage(t, validMetricName, "payment-service", 42.0)

	repo, cleanup := newRepo(t)
	defer cleanup()

	pool, err := storage.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open pool for DLQ query: %v", err)
	}
	defer pool.Close()

	c := newConsumer(t, "e2e-dlq-"+t.Name(), repo)

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	go c.Run(consumerCtx) //nolint:errcheck

	// The valid message must land in data_points — proving the consumer
	// was not stalled by the bad message.
	if !pollUntil(ctx, 200*time.Millisecond, func() bool {
		points, err := repo.QueryDataPoints(context.Background(), storage.DataPointFilter{
			MetricName: validMetricName,
			Limit:      1,
		})
		return err == nil && len(points) == 1
	}) {
		t.Fatal("consumer stalled: valid message never processed after encountering a bad one")
	}

	// The poisoned message must be in the dead-letter table.
	var count int
	err = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM dead_letter_queue WHERE error LIKE '%base64%'`,
	).Scan(&count)
	if err != nil {
		t.Fatalf("query dead_letter_queue: %v", err)
	}
	if count == 0 {
		t.Error("expected poisoned message in dead_letter_queue, found none")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newRepo(t *testing.T) (storage.Repository, func()) {
	t.Helper()
	ctx := context.Background()
	pool, err := storage.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to db: %v", err)
	}
	return storage.NewRepository(pool), func() { pool.Close() }
}

// newConsumer wires a Consumer with its own Kafka consumer group.
func newConsumer(t *testing.T, group string, repo storage.Repository) *consumer.Consumer {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(kafkaTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("create kafka client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return consumer.New(client, repo, slog.Default(), 3)
}

// publishOTLPMessage produces a single-gauge OTLP batch to Kafka, replicating
// the exact wire format written by the ingest server (PR #1).
func publishOTLPMessage(t *testing.T, metricName, serviceName string, value float64) {
	t.Helper()

	req := &collectorv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key:   "service.name",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: serviceName}},
						},
					},
				},
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: metricName,
								Data: &metricsv1.Metric_Gauge{
									Gauge: &metricsv1.Gauge{
										DataPoints: []*metricsv1.NumberDataPoint{
											{
												TimeUnixNano: uint64(time.Now().UnixNano()),
												Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: value},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	publishRawMessage(t, base64.StdEncoding.EncodeToString(raw))
}

// publishRawMessage produces a record with the given raw value, mirroring the
// ingest server's header format so the consumer can decode it.
func publishRawMessage(t *testing.T, value string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...))
	if err != nil {
		t.Fatalf("create kafka producer: %v", err)
	}
	defer client.Close()

	record := &kgo.Record{
		Topic: kafkaTopic,
		Value: []byte(value),
		Headers: []kgo.RecordHeader{
			{Key: "payload", Value: []byte("proto")},
			{Key: "ingested_at", Value: []byte(time.Now().UTC().Format("2006-01-02T15:04:05.999999999Z"))},
		},
	}

	if err := client.ProduceSync(context.Background(), record).FirstErr(); err != nil {
		t.Fatalf("produce to kafka: %v", err)
	}
}

// pollUntil calls cond every interval until it returns true or ctx expires.
func pollUntil(ctx context.Context, interval time.Duration, cond func() bool) bool {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if cond() {
				return true
			}
		}
	}
}

// uniqueName returns a metric name scoped to this test to prevent cross-test
// interference when running against a shared database.
func uniqueName(t *testing.T, base string) string {
	t.Helper()
	return t.Name() + "/" + base
}
