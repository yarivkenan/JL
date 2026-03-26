//go:build e2e

package e2e_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	collectorv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

var (
	storageAddr = envOr("STORAGE_ADDR", "localhost:8081")
	kafkaBrokers = strings.Split(envOr("KAFKA_BROKERS", "localhost:19092"), ",")
	kafkaTopic  = envOr("KAFKA_TOPIC", "otel.metrics")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestMain waits for the consumer's HTTP API to become healthy before running tests.
func TestMain(m *testing.M) {
	waitForAPI()
	os.Exit(m.Run())
}

func waitForAPI() {
	deadline := time.Now().Add(120 * time.Second)
	url := "http://" + storageAddr + "/healthz"
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	panic("storage API never became ready at " + storageAddr)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHealthz(t *testing.T) {
	resp, err := http.Get(storageURL("/healthz"))
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestConsumerPersistsGaugeMetric(t *testing.T) {
	metricName := uniqueName(t, "http.request.duration")
	publishOTLPGauge(t, metricName, "checkout-service", 250.0, time.Now())

	points := pollDataPoints(t, metricName, "", 30*time.Second)
	if len(points) == 0 {
		t.Fatal("data point never appeared")
	}
	if points[0].ServiceName != "checkout-service" {
		t.Errorf("service_name: got %q, want %q", points[0].ServiceName, "checkout-service")
	}
	if points[0].Value != 250.0 {
		t.Errorf("value: got %v, want 250.0", points[0].Value)
	}
}

func TestConsumerQueryByService(t *testing.T) {
	metricName := uniqueName(t, "http.request.duration")
	publishOTLPGauge(t, metricName, "svc-a", 1.0, time.Now())
	publishOTLPGauge(t, metricName, "svc-b", 2.0, time.Now())

	// Wait for both to land.
	pollDataPointsN(t, metricName, "", 2, 30*time.Second)

	// Query only svc-a.
	points := getDataPoints(t, metricName, "svc-a", "", 10)
	if len(points) != 1 {
		t.Fatalf("expected 1 point for svc-a, got %d", len(points))
	}
	if points[0].Value != 1.0 {
		t.Errorf("value: got %v, want 1.0", points[0].Value)
	}
}

func TestConsumerQuerySinceFilter(t *testing.T) {
	metricName := uniqueName(t, "http.request.duration")
	now := time.Now()
	publishOTLPGauge(t, metricName, "svc", 1.0, now)
	publishOTLPGauge(t, metricName, "svc", 2.0, now.Add(-10*time.Minute))

	// Wait for both points to land.
	pollDataPointsN(t, metricName, "", 2, 30*time.Second)

	// Query only the recent 5 minutes.
	since := now.Add(-5 * time.Minute).Format(time.RFC3339Nano)
	points := getDataPoints(t, metricName, "", since, 10)
	if len(points) != 1 {
		t.Fatalf("since filter: expected 1 point, got %d", len(points))
	}
	if points[0].Value != 1.0 {
		t.Errorf("expected value 1.0, got %v", points[0].Value)
	}
}

func TestConsumerDeadLettersInvalidMessage(t *testing.T) {
	// Publish a poisoned message followed by a valid one.
	publishRawMessage(t, "not-valid-base64!!!")
	validName := uniqueName(t, "healthy.metric")
	publishOTLPGauge(t, validName, "payment-service", 42.0, time.Now())

	// The valid message must land — proves the consumer wasn't stalled.
	points := pollDataPoints(t, validName, "", 30*time.Second)
	if len(points) == 0 {
		t.Fatal("consumer stalled: valid message never processed after bad one")
	}

	// The poisoned message must appear in the dead-letter queue.
	letters := pollDeadLetters(t, "base64", 30*time.Second)
	if len(letters) == 0 {
		t.Error("expected poisoned message in dead_letter_queue, found none")
	}
}

// ---------------------------------------------------------------------------
// Query API tests
// ---------------------------------------------------------------------------

func TestListMetrics(t *testing.T) {
	metricName := uniqueName(t, "http.request.duration")
	publishOTLPGauge(t, metricName, "svc", 1.0, time.Now())

	// Wait for the consumer to persist.
	pollDataPoints(t, metricName, "", 30*time.Second)

	metrics := getMetrics(t, "")
	var found bool
	for _, m := range metrics {
		if m.Name == metricName {
			found = true
			if m.Type != "gauge" {
				t.Errorf("type: got %q, want %q", m.Type, "gauge")
			}
		}
	}
	if !found {
		t.Errorf("metric %q not found in GET /v1/metrics", metricName)
	}
}

func TestListMetricsByName(t *testing.T) {
	name1 := uniqueName(t, "cpu.usage")
	name2 := uniqueName(t, "mem.usage")
	publishOTLPGauge(t, name1, "svc", 1.0, time.Now())
	publishOTLPGauge(t, name2, "svc", 2.0, time.Now())

	pollDataPoints(t, name1, "", 30*time.Second)
	pollDataPoints(t, name2, "", 30*time.Second)

	metrics := getMetrics(t, name1)
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}
	if metrics[0].Name != name1 {
		t.Errorf("got %q, want %q", metrics[0].Name, name1)
	}
}

func TestQueryDataPointsAttrFilter(t *testing.T) {
	metricName := uniqueName(t, "http.request.duration")
	now := time.Now()
	publishOTLPGaugeWithAttrs(t, metricName, "svc", 100.0, now,
		map[string]string{"method": "GET"})
	publishOTLPGaugeWithAttrs(t, metricName, "svc", 200.0, now.Add(-time.Second),
		map[string]string{"method": "POST"})

	pollDataPointsN(t, metricName, "", 2, 30*time.Second)

	url := fmt.Sprintf("%s/v1/data_points?metric_name=%s&attr=method:GET", storageURL(""), metricName)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var points []dataPoint
	if err := json.Unmarshal(body, &points); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("attr filter: expected 1 point, got %d", len(points))
	}
	if points[0].Value != 100.0 {
		t.Errorf("expected value 100.0, got %v", points[0].Value)
	}
}

func TestQueryDataPointsSinceDuration(t *testing.T) {
	metricName := uniqueName(t, "http.request.duration")
	now := time.Now()
	publishOTLPGauge(t, metricName, "svc", 1.0, now)
	publishOTLPGauge(t, metricName, "svc", 2.0, now.Add(-10*time.Minute))

	pollDataPointsN(t, metricName, "", 2, 30*time.Second)

	// Use duration-style since ("5m") instead of RFC3339.
	points := getDataPoints(t, metricName, "", "5m", 10)
	if len(points) != 1 {
		t.Fatalf("duration since: expected 1 point, got %d", len(points))
	}
	if points[0].Value != 1.0 {
		t.Errorf("expected value 1.0, got %v", points[0].Value)
	}
}

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

type metricDef struct {
	ID          string    `json:"ID"`
	Name        string    `json:"Name"`
	Description string    `json:"Description"`
	Unit        string    `json:"Unit"`
	Type        string    `json:"Type"`
	CreatedAt   time.Time `json:"CreatedAt"`
}

type dataPoint struct {
	ID                 string         `json:"id"`
	MetricID           string         `json:"metric_id"`
	MetricName         string         `json:"metric_name"`
	ServiceName        string         `json:"service_name"`
	Value              float64        `json:"value"`
	Timestamp          time.Time      `json:"timestamp"`
	IngestionTimestamp time.Time      `json:"ingestion_timestamp"`
	ResourceAttributes map[string]any `json:"resource_attributes"`
	Attributes         map[string]any `json:"attributes"`
}

type deadLetter struct {
	ID        string    `json:"id"`
	MessageID string    `json:"message_id"`
	Payload   string    `json:"payload"`
	Error     string    `json:"error"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"created_at"`
}

func storageURL(path string) string {
	return "http://" + storageAddr + path
}

func getMetrics(t *testing.T, name string) []metricDef {
	t.Helper()
	url := storageURL("/v1/metrics")
	if name != "" {
		url += "?name=" + name
	}
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /v1/metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/metrics returned %d: %s", resp.StatusCode, body)
	}
	var metrics []metricDef
	if err := json.Unmarshal(body, &metrics); err != nil {
		t.Fatalf("unmarshal metrics: %v\nbody: %s", err, body)
	}
	return metrics
}

func getDataPoints(t *testing.T, metricName, serviceName, since string, limit int) []dataPoint {
	t.Helper()
	url := fmt.Sprintf("%s/v1/data_points?metric_name=%s&limit=%d", storageURL(""), metricName, limit)
	if serviceName != "" {
		url += "&service_name=" + serviceName
	}
	if since != "" {
		url += "&since=" + since
	}
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /v1/data_points: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/data_points returned %d: %s", resp.StatusCode, body)
	}
	var points []dataPoint
	if err := json.Unmarshal(body, &points); err != nil {
		t.Fatalf("unmarshal data_points: %v\nbody: %s", err, body)
	}
	return points
}

func getDeadLetters(t *testing.T, limit int) []deadLetter {
	t.Helper()
	url := fmt.Sprintf("%s/v1/dead_letters?limit=%d", storageURL(""), limit)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /v1/dead_letters: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/dead_letters returned %d: %s", resp.StatusCode, body)
	}
	var letters []deadLetter
	if err := json.Unmarshal(body, &letters); err != nil {
		t.Fatalf("unmarshal dead_letters: %v\nbody: %s", err, body)
	}
	return letters
}

// pollDataPoints polls the API until at least one data point with the given
// metric name appears, or the timeout is reached.
func pollDataPoints(t *testing.T, metricName, serviceName string, timeout time.Duration) []dataPoint {
	t.Helper()
	return pollDataPointsN(t, metricName, serviceName, 1, timeout)
}

func pollDataPointsN(t *testing.T, metricName, serviceName string, n int, timeout time.Duration) []dataPoint {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		points := getDataPoints(t, metricName, serviceName, "", 100)
		if len(points) >= n {
			return points
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d data points (metric=%s)", n, metricName)
	return nil
}

// pollDeadLetters polls until at least one dead letter whose error contains
// the given substring appears.
func pollDeadLetters(t *testing.T, errorSubstring string, timeout time.Duration) []deadLetter {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := getDeadLetters(t, 100)
		var matched []deadLetter
		for _, dl := range all {
			if strings.Contains(dl.Error, errorSubstring) {
				matched = append(matched, dl)
			}
		}
		if len(matched) > 0 {
			return matched
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for dead letter containing %q", errorSubstring)
	return nil
}

// ---------------------------------------------------------------------------
// Kafka helpers
// ---------------------------------------------------------------------------

func publishOTLPGauge(t *testing.T, metricName, serviceName string, value float64, ts time.Time) {
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
												TimeUnixNano: uint64(ts.UnixNano()),
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

func publishOTLPGaugeWithAttrs(t *testing.T, metricName, serviceName string, value float64, ts time.Time, attrs map[string]string) {
	t.Helper()

	dpAttrs := make([]*commonv1.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		dpAttrs = append(dpAttrs, &commonv1.KeyValue{
			Key:   k,
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
		})
	}

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
												TimeUnixNano: uint64(ts.UnixNano()),
												Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: value},
												Attributes:   dpAttrs,
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

func uniqueName(t *testing.T, base string) string {
	t.Helper()
	return t.Name() + "/" + base
}
