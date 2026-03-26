//go:build e2e

package e2e_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"testing"
)

var serverURL = func() string {
	if u := os.Getenv("INGEST_URL"); u != "" {
		return u
	}
	return "http://localhost:4317"
}()

// validGaugePayload is a minimal OTLP JSON payload carrying a single gauge
// data point for the "http.request.duration" metric on "checkout-service".
const validGaugePayload = `{
	"resourceMetrics": [{
		"resource": {
			"attributes": [{
				"key": "service.name",
				"value": {"stringValue": "checkout-service"}
			}]
		},
		"scopeMetrics": [{
			"metrics": [{
				"name": "http.request.duration",
				"gauge": {
					"dataPoints": [{
						"asDouble": 123.4,
						"timeUnixNano": "1700000000000000000"
					}]
				}
			}]
		}]
	}]
}`

// validSumPayload carries a monotonic Sum metric.
const validSumPayload = `{
	"resourceMetrics": [{
		"resource": {
			"attributes": [{
				"key": "service.name",
				"value": {"stringValue": "payment-service"}
			}]
		},
		"scopeMetrics": [{
			"metrics": [{
				"name": "requests.total",
				"sum": {
					"dataPoints": [{
						"asInt": "42",
						"timeUnixNano": "1700000000000000000"
					}],
					"aggregationTemporality": 2,
					"isMonotonic": true
				}
			}]
		}]
	}]
}`

// histogramPayload carries a Histogram metric — unsupported by the ingest
// server (gauge and sum only), so we expect a 400.
const histogramPayload = `{
	"resourceMetrics": [{
		"resource": {},
		"scopeMetrics": [{
			"metrics": [{
				"name": "response.size",
				"histogram": {
					"dataPoints": [{
						"count": "10",
						"sum": 500,
						"timeUnixNano": "1700000000000000000"
					}],
					"aggregationTemporality": 2
				}
			}]
		}]
	}]
}`

func TestIngestGauge(t *testing.T) {
	resp := postMetrics(t, validGaugePayload, "application/json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestIngestSum(t *testing.T) {
	resp := postMetrics(t, validSumPayload, "application/json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestIngestUnsupportedMetricType(t *testing.T) {
	resp := postMetrics(t, histogramPayload, "application/json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for histogram, got %d", resp.StatusCode)
	}
}

func TestIngestWrongContentType(t *testing.T) {
	resp := postMetrics(t, validGaugePayload, "text/plain")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", resp.StatusCode)
	}
}

func TestIngestEmptyBody(t *testing.T) {
	resp := postMetrics(t, "", "application/json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty body, got %d", resp.StatusCode)
	}
}

func TestIngestResponseIsValidJSON(t *testing.T) {
	resp := postMetrics(t, validGaugePayload, "application/json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
}

// postMetrics is a test helper that POSTs payload to /v1/metrics.
func postMetrics(t *testing.T, payload, contentType string) *http.Response {
	t.Helper()

	resp, err := http.Post(
		serverURL+"/v1/metrics",
		contentType,
		bytes.NewBufferString(payload),
	)
	if err != nil {
		t.Fatalf("POST /v1/metrics: %v", err)
	}
	return resp
}
