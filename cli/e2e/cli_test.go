//go:build e2e

// Package e2e_test exercises the CLI's API client and the rules/alerts
// endpoints on a live query service (docker-compose).
//
// Run: go test -v -tags e2e ./cli/e2e/...
// Requires: docker compose up (query service on :8081)
package e2e_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/yarivkenan/JL/cli/internal/client"
)

var apiClient *client.Client

func TestMain(m *testing.M) {
	serverURL := envOr("JL_SERVER", "http://localhost:8081")
	apiClient = client.New(serverURL)
	waitForAPI(serverURL + "/healthz")
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// Rules CRUD
// ---------------------------------------------------------------------------

func TestRulesCreateListDelete(t *testing.T) {
	name := "e2e-rule-" + t.Name()

	// Create
	rule, err := apiClient.CreateRule(context.Background(), &client.Rule{
		Name:       name,
		MetricName: "http.request.duration",
		Condition: map[string]any{
			"aggregation": "avg",
			"operator":    ">",
			"threshold":   float64(500),
		},
		Window: map[string]any{
			"type":     "time",
			"duration": "5m",
		},
		Action: map[string]any{
			"type":     "alert",
			"severity": "warning",
		},
	})
	if err != nil {
		t.Fatalf("CreateRule: %v", err)
	}
	if rule.Name != name {
		t.Errorf("name: got %q, want %q", rule.Name, name)
	}
	if rule.ID == "" {
		t.Error("expected non-empty ID")
	}

	// List and verify it exists
	rules, err := apiClient.ListRules(context.Background())
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	found := false
	for _, r := range rules {
		if r.Name == name {
			found = true
			if r.MetricName != "http.request.duration" {
				t.Errorf("metric_name: got %q, want %q", r.MetricName, "http.request.duration")
			}
			th, _ := r.Condition["threshold"].(float64)
			if th != 500 {
				t.Errorf("threshold: got %v, want 500", th)
			}
		}
	}
	if !found {
		t.Errorf("rule %q not found in ListRules", name)
	}

	// Delete
	if err := apiClient.DeleteRule(context.Background(), name); err != nil {
		t.Fatalf("DeleteRule: %v", err)
	}

	// Verify deleted
	rules, err = apiClient.ListRules(context.Background())
	if err != nil {
		t.Fatalf("ListRules after delete: %v", err)
	}
	for _, r := range rules {
		if r.Name == name {
			t.Error("rule still exists after delete")
		}
	}
}

func TestRulesCreateDuplicate(t *testing.T) {
	name := "e2e-dup-" + t.Name()
	rule := &client.Rule{
		Name:       name,
		MetricName: "cpu.usage",
		Condition:  map[string]any{"operator": ">", "threshold": float64(90)},
		Window:     map[string]any{"type": "time", "duration": "1m"},
		Action:     map[string]any{"type": "alert"},
	}

	_, err := apiClient.CreateRule(context.Background(), rule)
	if err != nil {
		t.Fatalf("first CreateRule: %v", err)
	}
	t.Cleanup(func() {
		apiClient.DeleteRule(context.Background(), name)
	})

	_, err = apiClient.CreateRule(context.Background(), rule)
	if err == nil {
		t.Fatal("expected error on duplicate rule creation, got nil")
	}
}

func TestRulesDeleteNotFound(t *testing.T) {
	err := apiClient.DeleteRule(context.Background(), "nonexistent-rule-42")
	if err == nil {
		t.Error("expected error deleting nonexistent rule, got nil")
	}
}

func TestRulesCreateMissingFields(t *testing.T) {
	_, err := apiClient.CreateRule(context.Background(), &client.Rule{
		Name: "", // missing required field
	})
	if err == nil {
		t.Error("expected error on empty name, got nil")
	}
}

// ---------------------------------------------------------------------------
// Metrics (via API client)
// ---------------------------------------------------------------------------

func TestMetricsList(t *testing.T) {
	// This test just verifies the client can talk to the existing metrics
	// endpoint. Metric data is seeded by the storage e2e tests or prior runs.
	metrics, err := apiClient.ListMetrics(context.Background(), client.MetricsListOpts{})
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	// We don't assert on content since we can't guarantee what's been ingested,
	// but we verify the call succeeds and returns a valid slice.
	_ = metrics
}

func TestMetricsListByName(t *testing.T) {
	metrics, err := apiClient.ListMetrics(context.Background(), client.MetricsListOpts{
		Name: "nonexistent.metric.name.e2e",
	})
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	if len(metrics) != 0 {
		t.Errorf("expected 0 metrics for nonexistent name, got %d", len(metrics))
	}
}

func TestDataPointsList(t *testing.T) {
	points, err := apiClient.ListDataPoints(context.Background(), client.DataPointsListOpts{
		Limit: 5,
	})
	if err != nil {
		t.Fatalf("ListDataPoints: %v", err)
	}
	if len(points) > 5 {
		t.Errorf("expected at most 5 points, got %d", len(points))
	}
}

func TestDataPointsFilterSince(t *testing.T) {
	points, err := apiClient.ListDataPoints(context.Background(), client.DataPointsListOpts{
		Since: "1h",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListDataPoints with since: %v", err)
	}
	_ = points
}

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

func TestAlertsListEmpty(t *testing.T) {
	// The alerts table may be empty or have data from prior runs.
	// We just verify the endpoint works with various filters.
	alerts, err := apiClient.ListAlerts(context.Background(), client.AlertsListOpts{})
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	_ = alerts
}

func TestAlertsListFilterStatus(t *testing.T) {
	alerts, err := apiClient.ListAlerts(context.Background(), client.AlertsListOpts{
		Status: "firing",
	})
	if err != nil {
		t.Fatalf("ListAlerts status=firing: %v", err)
	}
	for _, a := range alerts {
		if a.Status != "firing" {
			t.Errorf("expected status firing, got %q", a.Status)
		}
	}
}

func TestAlertsListFilterRule(t *testing.T) {
	alerts, err := apiClient.ListAlerts(context.Background(), client.AlertsListOpts{
		Rule: "nonexistent-rule-e2e",
	})
	if err != nil {
		t.Fatalf("ListAlerts rule filter: %v", err)
	}
	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts for nonexistent rule, got %d", len(alerts))
	}
}

func TestAlertsListFilterSince(t *testing.T) {
	alerts, err := apiClient.ListAlerts(context.Background(), client.AlertsListOpts{
		Since: "1h",
	})
	if err != nil {
		t.Fatalf("ListAlerts since=1h: %v", err)
	}
	_ = alerts
}

func TestAlertGetNotFound(t *testing.T) {
	_, err := apiClient.GetAlert(context.Background(), "00000000-0000-0000-0000-000000000000")
	if err == nil {
		t.Error("expected error for nonexistent alert ID, got nil")
	}
}

func TestAlertGetInvalidID(t *testing.T) {
	_, err := apiClient.GetAlert(context.Background(), "not-a-uuid")
	if err == nil {
		t.Error("expected error for invalid UUID, got nil")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func waitForAPI(url string) {
	deadline := time.Now().Add(120 * time.Second)
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
	panic("query service never became ready at " + url)
}
