// Package client provides an HTTP client for the query service API.
//
// When backend endpoints change, update this package. The rest of the CLI
// depends only on the types and methods defined here, so changes are isolated
// to a single location.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client talks to the query service over HTTP.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a Client pointing at the given query service base URL.
func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

// --- Metrics ----------------------------------------------------------------

// MetricsListOpts are the optional filters for ListMetrics.
type MetricsListOpts struct {
	Name string
}

// ListMetrics returns metric definitions, optionally filtered by name.
func (c *Client) ListMetrics(ctx context.Context, opts MetricsListOpts) ([]MetricDef, error) {
	q := url.Values{}
	if opts.Name != "" {
		q.Set("name", opts.Name)
	}
	var out []MetricDef
	if err := c.get(ctx, "/v1/metrics", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// DataPointsListOpts are the optional filters for ListDataPoints.
type DataPointsListOpts struct {
	MetricName  string
	ServiceName string
	Since       string // Go duration ("1h") or RFC3339
	Limit       int
}

// ListDataPoints returns data points matching the given filters.
func (c *Client) ListDataPoints(ctx context.Context, opts DataPointsListOpts) ([]DataPoint, error) {
	q := url.Values{}
	if opts.MetricName != "" {
		q.Set("metric_name", opts.MetricName)
	}
	if opts.ServiceName != "" {
		q.Set("service_name", opts.ServiceName)
	}
	if opts.Since != "" {
		q.Set("since", opts.Since)
	}
	if opts.Limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", opts.Limit))
	}
	var out []DataPoint
	if err := c.get(ctx, "/v1/data_points", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// --- Rules ------------------------------------------------------------------

// ListRules returns all defined rules.
func (c *Client) ListRules(ctx context.Context) ([]Rule, error) {
	var out []Rule
	if err := c.get(ctx, "/v1/rules", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// CreateRule creates a new rule and returns the created record.
func (c *Client) CreateRule(ctx context.Context, rule *Rule) (*Rule, error) {
	var out Rule
	if err := c.post(ctx, "/v1/rules", rule, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteRule removes a rule by name. Returns nil on success.
func (c *Client) DeleteRule(ctx context.Context, name string) error {
	u := c.baseURL + "/v1/rules/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("rule %q not found", name)
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	return nil
}

// --- Alerts -----------------------------------------------------------------

// AlertsListOpts are the optional filters for ListAlerts.
type AlertsListOpts struct {
	Rule   string
	Status string
	Since  string
	Limit  int
}

// ListAlerts returns alerts matching the given filters.
func (c *Client) ListAlerts(ctx context.Context, opts AlertsListOpts) ([]Alert, error) {
	q := url.Values{}
	if opts.Rule != "" {
		q.Set("rule", opts.Rule)
	}
	if opts.Status != "" {
		q.Set("status", opts.Status)
	}
	if opts.Since != "" {
		q.Set("since", opts.Since)
	}
	if opts.Limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", opts.Limit))
	}
	var out []Alert
	if err := c.get(ctx, "/v1/alerts", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetAlert returns a single alert by ID.
func (c *Client) GetAlert(ctx context.Context, id string) (*Alert, error) {
	var out Alert
	if err := c.get(ctx, "/v1/alerts/"+url.PathEscape(id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- Helpers ----------------------------------------------------------------

func (c *Client) get(ctx context.Context, path string, query url.Values, dest any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	return c.do(req, dest)
}

func (c *Client) post(ctx context.Context, path string, body, dest any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, dest)
}

func (c *Client) do(req *http.Request, dest any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	if dest != nil && len(body) > 0 {
		if err := json.Unmarshal(body, dest); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
