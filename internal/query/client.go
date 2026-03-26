package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// DataPoint mirrors the JSON shape returned by the query service's
// GET /v1/data_points endpoint.
type DataPoint struct {
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// Client talks to the query service over HTTP.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a Client pointing at the query service base URL
// (e.g. "http://query:8081").
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// FetchDataPoints calls GET /v1/data_points with the given filters and returns
// the decoded response.
func (c *Client) FetchDataPoints(ctx context.Context, metricName, serviceName string, since time.Time) ([]DataPoint, error) {
	u, err := url.Parse(c.baseURL + "/v1/data_points")
	if err != nil {
		return nil, fmt.Errorf("parse query service URL: %w", err)
	}

	q := u.Query()
	if metricName != "" {
		q.Set("metric_name", metricName)
	}
	if serviceName != "" {
		q.Set("service_name", serviceName)
	}
	if !since.IsZero() {
		q.Set("since", since.Format(time.RFC3339Nano))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query service request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query service returned %d", resp.StatusCode)
	}

	var points []DataPoint
	if err := json.NewDecoder(resp.Body).Decode(&points); err != nil {
		return nil, fmt.Errorf("decode data points: %w", err)
	}
	return points, nil
}

// Aggregate computes the requested aggregation over the given data points.
// Returns (value, hasData). hasData is false when points is empty.
func Aggregate(points []DataPoint, aggregation string) (float64, bool, error) {
	if len(points) == 0 {
		return 0, false, nil
	}

	switch aggregation {
	case "avg":
		var sum float64
		for _, p := range points {
			sum += p.Value
		}
		return sum / float64(len(points)), true, nil

	case "count":
		return float64(len(points)), true, nil

	case "p95":
		vals := make([]float64, len(points))
		for i, p := range points {
			vals[i] = p.Value
		}
		sort.Float64s(vals)
		idx := int(math.Ceil(0.95*float64(len(vals)))) - 1
		if idx < 0 {
			idx = 0
		}
		return vals[idx], true, nil

	case "value":
		// MAX — any single point breaching the threshold is caught.
		maxVal := points[0].Value
		for _, p := range points[1:] {
			if p.Value > maxVal {
				maxVal = p.Value
			}
		}
		return maxVal, true, nil

	default:
		return 0, false, fmt.Errorf("unknown aggregation %q: must be avg, p95, count, or value", aggregation)
	}
}
