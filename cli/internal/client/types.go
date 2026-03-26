package client

import "time"

// MetricDef mirrors the query service's metric definition response.
type MetricDef struct {
	ID          string    `json:"id"`
	Name        string    `json:"Name"`
	Description string    `json:"Description"`
	Unit        string    `json:"Unit"`
	Type        string    `json:"Type"`
	CreatedAt   time.Time `json:"CreatedAt"`
}

// DataPoint mirrors the query service's data point response.
type DataPoint struct {
	ID                 string         `json:"id"`
	MetricID           string         `json:"metric_id"`
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

// Rule mirrors the query service's rule response.
type Rule struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	MetricName  string         `json:"metric_name"`
	ServiceName string         `json:"service_name,omitempty"`
	Condition   map[string]any `json:"condition"`
	Window      map[string]any `json:"window"`
	Action      map[string]any `json:"action"`
	CreatedAt   time.Time      `json:"created_at"`
}

// Alert mirrors the query service's alert response.
type Alert struct {
	ID             string     `json:"id"`
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
