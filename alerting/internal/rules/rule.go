package rules

import "time"

// Rule defines a threshold condition to evaluate against stored metrics.
// The schema mirrors the example in the spec; all fields are JSON-serialisable
// so the full rule can be embedded in a Kafka message.
type Rule struct {
	Name       string    `json:"name"`
	MetricName string    `json:"metric_name"`
	Filter     Filter    `json:"filter,omitempty"`
	Condition  Condition `json:"condition"`
	Window     Window    `json:"window"`
	Action     Action    `json:"action"`
}

// Filter is a flat map of label key→value pairs.
// The special key "service.name" is routed to the service_name column;
// all other keys are matched via JSONB containment on dp.attributes.
type Filter map[string]string

// Condition specifies how to aggregate and compare data point values.
type Condition struct {
	Aggregation string  `json:"aggregation"` // avg | p95 | count | value
	Operator    string  `json:"operator"`    // > | >= | < | <=
	Threshold   float64 `json:"threshold"`
}

// Window defines the look-back period for aggregation.
type Window struct {
	Type     string `json:"type"`     // "time" (the only supported type)
	Duration string `json:"duration"` // Go duration string, e.g. "5m", "1h"
}

// Action describes the response when the condition fires.
type Action struct {
	Type     string `json:"type"`               // "alert"
	Severity string `json:"severity,omitempty"` // "info" | "warning" | "critical"
	Message  string `json:"message,omitempty"`
}

// RuleCheckJob is the Kafka message the scheduler publishes to otel.rule-checks.
// Embedding the full Rule means the evaluator is stateless — it doesn't need
// to look up the rule definition separately.
type RuleCheckJob struct {
	Rule        Rule      `json:"rule"`
	EvaluateAt  time.Time `json:"evaluate_at"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
}
