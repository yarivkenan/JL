package models

import (
	"fmt"
	"strconv"
	"time"
)

// ExportMetricsServiceRequest is the top-level OTLP/HTTP metrics payload.
// Field names follow the protobuf JSON encoding (camelCase).
type ExportMetricsServiceRequest struct {
	ResourceMetrics []ResourceMetrics `json:"resourceMetrics"`
}

type ResourceMetrics struct {
	Resource     Resource       `json:"resource"`
	ScopeMetrics []ScopeMetrics `json:"scopeMetrics"`
}

type Resource struct {
	Attributes []KeyValue `json:"attributes"`
}

type ScopeMetrics struct {
	Scope   InstrumentationScope `json:"scope"`
	Metrics []Metric             `json:"metrics"`
}

type InstrumentationScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type Metric struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Unit        string `json:"unit"`
	Gauge       *Gauge `json:"gauge,omitempty"`
	Sum         *Sum   `json:"sum,omitempty"`
}

type Gauge struct {
	DataPoints []NumberDataPoint `json:"dataPoints"`
}

type Sum struct {
	DataPoints             []NumberDataPoint `json:"dataPoints"`
	IsMonotonic            bool              `json:"isMonotonic"`
	AggregationTemporality int               `json:"aggregationTemporality"`
}

// NumberDataPoint represents a single gauge or sum measurement.
// Timestamps are uint64 nanoseconds encoded as strings per the proto3 JSON spec.
type NumberDataPoint struct {
	Attributes        []KeyValue `json:"attributes"`
	StartTimeUnixNano string     `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string     `json:"timeUnixNano"`
	// Exactly one of AsDouble or AsInt will be set.
	AsDouble *float64 `json:"asDouble,omitempty"`
	AsInt    *string  `json:"asInt,omitempty"` // int64 encoded as string in proto3 JSON
}

// Value returns the data point's numeric value as a float64.
func (dp *NumberDataPoint) Value() (float64, error) {
	if dp.AsDouble != nil {
		return *dp.AsDouble, nil
	}
	if dp.AsInt != nil {
		v, err := strconv.ParseInt(*dp.AsInt, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse asInt %q: %w", *dp.AsInt, err)
		}
		return float64(v), nil
	}
	return 0, fmt.Errorf("data point has neither asDouble nor asInt")
}

// Timestamp parses TimeUnixNano into a time.Time.
func (dp *NumberDataPoint) Timestamp() (time.Time, error) {
	return parseUnixNano(dp.TimeUnixNano)
}

// StartTimestamp parses StartTimeUnixNano into a time.Time, returning nil if absent.
func (dp *NumberDataPoint) StartTimestamp() (*time.Time, error) {
	if dp.StartTimeUnixNano == "" {
		return nil, nil
	}
	t, err := parseUnixNano(dp.StartTimeUnixNano)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// KeyValue is an OTEL attribute key-value pair.
type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// AnyValue holds one of the supported OTEL attribute value types.
type AnyValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	IntValue    *string  `json:"intValue,omitempty"` // int64 as string in proto3 JSON
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
}

// FlattenAttributes converts a []KeyValue into a plain map suitable for JSONB storage.
func FlattenAttributes(attrs []KeyValue) map[string]any {
	result := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		result[kv.Key] = kv.Value.Unwrap()
	}
	return result
}

// Unwrap returns the underlying Go value for an AnyValue.
func (v AnyValue) Unwrap() any {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		if i, err := strconv.ParseInt(*v.IntValue, 10, 64); err == nil {
			return i
		}
		return *v.IntValue
	case v.DoubleValue != nil:
		return *v.DoubleValue
	case v.BoolValue != nil:
		return *v.BoolValue
	default:
		return nil
	}
}

// ExtractServiceName returns the value of the "service.name" resource attribute.
func ExtractServiceName(attrs []KeyValue) string {
	for _, kv := range attrs {
		if kv.Key == "service.name" && kv.Value.StringValue != nil {
			return *kv.Value.StringValue
		}
	}
	return ""
}

func parseUnixNano(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	ns, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse unix nano %q: %w", s, err)
	}
	return time.Unix(0, ns).UTC(), nil
}
