package models

import (
	"time"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// MetricType mirrors the OTEL metric data types we support.
type MetricType string

const (
	MetricTypeGauge MetricType = "gauge"
	MetricTypeSum   MetricType = "sum"
)

// TypeOf returns the MetricType for a proto Metric, or "" if unsupported.
func TypeOf(m *metricsv1.Metric) MetricType {
	switch m.Data.(type) {
	case *metricsv1.Metric_Gauge:
		return MetricTypeGauge
	case *metricsv1.Metric_Sum:
		return MetricTypeSum
	default:
		return ""
	}
}

// DataPointsOf extracts all NumberDataPoints from a proto Metric along with
// sum-specific fields. Returns nil for unsupported metric types.
func DataPointsOf(m *metricsv1.Metric) ([]*metricsv1.NumberDataPoint, *bool) {
	switch d := m.Data.(type) {
	case *metricsv1.Metric_Gauge:
		return d.Gauge.DataPoints, nil
	case *metricsv1.Metric_Sum:
		mono := d.Sum.IsMonotonic
		return d.Sum.DataPoints, &mono
	default:
		return nil, nil
	}
}

// PointValue returns the numeric value of a data point as float64.
func PointValue(dp *metricsv1.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *metricsv1.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricsv1.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// PointTimestamp converts a data point's TimeUnixNano to time.Time.
func PointTimestamp(dp *metricsv1.NumberDataPoint) time.Time {
	return time.Unix(0, int64(dp.TimeUnixNano)).UTC()
}

// PointStartTimestamp converts StartTimeUnixNano to *time.Time, nil if zero.
func PointStartTimestamp(dp *metricsv1.NumberDataPoint) *time.Time {
	if dp.StartTimeUnixNano == 0 {
		return nil
	}
	t := time.Unix(0, int64(dp.StartTimeUnixNano)).UTC()
	return &t
}

// FlattenAttributes converts proto KeyValue attributes to a plain map
// suitable for JSONB storage and GIN-indexed containment queries.
func FlattenAttributes(attrs []*commonv1.KeyValue) map[string]any {
	result := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		result[kv.Key] = unwrapAnyValue(kv.Value)
	}
	return result
}

// ExtractServiceName returns the value of the "service.name" resource attribute.
func ExtractServiceName(attrs []*commonv1.KeyValue) string {
	for _, kv := range attrs {
		if kv.Key == "service.name" {
			if sv, ok := kv.Value.Value.(*commonv1.AnyValue_StringValue); ok {
				return sv.StringValue
			}
		}
	}
	return ""
}

func unwrapAnyValue(v *commonv1.AnyValue) any {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return val.StringValue
	case *commonv1.AnyValue_IntValue:
		return val.IntValue
	case *commonv1.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonv1.AnyValue_BoolValue:
		return val.BoolValue
	default:
		return nil
	}
}
