package otel

import (
	"errors"
	"fmt"
	"strings"

	collectorv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Parse deserializes an OTLP ExportMetricsServiceRequest from either binary
// protobuf or JSON protobuf, determined by the Content-Type header value.
func Parse(contentType string, body []byte) (*collectorv1.ExportMetricsServiceRequest, error) {
	req := &collectorv1.ExportMetricsServiceRequest{}

	switch {
	case strings.Contains(contentType, "application/x-protobuf"):
		if err := proto.Unmarshal(body, req); err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
		}
	case strings.Contains(contentType, "application/json"):
		if err := protojson.Unmarshal(body, req); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	default:
		return nil, ErrUnsupportedContentType
	}

	return req, nil
}

// Validate checks that the request is well-formed and contains only
// supported metric types (gauge and sum).
func Validate(req *collectorv1.ExportMetricsServiceRequest) error {
	if len(req.ResourceMetrics) == 0 {
		return errors.New("request contains no resource metrics")
	}

	for _, rm := range req.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if err := validateMetricType(m); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func validateMetricType(m *metricsv1.Metric) error {
	switch m.Data.(type) {
	case *metricsv1.Metric_Gauge:
		return nil
	case *metricsv1.Metric_Sum:
		return nil
	case nil:
		return fmt.Errorf("metric %q has no data", m.Name)
	default:
		return fmt.Errorf("metric %q has unsupported type; only gauge and sum are accepted", m.Name)
	}
}

// ErrUnsupportedContentType is returned when the Content-Type is not a
// recognized OTLP media type.
var ErrUnsupportedContentType = errors.New("unsupported Content-Type: must be application/x-protobuf or application/json")
