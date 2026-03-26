package handler

import (
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/yarivkenan/JL/ingest/internal/otel"
	"github.com/yarivkenan/JL/ingest/internal/queue"
	collectorv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MetricsHandler handles POST /v1/metrics OTLP ingestion requests.
type MetricsHandler struct {
	publisher queue.Publisher
}

func NewMetricsHandler(publisher queue.Publisher) *MetricsHandler {
	return &MetricsHandler{publisher: publisher}
}

func (h *MetricsHandler) HandleExport(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		http.Error(w, "request body is empty", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	req, err := otel.Parse(contentType, body)
	if err != nil {
		if errors.Is(err, otel.ErrUnsupportedContentType) {
			http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
			return
		}
		http.Error(w, "failed to parse OTLP payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := otel.Validate(req); err != nil {
		http.Error(w, "invalid OTLP payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Re-encode to canonical binary proto before enqueuing so the consumer
	// always gets a consistent format regardless of what Content-Type arrived.
	payload, err := proto.Marshal(req)
	if err != nil {
		http.Error(w, "internal error encoding payload", http.StatusInternalServerError)
		return
	}

	if err := h.publisher.Publish(r.Context(), queue.Message{
		Payload:    payload,
		IngestedAt: time.Now().UTC(),
	}); err != nil {
		http.Error(w, "failed to enqueue metrics batch", http.StatusServiceUnavailable)
		return
	}

	writeOTLPResponse(w, contentType)
}

// writeOTLPResponse writes a successful ExportMetricsServiceResponse in the
// same encoding the client used (proto or JSON), per the OTLP spec.
func writeOTLPResponse(w http.ResponseWriter, contentType string) {
	resp := &collectorv1.ExportMetricsServiceResponse{}

	var (
		out []byte
		ct  string
	)

	if contentType == "application/x-protobuf" {
		out, _ = proto.Marshal(resp)
		ct = "application/x-protobuf"
	} else {
		out, _ = protojson.Marshal(resp)
		ct = "application/json"
	}

	w.Header().Set("Content-Type", ct)
	w.WriteHeader(http.StatusOK)
	w.Write(out) //nolint:errcheck
}
