package queue

import (
	"context"
	"time"
)

// Message represents a single ingested OTLP metrics batch to be processed.
type Message struct {
	// Payload is the binary-encoded ExportMetricsServiceRequest proto.
	// Consumers re-parse this to access the full metric tree.
	Payload []byte

	// IngestedAt is the wall-clock time the ingest server received the batch.
	// This is distinct from the OTel timestamps on individual data points.
	IngestedAt time.Time
}

// Publisher is the interface for publishing ingested metric batches to the queue.
// Implementations can be swapped (Redis, NATS, Kafka, etc.) without touching
// handler code.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
	Close() error
}
