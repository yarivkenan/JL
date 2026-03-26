package queue

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	fieldPayload    = "payload"
	fieldIngestedAt = "ingested_at"
)

// KafkaPublisher publishes metric batches to a Kafka topic.
// Compatible with Redpanda, which speaks the Kafka protocol natively.
//
// Message format: each Kafka record carries two headers (payload, ingested_at)
// and a base64-encoded proto payload as the record value, so the consumer
// can decode without needing a schema registry.
type KafkaPublisher struct {
	client *kgo.Client
	topic  string
}

// NewKafkaPublisher creates a publisher that writes to the given topic.
// brokers is a comma-separated list of broker addresses (e.g. "localhost:9092").
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		// RequiredAcks(kgo.LeaderAck) is the default — good balance of
		// durability and throughput for a single-broker Redpanda setup.
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}
	return &KafkaPublisher{client: client, topic: topic}, nil
}

// Publish sends a metric batch to Kafka synchronously.
// Using ProduceSync keeps error handling simple; switch to buffered async
// producing if throughput becomes a bottleneck.
func (p *KafkaPublisher) Publish(ctx context.Context, msg Message) error {
	record := &kgo.Record{
		Topic: p.topic,
		Value: []byte(base64.StdEncoding.EncodeToString(msg.Payload)),
		Headers: []kgo.RecordHeader{
			{Key: fieldPayload, Value: []byte("proto")},
			{Key: fieldIngestedAt, Value: []byte(msg.IngestedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"))},
		},
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce to topic %q: %w", p.topic, err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	p.client.Close()
	return nil
}
