package consumer

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	collectorv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/judgment-labs/ingest/storage/internal/models"
	"github.com/judgment-labs/ingest/storage/internal/storage"
)

const headerIngestedAt = "ingested_at"

// Consumer reads metric batches from a Kafka topic and persists them to the
// storage repository. Failed records are retried up to MaxRetries times; after
// that they are written to the dead-letter table so no message is silently lost.
type Consumer struct {
	client     *kgo.Client
	repo       storage.Repository
	logger     *slog.Logger
	maxRetries int
}

func New(client *kgo.Client, repo storage.Repository, logger *slog.Logger, maxRetries int) *Consumer {
	return &Consumer{
		client:     client,
		repo:       repo,
		logger:     logger,
		maxRetries: maxRetries,
	}
}

// Run polls Kafka until ctx is cancelled. Offsets are committed after each
// batch regardless of individual record outcomes — poisoned messages go to
// the dead-letter table rather than halting the partition.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)

		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			c.logger.Error("poll error", "error", err)
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			c.processWithRetry(ctx, r)
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				c.logger.Error("commit offsets", "error", err)
			}
		}
	}
}

// processWithRetry attempts to handle a record up to maxRetries times.
// On final failure the record is written to the dead-letter table.
func (c *Consumer) processWithRetry(ctx context.Context, r *kgo.Record) {
	var lastErr error
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if err := c.handle(ctx, r); err != nil {
			lastErr = err
			c.logger.Warn("record processing failed",
				"topic", r.Topic,
				"partition", r.Partition,
				"offset", r.Offset,
				"attempt", attempt,
				"error", err,
			)
			continue
		}
		return
	}

	// All retries exhausted — write to dead-letter table and move on.
	c.logger.Error("record exceeded max retries, writing to dead-letter queue",
		"topic", r.Topic,
		"partition", r.Partition,
		"offset", r.Offset,
		"error", lastErr,
	)
	messageID := fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
	if err := c.repo.InsertDeadLetter(ctx, messageID, string(r.Value), lastErr.Error(), c.maxRetries); err != nil {
		c.logger.Error("failed to write dead letter", "message_id", messageID, "error", err)
	}
}

// handle decodes one Kafka record and persists its metrics.
func (c *Consumer) handle(ctx context.Context, r *kgo.Record) error {
	ingestedAt := extractIngestedAt(r)

	raw, err := base64.StdEncoding.DecodeString(string(r.Value))
	if err != nil {
		return fmt.Errorf("base64 decode: %w", err)
	}

	req := &collectorv1.ExportMetricsServiceRequest{}
	if err := proto.Unmarshal(raw, req); err != nil {
		return fmt.Errorf("proto unmarshal: %w", err)
	}

	return c.persist(ctx, req, ingestedAt)
}

// persist iterates the OTLP payload and writes every gauge/sum data point to
// the repository. Unsupported metric types are skipped with a warning.
func (c *Consumer) persist(ctx context.Context, req *collectorv1.ExportMetricsServiceRequest, ingestedAt time.Time) error {
	for _, rm := range req.ResourceMetrics {
		resourceAttrs := models.FlattenAttributes(rm.GetResource().GetAttributes())
		serviceName := models.ExtractServiceName(rm.GetResource().GetAttributes())

		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				metricType := models.TypeOf(m)
				if metricType == "" {
					c.logger.Warn("skipping unsupported metric type", "metric", m.Name)
					continue
				}

				def, err := c.repo.UpsertMetric(ctx, &storage.MetricDef{
					Name:        m.Name,
					Description: m.Description,
					Unit:        m.Unit,
					Type:        string(metricType),
				})
				if err != nil {
					return fmt.Errorf("upsert metric %q: %w", m.Name, err)
				}

				rawPoints, isMonotonic := models.DataPointsOf(m)
				points := make([]storage.DataPoint, 0, len(rawPoints))
				for _, dp := range rawPoints {
					points = append(points, storage.DataPoint{
						ServiceName:        serviceName,
						Value:              models.PointValue(dp),
						Timestamp:          models.PointTimestamp(dp),
						StartTimestamp:     models.PointStartTimestamp(dp),
						IngestionTimestamp: ingestedAt,
						ResourceAttributes: resourceAttrs,
						Attributes:         models.FlattenAttributes(dp.Attributes),
						IsMonotonic:        isMonotonic,
					})
				}

				if err := c.repo.InsertDataPoints(ctx, def.ID, points); err != nil {
					return fmt.Errorf("insert data points for %q: %w", m.Name, err)
				}
			}
		}
	}
	return nil
}

// extractIngestedAt reads the ingested_at header written by the ingest server.
// Falls back to now if the header is absent or unparseable.
func extractIngestedAt(r *kgo.Record) time.Time {
	for _, h := range r.Headers {
		if h.Key == headerIngestedAt {
			if t, err := time.Parse("2006-01-02T15:04:05.999999999Z", string(h.Value)); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Now().UTC()
}
