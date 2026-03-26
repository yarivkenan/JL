package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yarivkenan/JL/internal/query"
	"github.com/yarivkenan/JL/internal/store"
)

// Evaluator consumes RuleCheckJob messages from Kafka, queries the query
// service for metric data, evaluates alert conditions, and writes an alert
// record for every evaluation (firing or resolved).
type Evaluator struct {
	client      *kgo.Client
	repo        *store.AlertRepository
	queryClient *query.Client
}

// NewEvaluator creates an Evaluator subscribed to topic using the given consumer group.
func NewEvaluator(brokers []string, topic, groupID string, repo *store.AlertRepository, queryClient *query.Client) (*Evaluator, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Evaluator{client: client, repo: repo, queryClient: queryClient}, nil
}

// Run polls Kafka and evaluates each RuleCheckJob until ctx is cancelled.
func (e *Evaluator) Run(ctx context.Context) {
	for {
		fetches := e.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Error("fetch error", "topic", t, "partition", p, "error", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if err := e.evaluate(ctx, r.Value); err != nil {
				slog.Error("evaluate rule check", "error", err)
			}
		})
	}
}

// evaluate deserialises one RuleCheckJob, queries the query service, and writes an alert.
func (e *Evaluator) evaluate(ctx context.Context, payload []byte) error {
	var job RuleCheckJob
	if err := json.Unmarshal(payload, &job); err != nil {
		return fmt.Errorf("unmarshal job: %w", err)
	}

	rule := job.Rule

	serviceName := rule.Filter["service.name"]

	points, err := e.queryClient.FetchDataPoints(ctx, rule.MetricName, serviceName, job.WindowStart)
	if err != nil {
		return fmt.Errorf("fetch data points for rule %q: %w", rule.Name, err)
	}

	// Filter to the evaluation window (query service supports since but not until).
	var filtered []query.DataPoint
	for _, p := range points {
		if !p.Timestamp.After(job.WindowEnd) {
			filtered = append(filtered, p)
		}
	}

	value, hasData, err := query.Aggregate(filtered, rule.Condition.Aggregation)
	if err != nil {
		return fmt.Errorf("aggregate for rule %q: %w", rule.Name, err)
	}
	if !hasData {
		slog.Debug("no data points in window, skipping evaluation", "rule", rule.Name)
		return nil
	}

	firing := compare(value, rule.Condition.Operator, rule.Condition.Threshold)

	dp := store.AlertDataPoint{
		RuleName:       rule.Name,
		MetricName:     rule.MetricName,
		ServiceName:    serviceName,
		EvaluatedValue: value,
		Threshold:      rule.Condition.Threshold,
		Severity:       rule.Action.Severity,
		Timestamp:      job.EvaluateAt,
		Firing:         firing,
	}
	if err := e.repo.WriteAlertDataPoint(ctx, dp); err != nil {
		return fmt.Errorf("write alert data point for rule %q: %w", rule.Name, err)
	}

	status := "resolved"
	if firing {
		status = "firing"
	}
	slog.Info("rule evaluated",
		"rule", rule.Name,
		"aggregation", rule.Condition.Aggregation,
		"value", value,
		"operator", rule.Condition.Operator,
		"threshold", rule.Condition.Threshold,
		"status", status,
	)
	return nil
}

// Close shuts down the Kafka consumer client.
func (e *Evaluator) Close() {
	e.client.Close()
}

// compare applies the rule operator to (value op threshold).
func compare(value float64, op string, threshold float64) bool {
	switch op {
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	case "=", "==":
		return value == threshold
	default:
		slog.Warn("unknown operator, condition treated as false", "operator", op)
		return false
	}
}
