package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yarivkenan/JL/alerting/internal/store"
)

// Evaluator consumes RuleCheckJob messages from Kafka, queries the database,
// and writes an alert record for every evaluation (firing or resolved).
type Evaluator struct {
	client *kgo.Client
	repo   *store.AlertRepository
}

// NewEvaluator creates an Evaluator subscribed to topic using the given consumer group.
func NewEvaluator(brokers []string, topic, groupID string, repo *store.AlertRepository) (*Evaluator, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Evaluator{client: client, repo: repo}, nil
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

// evaluate deserialises one RuleCheckJob, queries the DB, and writes an alert.
func (e *Evaluator) evaluate(ctx context.Context, payload []byte) error {
	var job RuleCheckJob
	if err := json.Unmarshal(payload, &job); err != nil {
		return fmt.Errorf("unmarshal job: %w", err)
	}

	rule := job.Rule

	aq := store.AggregateQuery{
		MetricName:  rule.MetricName,
		Aggregation: rule.Condition.Aggregation,
		Since:       job.WindowStart,
		Until:       job.WindowEnd,
	}

	// "service.name" maps to the dedicated service_name column;
	// all other filter keys are JSONB attribute matches.
	extraAttrs := map[string]any{}
	for k, v := range rule.Filter {
		if k == "service.name" {
			aq.ServiceName = v
		} else {
			extraAttrs[k] = v
		}
	}
	if len(extraAttrs) > 0 {
		aq.Attributes = extraAttrs
	}

	value, hasData, err := e.repo.QueryAggregate(ctx, aq)
	if err != nil {
		return fmt.Errorf("query aggregate for rule %q: %w", rule.Name, err)
	}
	if !hasData {
		slog.Debug("no data points in window, skipping evaluation", "rule", rule.Name)
		return nil
	}

	status := "resolved"
	if compare(value, rule.Condition.Operator, rule.Condition.Threshold) {
		status = "firing"
	}

	alert := store.AlertRecord{
		RuleName:       rule.Name,
		MetricName:     rule.MetricName,
		EvaluatedValue: value,
		Threshold:      rule.Condition.Threshold,
		FiredAt:        job.EvaluateAt,
		Status:         status,
	}
	if err := e.repo.InsertAlert(ctx, alert); err != nil {
		return fmt.Errorf("insert alert for rule %q: %w", rule.Name, err)
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
