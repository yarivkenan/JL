package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Scheduler publishes a RuleCheckJob to Kafka for every loaded rule on each tick.
// The evaluator consumer processes these jobs independently, keeping scheduling
// and evaluation decoupled.
type Scheduler struct {
	rules    []Rule
	client   *kgo.Client
	topic    string
	interval time.Duration
}

// NewScheduler creates a Scheduler that publishes to the given Kafka topic.
func NewScheduler(rules []Rule, brokers []string, topic string, interval time.Duration) (*Scheduler, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Scheduler{
		rules:    rules,
		client:   client,
		topic:    topic,
		interval: interval,
	}, nil
}

// Run starts the scheduling loop. It blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			s.dispatch(ctx, t)
		}
	}
}

// dispatch publishes one RuleCheckJob per rule, all sharing the same evaluation
// timestamp so the evaluator sees a consistent snapshot.
func (s *Scheduler) dispatch(ctx context.Context, now time.Time) {
	now = now.UTC()
	for _, rule := range s.rules {
		d, err := time.ParseDuration(rule.Window.Duration)
		if err != nil {
			slog.Warn("invalid window duration, skipping rule",
				"rule", rule.Name, "duration", rule.Window.Duration, "error", err)
			continue
		}

		job := RuleCheckJob{
			Rule:        rule,
			EvaluateAt:  now,
			WindowStart: now.Add(-d),
			WindowEnd:   now,
		}
		payload, err := json.Marshal(job)
		if err != nil {
			slog.Error("marshal rule check job", "rule", rule.Name, "error", err)
			continue
		}

		// Use rule name as the record key so all checks for the same rule land
		// on the same partition, preserving ordering for status transitions.
		record := &kgo.Record{
			Topic: s.topic,
			Key:   []byte(rule.Name),
			Value: payload,
		}
		results := s.client.ProduceSync(ctx, record)
		if err := results.FirstErr(); err != nil {
			slog.Error("dispatch rule check", "rule", rule.Name, "error", err)
			continue
		}
		slog.Info("dispatched rule check",
			"rule", rule.Name,
			"window_start", job.WindowStart,
			"window_end", job.WindowEnd,
		)
	}
}

// Close shuts down the underlying Kafka client.
func (s *Scheduler) Close() {
	s.client.Close()
}

// LoadRules reads and parses a JSON array of Rule definitions from path.
func LoadRules(path string) ([]Rule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rules file %q: %w", path, err)
	}
	var rules []Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("parse rules file %q: %w", path, err)
	}
	return rules, nil
}
