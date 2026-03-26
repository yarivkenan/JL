package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yarivkenan/JL/internal/query"
	"github.com/yarivkenan/JL/internal/rules"
	"github.com/yarivkenan/JL/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := loadConfig()

	ruleList, err := rules.LoadRules(cfg.RulesFile)
	if err != nil {
		slog.Error("load rules", "error", err)
		os.Exit(1)
	}
	slog.Info("loaded rules", "count", len(ruleList))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := store.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := store.NewAlertRepository(pool)
	queryClient := query.NewClient(cfg.QueryServiceURL)

	scheduler, err := rules.NewScheduler(ruleList, cfg.KafkaBrokers, cfg.RuleChecksTopic, cfg.Interval)
	if err != nil {
		slog.Error("create scheduler", "error", err)
		os.Exit(1)
	}
	defer scheduler.Close()

	evaluator, err := rules.NewEvaluator(cfg.KafkaBrokers, cfg.RuleChecksTopic, cfg.ConsumerGroup, repo, queryClient)
	if err != nil {
		slog.Error("create evaluator", "error", err)
		os.Exit(1)
	}
	defer evaluator.Close()

	slog.Info("rules engine started",
		"rules", len(ruleList),
		"interval", cfg.Interval,
		"topic", cfg.RuleChecksTopic,
		"query_service", cfg.QueryServiceURL,
	)

	go scheduler.Run(ctx)
	go evaluator.Run(ctx)

	<-ctx.Done()
	slog.Info("shutting down rules engine")
}

type config struct {
	KafkaBrokers    []string
	RuleChecksTopic string
	ConsumerGroup   string
	DatabaseURL     string
	QueryServiceURL string
	RulesFile       string
	Interval        time.Duration
}

func loadConfig() config {
	interval, err := time.ParseDuration(getEnv("RULES_INTERVAL", "30s"))
	if err != nil {
		interval = 30 * time.Second
	}
	return config{
		KafkaBrokers:    splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		RuleChecksTopic: getEnv("RULES_CHECKS_TOPIC", "otel.rule-checks"),
		ConsumerGroup:   getEnv("RULES_CONSUMER_GROUP", "rules-engine"),
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://otel:otel@localhost:5432/otel_metrics"),
		QueryServiceURL: getEnv("QUERY_SERVICE_URL", "http://localhost:8081"),
		RulesFile:       getEnv("RULES_FILE", "/etc/rules/rules.json"),
		Interval:        interval,
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
