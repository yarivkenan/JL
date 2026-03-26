package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/judgment-labs/ingest/internal/config"
	"github.com/judgment-labs/ingest/storage/internal/consumer"
	"github.com/judgment-labs/ingest/storage/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	// Apply schema migrations before accepting any messages. Goose is
	// idempotent so this is safe to run on every startup.
	if err := storage.RunMigrations(cfg.DatabaseURL); err != nil {
		slog.Error("migrations failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := storage.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.KafkaTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		slog.Error("create kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	repo := storage.NewRepository(pool)
	c := consumer.New(kafkaClient, repo, logger, cfg.MaxRetries)

	slog.Info("consumer started",
		"topic", cfg.KafkaTopic,
		"group", cfg.ConsumerGroup,
		"max_retries", cfg.MaxRetries,
	)

	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("consumer stopped")
}
