package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yarivkenan/JL/storage/internal/api"
	"github.com/yarivkenan/JL/storage/internal/config"
	"github.com/yarivkenan/JL/storage/internal/consumer"
	"github.com/yarivkenan/JL/storage/internal/storage"
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

	// Start the query API server so e2e tests and dashboards can read data.
	apiServer := &http.Server{
		Addr:    cfg.StorageAddr,
		Handler: api.New(repo).Handler(),
	}
	go func() {
		slog.Info("api server starting", "addr", cfg.StorageAddr)
		if err := apiServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("api server error", "error", err)
		}
	}()

	slog.Info("consumer started",
		"topic", cfg.KafkaTopic,
		"group", cfg.ConsumerGroup,
		"max_retries", cfg.MaxRetries,
	)

	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}

	apiServer.Shutdown(context.Background())
	slog.Info("consumer stopped")
}
