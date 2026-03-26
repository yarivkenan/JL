package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/judgment-labs/ingest/internal/config"
	"github.com/judgment-labs/ingest/internal/handler"
	"github.com/judgment-labs/ingest/internal/queue"
	"github.com/judgment-labs/ingest/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	publisher, err := queue.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		slog.Error("failed to create Kafka publisher", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	metricsHandler := handler.NewMetricsHandler(publisher)
	srv := server.New(cfg.ServerAddr, metricsHandler)

	// Start serving in the background.
	go func() {
		slog.Info("starting ingest server", "addr", cfg.ServerAddr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Block until SIGTERM or SIGINT, then gracefully drain in-flight requests.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	slog.Info("shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	}
}
