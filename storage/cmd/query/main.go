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

	"github.com/yarivkenan/JL/storage/internal/api"
	"github.com/yarivkenan/JL/storage/internal/config"
	"github.com/yarivkenan/JL/storage/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.LoadQuery()

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

	repo := storage.NewRepository(pool)
	srv := &http.Server{
		Addr:         cfg.Addr,
		Handler:      api.New(repo).Handler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("query server starting", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("query server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()

	slog.Info("shutting down query server")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	}
}
