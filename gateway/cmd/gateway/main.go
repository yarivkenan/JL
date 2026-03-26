package main

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"
)

func main() {
	addr := envOr("GATEWAY_ADDR", ":8080")
	ingestURL := envOr("INGEST_URL", "http://ingest:4317")
	queryURL := envOr("QUERY_URL", "http://query:8081")

	ingest, err := url.Parse(ingestURL)
	if err != nil {
		slog.Error("invalid INGEST_URL", "err", err)
		os.Exit(1)
	}
	query, err := url.Parse(queryURL)
	if err != nil {
		slog.Error("invalid QUERY_URL", "err", err)
		os.Exit(1)
	}

	ingestProxy := httputil.NewSingleHostReverseProxy(ingest)
	queryProxy := httputil.NewSingleHostReverseProxy(query)

	mux := http.NewServeMux()

	// Write path: POST /v1/metrics → ingest service.
	mux.HandleFunc("POST /v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		ingestProxy.ServeHTTP(w, r)
	})

	// Read path: GET /v1/* → query service.
	mux.HandleFunc("GET /v1/", func(w http.ResponseWriter, r *http.Request) {
		queryProxy.ServeHTTP(w, r)
	})

	// Local health check.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	slog.Info("gateway starting", "addr", addr, "ingest", ingestURL, "query", queryURL)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
