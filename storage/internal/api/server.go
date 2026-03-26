package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/yarivkenan/JL/storage/internal/storage"
)

// Server exposes a read-only HTTP API over the storage layer.
type Server struct {
	repo storage.Repository
}

// New creates a Server backed by the given repository.
func New(repo storage.Repository) *Server {
	return &Server{repo: repo}
}

// Handler returns an http.Handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /v1/data_points", s.dataPoints)
	mux.HandleFunc("GET /v1/dead_letters", s.deadLetters)
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) dataPoints(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := storage.DataPointFilter{
		MetricName:  q.Get("metric_name"),
		ServiceName: q.Get("service_name"),
	}
	if since := q.Get("since"); since != "" {
		if t, err := time.Parse(time.RFC3339Nano, since); err == nil {
			filter.Since = t
		}
	}
	if limit := q.Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			filter.Limit = l
		}
	}

	points, err := s.repo.QueryDataPoints(r.Context(), filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if points == nil {
		points = []*storage.DataPoint{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(points)
}

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			limit = n
		}
	}
	letters, err := s.repo.ListDeadLetters(r.Context(), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if letters == nil {
		letters = []*storage.DeadLetter{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(letters)
}
