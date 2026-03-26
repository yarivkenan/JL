package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
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
	mux.HandleFunc("GET /v1/metrics", s.metrics)
	mux.HandleFunc("GET /v1/data_points", s.dataPoints)
	mux.HandleFunc("GET /v1/dead_letters", s.deadLetters)
	mux.HandleFunc("GET /v1/rules", s.listRules)
	mux.HandleFunc("POST /v1/rules", s.createRule)
	mux.HandleFunc("DELETE /v1/rules/{name}", s.deleteRule)
	mux.HandleFunc("GET /v1/alerts", s.listAlerts)
	mux.HandleFunc("GET /v1/alerts/{id}", s.getAlert)
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	filter := storage.MetricFilter{
		Name: r.URL.Query().Get("name"),
	}
	metrics, err := s.repo.ListMetrics(r.Context(), filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if metrics == nil {
		metrics = []*storage.MetricDef{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (s *Server) dataPoints(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := storage.DataPointFilter{
		MetricName:  q.Get("metric_name"),
		ServiceName: q.Get("service_name"),
	}
	if since := q.Get("since"); since != "" {
		// Accept either a Go duration ("1h", "30m") or RFC3339.
		if d, err := time.ParseDuration(since); err == nil {
			filter.Since = time.Now().UTC().Add(-d)
		} else if t, err := time.Parse(time.RFC3339Nano, since); err == nil {
			filter.Since = t
		}
	}
	if limit := q.Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			filter.Limit = l
		}
	}
	// attr=key:value (repeatable) — AND semantics via JSONB containment.
	if attrs := q["attr"]; len(attrs) > 0 {
		filter.Attributes = make(map[string]any, len(attrs))
		for _, kv := range attrs {
			if k, v, ok := strings.Cut(kv, ":"); ok {
				filter.Attributes[k] = v
			}
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

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	rules, err := s.repo.ListRules(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if rules == nil {
		rules = []*storage.Rule{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rules)
}

func (s *Server) createRule(w http.ResponseWriter, r *http.Request) {
	var rule storage.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if rule.Name == "" || rule.MetricName == "" {
		http.Error(w, "name and metric_name are required", http.StatusBadRequest)
		return
	}
	created, err := s.repo.CreateRule(r.Context(), &rule)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			http.Error(w, "rule with this name already exists", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(created)
}

func (s *Server) deleteRule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	deleted, err := s.repo.DeleteRule(r.Context(), name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !deleted {
		http.Error(w, "rule not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := storage.AlertFilter{
		RuleName: q.Get("rule"),
		Status:   q.Get("status"),
	}
	if since := q.Get("since"); since != "" {
		if d, err := time.ParseDuration(since); err == nil {
			filter.Since = time.Now().UTC().Add(-d)
		} else if t, err := time.Parse(time.RFC3339Nano, since); err == nil {
			filter.Since = t
		}
	}
	if limit := q.Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			filter.Limit = l
		}
	}

	alerts, err := s.repo.ListAlerts(r.Context(), filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if alerts == nil {
		alerts = []*storage.Alert{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alerts)
}

func (s *Server) getAlert(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid alert ID", http.StatusBadRequest)
		return
	}
	alert, err := s.repo.GetAlert(r.Context(), id)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			http.Error(w, "alert not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alert)
}
