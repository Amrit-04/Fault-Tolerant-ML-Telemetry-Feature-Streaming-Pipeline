package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Amrit-04/telemetry-ingestor-go/internal/dlq"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/model"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/worker"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server sets up the HTTP router and endpoints.
type Server struct {
	pool      *worker.Pool
	dlqWorker *dlq.Worker
	router    *chi.Mux
}

// NewServer builds the HTTP router with middleware and endpoints.
func NewServer(p *worker.Pool, d *dlq.Worker) *Server {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(middleware.Heartbeat("/ping"))

	s := &Server{
		pool:      p,
		dlqWorker: d,
		router:    r,
	}

	s.routes()
	return s
}

// Router returns the chi router.
func (s *Server) Router() http.Handler {
	return s.router
}

func (s *Server) routes() {
	// Prometheus metrics
	s.router.Handle("/metrics", promhttp.Handler())

	// Health and readiness probes
	s.router.Get("/health", s.handleHealth)
	s.router.Get("/ready", s.handleReady)

	// High-throughput Telemetry Ingestion API
	s.router.Post("/api/v1/telemetry/ingest", s.handleIngest)

	// Admin DLQ Re-Drive API
	s.router.Post("/admin/dlq/retry", s.handleDLQRetry)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok","timestamp":"` + time.Now().UTC().Format(time.RFC3339) + `"}`))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready","queue_depth":` + strconv.Itoa(s.pool.QueueDepth()) + `}`))
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	// Restrict payload to 1MB max
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1048576))
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(model.IngestResponse{
			Status:  "rejected",
			Message: "Payload exceeds 1MB limit or read error",
		})
		return
	}

	isAsync := r.URL.Query().Get("async") == "true" || r.Header.Get("X-Async") == "true"

	if isAsync {
		job := &worker.IngestJob{
			Ctx:     context.Background(),
			Payload: body,
			Done:    nil,
			Async:   true,
		}

		if err := s.pool.Submit(job); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(model.IngestResponse{
				Status:  "rate_limited",
				Message: "Worker pool capacity saturated: backpressure triggered",
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(model.IngestResponse{
			Status:  "queued",
			Message: "Event accepted for asynchronous validation and streaming",
		})
		return
	}

	// Synchronous path with bounded channel response
	doneChan := make(chan worker.IngestResult, 1)
	job := &worker.IngestJob{
		Ctx:     r.Context(),
		Payload: body,
		Done:    doneChan,
		Async:   false,
	}

	if err := s.pool.Submit(job); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(model.IngestResponse{
			Status:  "rate_limited",
			Message: "Worker pool buffer full",
		})
		return
	}

	select {
	case res := <-doneChan:
		w.Header().Set("Content-Type", "application/json")
		if !res.Success {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(model.IngestResponse{
				Status:  "rejected",
				Message: res.ErrCode + ": " + res.ErrMsg,
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(model.IngestResponse{
			Status:    "ingested",
			EventID:   res.Event.EventID,
			Partition: res.Partition,
			Message:   "Telemetry event successfully ingested and routed",
		})

	case <-time.After(3 * time.Second):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		_ = json.NewEncoder(w).Encode(model.IngestResponse{
			Status:  "timeout",
			Message: "Ingestion pipeline timed out waiting for Kafka dispatch",
		})
	}
}

func (s *Server) handleDLQRetry(w http.ResponseWriter, r *http.Request) {
	if s.dlqWorker == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "DLQ worker is disabled or offline"})
		return
	}

	var req model.DLQRetryRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	resp := s.dlqWorker.ReDrive(r.Context(), req)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
