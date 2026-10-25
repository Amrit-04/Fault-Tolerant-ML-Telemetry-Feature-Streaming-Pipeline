package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Amrit-04/telemetry-ingestor-go/internal/dlq"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/model"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/worker"
)

func setupTestServer() (*Server, *worker.Pool, *dlq.Worker) {
	// Worker pool without live kafka (producer = nil handles in-memory pass-through for test)
	p := worker.NewPool(4, 100, nil)
	d := dlq.NewWorker(dlq.Config{WorkerCount: 1}, nil)
	s := NewServer(p, d)
	return s, p, d
}

func TestHealthEndpoint(t *testing.T) {
	s, p, _ := setupTestServer()
	defer p.Stop()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
}

func TestReadyEndpoint(t *testing.T) {
	s, p, _ := setupTestServer()
	defer p.Stop()

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	s, p, _ := setupTestServer()
	defer p.Stop()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("active_goroutines_count")) {
		t.Fatalf("expected active_goroutines_count in metrics output")
	}
}

func TestIngestEndpoint_ValidSync(t *testing.T) {
	s, p, _ := setupTestServer()
	defer p.Stop()

	validPayload := `{
		"event_id": "a0000000-0000-0000-0000-000000000001",
		"entity_id": "sensor-device-alpha",
		"timestamp": "2026-10-25T11:00:00Z",
		"feature_vector": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
		"schema_version": 1
	}`

	req := httptest.NewRequest("POST", "/api/v1/telemetry/ingest", bytes.NewBufferString(validPayload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp model.IngestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ingested" {
		t.Fatalf("expected status ingested, got %s", resp.Status)
	}
	if resp.EventID != "a0000000-0000-0000-0000-000000000001" {
		t.Fatalf("expected event_id match, got %s", resp.EventID)
	}
}

func TestIngestEndpoint_InvalidPayload(t *testing.T) {
	s, p, _ := setupTestServer()
	defer p.Stop()

	invalidPayload := `{
		"event_id": "invalid-uuid-format",
		"entity_id": "sensor-device-alpha",
		"timestamp": "2026-10-25T11:00:00Z",
		"feature_vector": [1.0, 2.0],
		"schema_version": 1
	}`

	req := httptest.NewRequest("POST", "/api/v1/telemetry/ingest", bytes.NewBufferString(invalidPayload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", w.Code)
	}

	var resp model.IngestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "rejected" {
		t.Fatalf("expected status rejected, got %s", resp.Status)
	}
}

func TestIngestEndpoint_AsyncAccepted(t *testing.T) {
	s, p, _ := setupTestServer()
	defer p.Stop()

	payload := `{
		"event_id": "a0000000-0000-0000-0000-000000000002",
		"entity_id": "sensor-device-beta",
		"timestamp": "2026-10-25T11:00:00Z",
		"feature_vector": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
		"schema_version": 1
	}`

	req := httptest.NewRequest("POST", "/api/v1/telemetry/ingest?async=true", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", w.Code)
	}

	var resp model.IngestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "queued" {
		t.Fatalf("expected status queued, got %s", resp.Status)
	}
}

func TestDLQRetryEndpoint(t *testing.T) {
	s, p, _ := setupTestServer()
	defer p.Stop()

	retryReq := `{"limit": 10}`
	req := httptest.NewRequest("POST", "/admin/dlq/retry", bytes.NewBufferString(retryReq))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	var resp model.DLQRetryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}
