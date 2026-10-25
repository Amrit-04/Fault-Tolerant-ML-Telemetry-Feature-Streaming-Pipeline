package dlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Amrit-04/telemetry-ingestor-go/internal/kafka"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/metrics"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/model"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/validator"
	kafkaGo "github.com/segmentio/kafka-go"
)

// CategorizeError maps error codes to high-level error categories.
func CategorizeError(code string) string {
	c := strings.ToUpper(code)
	switch {
	case strings.Contains(c, "SYNTAX"), strings.Contains(c, "SERIALIZATION"), strings.Contains(c, "EMPTY"):
		return "SERIALIZATION_ERROR"
	case strings.Contains(c, "TIMEOUT"), strings.Contains(c, "DEADLINE"):
		return "TIMEOUT_ERROR"
	case strings.Contains(c, "SCHEMA"), strings.Contains(c, "VECTOR"), strings.Contains(c, "UUID"), strings.Contains(c, "EXTRA"):
		return "SCHEMA_VALIDATION_ERROR"
	default:
		return "PIPELINE_ERROR"
	}
}

// Config defines settings for DLQ worker and audit consumer.
type Config struct {
	Brokers       []string
	DLQTopic      string
	GroupID       string
	PostgresURL   string
	WorkerCount   int
	PollBatchSize int
}

// Worker consumes from telemetry.dlq, audits errors to PostgreSQL, and enables re-drive replay.
type Worker struct {
	cfg       Config
	reader    *kafkaGo.Reader
	producer  *kafka.Producer
	db        *sql.DB
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.RWMutex
	inMemDLQ  []model.DLQAuditRecord // Fallback memory store when postgres is offline
	maxInMem  int
}

// NewWorker creates and initializes the DLQ consumer worker.
func NewWorker(cfg Config, producer *kafka.Producer) *Worker {
	if cfg.GroupID == "" {
		cfg.GroupID = "telemetry-dlq-audit-group"
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 4
	}
	if cfg.PollBatchSize <= 0 {
		cfg.PollBatchSize = 100
	}

	ctx, cancel := context.WithCancel(context.Background())

	var db *sql.DB
	if cfg.PostgresURL != "" {
		var err error
		db, err = sql.Open("postgres", cfg.PostgresURL)
		if err != nil {
			log.Printf("[DLQ] Warning: unable to connect to Postgres for audit logging: %v", err)
			db = nil
		} else {
			db.SetMaxOpenConns(10)
			db.SetMaxIdleConns(5)
			db.SetConnMaxLifetime(5 * time.Minute)
			initSchema(db)
		}
	}

	var reader *kafkaGo.Reader
	if len(cfg.Brokers) > 0 && cfg.DLQTopic != "" {
		reader = kafkaGo.NewReader(kafkaGo.ReaderConfig{
			Brokers:        cfg.Brokers,
			Topic:          cfg.DLQTopic,
			GroupID:        cfg.GroupID,
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			CommitInterval: 1 * time.Second,
			StartOffset:    kafkaGo.FirstOffset,
		})
	}

	w := &Worker{
		cfg:      cfg,
		reader:   reader,
		producer: producer,
		db:       db,
		ctx:      ctx,
		cancel:   cancel,
		maxInMem: 10000,
	}

	return w
}

func initSchema(db *sql.DB) {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS dlq_audit_events (
		id BIGSERIAL PRIMARY KEY,
		event_id VARCHAR(64),
		error_code VARCHAR(64) NOT NULL,
		error_category VARCHAR(64) NOT NULL,
		error_message TEXT,
		source_offset VARCHAR(64),
		raw_payload TEXT NOT NULL,
		rejected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
		retried_at TIMESTAMP WITH TIME ZONE,
		status VARCHAR(32) DEFAULT 'PENDING'
	);
	CREATE INDEX IF NOT EXISTS idx_dlq_audit_err_code ON dlq_audit_events (error_code);
	CREATE INDEX IF NOT EXISTS idx_dlq_audit_status ON dlq_audit_events (status);
	`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
		log.Printf("[DLQ] Warning: failed to initialize dlq_audit_events table: %v", err)
	}
}

// Start begins consuming from the DLQ topic with concurrent workers.
func (w *Worker) Start() {
	if w.reader == nil {
		log.Println("[DLQ] Worker started without Kafka reader (standalone mode).")
		return
	}

	for i := 0; i < w.cfg.WorkerCount; i++ {
		w.wg.Add(1)
		go w.consumeLoop(i)
	}
	log.Printf("[DLQ] Worker started with %d consumers on topic %s", w.cfg.WorkerCount, w.cfg.DLQTopic)
}

func (w *Worker) consumeLoop(workerID int) {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			msg, err := w.reader.FetchMessage(w.ctx)
			if err != nil {
				if w.ctx.Err() != nil {
					return
				}
				time.Sleep(200 * time.Millisecond)
				continue
			}

			w.processDLQMessage(msg)
			if err := w.reader.CommitMessages(w.ctx, msg); err != nil {
				log.Printf("[DLQ] Worker %d error committing offset %d: %v", workerID, msg.Offset, err)
			}
		}
	}
}

func (w *Worker) processDLQMessage(msg kafkaGo.Message) {
	var env map[string]interface{}
	_ = json.Unmarshal(msg.Value, &env)

	errorCode := "ERR_UNKNOWN"
	errorMsg := "Unspecified DLQ failure"
	sourceOffset := fmt.Sprintf("partition:%d,offset:%d", msg.Partition, msg.Offset)
	rawPayload := string(msg.Value)

	if c, ok := env["error_code"].(string); ok && c != "" {
		errorCode = c
	}
	if m, ok := env["error_message"].(string); ok && m != "" {
		errorMsg = m
	}
	if o, ok := env["source_offset"].(string); ok && o != "" {
		sourceOffset = o
	}
	if p, ok := env["raw_payload"].(string); ok && p != "" {
		rawPayload = p
	}

	category := CategorizeError(errorCode)

	record := model.DLQAuditRecord{
		ErrorCode:     errorCode,
		ErrorCategory: category,
		ErrorMessage:  errorMsg,
		SourceOffset:  sourceOffset,
		RawPayload:    rawPayload,
		RejectedAt:    time.Now().UTC(),
		Status:        "PENDING",
	}

	// Try extracting event_id from payload if present
	var partial struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal([]byte(rawPayload), &partial); err == nil && partial.EventID != "" {
		record.EventID = partial.EventID
	}

	w.persistAuditRecord(record)
}

func (w *Worker) persistAuditRecord(rec model.DLQAuditRecord) {
	if w.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		query := `
		INSERT INTO dlq_audit_events (event_id, error_code, error_category, error_message, source_offset, raw_payload, rejected_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
		_, err := w.db.ExecContext(ctx, query,
			rec.EventID, rec.ErrorCode, rec.ErrorCategory, rec.ErrorMessage, rec.SourceOffset, rec.RawPayload, rec.RejectedAt, rec.Status,
		)
		if err == nil {
			return
		}
		log.Printf("[DLQ] Warning: Postgres audit insert failed: %v, falling back to memory store", err)
	}

	// In-memory fallback
	w.mu.Lock()
	defer w.mu.Unlock()
	rec.ID = int64(len(w.inMemDLQ) + 1)
	if len(w.inMemDLQ) >= w.maxInMem {
		w.inMemDLQ = w.inMemDLQ[1:] // FIFO drop
	}
	w.inMemDLQ = append(w.inMemDLQ, rec)
}

// ReDrive replays eligible events matching filter criteria back into the main pipeline.
func (w *Worker) ReDrive(ctx context.Context, req model.DLQRetryRequest) model.DLQRetryResponse {
	limit := req.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	resp := model.DLQRetryResponse{
		Errors: make([]string, 0),
	}

	records := w.fetchEligibleRecords(ctx, req.ErrorCode, limit)
	if len(records) == 0 {
		return resp
	}

	for _, rec := range records {
		resp.RetriedCount++

		// Re-validate payload
		event, errCode, errMsg := validator.ValidateBytes([]byte(rec.RawPayload))
		if errCode != "" {
			resp.FailedCount++
			resp.Errors = append(resp.Errors, fmt.Sprintf("event_id=%s failed re-validation: %s: %s", rec.EventID, errCode, errMsg))
			w.updateRecordStatus(ctx, rec.ID, "FAILED_REVALIDATION")
			continue
		}

		// Re-publish to Kafka raw topic
		if w.producer != nil {
			_, err := w.producer.PublishRaw(ctx, event)
			if err != nil {
				resp.FailedCount++
				resp.Errors = append(resp.Errors, fmt.Sprintf("event_id=%s failed kafka republish: %v", rec.EventID, err))
				w.updateRecordStatus(ctx, rec.ID, "FAILED_PUBLISH")
				continue
			}
		}

		resp.SucceededCount++
		w.updateRecordStatus(ctx, rec.ID, "REPLAYED")
		metrics.IngestedEvents.WithLabelValues("redrive_success", "replayed").Inc()
	}

	return resp
}

func (w *Worker) fetchEligibleRecords(ctx context.Context, errCode string, limit int) []model.DLQAuditRecord {
	if w.db != nil {
		var rows *sql.Rows
		var err error
		if errCode != "" {
			query := `
			SELECT id, coalesce(event_id, ''), error_code, error_category, coalesce(error_message, ''), coalesce(source_offset, ''), raw_payload, rejected_at, status
			FROM dlq_audit_events
			WHERE status = 'PENDING' AND error_code = $1
			ORDER BY id ASC
			LIMIT $2`
			rows, err = w.db.QueryContext(ctx, query, errCode, limit)
		} else {
			query := `
			SELECT id, coalesce(event_id, ''), error_code, error_category, coalesce(error_message, ''), coalesce(source_offset, ''), raw_payload, rejected_at, status
			FROM dlq_audit_events
			WHERE status = 'PENDING'
			ORDER BY id ASC
			LIMIT $1`
			rows, err = w.db.QueryContext(ctx, query, limit)
		}

		if err == nil {
			defer rows.Close()
			var records []model.DLQAuditRecord
			for rows.Next() {
				var r model.DLQAuditRecord
				if err := rows.Scan(&r.ID, &r.EventID, &r.ErrorCode, &r.ErrorCategory, &r.ErrorMessage, &r.SourceOffset, &r.RawPayload, &r.RejectedAt, &r.Status); err == nil {
					records = append(records, r)
				}
			}
			if len(records) > 0 {
				return records
			}
		}
	}

	// In-memory fallback
	w.mu.Lock()
	defer w.mu.Unlock()
	var records []model.DLQAuditRecord
	for i := range w.inMemDLQ {
		if len(records) >= limit {
			break
		}
		if w.inMemDLQ[i].Status == "PENDING" && (errCode == "" || w.inMemDLQ[i].ErrorCode == errCode) {
			records = append(records, w.inMemDLQ[i])
		}
	}
	return records
}

func (w *Worker) updateRecordStatus(ctx context.Context, id int64, status string) {
	if w.db != nil {
		query := `UPDATE dlq_audit_events SET status = $1, retried_at = $2 WHERE id = $3`
		_, _ = w.db.ExecContext(ctx, query, status, time.Now().UTC(), id)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.inMemDLQ {
		if w.inMemDLQ[i].ID == id {
			w.inMemDLQ[i].Status = status
			t := time.Now().UTC()
			w.inMemDLQ[i].RetriedAt = &t
			break
		}
	}
}

// Close gracefully stops the DLQ worker.
func (w *Worker) Close() error {
	w.cancel()
	w.wg.Wait()
	var err error
	if w.reader != nil {
		err = w.reader.Close()
	}
	if w.db != nil {
		_ = w.db.Close()
	}
	return err
}
