package model

import "time"

// TelemetryEvent represents an incoming ML telemetry payload conforming to telemetry_v1 schema.
type TelemetryEvent struct {
	EventID       string    `json:"event_id"`
	EntityID      string    `json:"entity_id"`
	Timestamp     string    `json:"timestamp"`
	FeatureVector []float64 `json:"feature_vector"`
	SchemaVersion int       `json:"schema_version"`
}

// IngestResponse represents the HTTP response for ingestion operations.
type IngestResponse struct {
	Status    string `json:"status"`
	EventID   string `json:"event_id,omitempty"`
	EntityID  string `json:"entity_id,omitempty"`
	Message   string `json:"message,omitempty"`
	Error     string `json:"error,omitempty"`
	Partition int    `json:"partition,omitempty"`
}

// DLQRetryRequest specifies the parameters for re-driving messages from the DLQ.
type DLQRetryRequest struct {
	Limit       int    `json:"limit"`
	ErrorCode   string `json:"error_code,omitempty"`
	TargetTopic string `json:"target_topic,omitempty"`
}

// DLQRetryResponse conveys the outcome of a DLQ re-drive operation.
type DLQRetryResponse struct {
	RetriedCount   int      `json:"retried_count"`
	SucceededCount int      `json:"succeeded_count"`
	FailedCount    int      `json:"failed_count"`
	Errors         []string `json:"errors,omitempty"`
}

// DLQAuditRecord models an audited dead-letter queue message stored in PostgreSQL or in memory.
type DLQAuditRecord struct {
	ID            int64      `json:"id"`
	EventID       string     `json:"event_id"`
	EntityID      string     `json:"entity_id"`
	ErrorCode     string     `json:"error_code"`
	ErrorCategory string     `json:"error_category"`
	ErrorMessage  string     `json:"error_message"`
	SourceOffset  string     `json:"source_offset"`
	RawPayload    string     `json:"raw_payload"`
	RejectedAt    time.Time  `json:"rejected_at"`
	RetriedAt     *time.Time `json:"retried_at,omitempty"`
	Status        string     `json:"status"`
}
