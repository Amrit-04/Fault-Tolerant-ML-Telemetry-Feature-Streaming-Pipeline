package validator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/Amrit-04/telemetry-ingestor-go/internal/model"
)

var (
	uuidRegex = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

	ErrInvalidJSON     = errors.New("invalid_json")
	ErrSchemaViolation = errors.New("schema_violation")

	defaultValidator = New()
)

// ValidateBytes is a package-level helper validating raw JSON against telemetry contracts.
func ValidateBytes(data []byte) (*model.TelemetryEvent, string, string) {
	evt, valErr := defaultValidator.Validate(data)
	if valErr != nil {
		return nil, valErr.Code, valErr.Reason
	}
	return evt, "", ""
}

// ValidationError provides structured error descriptions matching Python validator semantics.
type ValidationError struct {
	Code   string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Reason)
}

// Validator validates incoming payloads against telemetry_v1 constraints.
type Validator struct{}

// New creates a new Validator.
func New() *Validator {
	return &Validator{}
}

// Validate checks raw bytes against JSON Schema constraints and returns the parsed event or an error.
func (v *Validator) Validate(data []byte) (*model.TelemetryEvent, *ValidationError) {
	if len(data) == 0 {
		return nil, &ValidationError{Code: "invalid_json", Reason: "empty payload"}
	}

	// 1. Strict JSON decoding (disallowing unknown fields for additionalProperties: false)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var event model.TelemetryEvent
	if err := decoder.Decode(&event); err != nil {
		return nil, &ValidationError{
			Code:   "schema_violation",
			Reason: fmt.Sprintf("json decoding / additional properties error: %v", err),
		}
	}

	// Ensure no trailing tokens
	if decoder.More() {
		return nil, &ValidationError{Code: "invalid_json", Reason: "unexpected trailing data"}
	}

	// 2. Validate required fields: event_id
	if event.EventID == "" {
		return nil, &ValidationError{Code: "schema_violation", Reason: "event_id is required"}
	}
	if !uuidRegex.MatchString(event.EventID) {
		return nil, &ValidationError{Code: "schema_violation", Reason: fmt.Sprintf("event_id does not match UUID regex: %s", event.EventID)}
	}

	// 3. Validate entity_id
	if len(event.EntityID) < 1 || len(event.EntityID) > 256 {
		return nil, &ValidationError{Code: "schema_violation", Reason: "entity_id must be between 1 and 256 characters"}
	}

	// 4. Validate timestamp (RFC3339 date-time format)
	if event.Timestamp == "" {
		return nil, &ValidationError{Code: "schema_violation", Reason: "timestamp is required"}
	}
	if _, err := time.Parse(time.RFC3339, event.Timestamp); err != nil {
		if _, errNano := time.Parse(time.RFC3339Nano, event.Timestamp); errNano != nil {
			return nil, &ValidationError{Code: "schema_violation", Reason: fmt.Sprintf("timestamp must be RFC3339 formatted: %v", err)}
		}
	}

	// 5. Validate feature_vector
	if event.FeatureVector == nil {
		return nil, &ValidationError{Code: "schema_violation", Reason: "feature_vector is required"}
	}
	vecLen := len(event.FeatureVector)
	if vecLen < 8 || vecLen > 16 {
		return nil, &ValidationError{Code: "schema_violation", Reason: fmt.Sprintf("feature_vector length must be between 8 and 16, got %d", vecLen)}
	}
	for i, val := range event.FeatureVector {
		if val < -1e6 || val > 1e6 {
			return nil, &ValidationError{Code: "schema_violation", Reason: fmt.Sprintf("feature_vector[%d]=%f out of range [-1e6, 1e6]", i, val)}
		}
	}

	// 6. Validate schema_version
	if event.SchemaVersion < 1 || event.SchemaVersion > 100 {
		return nil, &ValidationError{Code: "schema_violation", Reason: fmt.Sprintf("schema_version must be between 1 and 100, got %d", event.SchemaVersion)}
	}

	return &event, nil
}
