package validator

import (
	"encoding/json"
	"testing"
)

func TestValidator_ValidEvent(t *testing.T) {
	v := New()
	payload := []byte(`{
		"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
		"entity_id": "entity-000042",
		"timestamp": "2026-10-25T11:00:00Z",
		"feature_vector": [0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, -0.8],
		"schema_version": 1
	}`)

	event, err := v.Validate(payload)
	if err != nil {
		t.Fatalf("expected valid event, got error: %v", err)
	}
	if event.EntityID != "entity-000042" {
		t.Errorf("expected entity_id entity-000042, got %s", event.EntityID)
	}
}

func TestValidator_InvalidCases(t *testing.T) {
	v := New()
	tests := []struct {
		name    string
		payload string
		errCode string
	}{
		{
			name:    "empty payload",
			payload: ``,
			errCode: "invalid_json",
		},
		{
			name:    "invalid json syntax",
			payload: `{"event_id": 123`,
			errCode: "schema_violation",
		},
		{
			name: "additional properties rejected",
			payload: `{
				"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"entity_id": "entity-000042",
				"timestamp": "2026-10-25T11:00:00Z",
				"feature_vector": [0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, -0.8],
				"schema_version": 1,
				"extra_field": "disallowed"
			}`,
			errCode: "schema_violation",
		},
		{
			name: "invalid event_id format",
			payload: `{
				"event_id": "invalid-uuid",
				"entity_id": "entity-000042",
				"timestamp": "2026-10-25T11:00:00Z",
				"feature_vector": [0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, -0.8],
				"schema_version": 1
			}`,
			errCode: "schema_violation",
		},
		{
			name: "feature vector too short (7 items)",
			payload: `{
				"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"entity_id": "entity-000042",
				"timestamp": "2026-10-25T11:00:00Z",
				"feature_vector": [0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7],
				"schema_version": 1
			}`,
			errCode: "schema_violation",
		},
		{
			name: "feature vector item out of range (> 1e6)",
			payload: `{
				"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"entity_id": "entity-000042",
				"timestamp": "2026-10-25T11:00:00Z",
				"feature_vector": [10000000.0, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8],
				"schema_version": 1
			}`,
			errCode: "schema_violation",
		},
		{
			name: "schema version out of bounds (101)",
			payload: `{
				"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"entity_id": "entity-000042",
				"timestamp": "2026-10-25T11:00:00Z",
				"feature_vector": [0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8],
				"schema_version": 101
			}`,
			errCode: "schema_violation",
		},
		{
			name: "empty entity_id",
			payload: `{
				"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"entity_id": "",
				"timestamp": "2026-10-25T11:00:00Z",
				"feature_vector": [0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8],
				"schema_version": 1
			}`,
			errCode: "schema_violation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v.Validate([]byte(tt.payload))
			if err == nil {
				t.Fatalf("expected error for case %s, got nil", tt.name)
			}
			if err.Code != tt.errCode {
				t.Errorf("expected error code %s, got %s (%s)", tt.errCode, err.Code, err.Reason)
			}
		})
	}
}

func BenchmarkValidator_Validate(b *testing.B) {
	v := New()
	payload, _ := json.Marshal(map[string]interface{}{
		"event_id":       "3fa85f64-5717-4562-b3fc-2c963f66afa6",
		"entity_id":      "entity-000042",
		"timestamp":      "2026-10-25T11:00:00Z",
		"feature_vector": []float64{0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, -0.8},
		"schema_version": 1,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := v.Validate(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}
