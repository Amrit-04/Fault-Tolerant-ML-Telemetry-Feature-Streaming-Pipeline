package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/Amrit-04/telemetry-ingestor-go/internal/metrics"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/model"
	"github.com/segmentio/kafka-go"
)

// Producer handles partitioned message dispatch to Kafka topics.
type Producer struct {
	rawWriter *kafka.Writer
	dlqWriter *kafka.Writer
	brokers   []string
	numParts  int
}

// Config encapsulates Kafka producer settings.
type Config struct {
	Brokers        []string
	RawTopic       string
	DLQTopic       string
	NumPartitions  int
	BatchSize      int
	BatchTimeout   time.Duration
	WriteTimeout   time.Duration
	RequiredAcks   int
}

// NewProducer initializes optimized partitioned kafka-go writers.
func NewProducer(cfg Config) *Producer {
	if cfg.NumPartitions <= 0 {
		cfg.NumPartitions = 12
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 10 * time.Millisecond
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}

	rawWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.RawTopic,
		Balancer:               &kafka.Hash{},
		Compression:            kafka.Snappy,
		BatchSize:              cfg.BatchSize,
		BatchTimeout:           cfg.BatchTimeout,
		WriteTimeout:           cfg.WriteTimeout,
		RequiredAcks:           kafka.RequiredAcks(cfg.RequiredAcks),
		Async:                  false, // synchronous block with batching behind the scenes
		AllowAutoTopicCreation: true,
	}

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.DLQTopic,
		Balancer:               &kafka.Hash{},
		Compression:            kafka.Snappy,
		BatchSize:              100,
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           cfg.WriteTimeout,
		RequiredAcks:           kafka.RequiredAcks(cfg.RequiredAcks),
		Async:                  false,
		AllowAutoTopicCreation: true,
	}

	return &Producer{
		rawWriter: rawWriter,
		dlqWriter: dlqWriter,
		brokers:   cfg.Brokers,
		numParts:  cfg.NumPartitions,
	}
}

// Close gracefully flushes pending buffers and closes Kafka writers.
func (p *Producer) Close() error {
	var err1, err2 error
	if p.rawWriter != nil {
		err1 = p.rawWriter.Close()
	}
	if p.dlqWriter != nil {
		err2 = p.dlqWriter.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// ComputePartition calculates target partition deterministically from entityID for metrics.
func (p *Producer) ComputePartition(entityID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(entityID))
	return int(h.Sum32() % uint32(p.numParts))
}

// PublishRaw serializes and publishes a validated telemetry event to telemetry.raw.
func (p *Producer) PublishRaw(ctx context.Context, event *model.TelemetryEvent) (int, error) {
	valBytes, err := json.Marshal(event)
	if err != nil {
		return -1, fmt.Errorf("failed to marshal validated event: %w", err)
	}

	targetPart := p.ComputePartition(event.EntityID)

	msg := kafka.Message{
		Key:   []byte(event.EntityID),
		Value: valBytes,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_id", Value: []byte(event.EventID)},
			{Key: "schema_version", Value: []byte(strconv.Itoa(event.SchemaVersion))},
			{Key: "producer", Value: []byte("telemetry-ingestor-go")},
		},
	}

	if err := p.rawWriter.WriteMessages(ctx, msg); err != nil {
		metrics.IngestedEvents.WithLabelValues("failed_kafka_write", strconv.Itoa(targetPart)).Inc()
		return targetPart, fmt.Errorf("failed to write message to raw topic: %w", err)
	}

	metrics.IngestedEvents.WithLabelValues("published_raw", strconv.Itoa(targetPart)).Inc()
	return targetPart, nil
}

// PublishDLQ sends an invalid or rejected payload along with failure reason to telemetry.dlq.
func (p *Producer) PublishDLQ(ctx context.Context, payload []byte, errorCode, errorMsg, sourceOffset string) error {
	dlqEnvelope := map[string]interface{}{
		"error_code":      errorCode,
		"error_message":   errorMsg,
		"source_offset":   sourceOffset,
		"rejected_at":     time.Now().UTC().Format(time.RFC3339Nano),
		"raw_payload":     string(payload),
		"origin_service":  "telemetry-ingestor-go",
	}

	envelopeBytes, err := json.Marshal(dlqEnvelope)
	if err != nil {
		return fmt.Errorf("failed to encode DLQ envelope: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(errorCode),
		Value: envelopeBytes,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "error_code", Value: []byte(errorCode)},
			{Key: "routed_by", Value: []byte("telemetry-ingestor-go")},
		},
	}

	if err := p.dlqWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write to dlq topic: %w", err)
	}

	metrics.DLQRoutedEvents.WithLabelValues(errorCode).Inc()
	return nil
}
