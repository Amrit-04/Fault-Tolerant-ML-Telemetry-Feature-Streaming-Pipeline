package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// AppConfig holds all runtime settings for the Go Telemetry Ingestor.
type AppConfig struct {
	HTTPPort          string
	KafkaBrokers      []string
	KafkaRawTopic     string
	KafkaDLQTopic     string
	KafkaPartitions   int
	WorkerPoolSize    int
	WorkerBufferSize  int
	PostgresURL       string
	KafkaBatchSize    int
	KafkaBatchTimeout time.Duration
	KafkaWriteTimeout time.Duration
	KafkaRequiredAcks int
	DLQWorkerCount    int
}

// LoadFromEnv loads configuration from environment variables with defaults.
func LoadFromEnv() *AppConfig {
	brokersStr := getEnv("KAFKA_BOOTSTRAP_SERVERS", getEnv("KAFKA_BROKERS", "localhost:9092"))
	brokers := strings.Split(brokersStr, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	return &AppConfig{
		HTTPPort:          getEnv("HTTP_PORT", "8080"),
		KafkaBrokers:      brokers,
		KafkaRawTopic:     getEnv("KAFKA_RAW_TOPIC", "telemetry.raw"),
		KafkaDLQTopic:     getEnv("KAFKA_DLQ_TOPIC", "telemetry.dlq"),
		KafkaPartitions:   getEnvInt("KAFKA_PARTITIONS", 12),
		WorkerPoolSize:    getEnvInt("WORKER_POOL_SIZE", 50),
		WorkerBufferSize:  getEnvInt("WORKER_BUFFER_SIZE", 10000),
		PostgresURL:       getEnv("DATABASE_URL", getEnv("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable")),
		KafkaBatchSize:    getEnvInt("KAFKA_BATCH_SIZE", 1000),
		KafkaBatchTimeout: time.Duration(getEnvInt("KAFKA_BATCH_TIMEOUT_MS", 10)) * time.Millisecond,
		KafkaWriteTimeout: time.Duration(getEnvInt("KAFKA_WRITE_TIMEOUT_MS", 5000)) * time.Millisecond,
		KafkaRequiredAcks: getEnvInt("KAFKA_REQUIRED_ACKS", -1), // -1 is RequireAll
		DLQWorkerCount:    getEnvInt("DLQ_WORKER_CONCURRENCY", 4),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}
