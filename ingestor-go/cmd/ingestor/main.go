package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Amrit-04/telemetry-ingestor-go/internal/api"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/config"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/dlq"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/kafka"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/metrics"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/worker"
	_ "github.com/lib/pq"
)

func main() {
	log.Println("[Ingestor] Starting Go Telemetry Ingestor Service...")

	cfg := config.LoadFromEnv()

	// Periodic reporter for runtime goroutine metrics
	metricsStop := make(chan struct{})
	metrics.StartGoroutineReporter(2*time.Second, metricsStop)

	// 1. Initialize Kafka partitioned producer
	var kafkaProducer *kafka.Producer
	if os.Getenv("KAFKA_DISPATCH_ENABLED") != "false" {
		kafkaProducer = kafka.NewProducer(kafka.Config{
			Brokers:       cfg.KafkaBrokers,
			RawTopic:      cfg.KafkaRawTopic,
			DLQTopic:      cfg.KafkaDLQTopic,
			NumPartitions: cfg.KafkaPartitions,
			BatchSize:     cfg.KafkaBatchSize,
			BatchTimeout:  cfg.KafkaBatchTimeout,
			WriteTimeout:  cfg.KafkaWriteTimeout,
			RequiredAcks:  cfg.KafkaRequiredAcks,
		})
		log.Printf("[Ingestor] Kafka partitioned producer initialized for %s (%d partitions) via %v",
			cfg.KafkaRawTopic, cfg.KafkaPartitions, cfg.KafkaBrokers)
	} else {
		log.Println("[Ingestor] Kafka broker dispatch disabled (benchmark / offline ingress validation mode)")
	}

	// 2. Initialize Goroutine Worker Pool
	pool := worker.NewPool(cfg.WorkerPoolSize, cfg.WorkerBufferSize, kafkaProducer)
	log.Printf("[Ingestor] Worker pool initialized with %d workers and %d queue capacity",
		cfg.WorkerPoolSize, cfg.WorkerBufferSize)

	// 3. Initialize DLQ Worker & Audit Consumer
	var dlqWorker *dlq.Worker
	if os.Getenv("KAFKA_DISPATCH_ENABLED") != "false" {
		dlqWorker = dlq.NewWorker(dlq.Config{
			Brokers:       cfg.KafkaBrokers,
			DLQTopic:      cfg.KafkaDLQTopic,
			GroupID:       "telemetry-dlq-audit-group",
			PostgresURL:   cfg.PostgresURL,
			WorkerCount:   cfg.DLQWorkerCount,
			PollBatchSize: 100,
		}, kafkaProducer)
		dlqWorker.Start()
	} else {
		dlqWorker = dlq.NewWorker(dlq.Config{WorkerCount: 1}, nil)
	}

	// 4. Initialize HTTP API Server
	srv := api.NewServer(pool, dlqWorker)
	httpServer := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      srv.Router(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("[Ingestor] Ingress HTTP server listening on :%s", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Ingestor] Fatal HTTP server error: %v", err)
		}
	}()

	// 5. Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[Ingestor] Shutdown signal received. Initiating graceful drain...")
	close(metricsStop)

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	if err := httpServer.Shutdown(ctxShutdown); err != nil {
		log.Printf("[Ingestor] HTTP server shutdown error: %v", err)
	}

	pool.Stop()
	log.Println("[Ingestor] Worker pool drained.")

	if err := dlqWorker.Close(); err != nil {
		log.Printf("[Ingestor] DLQ worker shutdown error: %v", err)
	}

	if kafkaProducer != nil {
		if err := kafkaProducer.Close(); err != nil {
			log.Printf("[Ingestor] Kafka producer close error: %v", err)
		}
	}

	fmt.Println("[Ingestor] Service cleanly terminated.")
}
