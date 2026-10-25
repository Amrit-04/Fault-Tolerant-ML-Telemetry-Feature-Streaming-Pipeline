package metrics

import (
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// IngestedEvents tracks total ingest requests categorized by status and target partition
	IngestedEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "telemetry_ingested_events_total",
			Help: "Total count of telemetry events ingested, partitioned by status and kafka partition.",
		},
		[]string{"status", "partition"},
	)

	// IngestLatency tracks end-to-end ingestion latency in seconds
	IngestLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "telemetry_ingest_latency_seconds",
			Help:    "Latency of telemetry event ingestion in seconds.",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
	)

	// DLQRoutedEvents tracks events routed to the DLQ categorized by error_code
	DLQRoutedEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_routed_events_total",
			Help: "Total count of events routed to dead-letter queue by error code.",
		},
		[]string{"error_code"},
	)

	// ActiveGoroutines monitors the current number of running goroutines
	ActiveGoroutines = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_goroutines_count",
			Help: "Current number of active goroutines in the telemetry ingestor service.",
		},
	)

	// WorkerPoolQueueDepth monitors the current queue depth of the ingestor channel
	WorkerPoolQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "telemetry_worker_pool_queue_depth",
			Help: "Current number of pending ingestion jobs in the worker channel.",
		},
	)
)

// StartGoroutineReporter periodically updates the active goroutine count gauge.
func StartGoroutineReporter(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ActiveGoroutines.Set(float64(runtime.NumGoroutine()))
			case <-stopCh:
				return
			}
		}
	}()
}
