package worker

import (
	"context"
	"errors"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/Amrit-04/telemetry-ingestor-go/internal/kafka"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/metrics"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/model"
	"github.com/Amrit-04/telemetry-ingestor-go/internal/validator"
)

var (
	ErrQueueFull = errors.New("worker pool buffer is full, shedding load")
	ErrPoolClosed = errors.New("worker pool is stopped")
)

// IngestResult captures the processing outcome of an IngestJob.
type IngestResult struct {
	Event     *model.TelemetryEvent
	Partition int
	ErrCode   string
	ErrMsg    string
	Success   bool
}

// IngestJob encapsulates a telemetry ingestion task.
type IngestJob struct {
	Ctx     context.Context
	Payload []byte
	Done    chan IngestResult
	Async   bool
}

// Pool manages a bounded goroutine worker pool for high-throughput stream ingestion.
type Pool struct {
	jobs       chan *IngestJob
	producer   *kafka.Producer
	workerNum  int
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	closed     bool
	closedLock sync.RWMutex
}

// NewPool spins up workerNum goroutines listening on a buffered queue of size bufferCap.
func NewPool(workerNum int, bufferCap int, producer *kafka.Producer) *Pool {
	if workerNum <= 0 {
		workerNum = 50
	}
	if bufferCap <= 0 {
		bufferCap = 10000
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		jobs:      make(chan *IngestJob, bufferCap),
		producer:  producer,
		workerNum: workerNum,
		ctx:       ctx,
		cancel:    cancel,
	}

	p.startWorkers()
	return p
}

func (p *Pool) startWorkers() {
	for i := 0; i < p.workerNum; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

func (p *Pool) workerLoop(workerID int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.processJob(job)
		}
	}
}

func (p *Pool) processJob(job *IngestJob) {
	metrics.WorkerPoolQueueDepth.Set(float64(len(p.jobs)))
	startTime := time.Now()
	defer func() {
		metrics.IngestLatency.Observe(time.Since(startTime).Seconds())
	}()

	// 1. Strict schema validation
	event, errCode, errMsg := validator.ValidateBytes(job.Payload)
	if errCode != "" {
		// Route invalid event to Kafka DLQ
		if p.producer != nil {
			_ = p.producer.PublishDLQ(job.Ctx, job.Payload, errCode, errMsg, "http-ingress")
		}
		if job.Done != nil {
			job.Done <- IngestResult{
				Success: false,
				ErrCode: errCode,
				ErrMsg:  errMsg,
			}
		}
		return
	}

	// 2. Publish validated event to partitioned Kafka raw topic
	part := 0
	if p.producer != nil {
		var err error
		part, err = p.producer.PublishRaw(job.Ctx, event)
		if err != nil {
			if job.Done != nil {
				job.Done <- IngestResult{
					Success: false,
					ErrCode: "ERR_KAFKA_PUBLISH",
					ErrMsg:  err.Error(),
				}
			}
			return
		}
	} else {
		// Mock partition assignment for benchmark / offline mode
		h := fnv.New32a()
		_, _ = h.Write([]byte(event.EntityID))
		part = int(h.Sum32() % 12)
		metrics.IngestedEvents.WithLabelValues("published_raw", strconv.Itoa(part)).Inc()
	}

	if job.Done != nil {
		job.Done <- IngestResult{
			Success:   true,
			Event:     event,
			Partition: part,
		}
	}
}

// Submit queues an ingestion job into the worker pool.
// Returns ErrQueueFull immediately if the bounded channel cannot accept new work (backpressure).
func (p *Pool) Submit(job *IngestJob) error {
	p.closedLock.RLock()
	defer p.closedLock.RUnlock()

	if p.closed {
		return ErrPoolClosed
	}

	select {
	case p.jobs <- job:
		metrics.WorkerPoolQueueDepth.Set(float64(len(p.jobs)))
		return nil
	default:
		metrics.IngestedEvents.WithLabelValues("shed_queue_full", "none").Inc()
		return ErrQueueFull
	}
}

// Stop drains pending jobs and gracefully terminates all worker goroutines.
func (p *Pool) Stop() {
	p.closedLock.Lock()
	if p.closed {
		p.closedLock.Unlock()
		return
	}
	p.closed = true
	p.closedLock.Unlock()

	close(p.jobs)
	p.wg.Wait()
	p.cancel()
}

// QueueDepth returns the current number of queued jobs waiting for execution.
func (p *Pool) QueueDepth() int {
	return len(p.jobs)
}
