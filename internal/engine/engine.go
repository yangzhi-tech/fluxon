// Package engine manages the Kafka consumer, partition workers, and rebalance lifecycle.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon/internal/dlq"
	"github.com/dropbox/fluxon/internal/es"
	"github.com/dropbox/fluxon/internal/worker"
	"github.com/dropbox/fluxon/pkg/types"
)

// WorkerIface allows mock injection in tests.
type WorkerIface interface {
	Submit(r *kgo.Record)
	Flush(ctx context.Context) error
	Run(ctx context.Context)
}

// WorkerFactory creates a WorkerIface for a given partition.
type WorkerFactory func(partition int32) WorkerIface

// Engine manages the full pipeline.
type Engine struct {
	cfg     types.Config
	handler types.Handler
	esw     *es.Writer
	dlqw    worker.DLQWriter

	factory      WorkerFactory
	mu           sync.Mutex
	workers      map[int32]WorkerIface
	client       *kgo.Client
	workerCtx    context.Context
	workerCancel context.CancelFunc
}

// New creates a production Engine wired with real ES and DLQ writers.
func New(cfg types.Config, handler types.Handler) (*Engine, error) {
	esw, err := es.NewWriter(cfg.ES)
	if err != nil {
		return nil, fmt.Errorf("engine: es writer: %w", err)
	}

	var dlqw worker.DLQWriter = &noopDLQ{}
	if cfg.Kafka.DLQTopic != "" {
		dw, err := dlq.NewWriter(cfg.Kafka.Brokers, cfg.Kafka.DLQTopic)
		if err != nil {
			return nil, fmt.Errorf("engine: dlq writer: %w", err)
		}
		dlqw = dw
	}

	return &Engine{
		cfg:     cfg,
		handler: handler,
		esw:     esw,
		dlqw:    dlqw,
		workers: make(map[int32]WorkerIface),
	}, nil
}

// NewWithFactory creates an Engine with a custom worker factory (used in unit tests).
func NewWithFactory(cfg types.Config, handler types.Handler, factory WorkerFactory) *Engine {
	return &Engine{
		cfg:     cfg,
		handler: handler,
		factory: factory,
		workers: make(map[int32]WorkerIface),
	}
}

// Run starts the Kafka poll loop and blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	e.workerCtx, e.workerCancel = context.WithCancel(ctx)
	defer e.workerCancel()

	opts := []kgo.Opt{
		kgo.SeedBrokers(e.cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(e.cfg.Kafka.GroupID),
		kgo.ConsumeTopics(e.cfg.Kafka.Topic),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsAssigned(e.onAssigned),
		kgo.OnPartitionsRevoked(e.onRevoked),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("engine: kafka client: %w", err)
	}
	defer client.Close()
	e.client = client

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("engine: poll error", "err", err)
			continue
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			w := e.getWorker(p.Partition)
			if w == nil {
				slog.Warn("engine: no worker for partition", "partition", p.Partition)
				return
			}
			for _, r := range p.Records {
				w.Submit(r)
			}
		})
	}
}

func (e *Engine) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, partitions := range assigned {
		for _, p := range partitions {
			w := e.makeWorker(p)
			e.workers[p] = w
			go w.Run(e.workerCtx)
			slog.Info("engine: partition assigned", "partition", p)
		}
	}
}

func (e *Engine) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, partitions := range revoked {
		for _, p := range partitions {
			w, ok := e.workers[p]
			if !ok {
				continue
			}
			if err := w.Flush(ctx); err != nil {
				slog.Error("engine: flush on revoke failed", "partition", p, "err", err)
			}
			delete(e.workers, p)
			slog.Info("engine: partition revoked", "partition", p)
		}
	}
}

func (e *Engine) getWorker(partition int32) WorkerIface {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.workers[partition]
}

func (e *Engine) makeWorker(partition int32) WorkerIface {
	if e.factory != nil {
		return e.factory(partition)
	}
	maxRetries := e.cfg.Kafka.MaxHandlerRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	return worker.New(partition, e.handler, e.esw, e.dlqw, e.client,
		maxRetries, e.cfg.Buffer.MaxEvents, e.cfg.Buffer.MaxWaitMs)
}

type noopDLQ struct{}

func (n *noopDLQ) Send(_ context.Context, _ []byte, _ int32, _ int64, _ error) {}
