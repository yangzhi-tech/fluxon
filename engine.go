package fluxon

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon/internal/dlq"
)

// workerIface allows mock injection in engine tests.
type workerIface interface {
	submit(r *kgo.Record)
	flushSync(ctx context.Context) error
	run(ctx context.Context)
}

// workerFactory creates a workerIface for a given partition.
type workerFactory func(partition int32) workerIface

// engineImpl holds runtime state for a running Engine.
type engineImpl struct {
	cfg     Config
	handler Handler
	esw     *esWriter
	dlqw    dlqWriterIface

	factory      workerFactory // nil = production; set in tests
	mu           sync.Mutex
	workers      map[int32]workerIface
	client       *kgo.Client
	workerCtx    context.Context
	workerCancel context.CancelFunc
}

// runEngine is called by Engine.Run.
func runEngine(ctx context.Context, cfg Config, h Handler) error {
	esw, err := newESWriter(cfg.ES)
	if err != nil {
		return fmt.Errorf("engine: es writer: %w", err)
	}

	var dlqAdapter dlqWriterIface
	if cfg.Kafka.DLQTopic != "" {
		dw, err := dlq.NewWriter(cfg.Kafka.Brokers, cfg.Kafka.DLQTopic)
		if err != nil {
			return fmt.Errorf("engine: dlq writer: %w", err)
		}
		dlqAdapter = &dlqWriterAdapter{w: dw}
	} else {
		dlqAdapter = &noopDLQWriter{}
	}

	e := &engineImpl{
		cfg:     cfg,
		handler: h,
		esw:     esw,
		dlqw:    dlqAdapter,
		workers: make(map[int32]workerIface),
	}
	return e.run(ctx)
}

func (e *engineImpl) run(ctx context.Context) error {
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
				w.submit(r)
			}
		})
	}
}

func (e *engineImpl) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, partitions := range assigned {
		for _, p := range partitions {
			w := e.makeWorker(p)
			e.workers[p] = w
			go w.run(e.workerCtx)
			slog.Info("engine: partition assigned", "partition", p)
		}
	}
}

func (e *engineImpl) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, partitions := range revoked {
		for _, p := range partitions {
			w, ok := e.workers[p]
			if !ok {
				continue
			}
			if err := w.flushSync(ctx); err != nil {
				slog.Error("engine: flush on revoke failed", "partition", p, "err", err)
			}
			delete(e.workers, p)
			slog.Info("engine: partition revoked", "partition", p)
		}
	}
}

func (e *engineImpl) getWorker(partition int32) workerIface {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.workers[partition]
}

func (e *engineImpl) makeWorker(partition int32) workerIface {
	if e.factory != nil {
		return e.factory(partition)
	}
	maxRetries := e.cfg.Kafka.MaxHandlerRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	return newWorker(
		partition,
		e.handler,
		e.esw,
		e.dlqw,
		e.client,
		maxRetries,
		e.cfg.Buffer.MaxEvents,
		e.cfg.Buffer.MaxWaitMs,
	)
}

// noopDLQWriter discards DLQ messages when no DLQ topic is configured.
type noopDLQWriter struct{}

func (n *noopDLQWriter) Send(_ context.Context, _ []byte, _ int32, _ int64, _ error) {}
