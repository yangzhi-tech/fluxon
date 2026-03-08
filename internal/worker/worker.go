// Package worker provides the per-partition processing goroutine.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon/internal/buffer"
	"github.com/dropbox/fluxon/internal/cdc"
	"github.com/dropbox/fluxon/pkg/types"
)

// ESWriter is the interface satisfied by *es.Writer.
type ESWriter interface {
	Flush(ctx context.Context, actions []*types.Action) error
}

// DLQWriter is the interface satisfied by *dlq.Writer.
type DLQWriter interface {
	Send(ctx context.Context, payload []byte, partition int32, offset int64, reason error)
}

// CommitMarker is satisfied by *kgo.Client.
type CommitMarker interface {
	MarkCommitRecords(records ...*kgo.Record)
}

// Worker processes records for a single Kafka partition sequentially.
type Worker struct {
	partition  int32
	msgCh      chan *kgo.Record
	flushCh    chan chan error
	buf        *buffer.Buffer
	handler    types.Handler
	esw        ESWriter
	dlqw       DLQWriter
	client     CommitMarker
	maxRetries int
}

// New creates a Worker. Call Run in a goroutine.
func New(
	partition int32,
	handler types.Handler,
	esw ESWriter,
	dlqw DLQWriter,
	client CommitMarker,
	maxRetries, maxEvents, maxWaitMs int,
) *Worker {
	if maxRetries == 0 {
		maxRetries = 3
	}
	if maxEvents == 0 {
		maxEvents = 1000
	}
	if maxWaitMs == 0 {
		maxWaitMs = 500
	}
	return &Worker{
		partition:  partition,
		msgCh:      make(chan *kgo.Record, 256),
		flushCh:    make(chan chan error, 1),
		buf:        buffer.New(maxEvents, time.Duration(maxWaitMs)*time.Millisecond),
		handler:    handler,
		esw:        esw,
		dlqw:       dlqw,
		client:     client,
		maxRetries: maxRetries,
	}
}

// Submit enqueues a record. Called by the engine poll loop.
func (w *Worker) Submit(r *kgo.Record) { w.msgCh <- r }

// Flush signals the worker to drain and flush synchronously (used on rebalance).
func (w *Worker) Flush(ctx context.Context) error {
	errCh := make(chan error, 1)
	w.flushCh <- errCh
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run is the worker event loop. Blocks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case record := <-w.msgCh:
			w.processRecord(ctx, record)
			if w.buf.ShouldFlush() {
				if err := w.flushBuffer(ctx); err != nil {
					slog.Error("worker flush failed", "partition", w.partition, "err", err)
				}
			}

		case <-ticker.C:
			if w.buf.ShouldFlush() {
				if err := w.flushBuffer(ctx); err != nil {
					slog.Error("worker tick flush failed", "partition", w.partition, "err", err)
				}
			}

		case errCh := <-w.flushCh:
			for {
				select {
				case record := <-w.msgCh:
					w.processRecord(ctx, record)
				default:
					goto drained
				}
			}
		drained:
			errCh <- w.flushBuffer(ctx)

		case <-ctx.Done():
			for {
				select {
				case record := <-w.msgCh:
					w.processRecord(ctx, record)
				default:
					goto shutdownFlush
				}
			}
		shutdownFlush:
			if err := w.flushBuffer(ctx); err != nil {
				slog.Error("worker shutdown flush failed", "partition", w.partition, "err", err)
			}
			return
		}
	}
}

func (w *Worker) processRecord(ctx context.Context, record *kgo.Record) {
	event, err := cdc.Parse(record.Value)
	if err != nil {
		slog.Error("cdc parse error", "partition", w.partition, "offset", record.Offset, "err", err)
		w.dlqw.Send(ctx, record.Value, record.Partition, record.Offset, err)
		w.buf.Add(nil, record)
		return
	}

	var action *types.Action
	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		action, err = w.handler.Handle(event)
		if err == nil {
			break
		}
		slog.Warn("handler error", "partition", w.partition, "offset", record.Offset, "attempt", attempt, "err", err)
	}

	if err != nil {
		slog.Error("handler exhausted retries", "partition", w.partition, "offset", record.Offset)
		w.dlqw.Send(ctx, record.Value, record.Partition, record.Offset, err)
		w.buf.Add(nil, record)
		return
	}

	w.buf.Add(action, record)
}

func (w *Worker) flushBuffer(ctx context.Context) error {
	entries := w.buf.Peek()
	if len(entries) == 0 {
		return nil
	}

	var actions []*types.Action
	for _, e := range entries {
		if e.Action != nil {
			actions = append(actions, e.Action.(*types.Action))
		}
	}

	if err := w.esw.Flush(ctx, actions); err != nil {
		return err
	}

	w.buf.Clear()
	w.client.MarkCommitRecords(entries[len(entries)-1].Record)
	return nil
}
