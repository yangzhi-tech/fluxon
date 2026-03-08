// Package worker provides the per-partition processing goroutine.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon"
	"github.com/dropbox/fluxon/internal/buffer"
	"github.com/dropbox/fluxon/internal/cdc"
)

// esWriter is the interface satisfied by *es.Writer.
type esWriter interface {
	Flush(ctx context.Context, actions []*fluxon.Action) error
}

// dlqWriter is the interface satisfied by *dlq.Writer.
type dlqWriter interface {
	Send(ctx context.Context, payload []byte, partition int32, offset int64, reason error)
}

// commitMarker is the interface satisfied by *kgo.Client for marking offsets.
type commitMarker interface {
	MarkCommitRecords(records ...*kgo.Record)
}

// Worker processes records for a single Kafka partition sequentially.
type Worker struct {
	partition  int32
	msgCh      chan *kgo.Record // buffered, capacity 256
	flushCh    chan chan error   // capacity 1; rebalance flush signal
	buf        *buffer.Buffer
	handler    fluxon.Handler
	esWriter   esWriter
	dlqWriter  dlqWriter
	client     commitMarker
	maxRetries int
}

// New creates a Worker. Call Run in a goroutine.
func New(
	partition int32,
	handler fluxon.Handler,
	esw esWriter,
	dlqw dlqWriter,
	client commitMarker,
	maxRetries int,
	maxEvents int,
	maxWaitMs int,
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
		esWriter:   esw,
		dlqWriter:  dlqw,
		client:     client,
		maxRetries: maxRetries,
	}
}

// Submit enqueues a record for processing. Called by the engine poll loop.
func (w *Worker) Submit(r *kgo.Record) {
	w.msgCh <- r
}

// Flush signals the worker to drain its channel and flush the buffer synchronously.
// Used by the rebalance handler. Blocks until complete.
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

// Run is the worker event loop. Runs until ctx is cancelled.
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
			// Drain msgCh fully before flushing.
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
			// Drain and flush on shutdown.
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

// processRecord parses the CDC event, calls the handler, and buffers the result.
func (w *Worker) processRecord(ctx context.Context, record *kgo.Record) {
	event, err := cdc.Parse(record.Value)
	if err != nil {
		slog.Error("cdc parse error", "partition", w.partition, "offset", record.Offset, "err", err)
		w.dlqWriter.Send(ctx, record.Value, record.Partition, record.Offset, err)
		w.buf.Add(nil, record)
		return
	}

	var action *fluxon.Action
	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		action, err = w.handler.Handle(event)
		if err == nil {
			break
		}
		slog.Warn("handler error", "partition", w.partition, "offset", record.Offset, "attempt", attempt, "err", err)
	}

	if err != nil {
		slog.Error("handler exhausted retries", "partition", w.partition, "offset", record.Offset)
		w.dlqWriter.Send(ctx, record.Value, record.Partition, record.Offset, err)
		w.buf.Add(nil, record)
		return
	}

	// action may be nil (skip)
	w.buf.Add(action, record)
}

// flushBuffer sends buffered actions to ES and commits offsets on success.
func (w *Worker) flushBuffer(ctx context.Context) error {
	entries := w.buf.Peek()
	if len(entries) == 0 {
		return nil
	}

	var actions []*fluxon.Action
	for _, e := range entries {
		if e.Action != nil {
			actions = append(actions, e.Action.(*fluxon.Action))
		}
	}

	if err := w.esWriter.Flush(ctx, actions); err != nil {
		return err
	}

	w.buf.Clear()
	w.client.MarkCommitRecords(entries[len(entries)-1].Record)
	return nil
}
