package fluxon

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon/internal/buffer"
	"github.com/dropbox/fluxon/internal/dlq"
)

// esWriterIface allows mock injection in worker tests.
type esWriterIface interface {
	flush(ctx context.Context, actions []*Action) error
}

// dlqWriterIface allows mock injection in worker tests.
type dlqWriterIface interface {
	Send(ctx context.Context, payload []byte, partition int32, offset int64, reason error)
}

// commitMarker is satisfied by *kgo.Client.
type commitMarker interface {
	MarkCommitRecords(records ...*kgo.Record)
}

// worker processes records for a single Kafka partition sequentially.
type worker struct {
	partition  int32
	msgCh      chan *kgo.Record
	flushCh    chan chan error
	buf        *buffer.Buffer
	handler    Handler
	esw        esWriterIface
	dlqw       dlqWriterIface
	client     commitMarker
	maxRetries int
}

func newWorker(
	partition int32,
	handler Handler,
	esw esWriterIface,
	dlqw dlqWriterIface,
	client commitMarker,
	maxRetries, maxEvents, maxWaitMs int,
) *worker {
	if maxRetries == 0 {
		maxRetries = 3
	}
	if maxEvents == 0 {
		maxEvents = 1000
	}
	if maxWaitMs == 0 {
		maxWaitMs = 500
	}
	return &worker{
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

func (w *worker) submit(r *kgo.Record) { w.msgCh <- r }

func (w *worker) flushSync(ctx context.Context) error {
	errCh := make(chan error, 1)
	w.flushCh <- errCh
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *worker) run(ctx context.Context) {
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

func (w *worker) processRecord(ctx context.Context, record *kgo.Record) {
	event, err := parseCDC(record.Value)
	if err != nil {
		slog.Error("cdc parse error", "partition", w.partition, "offset", record.Offset, "err", err)
		w.dlqw.Send(ctx, record.Value, record.Partition, record.Offset, err)
		w.buf.Add(nil, record)
		return
	}

	var action *Action
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

func (w *worker) flushBuffer(ctx context.Context) error {
	entries := w.buf.Peek()
	if len(entries) == 0 {
		return nil
	}

	var actions []*Action
	for _, e := range entries {
		if e.Action != nil {
			actions = append(actions, e.Action.(*Action))
		}
	}

	if err := w.esw.flush(ctx, actions); err != nil {
		return err
	}

	w.buf.Clear()
	w.client.MarkCommitRecords(entries[len(entries)-1].Record)
	return nil
}

// dlqWriterAdapter wraps *dlq.Writer to satisfy dlqWriterIface.
type dlqWriterAdapter struct{ w *dlq.Writer }

func (a *dlqWriterAdapter) Send(ctx context.Context, payload []byte, partition int32, offset int64, reason error) {
	a.w.Send(ctx, payload, partition, offset, reason)
}
