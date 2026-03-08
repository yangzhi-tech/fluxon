package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon"
)

// ---- mocks ----

type mockESWriter struct {
	calls   int
	failErr error
	actions [][]*fluxon.Action
}

func (m *mockESWriter) Flush(_ context.Context, actions []*fluxon.Action) error {
	m.calls++
	if m.failErr != nil {
		return m.failErr
	}
	cp := make([]*fluxon.Action, len(actions))
	copy(cp, actions)
	m.actions = append(m.actions, cp)
	return nil
}

type mockDLQWriter struct {
	sent [][]byte
}

func (m *mockDLQWriter) Send(_ context.Context, payload []byte, _ int32, _ int64, _ error) {
	m.sent = append(m.sent, payload)
}

type mockCommitMarker struct {
	marked []*kgo.Record
}

func (m *mockCommitMarker) MarkCommitRecords(records ...*kgo.Record) {
	m.marked = append(m.marked, records...)
}

// ---- helpers ----

func insertRecord(payload string) *kgo.Record {
	return &kgo.Record{Value: []byte(payload), Partition: 0, Offset: 0}
}

func validInsertPayload(id, name string) string {
	return `{"before":null,"after":{"id":"` + id + `","name":"` + name + `"},"source":{"lsn":1,"table":"users","schema":"public"},"op":"c"}`
}

func validDeletePayload(id string) string {
	return `{"before":{"id":"` + id + `"},"after":null,"source":{"lsn":2,"table":"users","schema":"public"},"op":"d"}`
}

func newWorker(h fluxon.Handler, esw *mockESWriter, dlq *mockDLQWriter, cm *mockCommitMarker) *Worker {
	return New(0, h, esw, dlq, cm, 3, 5, 100)
}

func runWorkerWithRecords(t *testing.T, w *Worker, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	for _, r := range records {
		w.Submit(r)
	}
	// Give the worker time to process
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done
}

// ---- tests ----

func TestHappyPathInsert(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	called := false
	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		called = true
		return fluxon.Index("users", e.After.String("id"), e.Source.LSN, fluxon.Doc{}), nil
	})

	w := newWorker(h, esw, dlq, cm)
	runWorkerWithRecords(t, w, insertRecord(validInsertPayload("u1", "alice")))

	if !called {
		t.Error("handler not called")
	}
	if len(dlq.sent) != 0 {
		t.Error("no DLQ expected")
	}
}

func TestHappyPathDelete(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		if e.Op == fluxon.OpDelete {
			return fluxon.SoftDelete("users", e.Before.String("id"), e.Source.LSN), nil
		}
		return nil, nil
	})

	w := newWorker(h, esw, dlq, cm)
	runWorkerWithRecords(t, w, insertRecord(validDeletePayload("u2")))

	if len(dlq.sent) != 0 {
		t.Error("no DLQ expected")
	}
}

func TestHandlerReturnsNilNil(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return nil, nil // skip
	})

	w := newWorker(h, esw, dlq, cm)
	runWorkerWithRecords(t, w, insertRecord(validInsertPayload("u1", "alice")))

	if len(dlq.sent) != 0 {
		t.Error("skip should not send to DLQ")
	}
}

func TestHandlerErrorRetryThenSuccess(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	attempts := 0
	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("transient")
		}
		return fluxon.Index("users", "u1", 1, fluxon.Doc{}), nil
	})

	w := newWorker(h, esw, dlq, cm)
	runWorkerWithRecords(t, w, insertRecord(validInsertPayload("u1", "alice")))

	if attempts < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attempts)
	}
	if len(dlq.sent) != 0 {
		t.Error("should not DLQ when retry succeeds")
	}
}

func TestHandlerExhaustsRetries(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return nil, errors.New("always fails")
	})

	w := newWorker(h, esw, dlq, cm)
	runWorkerWithRecords(t, w, insertRecord(validInsertPayload("u1", "alice")))

	if len(dlq.sent) != 1 {
		t.Errorf("expected 1 DLQ message, got %d", len(dlq.sent))
	}
}

func TestCDCParseFailureSendsToDLQ(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) { return nil, nil })
	w := newWorker(h, esw, dlq, cm)

	runWorkerWithRecords(t, w, insertRecord(`{bad json`))

	if len(dlq.sent) != 1 {
		t.Errorf("expected 1 DLQ message for parse failure, got %d", len(dlq.sent))
	}
}

func TestSizeThresholdFlush(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return fluxon.Index("users", "u1", 1, fluxon.Doc{}), nil
	})

	// maxEvents=3, long maxWait
	w := New(0, h, esw, dlq, cm, 3, 3, 10000)

	runWorkerWithRecords(t, w,
		insertRecord(validInsertPayload("u1", "a")),
		insertRecord(validInsertPayload("u2", "b")),
		insertRecord(validInsertPayload("u3", "c")),
	)

	if esw.calls == 0 {
		t.Error("expected ES flush to be called when size threshold reached")
	}
}

func TestTimeThresholdFlush(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return fluxon.Index("users", "u1", 1, fluxon.Doc{}), nil
	})

	// maxEvents=1000, short maxWait=50ms
	w := New(0, h, esw, dlq, cm, 3, 1000, 50)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	w.Submit(insertRecord(validInsertPayload("u1", "alice")))
	time.Sleep(200 * time.Millisecond) // wait for time threshold
	cancel()
	<-done

	if esw.calls == 0 {
		t.Error("expected ES flush due to time threshold")
	}
}

func TestESFlushFailureEntriesRemain(t *testing.T) {
	esw := &mockESWriter{failErr: errors.New("es down")}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return fluxon.Index("users", "u1", 1, fluxon.Doc{}), nil
	})

	w := New(0, h, esw, dlq, cm, 3, 1, 10000) // flush after 1 event

	runWorkerWithRecords(t, w, insertRecord(validInsertPayload("u1", "alice")))

	// Flush failed — no offset should be committed
	if len(cm.marked) != 0 {
		t.Error("offset should not be committed when ES flush fails")
	}
}

func TestWorkerFlushDrainsAndFlushes(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return fluxon.Index("users", "u1", 1, fluxon.Doc{}), nil
	})

	w := New(0, h, esw, dlq, cm, 3, 1000, 10000) // high thresholds — won't auto-flush

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	w.Submit(insertRecord(validInsertPayload("u1", "alice")))
	time.Sleep(20 * time.Millisecond) // let it be processed

	err := w.Flush(ctx)
	if err != nil {
		t.Errorf("unexpected flush error: %v", err)
	}
	if esw.calls == 0 {
		t.Error("expected ES flush to be called")
	}
	if len(cm.marked) == 0 {
		t.Error("expected offset commit after flush")
	}

	// stop worker
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	w.Run(ctxCancel) // runs briefly and exits
}

func TestMarkCommitRecordsCalledAfterFlush(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return fluxon.Index("users", "u1", 1, fluxon.Doc{}), nil
	})

	w := New(0, h, esw, dlq, cm, 3, 1, 10000) // maxEvents=1 → flush after 1 action

	runWorkerWithRecords(t, w, insertRecord(validInsertPayload("u1", "alice")))

	if len(cm.marked) == 0 {
		t.Error("MarkCommitRecords should be called after successful flush")
	}
}

func TestMixedDLQAndValidInOneBatch(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return fluxon.Index("users", "u1", 1, fluxon.Doc{}), nil
	})

	w := New(0, h, esw, dlq, cm, 3, 1000, 100)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	w.Submit(insertRecord(`bad json`))
	w.Submit(insertRecord(validInsertPayload("u1", "alice")))
	time.Sleep(30 * time.Millisecond)

	err := w.Flush(ctx)
	if err != nil {
		t.Errorf("flush error: %v", err)
	}
	cancel()
	<-done

	if len(dlq.sent) != 1 {
		t.Errorf("expected 1 DLQ, got %d", len(dlq.sent))
	}
	// Both records should be committed together
	if len(cm.marked) == 0 {
		t.Error("expected offset commit covering both DLQ'd and valid record")
	}
}

func TestCtxDoneExitsCleanly(t *testing.T) {
	esw := &mockESWriter{}
	dlq := &mockDLQWriter{}
	cm := &mockCommitMarker{}

	h := fluxon.HandlerFunc(func(e *fluxon.Event) (*fluxon.Action, error) {
		return nil, nil
	})

	w := newWorker(h, esw, dlq, cm)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("worker did not exit after ctx.Done")
	}
}
