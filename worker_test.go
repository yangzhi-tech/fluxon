package fluxon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ---- mocks ----

type mockESW struct {
	calls   int
	failErr error
}

func (m *mockESW) flush(_ context.Context, actions []*Action) error {
	m.calls++
	return m.failErr
}

type mockDLQW struct {
	sent [][]byte
}

func (m *mockDLQW) Send(_ context.Context, payload []byte, _ int32, _ int64, _ error) {
	m.sent = append(m.sent, payload)
}

type mockCommit struct {
	marked []*kgo.Record
}

func (m *mockCommit) MarkCommitRecords(records ...*kgo.Record) {
	m.marked = append(m.marked, records...)
}

// ---- helpers ----

func wRecord(payload string) *kgo.Record {
	return &kgo.Record{Value: []byte(payload)}
}

func insertPayload(id, name string) string {
	return `{"before":null,"after":{"id":"` + id + `","name":"` + name + `"},"source":{"lsn":1,"table":"users","schema":"public"},"op":"c"}`
}

func deletePayload(id string) string {
	return `{"before":{"id":"` + id + `"},"after":null,"source":{"lsn":2,"table":"users","schema":"public"},"op":"d"}`
}

func makeWorker(h Handler, esw *mockESW, dlqw *mockDLQW, cm *mockCommit) *worker {
	return newWorker(0, h, esw, dlqw, cm, 3, 5, 100)
}

func runWith(t *testing.T, w *worker, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.run(ctx) }()
	for _, r := range records {
		w.submit(r)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done
}

// ---- tests ----

func TestWorkerHappyPathInsert(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	called := false
	h := HandlerFunc(func(e *Event) (*Action, error) {
		called = true
		return Index("users", e.After.String("id"), e.Source.LSN, Doc{}), nil
	})
	runWith(t, makeWorker(h, esw, dlqw, cm), wRecord(insertPayload("u1", "alice")))
	if !called {
		t.Error("handler not called")
	}
	if len(dlqw.sent) != 0 {
		t.Error("unexpected DLQ")
	}
}

func TestWorkerHappyPathDelete(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) {
		if e.Op == OpDelete {
			return SoftDelete("users", e.Before.String("id"), e.Source.LSN), nil
		}
		return nil, nil
	})
	runWith(t, makeWorker(h, esw, dlqw, cm), wRecord(deletePayload("u2")))
	if len(dlqw.sent) != 0 {
		t.Error("unexpected DLQ")
	}
}

func TestWorkerHandlerReturnsNilNil(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) { return nil, nil })
	runWith(t, makeWorker(h, esw, dlqw, cm), wRecord(insertPayload("u1", "alice")))
	if len(dlqw.sent) != 0 {
		t.Error("skip should not DLQ")
	}
}

func TestWorkerHandlerRetryThenSuccess(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	attempts := 0
	h := HandlerFunc(func(e *Event) (*Action, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("transient")
		}
		return Index("users", "u1", 1, Doc{}), nil
	})
	runWith(t, makeWorker(h, esw, dlqw, cm), wRecord(insertPayload("u1", "alice")))
	if attempts < 3 {
		t.Errorf("expected >=3 attempts, got %d", attempts)
	}
	if len(dlqw.sent) != 0 {
		t.Error("should not DLQ when retry succeeds")
	}
}

func TestWorkerHandlerExhaustsRetries(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) { return nil, errors.New("always fails") })
	runWith(t, makeWorker(h, esw, dlqw, cm), wRecord(insertPayload("u1", "alice")))
	if len(dlqw.sent) != 1 {
		t.Errorf("expected 1 DLQ, got %d", len(dlqw.sent))
	}
}

func TestWorkerCDCParseFailure(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) { return nil, nil })
	runWith(t, makeWorker(h, esw, dlqw, cm), wRecord(`{bad json`))
	if len(dlqw.sent) != 1 {
		t.Errorf("expected 1 DLQ for parse failure, got %d", len(dlqw.sent))
	}
}

func TestWorkerSizeThresholdFlush(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) {
		return Index("users", "u1", 1, Doc{}), nil
	})
	w := newWorker(0, h, esw, dlqw, cm, 3, 3, 10000)
	runWith(t, w,
		wRecord(insertPayload("u1", "a")),
		wRecord(insertPayload("u2", "b")),
		wRecord(insertPayload("u3", "c")),
	)
	if esw.calls == 0 {
		t.Error("expected ES flush when size threshold reached")
	}
}

func TestWorkerTimeThresholdFlush(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) {
		return Index("users", "u1", 1, Doc{}), nil
	})
	w := newWorker(0, h, esw, dlqw, cm, 3, 1000, 50)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.run(ctx) }()
	w.submit(wRecord(insertPayload("u1", "alice")))
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
	if esw.calls == 0 {
		t.Error("expected ES flush due to time threshold")
	}
}

func TestWorkerESFlushFailureNoCommit(t *testing.T) {
	esw := &mockESW{failErr: errors.New("es down")}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) {
		return Index("users", "u1", 1, Doc{}), nil
	})
	w := newWorker(0, h, esw, dlqw, cm, 3, 1, 10000)
	runWith(t, w, wRecord(insertPayload("u1", "alice")))
	if len(cm.marked) != 0 {
		t.Error("offset should not be committed when ES flush fails")
	}
}

func TestWorkerFlushSyncDrainsAndFlushes(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) {
		return Index("users", "u1", 1, Doc{}), nil
	})
	w := newWorker(0, h, esw, dlqw, cm, 3, 1000, 10000)
	ctx := context.Background()
	done := make(chan struct{})
	go func() { defer close(done); w.run(ctx) }()
	w.submit(wRecord(insertPayload("u1", "alice")))
	time.Sleep(20 * time.Millisecond)
	if err := w.flushSync(ctx); err != nil {
		t.Errorf("unexpected flush error: %v", err)
	}
	if esw.calls == 0 {
		t.Error("expected ES flush")
	}
	if len(cm.marked) == 0 {
		t.Error("expected offset commit")
	}
	// stop goroutine
	ctxC, cancel := context.WithCancel(ctx)
	cancel()
	w.run(ctxC)
}

func TestWorkerMarkCommitAfterFlush(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) {
		return Index("users", "u1", 1, Doc{}), nil
	})
	w := newWorker(0, h, esw, dlqw, cm, 3, 1, 10000)
	runWith(t, w, wRecord(insertPayload("u1", "alice")))
	if len(cm.marked) == 0 {
		t.Error("MarkCommitRecords should be called after successful flush")
	}
}

func TestWorkerMixedDLQAndValidBatch(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) {
		return Index("users", "u1", 1, Doc{}), nil
	})
	w := newWorker(0, h, esw, dlqw, cm, 3, 1000, 100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.run(ctx) }()
	w.submit(wRecord(`bad json`))
	w.submit(wRecord(insertPayload("u1", "alice")))
	time.Sleep(30 * time.Millisecond)
	w.flushSync(ctx)
	cancel()
	<-done
	if len(dlqw.sent) != 1 {
		t.Errorf("expected 1 DLQ, got %d", len(dlqw.sent))
	}
	if len(cm.marked) == 0 {
		t.Error("expected offset commit covering both records")
	}
}

func TestWorkerCtxDoneExitsCleanly(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := HandlerFunc(func(e *Event) (*Action, error) { return nil, nil })
	w := makeWorker(h, esw, dlqw, cm)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.run(ctx) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("worker did not exit after ctx.Done")
	}
}
