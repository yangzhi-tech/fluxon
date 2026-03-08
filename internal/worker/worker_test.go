package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dropbox/fluxon/pkg/types"
)

// ---- mocks ----

type mockESW struct {
	calls   int
	failErr error
}

func (m *mockESW) Flush(_ context.Context, _ []*types.Action) error {
	m.calls++
	return m.failErr
}

type mockDLQW struct{ sent [][]byte }

func (m *mockDLQW) Send(_ context.Context, payload []byte, _ int32, _ int64, _ error) {
	m.sent = append(m.sent, payload)
}

type mockCommit struct{ marked []*kgo.Record }

func (m *mockCommit) MarkCommitRecords(records ...*kgo.Record) {
	m.marked = append(m.marked, records...)
}

// ---- helpers ----

func rec(payload string) *kgo.Record { return &kgo.Record{Value: []byte(payload)} }

func insertPayload(id, name string) string {
	return `{"before":null,"after":{"id":"` + id + `","name":"` + name + `"},"source":{"lsn":1,"table":"users","schema":"public"},"op":"c"}`
}

func deletePayload(id string) string {
	return `{"before":{"id":"` + id + `"},"after":null,"source":{"lsn":2,"table":"users","schema":"public"},"op":"d"}`
}

func makeWorker(h types.Handler, esw *mockESW, dlqw *mockDLQW, cm *mockCommit) *Worker {
	return New(0, h, esw, dlqw, cm, 3, 5, 100)
}

func runWith(t *testing.T, w *Worker, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	for _, r := range records {
		w.Submit(r)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done
}

// ---- tests ----

func TestHappyPathInsert(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	called := false
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		called = true
		return types.Index("users", e.After.String("id"), e.Source.LSN, types.Doc{}), nil
	})
	runWith(t, makeWorker(h, esw, dlqw, cm), rec(insertPayload("u1", "alice")))
	if !called {
		t.Error("handler not called")
	}
}

func TestHappyPathDelete(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		if e.Op == types.OpDelete {
			return types.SoftDelete("users", e.Before.String("id"), e.Source.LSN), nil
		}
		return nil, nil
	})
	runWith(t, makeWorker(h, esw, dlqw, cm), rec(deletePayload("u2")))
	if len(dlqw.sent) != 0 {
		t.Error("unexpected DLQ")
	}
}

func TestHandlerReturnsNilNil(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) { return nil, nil })
	runWith(t, makeWorker(h, esw, dlqw, cm), rec(insertPayload("u1", "alice")))
	if len(dlqw.sent) != 0 {
		t.Error("skip should not DLQ")
	}
}

func TestHandlerRetryThenSuccess(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	attempts := 0
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("transient")
		}
		return types.Index("users", "u1", 1, types.Doc{}), nil
	})
	runWith(t, makeWorker(h, esw, dlqw, cm), rec(insertPayload("u1", "alice")))
	if attempts < 3 {
		t.Errorf("expected >=3 attempts, got %d", attempts)
	}
	if len(dlqw.sent) != 0 {
		t.Error("should not DLQ when retry succeeds")
	}
}

func TestHandlerExhaustsRetries(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) { return nil, errors.New("always fails") })
	runWith(t, makeWorker(h, esw, dlqw, cm), rec(insertPayload("u1", "alice")))
	if len(dlqw.sent) != 1 {
		t.Errorf("expected 1 DLQ, got %d", len(dlqw.sent))
	}
}

func TestCDCParseFailure(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) { return nil, nil })
	runWith(t, makeWorker(h, esw, dlqw, cm), rec(`{bad json`))
	if len(dlqw.sent) != 1 {
		t.Errorf("expected 1 DLQ for parse failure, got %d", len(dlqw.sent))
	}
}

func TestSizeThresholdFlush(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		return types.Index("users", "u1", 1, types.Doc{}), nil
	})
	w := New(0, h, esw, dlqw, cm, 3, 3, 10000)
	runWith(t, w, rec(insertPayload("u1", "a")), rec(insertPayload("u2", "b")), rec(insertPayload("u3", "c")))
	if esw.calls == 0 {
		t.Error("expected ES flush at size threshold")
	}
}

func TestTimeThresholdFlush(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		return types.Index("users", "u1", 1, types.Doc{}), nil
	})
	w := New(0, h, esw, dlqw, cm, 3, 1000, 50)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	w.Submit(rec(insertPayload("u1", "alice")))
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
	if esw.calls == 0 {
		t.Error("expected ES flush at time threshold")
	}
}

func TestESFlushFailureNoCommit(t *testing.T) {
	esw := &mockESW{failErr: errors.New("es down")}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		return types.Index("users", "u1", 1, types.Doc{}), nil
	})
	w := New(0, h, esw, dlqw, cm, 3, 1, 10000)
	runWith(t, w, rec(insertPayload("u1", "alice")))
	if len(cm.marked) != 0 {
		t.Error("offset should not be committed when ES flush fails")
	}
}

func TestFlushSyncDrainsAndFlushes(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		return types.Index("users", "u1", 1, types.Doc{}), nil
	})
	w := New(0, h, esw, dlqw, cm, 3, 1000, 10000)
	ctx := context.Background()
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	w.Submit(rec(insertPayload("u1", "alice")))
	time.Sleep(20 * time.Millisecond)
	if err := w.Flush(ctx); err != nil {
		t.Errorf("unexpected flush error: %v", err)
	}
	if esw.calls == 0 || len(cm.marked) == 0 {
		t.Error("expected ES flush and offset commit")
	}
	ctxC, cancel := context.WithCancel(ctx)
	cancel()
	w.Run(ctxC)
}

func TestMarkCommitAfterFlush(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		return types.Index("users", "u1", 1, types.Doc{}), nil
	})
	w := New(0, h, esw, dlqw, cm, 3, 1, 10000)
	runWith(t, w, rec(insertPayload("u1", "alice")))
	if len(cm.marked) == 0 {
		t.Error("MarkCommitRecords should be called after successful flush")
	}
}

func TestMixedDLQAndValidBatch(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) {
		return types.Index("users", "u1", 1, types.Doc{}), nil
	})
	w := New(0, h, esw, dlqw, cm, 3, 1000, 100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	w.Submit(rec(`bad json`))
	w.Submit(rec(insertPayload("u1", "alice")))
	time.Sleep(30 * time.Millisecond)
	w.Flush(ctx)
	cancel()
	<-done
	if len(dlqw.sent) != 1 {
		t.Errorf("expected 1 DLQ, got %d", len(dlqw.sent))
	}
	if len(cm.marked) == 0 {
		t.Error("expected offset commit covering both records")
	}
}

func TestCtxDoneExitsCleanly(t *testing.T) {
	esw := &mockESW{}
	dlqw := &mockDLQW{}
	cm := &mockCommit{}
	h := types.HandlerFunc(func(e *types.Event) (*types.Action, error) { return nil, nil })
	w := makeWorker(h, esw, dlqw, cm)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("worker did not exit after ctx.Done")
	}
}
