package fluxon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ---- mock worker ----

type mockWorker struct {
	mu        sync.Mutex
	submitted []*kgo.Record
	flushed   int
	partition int32
	runCalled bool
}

func (m *mockWorker) submit(r *kgo.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submitted = append(m.submitted, r)
}

func (m *mockWorker) flushSync(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushed++
	return nil
}

func (m *mockWorker) run(_ context.Context) {
	m.mu.Lock()
	m.runCalled = true
	m.mu.Unlock()
}

// ---- engine helpers ----

func newTestEngine(factory workerFactory) *engineImpl {
	return &engineImpl{
		cfg:     Config{},
		factory: factory,
		workers: make(map[int32]workerIface),
	}
}

// ---- tests ----

func TestEngineOnAssignedCreatesWorkers(t *testing.T) {
	workers := map[int32]*mockWorker{}
	var mu sync.Mutex
	factory := func(p int32) workerIface {
		w := &mockWorker{partition: p}
		mu.Lock()
		workers[p] = w
		mu.Unlock()
		return w
	}

	e := newTestEngine(factory)
	e.workerCtx, e.workerCancel = context.WithCancel(context.Background())
	defer e.workerCancel()

	e.onAssigned(context.Background(), nil, map[string][]int32{"topic": {0, 1, 2}})

	e.mu.Lock()
	n := len(e.workers)
	e.mu.Unlock()
	if n != 3 {
		t.Errorf("expected 3 workers, got %d", n)
	}
	time.Sleep(10 * time.Millisecond) // let goroutines start
	mu.Lock()
	for p, w := range workers {
		if !w.runCalled {
			t.Errorf("worker for partition %d: run not called", p)
		}
	}
	mu.Unlock()
}

func TestEngineOnRevokedFlushesAndRemoves(t *testing.T) {
	w0 := &mockWorker{partition: 0}
	w1 := &mockWorker{partition: 1}

	e := newTestEngine(nil)
	e.workers[0] = w0
	e.workers[1] = w1

	e.onRevoked(context.Background(), nil, map[string][]int32{"topic": {0}})

	e.mu.Lock()
	_, still0 := e.workers[0]
	_, still1 := e.workers[1]
	e.mu.Unlock()

	if still0 {
		t.Error("worker 0 should be removed after revoke")
	}
	if !still1 {
		t.Error("worker 1 should remain")
	}
	if w0.flushed != 1 {
		t.Errorf("expected flush called once on revoke, got %d", w0.flushed)
	}
	if w1.flushed != 0 {
		t.Error("worker 1 should not be flushed")
	}
}

func TestEngineGetWorkerUnknownPartitionNoPanic(t *testing.T) {
	e := newTestEngine(nil)
	w := e.getWorker(99)
	if w != nil {
		t.Error("expected nil for unknown partition")
	}
}

func TestEnginePollDispatchRoutesToCorrectWorker(t *testing.T) {
	w0 := &mockWorker{partition: 0}
	w1 := &mockWorker{partition: 1}

	e := newTestEngine(nil)
	e.workers[0] = w0
	e.workers[1] = w1

	r0 := &kgo.Record{Partition: 0}
	r1 := &kgo.Record{Partition: 1}

	// Simulate what the poll loop does.
	e.getWorker(0).submit(r0)
	e.getWorker(1).submit(r1)

	w0.mu.Lock()
	n0 := len(w0.submitted)
	w0.mu.Unlock()
	w1.mu.Lock()
	n1 := len(w1.submitted)
	w1.mu.Unlock()

	if n0 != 1 || w0.submitted[0] != r0 {
		t.Error("record not routed to worker 0")
	}
	if n1 != 1 || w1.submitted[0] != r1 {
		t.Error("record not routed to worker 1")
	}
}
