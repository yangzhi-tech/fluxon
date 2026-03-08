package buffer

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func rec() *kgo.Record { return &kgo.Record{} }
func action() interface{} { return struct{}{} } // non-nil sentinel

func TestAddAndPeek(t *testing.T) {
	b := New(10, time.Second)
	a := action()
	r := rec()
	b.Add(a, r)
	entries := b.Peek()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Action != a {
		t.Error("action mismatch")
	}
	if entries[0].Record != r {
		t.Error("record mismatch")
	}
}

func TestPeekDoesNotDrain(t *testing.T) {
	b := New(10, time.Second)
	b.Add(action(), rec())
	b.Peek()
	if b.Len() != 1 {
		t.Errorf("Peek should not drain buffer, got len=%d", b.Len())
	}
}

func TestPeekThenClear(t *testing.T) {
	b := New(10, time.Second)
	b.Add(action(), rec())
	b.Peek()
	b.Clear()
	if b.Len() != 0 {
		t.Errorf("expected empty after Clear, got %d", b.Len())
	}
}

func TestClearResetsLastFlush(t *testing.T) {
	b := New(10, 50*time.Millisecond)
	b.Add(action(), rec())
	time.Sleep(60 * time.Millisecond)
	if !b.ShouldFlush() {
		t.Error("expected ShouldFlush=true before Clear")
	}
	b.Clear()
	// add a new entry so buffer is non-empty
	b.Add(action(), rec())
	// lastFlush was just reset — should NOT flush yet on time basis
	if b.ShouldFlush() && b.actionCount < b.maxEvents {
		// only the time threshold could fire here; since we just cleared, it shouldn't
		t.Error("expected ShouldFlush=false right after Clear (time threshold)")
	}
}

func TestShouldFlushSize(t *testing.T) {
	b := New(3, time.Hour)
	b.Add(action(), rec())
	b.Add(action(), rec())
	if b.ShouldFlush() {
		t.Error("should not flush at 2 of 3")
	}
	b.Add(action(), rec())
	if !b.ShouldFlush() {
		t.Error("should flush at 3 of 3")
	}
}

func TestShouldFlushTime(t *testing.T) {
	b := New(1000, 50*time.Millisecond)
	b.Add(action(), rec())
	if b.ShouldFlush() {
		t.Error("should not flush immediately")
	}
	time.Sleep(60 * time.Millisecond)
	if !b.ShouldFlush() {
		t.Error("should flush after maxWait")
	}
}

func TestShouldFlushEmpty(t *testing.T) {
	b := New(1, 0)
	if b.ShouldFlush() {
		t.Error("should not flush when empty, even with zero maxWait")
	}
}

func TestNilActionDoesNotCountTowardSize(t *testing.T) {
	b := New(2, time.Hour)
	b.Add(nil, rec())
	b.Add(nil, rec())
	b.Add(nil, rec())
	if b.ShouldFlush() {
		t.Error("nil actions should not count toward size threshold")
	}
	if b.Len() != 3 {
		t.Errorf("expected 3 entries, got %d", b.Len())
	}
}

func TestPeekCopyNotAliased(t *testing.T) {
	b := New(10, time.Second)
	a := action()
	r := rec()
	b.Add(a, r)
	entries := b.Peek()
	entries[0].Action = nil // mutate copy
	// original should be unaffected
	orig := b.Peek()
	if orig[0].Action == nil {
		t.Error("Peek returned aliased slice; mutation affected original")
	}
}
