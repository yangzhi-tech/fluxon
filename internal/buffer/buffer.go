package buffer

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Entry holds a single processed record. Action is nil for skipped/DLQ'd records.
type Entry struct {
	Action interface{} // *fluxon.Action — using interface{} to avoid circular import
	Record *kgo.Record // always set; used for MarkCommitRecords
}

// Buffer is an in-memory buffer owned exclusively by one worker goroutine — no locking.
type Buffer struct {
	entries      []Entry
	maxEvents    int // flush when non-nil action count >= maxEvents
	maxWait      time.Duration
	lastFlush    time.Time
	actionCount  int // cached count of non-nil actions
}

// New creates a new Buffer.
func New(maxEvents int, maxWait time.Duration) *Buffer {
	return &Buffer{
		maxEvents: maxEvents,
		maxWait:   maxWait,
		lastFlush: time.Now(),
	}
}

// Add appends an entry to the buffer.
func (b *Buffer) Add(action interface{}, record *kgo.Record) {
	b.entries = append(b.entries, Entry{Action: action, Record: record})
	if action != nil {
		b.actionCount++
	}
}

// ShouldFlush returns true if the buffer should be flushed.
// It never triggers on an empty buffer.
func (b *Buffer) ShouldFlush() bool {
	if len(b.entries) == 0 {
		return false
	}
	if b.actionCount >= b.maxEvents {
		return true
	}
	if time.Since(b.lastFlush) >= b.maxWait {
		return true
	}
	return false
}

// Peek returns a copy of the current entries without removing them.
func (b *Buffer) Peek() []Entry {
	if len(b.entries) == 0 {
		return nil
	}
	cp := make([]Entry, len(b.entries))
	copy(cp, b.entries)
	return cp
}

// Clear resets the buffer and resets the flush timer.
func (b *Buffer) Clear() {
	b.entries = b.entries[:0]
	b.actionCount = 0
	b.lastFlush = time.Now()
}

// Len returns the total number of entries in the buffer.
func (b *Buffer) Len() int {
	return len(b.entries)
}
