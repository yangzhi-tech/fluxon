// Package fluxon is a lightweight stream processing engine that consumes
// Postgres CDC events from Kafka and writes to Elasticsearch.
package fluxon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Op represents a CDC operation type.
type Op string

const (
	OpInsert Op = "c"
	OpUpdate Op = "u"
	OpDelete Op = "d"
	OpRead   Op = "r" // snapshot
)

// Source holds metadata about where a CDC event originated.
type Source struct {
	LSN    int64
	Table  string
	Schema string
}

// Event is a parsed CDC event delivered to the user's Handler.
type Event struct {
	Op     Op
	Before Row // nil on insert / snapshot
	After  Row // nil on delete
	Source Source
}

// Row is a typed accessor over a Postgres row (map[string]interface{}).
// All accessors return zero values for missing or unconvertible keys — no panics.
type Row map[string]interface{}

// String returns the string value for the given key.
func (r Row) String(key string) string {
	v, ok := r[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

// Int returns the int64 value for the given key. JSON numbers are float64.
func (r Row) Int(key string) int64 {
	v, ok := r[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

// Float returns the float64 value for the given key.
func (r Row) Float(key string) float64 {
	v, ok := r[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	}
	return 0
}

// Bool returns the bool value for the given key.
func (r Row) Bool(key string) bool {
	v, ok := r[key]
	if !ok || v == nil {
		return false
	}
	b, ok := v.(bool)
	if !ok {
		return false
	}
	return b
}

// Time parses an epoch-millisecond integer into time.Time.
func (r Row) Time(key string) time.Time {
	ms := r.Int(key)
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// JSON unmarshals a JSONB string value into a nested object.
// If the value is already a non-string type, it is returned as-is.
func (r Row) JSON(key string) interface{} {
	v, ok := r[key]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return v
	}
	var out interface{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return s
	}
	return out
}

// Raw returns the raw value without any conversion.
func (r Row) Raw(key string) interface{} {
	return r[key]
}

// Doc is the Elasticsearch document body.
type Doc map[string]interface{}

// Action is returned by Handler.Handle. Constructed via Index or SoftDelete.
type Action struct {
	OpType string // "index" | "delete"
	Index  string
	ID     string
	LSN    int64
	Doc    Doc
}

// Index produces an insert/update action using external version for idempotency.
func Index(index, id string, lsn int64, doc Doc) *Action {
	return &Action{
		OpType: "index",
		Index:  index,
		ID:     id,
		LSN:    lsn,
		Doc:    doc,
	}
}

// SoftDelete produces a delete action as a scripted upsert setting _deleted:true.
func SoftDelete(index, id string, lsn int64) *Action {
	return &Action{
		OpType: "delete",
		Index:  index,
		ID:     id,
		LSN:    lsn,
	}
}

// Handler is the single interface users must implement.
type Handler interface {
	Handle(e *Event) (*Action, error)
}

// HandlerFunc is a function adapter for Handler.
type HandlerFunc func(e *Event) (*Action, error)

func (f HandlerFunc) Handle(e *Event) (*Action, error) { return f(e) }

// Config holds all configuration for the engine.
type Config struct {
	Kafka  KafkaConfig
	ES     ESConfig
	Buffer BufferConfig
	Sweep  SweepConfig
}

// KafkaConfig holds Kafka consumer configuration.
type KafkaConfig struct {
	Brokers           []string
	Topic             string
	GroupID           string
	DLQTopic          string
	MaxHandlerRetries int // default: 3
}

// ESConfig holds Elasticsearch client configuration.
type ESConfig struct {
	Addresses      []string
	MaxRetries     int // default: 5
	RetryBackoffMs int // default: 200
}

// BufferConfig holds buffer flush policy configuration.
type BufferConfig struct {
	MaxEvents int // default: 1000
	MaxWaitMs int // default: 500
}

// SweepConfig holds soft-delete sweep configuration.
type SweepConfig struct {
	Enabled     bool
	IntervalS   int   // default: 300
	SafeHorizon int64 // LSN below which soft-deleted docs are purged
}

// Engine is the main entry point. Create with New, register a handler, then call Run.
type Engine struct {
	cfg     Config
	handler Handler
}

// New creates a new Engine with the given configuration.
func New(cfg Config) *Engine {
	return &Engine{cfg: cfg}
}

// Register sets the Handler that processes all events. Calling it multiple times
// replaces the previous handler.
func (e *Engine) Register(h Handler) *Engine {
	e.handler = h
	return e
}

// Run starts the engine and blocks until ctx is cancelled or a fatal error occurs.
func (e *Engine) Run(ctx context.Context) error {
	// Filled in by internal/engine.
	return runEngine(ctx, e.cfg, e.handler)
}
