// Package types holds the shared public types for Fluxon.
// Users import github.com/dropbox/fluxon which re-exports everything here.
package types

import (
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

// Raw returns the raw value without conversion.
func (r Row) Raw(key string) interface{} {
	return r[key]
}

// Doc is the Elasticsearch document body.
type Doc map[string]interface{}

// Action is returned by Handler.Handle. Constructed via Index or SoftDelete.
type Action struct {
	OpType string
	Index  string
	ID     string
	LSN    int64
	Doc    Doc
}

// Index produces an insert/update action using external version for idempotency.
func Index(index, id string, lsn int64, doc Doc) *Action {
	return &Action{OpType: "index", Index: index, ID: id, LSN: lsn, Doc: doc}
}

// SoftDelete produces a delete action as a scripted upsert setting _deleted:true.
func SoftDelete(index, id string, lsn int64) *Action {
	return &Action{OpType: "delete", Index: index, ID: id, LSN: lsn}
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

type KafkaConfig struct {
	Brokers           []string
	Topic             string
	GroupID           string
	DLQTopic          string
	MaxHandlerRetries int
}

type ESConfig struct {
	Addresses      []string
	Username       string
	Password       string
	InsecureTLS    bool
	MaxRetries     int
	RetryBackoffMs int
}

type BufferConfig struct {
	MaxEvents int
	MaxWaitMs int
}

type SweepConfig struct {
	Enabled     bool
	IntervalS   int
	SafeHorizon int64
}
