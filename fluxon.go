// Package fluxon is a lightweight stream processing engine that consumes
// Postgres CDC events from Kafka and writes to Elasticsearch.
//
// Users implement the Handler interface and pass it to Engine.Register,
// then call Engine.Run to start the pipeline.
package fluxon

import (
	"context"

	"github.com/dropbox/fluxon/internal/engine"
	"github.com/dropbox/fluxon/pkg/types"
)

// Re-export all public types from pkg/types so users import only this package.

type Op = types.Op

const (
	OpInsert = types.OpInsert
	OpUpdate = types.OpUpdate
	OpDelete = types.OpDelete
	OpRead   = types.OpRead
)

type Source = types.Source
type Event = types.Event
type Row = types.Row
type Doc = types.Doc
type Action = types.Action

// Index produces an insert/update action using external version for idempotency.
func Index(index, id string, lsn int64, doc Doc) *Action { return types.Index(index, id, lsn, doc) }

// SoftDelete produces a delete action as a scripted upsert setting _deleted:true.
func SoftDelete(index, id string, lsn int64) *Action { return types.SoftDelete(index, id, lsn) }

type Handler = types.Handler
type HandlerFunc = types.HandlerFunc

type Config = types.Config
type KafkaConfig = types.KafkaConfig
type ESConfig = types.ESConfig
type BufferConfig = types.BufferConfig
type SweepConfig = types.SweepConfig

// Engine is the main entry point. Create with New, register a handler, then call Run.
type Engine struct {
	cfg     Config
	handler Handler
}

// New creates a new Engine with the given configuration.
func New(cfg Config) *Engine { return &Engine{cfg: cfg} }

// Register sets the Handler that processes all events.
// Calling Register multiple times replaces the previous handler.
func (e *Engine) Register(h Handler) *Engine {
	e.handler = h
	return e
}

// Run starts the engine and blocks until ctx is cancelled or a fatal error occurs.
func (e *Engine) Run(ctx context.Context) error {
	eng, err := engine.New(e.cfg, e.handler)
	if err != nil {
		return err
	}
	return eng.Run(ctx)
}
