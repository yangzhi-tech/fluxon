# Fluxon — Low-Level Implementation Design

## Technology Choices

| Concern | Choice |
|---|---|
| Kafka client | `github.com/twmb/franz-go` (pure Go, first-class rebalance callbacks) |
| ES client | `github.com/elastic/go-elasticsearch/v8` (esapi style) |
| Config | `gopkg.in/yaml.v3`, duration fields in milliseconds/seconds as plain `int` |
| Logging | `log/slog` (stdlib, Go 1.21+) |
| Go version | 1.22+ |
| Module | `github.com/dropbox/fluxon` |

---

## Framework Design Principle

Fluxon is a **Go library**. Users import it, implement one interface, and compile their own binary.

The framework's contract is:

```
CDC Event → Handler.Handle() → *Action (or nil to skip)
```

The framework has no opinion on table routing, field mapping, index selection, or filtering — that is entirely user code. The framework delivers events and executes whatever the user returns.

---

## Public API (fluxon package)

### Handler Interface

```go
// The only interface users must implement.
type Handler interface {
    Handle(e *Event) (*Action, error)
}

// HandlerFunc is a function adapter for Handler.
type HandlerFunc func(e *Event) (*Action, error)
func (f HandlerFunc) Handle(e *Event) (*Action, error) { return f(e) }
```

Return values:
- `(*Action, nil)` — write the action to ES
- `(nil, nil)` — skip this event; offset is committed, nothing written to ES
- `(nil, error)` — handler failed; framework retries up to `MaxRetries`, then sends raw event to DLQ

### Event

```go
type Event struct {
    Op     Op
    Before Row    // nil on insert / snapshot
    After  Row    // nil on delete
    Source Source
}

type Source struct {
    LSN    int64
    Table  string
    Schema string
}

type Op string
const (
    OpInsert Op = "c"
    OpUpdate Op = "u"
    OpDelete Op = "d"
    OpRead   Op = "r"  // snapshot
)
```

### Row

Typed accessor over a Postgres row (`map[string]interface{}`). Zero-values are returned for missing or unconvertible keys — no panics.

```go
type Row map[string]interface{}

func (r Row) String(key string) string
func (r Row) Int(key string) int64
func (r Row) Float(key string) float64
func (r Row) Bool(key string) bool
func (r Row) Time(key string) time.Time    // parses epoch-ms int → time.Time
func (r Row) JSON(key string) interface{}  // unmarshals JSONB string → nested object
func (r Row) Raw(key string) interface{}   // raw value, no conversion
```

### Doc and Action

```go
// Doc is the ES document body the user constructs.
type Doc map[string]interface{}

// Action is returned by Handler.Handle. Opaque to the user; constructed via helpers.
type Action struct {
    opType string // "index" | "delete"
    index  string
    id     string
    lsn    int64
    doc    Doc
}

// Index produces an insert/update action (uses external version for idempotency).
func Index(index, id string, lsn int64, doc Doc) *Action

// SoftDelete produces a delete action (scripted upsert setting _deleted:true with LSN guard).
func SoftDelete(index, id string, lsn int64) *Action
```

### Engine

```go
type Engine struct { /* internal */ }

func New(cfg Config) *Engine

// Register sets the single Handler that processes all events.
// Calling Register multiple times replaces the previous handler.
func (e *Engine) Register(h Handler) *Engine

// Run starts the engine and blocks until ctx is cancelled or a fatal error occurs.
func (e *Engine) Run(ctx context.Context) error
```

---

## Config

No YAML file. Config is passed programmatically by the user in Go code.

```go
type Config struct {
    Kafka  KafkaConfig
    ES     ESConfig
    Buffer BufferConfig
    Sweep  SweepConfig
}

type KafkaConfig struct {
    Brokers  []string
    Topic    string
    GroupID  string
    DLQTopic string
    MaxHandlerRetries int // default: 3
}

type ESConfig struct {
    Addresses      []string
    MaxRetries     int // default: 5
    RetryBackoffMs int // default: 200
}

type BufferConfig struct {
    MaxEvents int // default: 1000
    MaxWaitMs int // default: 500
}

type SweepConfig struct {
    Enabled     bool
    IntervalS   int   // default: 300
    SafeHorizon int64 // LSN below which soft-deleted docs are purged
}
```

---

## Package Layout

```
fluxon.go                   — public API: Handler, Event, Row, Doc, Action, Engine, Config
internal/
  cdc/event.go              — Debezium wire format (json tags); parsed into public Event
  es/
    writer.go               — bulk write with retry
    sweep.go                — background soft-delete sweeper
  dlq/
    writer.go               — dead letter queue producer
  buffer/
    buffer.go               — per-partition in-memory buffer
    buffer_test.go
  worker/
    worker.go               — per-partition processing goroutine
  engine/
    engine.go               — kafka consumer, rebalance, worker lifecycle
```

`fluxon.go` is the single public-facing file. All internals are unexported.

---

## Internal: CDC Wire Format

The Debezium JSON on the wire is parsed into an internal struct, then converted to the public `Event`:

```go
// internal/cdc/event.go — never exposed to users
type rawEvent struct {
    Before map[string]interface{} `json:"before"`
    After  map[string]interface{} `json:"after"`
    Source struct {
        LSN    int64  `json:"lsn"`
        Table  string `json:"table"`
        Schema string `json:"schema"`
    } `json:"source"`
    Op string `json:"op"`
}
```

Conversion to public `fluxon.Event` happens in the worker before calling `Handler.Handle`.

---

## Internal: Buffer

**Invariant**: owned exclusively by one worker goroutine — no locking.

```go
// internal/buffer/buffer.go
type Entry struct {
    Action *fluxon.Action // nil for DLQ'd / skipped records
    Record *kgo.Record    // always set; used for MarkCommitRecords
}

type Buffer struct {
    entries   []Entry
    maxEvents int           // flush threshold: count of non-nil actions
    maxWait   time.Duration
    lastFlush time.Time
}

func (b *Buffer) Add(action *fluxon.Action, record *kgo.Record)
func (b *Buffer) ShouldFlush() bool  // non-nil action count >= maxEvents OR elapsed >= maxWait
func (b *Buffer) Peek() []Entry      // returns a copy; does not remove entries
func (b *Buffer) Clear()             // resets entries slice and lastFlush
func (b *Buffer) Len() int
```

**Flush semantics**: `Peek → flush ES → Clear` on success. If ES flush fails, entries remain in the buffer and are retried on the next flush trigger.

---

## Internal: ES Writer

```go
// internal/es/writer.go
type Writer struct {
    client     *elasticsearch.Client
    maxRetries int
    backoff    time.Duration
}

func (w *Writer) Flush(ctx context.Context, actions []*fluxon.Action) error
```

**Bulk body format** (NDJSON):

For `Index` actions:
```json
{"index":{"_index":"<idx>","_id":"<id>","version":<lsn>,"version_type":"external"}}
{"field":"value","_lsn":<lsn>}
```

For `SoftDelete` actions (scripted upsert):
```json
{"update":{"_index":"<idx>","_id":"<id>"}}
{"scripted_upsert":true,"script":{"source":"<painless>","lang":"painless","params":{"lsn":<lsn>}},"upsert":{"_lsn":<lsn>,"_deleted":true}}
```

**Painless script**:
```
if (ctx._source._lsn == null || params.lsn > ctx._source._lsn) {
  ctx._source._deleted = true;
  ctx._source._lsn = params.lsn;
} else {
  ctx.op = 'none';
}
```

**Retry logic**:
- Network / 5xx: retry whole batch with exponential backoff (`backoff * 2^attempt`)
- Per-item 409 (version conflict): treat as success — idempotent
- Per-item other error: retry whole batch (all ops are idempotent)
- Retries exhausted: return error; worker does not commit offset

**ES API**: `esapi.BulkRequest{Body: body}.Do(ctx, client)`

---

## Internal: Worker

```go
// internal/worker/worker.go
type Worker struct {
    partition int32
    msgCh     chan *kgo.Record // buffered, capacity 256
    flushCh   chan chan error  // capacity 1; synchronous flush signal from rebalance handler
    buf       *buffer.Buffer
    handler   fluxon.Handler
    esWriter  *es.Writer
    dlqWriter *dlq.Writer
    client    *kgo.Client     // shared; used for MarkCommitRecords
    maxRetries int
}
```

**Event loop** (`Worker.Run(ctx)`):

```
select {
  case record := <-msgCh:
      processRecord(record)
      if buf.ShouldFlush() → flushBuffer()

  case <-ticker.C (50ms):
      if buf.ShouldFlush() → flushBuffer()

  case errCh := <-flushCh:
      drain msgCh fully
      errCh <- flushBuffer()

  case <-ctx.Done():
      drain msgCh fully
      flushBuffer()
      return
}
```

**processRecord**:
1. `json.Unmarshal` → `cdc.rawEvent` → `fluxon.Event`; on parse error → DLQ, `buf.Add(nil, record)`, return
2. Call `handler.Handle(event)` up to `maxRetries` times on error
3. On exhausted retries → DLQ, `buf.Add(nil, record)`, return
4. On `nil` action → `buf.Add(nil, record)` (skip; offset still committed with next flush)
5. On valid action → `buf.Add(action, record)`

**flushBuffer**:
1. `entries := buf.Peek()` — if empty, return nil
2. Collect non-nil actions
3. `esWriter.Flush(ctx, actions)` — on error, return error (entries remain)
4. On success: `buf.Clear()`, `client.MarkCommitRecords(entries[last].Record)`

**Worker.Flush(ctx) error** (called by rebalance handler):
- Sends `errCh` on `flushCh`, blocks until result

---

## Internal: Engine

```go
// internal/engine/engine.go
type Engine struct {
    client    *kgo.Client
    workers   map[int32]*Worker
    mu        sync.Mutex
    handler   fluxon.Handler
    esWriter  *es.Writer
    dlqWriter *dlq.Writer
    cfg       fluxon.Config
}
```

**Franz-go options**:
```go
kgo.SeedBrokers(cfg.Kafka.Brokers...),
kgo.ConsumerGroup(cfg.Kafka.GroupID),
kgo.ConsumeTopics(cfg.Kafka.Topic),
kgo.BlockRebalanceOnPoll(),
kgo.AutoCommitMarks(),
kgo.OnPartitionsAssigned(e.onAssigned),
kgo.OnPartitionsRevoked(e.onRevoked),
```

**onAssigned**: create Worker per partition, `go worker.Run(ctx)`

**onRevoked**: `worker.Flush(ctx)` for each revoked partition (synchronous), then `removeWorker`. `AutoCommitMarks` commits all marked offsets before rebalance proceeds.

**Poll loop**:
```go
for {
    fetches := client.PollFetches(ctx)
    if err := fetches.Err(); err != nil { /* log; shutdown on persistent error */ }
    fetches.EachPartition(func(p kgo.FetchTopicPartition) {
        w := e.getWorker(p.Partition)
        for _, r := range p.Records {
            w.msgCh <- r  // blocking; backpressure from worker channel
        }
    })
}
```

---

## Internal: DLQ Writer

```go
// internal/dlq/writer.go
type Writer struct {
    client *kgo.Client // dedicated producer client
    topic  string
}

type Message struct {
    OriginalPayload []byte    `json:"original_payload"`
    Error           string    `json:"error"`
    Partition       int32     `json:"partition"`
    Offset          int64     `json:"offset"`
    Timestamp       time.Time `json:"timestamp"`
}

func (w *Writer) Send(ctx context.Context, payload []byte, partition int32, offset int64, reason error)
```

Uses `client.ProduceSync`. Logs on failure; does not block the worker.

---

## Internal: Sweeper

```go
// internal/es/sweep.go
type Sweeper struct {
    client      *elasticsearch.Client
    index       string
    interval    time.Duration
    safeHorizon int64
}
```

Runs `delete_by_query` on a ticker. Query: `_deleted: true AND _lsn < safeHorizon`.

Uses `esapi.DeleteByQueryRequest{Index: []string{idx}, Body: body}.Do(ctx, client)`.

---

## Offset Commit Strategy

**Invariant**: offsets are committed in monotonically increasing order, only after ES confirms.

- Every record — including DLQ'd and skipped ones — enters the buffer as an `Entry` with its `*kgo.Record`.
- After a successful ES flush, `MarkCommitRecords(entries[last].Record)` advances the commit watermark across the entire batch, including any DLQ/skip entries it contains.
- DLQ/skip records are therefore committed at the next successful flush, delayed by at most `MaxWaitMs`. This is acceptable.
- `AutoCommitMarks` commits periodically in the background and synchronously during `OnPartitionsRevoked`.

---

## Error Handling Matrix

| Scenario | Action |
|---|---|
| CDC JSON parse failure | → DLQ (not retryable); buffer nil-action entry |
| `handler.Handle` error, retries remaining | Retry up to `MaxHandlerRetries` |
| `handler.Handle` retries exhausted | → DLQ; buffer nil-action entry |
| `handler.Handle` returns `(nil, nil)` | Skip; buffer nil-action entry |
| ES bulk failure (network / 5xx) | Retry whole batch with exponential backoff |
| ES item 409 version conflict | Treat as success (idempotent) |
| ES item other error | Retry whole batch |
| ES retries exhausted | Return error; offsets not committed; retries on restart |
| Kafka consumer transient error | Log + re-poll |
| Kafka consumer persistent error | Controlled shutdown |

---

## User Code Example

```go
package main

import (
    "context"
    "log"
    "github.com/dropbox/fluxon"
)

type MyHandler struct{}

func (h *MyHandler) Handle(e *fluxon.Event) (*fluxon.Action, error) {
    switch e.Source.Table {
    case "users":
        if e.Op == fluxon.OpDelete {
            return fluxon.SoftDelete("users", e.Before.String("id"), e.Source.LSN), nil
        }
        return fluxon.Index("users", e.After.String("id"), e.Source.LSN, fluxon.Doc{
            "email":     e.After.String("email"),
            "createdAt": e.After.Time("created_at"),
            "profile":   e.After.JSON("profile"),
            "fullName":  e.After.String("first_name") + " " + e.After.String("last_name"),
        }), nil

    case "orders":
        if e.Op == fluxon.OpDelete {
            return fluxon.SoftDelete("orders", e.Before.String("order_id"), e.Source.LSN), nil
        }
        return fluxon.Index("orders", e.After.String("order_id"), e.Source.LSN, fluxon.Doc{
            "userId": e.After.String("user_id"),
            "total":  e.After.Float("total_amount"),
        }), nil

    default:
        return nil, nil // skip unknown tables
    }
}

func main() {
    engine := fluxon.New(fluxon.Config{
        Kafka: fluxon.KafkaConfig{
            Brokers:           []string{"localhost:9092"},
            Topic:             "postgres.cdc",
            GroupID:           "fluxon-group",
            DLQTopic:          "fluxon-dlq",
            MaxHandlerRetries: 3,
        },
        ES: fluxon.ESConfig{
            Addresses:      []string{"http://localhost:9200"},
            MaxRetries:     5,
            RetryBackoffMs: 200,
        },
        Buffer: fluxon.BufferConfig{
            MaxEvents: 1000,
            MaxWaitMs: 500,
        },
        Sweep: fluxon.SweepConfig{
            Enabled:     true,
            IntervalS:   300,
            SafeHorizon: 0, // set to a real LSN in production
        },
    })

    if err := engine.Register(&MyHandler{}).Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

---

## Implementation Order

1. `internal/buffer` + tests — no external deps, pure logic
2. `internal/cdc` — Debezium wire types
3. `fluxon.go` — public types: `Event`, `Row`, `Doc`, `Action`, `Handler`, `Config`, `Engine` shell
4. `internal/es/writer` — bulk body builder + retry
5. `internal/dlq/writer` — Kafka producer
6. `internal/worker` — ties handler + buffer + es + dlq together
7. `internal/engine` — rebalance + poll loop; completes `Engine.Run`
8. `internal/es/sweep` — independent background goroutine
