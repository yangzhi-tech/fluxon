# Fluxon

A lightweight, stateless stream processing engine written in Go. Fluxon consumes Postgres CDC events from Kafka (produced by [Debezium](https://debezium.io/)), transforms them through user-supplied logic, and performs idempotent bulk writes to Elasticsearch.

## Overview

```
Kafka (Debezium CDC)
        │
        ▼
  [Reader] multi-partition consumer
        │
        ▼
  [Handler] user-supplied transform
        │
        ▼
  [Buffer] per-partition in-memory batch
        │
        ▼
  [ES Writer] _bulk API with retry
        │
        ▼
Elasticsearch
```

Fluxon is a **library**. You implement one interface, wire up config, and compile your own binary. The framework handles Kafka consumption, rebalance, buffering, retry, offset commits, and dead-letter routing. You own the mapping logic.

## Installation

```bash
go get github.com/dropbox/fluxon
```

Requires Go 1.22+.

## Usage

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
            "fullName":  e.After.String("first_name") + " " + e.After.String("last_name"),
            "createdAt": e.After.Time("created_at"),
            "profile":   e.After.JSON("profile_jsonb"),
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
        return nil, nil // skip — offset still committed
    }
}

func main() {
    engine := fluxon.New(fluxon.Config{
        Kafka: fluxon.KafkaConfig{
            Brokers:           []string{"localhost:9092"},
            Topic:             "postgres.cdc",
            GroupID:           "fluxon-indexer",
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

### Handler contract

| Return value | Meaning |
|---|---|
| `(*Action, nil)` | Write action to ES |
| `(nil, nil)` | Skip event; offset committed, nothing written |
| `(nil, error)` | Handler failed; retried up to `MaxHandlerRetries`, then sent to DLQ |

### Row accessors

`Row` provides typed, zero-value-safe accessors over a Postgres row:

| Method | Notes |
|---|---|
| `String(key)` | Returns `""` if missing |
| `Int(key)` | Converts JSON `float64` automatically |
| `Float(key)` | Returns `0` if missing |
| `Bool(key)` | Returns `false` if missing |
| `Time(key)` | Parses epoch-millisecond integer to `time.Time` |
| `JSON(key)` | Unmarshals a JSONB string to a nested object |
| `Raw(key)` | Raw value, no conversion |

## Configuration

| Field | Default | Description |
|---|---|---|
| `Kafka.Brokers` | — | Kafka broker addresses |
| `Kafka.Topic` | — | Topic to consume |
| `Kafka.GroupID` | — | Consumer group ID |
| `Kafka.DLQTopic` | — | Dead-letter topic (optional) |
| `Kafka.MaxHandlerRetries` | 3 | Handler retries before DLQ |
| `ES.Addresses` | — | Elasticsearch addresses |
| `ES.MaxRetries` | 5 | Bulk request retries |
| `ES.RetryBackoffMs` | 200 | Initial backoff (doubles each retry) |
| `Buffer.MaxEvents` | 1000 | Flush when buffer reaches this many actions |
| `Buffer.MaxWaitMs` | 500 | Flush after this many ms even if buffer is not full |
| `Sweep.Enabled` | false | Enable background soft-delete purge |
| `Sweep.IntervalS` | 300 | Sweep interval in seconds |
| `Sweep.SafeHorizon` | 0 | LSN below which soft-deleted docs are purged |

## Design

### Idempotency

Fluxon uses an **at-least-once** delivery model: offsets are committed only after Elasticsearch confirms a successful bulk write. Redelivery on crash/restart is expected and handled without data corruption.

**INSERT / UPDATE** — uses ES `index` with `version_type=external` and the Postgres WAL LSN as the version number. Elasticsearch natively rejects writes whose version is not strictly greater than the stored version, making redelivery a no-op.

**DELETE** — uses a scripted upsert (Painless) that sets `_deleted: true` and updates `_lsn`. A Painless script is required because the operation must be conditional (LSN guard) and must upsert a tombstone if the document does not exist yet.

```
if (ctx._source._lsn == null || params.lsn > ctx._source._lsn) {
    ctx._source._deleted = true;
    ctx._source._lsn = params.lsn;
} else {
    ctx.op = 'none';  // no-op: stale redelivery
}
```

This means a redelivered older INSERT cannot resurrect a soft-deleted document.

### Soft deletes and the filtered alias

DELETE events are written as tombstones rather than hard deletes. This preserves the LSN version guard across redeliveries. All Elasticsearch queries should target a **filtered alias** that excludes `_deleted: true` documents.

A background **Sweeper** periodically runs `delete_by_query` to purge tombstones whose `_lsn` is below a configured `SafeHorizon` (e.g., derived from consumer group lag), reclaiming storage once it is safe to do so.

### Partitioning and ordering

Each Kafka partition maps to a dedicated goroutine (worker). Rows that hash to the same Kafka partition are processed in WAL order; different partitions are processed concurrently. This gives natural parallelism without requiring any cross-partition coordination.

### Buffer flush policy

Each worker owns an in-memory buffer that flushes when either condition is met:

- **Size threshold** — buffer reaches `Buffer.MaxEvents` non-nil actions.
- **Time threshold** — `Buffer.MaxWaitMs` has elapsed since the last flush.

All workers send to a shared ES `_bulk` request, keeping individual HTTP calls large and efficient.

### Rebalance safety

When Kafka triggers a partition rebalance:

1. Ingestion on affected partitions is paused.
2. Any in-flight buffered events are flushed to ES and confirmed.
3. Offsets are committed for the flushed batch.
4. The partition is released to the coordinator.

No buffered data is lost or double-committed across a rebalance boundary.

### Error handling

| Scenario | Behaviour |
|---|---|
| CDC JSON parse failure | Sent to DLQ; offset committed at next flush |
| `Handler.Handle` error, retries remaining | Retried up to `MaxHandlerRetries` |
| `Handler.Handle` retries exhausted | Sent to DLQ; offset committed at next flush |
| `Handler.Handle` returns `(nil, nil)` | Skipped; offset committed at next flush |
| ES bulk failure (network / 5xx) | Whole batch retried with exponential backoff |
| ES item 409 version conflict | Treated as success (idempotent redelivery) |
| ES item other error | Whole batch retried |
| ES retries exhausted | Error returned; offsets not committed; retried on restart |
| Kafka transient error | Logged; re-poll |
| Kafka persistent error | Controlled shutdown |

## Package layout

```
fluxon.go                   Public API — Engine, Handler, Event, Row, Doc, Action, Config
pkg/types/types.go          Shared types (no external deps)
internal/
  buffer/buffer.go          Per-partition in-memory buffer
  cdc/event.go              Debezium JSON parser
  dlq/writer.go             Kafka dead-letter producer
  es/writer.go              Elasticsearch bulk writer with retry
  es/sweep.go               Background soft-delete sweeper
  worker/worker.go          Per-partition processing goroutine
  engine/engine.go          Kafka consumer, rebalance, worker lifecycle
e2e_test.go                 End-to-end tests (Kafka + ES via testcontainers)
```

## Key considerations

**LSN is a global monotonic sequence.** A shared Kafka topic may carry events from multiple tables interleaved. Because LSN is global across all tables and per-row events are always ordered within a Kafka partition, the external version guard works correctly even for mixed-table topics.

**DLQ records are committed lazily.** Skipped and DLQ'd records enter the buffer as nil-action entries. Their offsets advance with the next successful ES flush (delayed by at most `MaxWaitMs`). This is acceptable because DLQ delivery is best-effort and the original payload is preserved in the DLQ topic.

**SafeHorizon must be set conservatively.** The Sweeper purges tombstones below `SafeHorizon`. If this is set too aggressively and a consumer replays events from before the horizon, a redelivered DELETE will find no tombstone, and a subsequent INSERT with an older LSN could create a ghost document. Derive `SafeHorizon` from the minimum committed offset LSN across all active consumer groups.

**One consumer group per pipeline.** Fluxon uses `AutoCommitMarks` from franz-go, which commits the watermark only up to the last marked record. Never share a consumer group between multiple Fluxon instances writing to different ES indices — use separate groups.

**The Handler must be stateless and thread-safe.** A single Handler instance is shared across all partition workers. If your handler carries per-table state (e.g., schema cache), protect it with a mutex or use `sync.Map`.

## Testing

```bash
# Unit tests only (no Docker required)
go test -short ./...

# Full suite including E2E (requires Docker)
go test ./...
```

E2E tests use [testcontainers-go](https://testcontainers.com/guides/getting-started-with-testcontainers-for-go/) to spin up real Kafka and Elasticsearch instances. They are skipped automatically when Docker is not available.
