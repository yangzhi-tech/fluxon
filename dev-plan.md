# Fluxon — Development Plan

## Guiding Principles

- Each component is built and validated in isolation before the next starts
- External dependencies (Kafka, ES) are never required for unit tests — internal interfaces enable mocking
- Testcontainers bring up real Kafka/ES for integration and E2E tests
- Each phase ends with a clear "done" checkpoint before moving on

---

## Dependency Graph

```
Phase 1:  buffer ──────────────────────────────────────┐
          fluxon.go (Row, Doc, Action, Event) ──────────┤
          cdc (Parse) ─────────────────────────────────┤
                                                        ▼
Phase 2:  es/writer (uses fluxon.Action) ──────────────┐
          dlq/writer ──────────────────────────────────┤
                                                        ▼
Phase 3:  worker (uses buffer, cdc, fluxon, es, dlq) ──┐
                                                        ▼
Phase 4:  engine (uses worker, es/writer, dlq/writer) ──┐
          es/sweep (independent) ───────────────────────┤
                                                        ▼
Phase 5:  E2E (engine + es + kafka + user Handler)
```

---

## Test Infrastructure

| Tool | Used for |
|---|---|
| `testing` (stdlib) | all unit tests |
| `net/http/httptest` | fake ES server in Phase 2a |
| `testcontainers-go` | real Kafka (Phase 2b, 4a, 5), real ES (Phase 4b, 5) |
| Internal interfaces (`esWriter`, `dlqWriter`, `workerIface`) | mock injection in Phase 3, 4 |

---

## Phase 1 — Foundation (pure, no external deps)

### 1a. `internal/buffer`

**Implement**: `Entry`, `Buffer`, `Add`, `Peek`, `Clear`, `ShouldFlush`, `Len`

**Unit tests**:

| Test | What it checks |
|---|---|
| `Add` + `Peek` | entries are copied, not aliased |
| `Clear` resets `lastFlush` | next `ShouldFlush` uses fresh timer |
| `ShouldFlush` — size | triggers when non-nil action count ≥ maxEvents |
| `ShouldFlush` — time | triggers when elapsed ≥ maxWait, even with few entries |
| `ShouldFlush` — empty | never triggers on empty buffer |
| nil-action entries | do not count toward size threshold |
| `Peek` does not drain | entries survive a `Peek` call |
| `Peek → Clear` | entries are gone after `Clear` |

**Done when**: all tests pass, no `kgo` or ES import in this package.

---

### 1b. `fluxon.go` — public types

**Implement**: `Op`, `Source`, `Event`, `Row`, `Doc`, `Action`, `Index()`, `SoftDelete()`

**Unit tests**:

| Test | What it checks |
|---|---|
| `Row.String` — present | returns string value |
| `Row.String` — missing | returns `""`, no panic |
| `Row.Int` — float64 from JSON | converts correctly |
| `Row.Bool` — missing | returns `false` |
| `Row.Time` — epoch ms int | parses to correct `time.Time` |
| `Row.JSON` — valid JSONB string | returns parsed nested object |
| `Row.JSON` — plain string | returns string as-is |
| `Row.Raw` — nil value | returns nil, no panic |
| `Index()` | sets opType, index, id, lsn, doc |
| `SoftDelete()` | sets opType, index, id, lsn; doc is nil |

**Done when**: all tests pass, `fluxon.go` compiles with zero internal imports.

---

### 1c. `internal/cdc`

**Implement**: `rawEvent` struct with JSON tags, `Parse([]byte) (*fluxon.Event, error)`

**Unit tests**:

| Test | What it checks |
|---|---|
| Insert (`op:"c"`) | `After` populated, `Before` nil |
| Update (`op:"u"`) | both `Before` and `After` populated |
| Delete (`op:"d"`) | `Before` populated, `After` nil |
| Snapshot (`op:"r"`) | treated like insert |
| `source.lsn` | parsed to `int64` correctly |
| Malformed JSON | returns error |

**Done when**: all tests pass.

---

## Phase 2 — IO Layer

### 2a. `internal/es/writer`

**Implement**: bulk body builder, `Flush` with retry loop, bulk response parser

**Unit tests** (no ES — test bulk body serialization):

| Test | What it checks |
|---|---|
| `Index` action | NDJSON has correct `version_type: external` meta + doc line |
| `SoftDelete` action | NDJSON has correct `scripted_upsert`, Painless script, upsert body |
| Mixed batch | both action types serialized in correct order |
| Empty batch | returns nil immediately |

**Integration tests** (fake HTTP server via `net/http/httptest`):

| Test | What it checks |
|---|---|
| Success response (`errors: false`) | `Flush` returns nil |
| 409 per-item | treated as success, no error returned |
| Other per-item error | triggers retry |
| 5xx response | retries with exponential backoff |
| Retries exhausted | returns error |
| Context cancellation | stops retry loop immediately |

**Done when**: unit + integration tests pass; no real ES process needed.

---

### 2b. `internal/dlq/writer`

**Implement**: `Writer`, `Message`, `Send`

**Unit test** (mock `kgo.Client` via interface):

| Test | What it checks |
|---|---|
| `Send` success | serializes `Message` to JSON correctly |
| `Send` on produce failure | logs error, does not panic or block caller |

**Integration test** (Kafka via testcontainers):

| Test | What it checks |
|---|---|
| Round-trip | produce a DLQ message, consume it back, verify all JSON fields |

**Done when**: integration test passes with real Kafka.

---

## Phase 3 — Processing Core

### 3a. `internal/worker`

Testability requires internal interfaces so ES and DLQ can be mocked without real services.

**Add internal interfaces**:
```go
type esWriter interface {
    Flush(ctx context.Context, actions []*fluxon.Action) error
}
type dlqWriter interface {
    Send(ctx context.Context, payload []byte, partition int32, offset int64, reason error)
}
```
Concrete `*es.Writer` and `*dlq.Writer` satisfy these automatically.

**Implement**: `Worker`, `Run`, `Submit`, `Flush`, `processRecord`, `flushBuffer`

**Unit tests** (all deps mocked):

| Test | What it checks |
|---|---|
| Happy path insert | record → parsed → handler called → action buffered |
| Happy path delete | `SoftDelete` action buffered |
| Handler returns `(nil, nil)` | nil-action entry buffered, no ES write |
| Handler error → retry → success | handler called N times, succeeds on Nth |
| Handler exhausts retries | DLQ called, nil-action buffered |
| CDC parse failure | DLQ called, nil-action buffered |
| Size threshold flush | `esWriter.Flush` called after `maxEvents` non-nil actions |
| Time threshold flush | `esWriter.Flush` called after `maxWait` elapses |
| ES flush failure | entries remain in buffer (Peek not cleared) |
| `Worker.Flush` | drains `msgCh`, flushes buffer, returns ES error |
| `MarkCommitRecords` called | after successful flush, with the last record in batch |
| Mixed DLQ + valid in one batch | DLQ record offset committed together with valid records |
| `ctx.Done` | drains channel, flushes, exits cleanly with no goroutine leak |

**Done when**: all tests pass with zero real Kafka/ES.

---

## Phase 4 — Orchestration

### 4a. `internal/engine`

**Implement**: `Engine`, `Run`, `onAssigned`, `onRevoked`, poll loop, worker lifecycle

**Add internal interface** for mockable worker:
```go
type workerIface interface {
    Submit(r *kgo.Record)
    Flush(ctx context.Context) error
    Run(ctx context.Context)
}
```

**Unit tests** (mock worker):

| Test | What it checks |
|---|---|
| `onAssigned` | creates and starts workers for each assigned partition |
| `onRevoked` | calls `Flush` on revoked workers, removes them from map |
| Poll loop dispatch | records routed to correct partition worker |
| Defensive get | unknown partition does not panic |

**Integration tests** (Kafka via testcontainers, mock Handler):

| Test | What it checks |
|---|---|
| Single partition | handler called in arrival order |
| Multiple partitions | handlers called concurrently, each partition ordered internally |
| Partition rebalance | no message loss across rebalance boundary |
| Offset commit | after handler success, offset committed; verified by restarting consumer |
| Clean shutdown | `ctx.Done` flushes all workers, no deadlock |

**Done when**: integration tests pass.

---

### 4b. `internal/es/sweep`

**Implement**: `Sweeper`, `Run`, `sweep`

**Integration tests** (ES via testcontainers):

| Test | What it checks |
|---|---|
| `_deleted:true`, `_lsn < horizon` | document deleted by sweep |
| `_deleted:true`, `_lsn >= horizon` | document not deleted |
| No `_deleted` field | document not touched |
| Ticker interval | sweep runs on schedule, not before |

**Done when**: integration tests pass.

---

## Phase 5 — E2E

Full pipeline with real Kafka + real ES and a user-supplied `Handler`.

**Test setup**:
- Testcontainers: Kafka + Elasticsearch
- A test `Handler` routing `users` and `orders` tables

**Test cases**:

| Test | What it checks |
|---|---|
| Insert | ES document created, correct fields, `_lsn` set |
| Update | ES document updated, external version incremented |
| Delete | `_deleted: true` set, `_lsn` updated |
| Duplicate insert (redelivery) | idempotent — ES rejects stale version, no change |
| Duplicate delete (redelivery) | Painless script no-ops due to LSN guard |
| Poison pill (bad JSON) | DLQ receives message, pipeline continues |
| Handler error exhausted | DLQ receives message, pipeline continues |
| Multi-partition ordering | all partitions processed, each partition ordered |
| Restart mid-batch | resumes from last committed offset, no data loss |

**Done when**: all E2E tests pass end-to-end.

---

## Checkpoints Summary

| Phase | Deliverable | Validated by |
|---|---|---|
| 1a | `internal/buffer` | unit tests, no external deps |
| 1b | `fluxon.go` public types | unit tests, no external deps |
| 1c | `internal/cdc` parser | unit tests, no external deps |
| 2a | `internal/es/writer` | unit tests + `httptest` fake server |
| 2b | `internal/dlq/writer` | unit test (mock) + Kafka testcontainer |
| 3a | `internal/worker` | unit tests with mocked interfaces |
| 4a | `internal/engine` | unit tests + Kafka testcontainer |
| 4b | `internal/es/sweep` | ES testcontainer |
| 5  | full pipeline | Kafka + ES testcontainers |

Each phase starts only after the previous phase's tests are green.
