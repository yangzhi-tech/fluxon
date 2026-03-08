# Fluxon E2E Validation

## Overview

End-to-end tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up real Kafka and Elasticsearch instances in Docker. Each test gets its own isolated pair of containers and a unique consumer group ID.

**Stack:** `confluentinc/confluent-local:7.5.0` + `docker.elastic.co/elasticsearch/elasticsearch:8.17.0`

---

## Test Cases

| Test | What it validates |
|------|------------------|
| `TestE2EInsert` | INSERT event → doc appears in ES with correct `name` and `_lsn` |
| `TestE2EUpdate` | INSERT then UPDATE → doc reflects updated field |
| `TestE2ESoftDelete` | INSERT then DELETE → doc has `_deleted=true` (tombstone, not hard delete) |
| `TestE2EDuplicateInsertIdempotent` | Newer doc written first, stale redelivery (lower LSN) does not overwrite |
| `TestE2EPoisonPillContinuesPipeline` | Malformed JSON → sent to DLQ, pipeline continues and next event lands in ES |
| `TestE2EMultipleRecords` | 5 concurrent INSERT events → all 5 docs appear in ES |
| `TestE2EHandlerSkipUnknownTable` | Handler returns `(nil, nil)` for unknown table → offset advances, pipeline continues |

---

## Infrastructure Issues Found

### 1. Wrong Kafka image
**Symptom:** Container exited with code 127 immediately after start.

**Root cause:** The testcontainers Kafka module injects a custom startup script (`/usr/sbin/testcontainers_start.sh`) that sources Confluent-specific paths (`/etc/confluent/docker/`). The image `apache/kafka-native:3.8.0` doesn't have these scripts.

**Fix:** Switch to `confluentinc/confluent-local:7.5.0`, which the module is designed for.

---

### 2. Topic auto-create race
**Symptom:** `UNKNOWN_TOPIC_OR_PARTITION` warning; produced messages silently lost before engine consumer created the topic.

**Root cause:** The producer was sending before the Kafka consumer group had subscribed and triggered auto-topic creation. The `produce` function only logged the error (`t.Logf`), so lost messages were invisible.

**Fix:**
- Pre-create `cdc.events` and `cdc.dlq` topics explicitly via `kadm.CreateTopics` in `setupE2E`, before the engine starts.
- Changed `produce` to `t.Fatalf` so any future produce failures are immediately visible.

---

### 3. ES TLS certificate verification failure
**Symptom:** All ES bulk writes failed with `tls: failed to verify certificate: x509: certificate signed by unknown authority`. Engine retried indefinitely, no documents written.

**Root cause:** The ES testcontainer runs HTTPS with a self-signed certificate. Neither the engine's `ESConfig` nor the test's `esClient` had any TLS configuration, so both used Go's default strict TLS verification.

**Fix:**
- Added `Username`, `Password`, and `InsecureTLS` fields to `ESConfig` in `pkg/types/types.go`.
- In `es.NewWriter`, when `InsecureTLS` is set, configure the HTTP transport with `InsecureSkipVerify: true`.
- Apply the same skip-verify transport to the test's own `esClient` in `setupE2E` (it was calling `getDoc` silently returning `nil` on every TLS error, causing `waitForDoc` to spin until timeout).
- Pass `esUser`/`esPass`/`InsecureTLS: true` from `e2eEnv` into `startEngine`.

---

## Real Bug Found

### Nil-interface panic in `worker.flushBuffer`

**Symptom:** `TestE2EHandlerSkipUnknownTable` caused a panic:
```
panic: runtime error: invalid memory address or nil pointer dereference
github.com/dropbox/fluxon/internal/es.BuildBulkBody(...)
    internal/es/writer.go:115
```

**Root cause:** The classic Go typed-nil-in-interface trap.

When a handler returns `(nil, nil)` (skip), the worker calls:
```go
w.buf.Add(nil, record)  // nil is typed as *types.Action
```

`buffer.Add` accepts `action interface{}`. Assigning a typed nil `*types.Action` to `interface{}` produces a **non-nil interface** (the interface has a type descriptor but a nil value pointer). So in `flushBuffer`:

```go
// BEFORE (buggy)
for _, e := range entries {
    if e.Action != nil {  // true! interface is non-nil
        actions = append(actions, e.Action.(*types.Action))  // returns nil *Action
    }
}
// Then BuildBulkBody accesses a.Doc on a nil pointer → panic
```

**Fix** in `internal/worker/worker.go`:
```go
// AFTER (correct)
for _, e := range entries {
    if a, ok := e.Action.(*types.Action); ok && a != nil {
        actions = append(actions, a)
    }
}
```

This bug only manifests at runtime when a handler skips an event — the unit tests for the worker mocked the ES writer and never exercised the nil-action path through the real `BuildBulkBody`. The e2e test `TestE2EHandlerSkipUnknownTable` was the only path that caught it.

---

## Planned Test Cases (Not Yet Implemented)

| # | Area | What it validates |
|---|------|-----------------|
| TC1 | DLQ content | Poison pill message payload and metadata appear in DLQ topic |
| TC2 | DLQ on retry exhaustion | Handler that always errors → event in DLQ, pipeline continues |
| TC3 | DLQ vs skip | Handler `(nil,nil)` does NOT produce to DLQ |
| TC4 | Out-of-order delete | DELETE before INSERT → tombstone holds; stale INSERT doesn't resurrect |
| TC5 | Same LSN twice | Identical LSN redelivered → idempotent, no error |
| TC6 | Rebalance safety | Second engine joins group mid-stream → no events lost across rebalance |
| TC7 | Shutdown flush | Engine ctx cancelled mid-batch → shutdown flush path commits all buffered events |
| TC8 | Time-based flush | Single event with large `MaxEvents` → appears within `MaxWaitMs` |
| TC9 | Size-based flush | `MaxEvents` threshold reached → flush fires before `MaxWaitMs` |
