# Fluxon

## Introduction
Fluxon is a lightweight, stateless stream processing engine written in Go, which consumes Postgres CDC events from Kafka, transforms them into search-optimized JSON, and performs idempotent bulk writes to Elasticsearch.

## Overall Architecture
Source (Kafka): Fluxon subscribes to a Kafka topic containing Postgres CDC events (JSON format, typically produced by Debezium).

Ingestion (Reader): A multi-threaded Go consumer fetches batches of messages.

Transformation (Logic): Each event is passed through a stateless transformer that:

Maps Postgres column names to Elasticsearch field names.

Filters out unnecessary metadata.

Handles data type conversions (e.g., JSONB to Nested Objects).

Buffering (Collector): Transformed events are gathered into a memory buffer to prepare for a bulk operation.

Sink (Elasticsearch): Fluxon uses the Elasticsearch _bulk API to execute high-speed writes (Index, Update, or Delete).

Confirmation (Commit): Once Elasticsearch confirms the write, Fluxon commits the offset back to Kafka to mark the data as "processed."

## Deduplication & Consistency

Fluxon uses an at-least-once delivery model: Kafka offsets are committed only after Elasticsearch confirms a successful bulk write. This means redelivery can occur on crash/restart, so writes must be idempotent.

**Strategy: Primary Key + LSN version guard**

- Each CDC event carries a `source.lsn` field (Postgres WAL Log Sequence Number), which is a globally monotonic sequence across all tables.
- The Postgres primary key is mapped to the Elasticsearch `_id`, so every write targets the same document regardless of redelivery.
- Each ES document stores the LSN of the last applied event (`_lsn` field).
- Write operations are split into two paths based on event type to minimize script overhead:
  - **INSERT / UPDATE**: uses a plain `index` operation with `version_type=external` and the LSN as the external version. ES natively rejects writes whose version is not strictly greater than the stored version, providing idempotency with no Painless script.
  - **DELETE**: uses a scripted upsert (Painless) to set `_deleted: true` and `_lsn`. A script is required here because the operation must be conditional (LSN guard) and must upsert a tombstone document if one does not yet exist.
- This guarantees idempotency even when a shared Kafka topic carries events from multiple tables interleaved, since LSN is global and per-row events are always ordered within a Kafka partition.

**Delete handling**: DELETE events are written as soft-deletes: a scripted upsert sets `_deleted: true` and updates `_lsn` on the document rather than removing it. This preserves the LSN version guard — a redelivered older INSERT finds `params.lsn > ctx._source._lsn` is false and is a no-op, preventing resurrection. All Elasticsearch queries target a filtered alias that excludes `_deleted: true` documents, hiding soft-deleted rows from consumers. A periodic background sweep runs `delete_by_query` on documents where `_deleted: true` and `_lsn` is below a configured safe horizon (derived from consumer group lag), reclaiming storage.

**Ordering guarantee**: Kafka partition order mirrors WAL order for any given row. Fluxon processes messages within a partition sequentially, ensuring LSNs arrive in ascending order under normal conditions.

## Shard

Fluxon maps each Kafka partition to a dedicated goroutine (worker). This provides natural parallelism — rows that hash to the same Kafka partition are processed in order, while different partitions are processed concurrently.

**Rebalance handling**: When Kafka triggers a partition rebalance (e.g., consumer group membership change or topic repartition), Fluxon:

1. Pauses ingestion on affected partitions.
2. Flushes any in-flight buffered events to Elasticsearch and waits for confirmation.
3. Commits offsets for the flushed data.
4. Releases the partition back to the consumer group coordinator.

This ensures no buffered data is lost or double-committed across a rebalance boundary.

## Buffer Flush Policy

Each partition worker owns a dedicated in-memory buffer. This matches the one-goroutine-per-partition model: no locking is required, offset tracking is trivial (the committed offset is simply the last event's offset in the buffer), and rebalance handling is clean (each worker flushes and commits its own buffer independently).

Each buffer flushes to Elasticsearch when either condition is met:

- **Size threshold**: buffer reaches a configured max number of events (e.g., 1000 events).
- **Time threshold**: a configured max wait time elapses since the last flush (e.g., 500ms).

When flushing, all partition workers send their prepared bulk actions to a shared ES writer, which fans them into a single `_bulk` HTTP request. This preserves the simplicity of per-partition ownership while retaining the efficiency of large bulk requests.

This bounds both latency and memory usage without requiring a flush-per-event.

## Error Handling

- **ES write failure**: Fluxon retries the bulk request with exponential backoff up to a configured limit. Offsets are not committed until the write succeeds.
- **Poison pill / transformation error**: Events that cannot be transformed are retried up to a configured limit. If retries are exhausted, the event is forwarded to a dead letter queue (DLQ) Kafka topic for out-of-band inspection and reprocessing. The offset is then committed and ingestion continues, preventing a single bad event from halting the pipeline indefinitely.
- **Kafka consumer error**: Fluxon logs the error and re-polls; transient errors are retried, persistent errors trigger a controlled shutdown.