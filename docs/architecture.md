# OverSync Architecture

## Overview

Poll-based delta engine: fetches data from sources, compares with previous state, generates events (created/updated/deleted), sends to sinks.

Alternative to Kafka Connect / Debezium for scenarios where WAL-based CDC is impossible or unnecessary (system catalogs, APIs, metadata).

## Data Flow

```
PostgreSQL (or any source)
    |
    v fetch_all(sql, key_column)
Vec<RawRow> { row_key, row_data }
    |
    v read_snapshot_keys() -> HashMap<row_key, row_hash>
    |
    v compute_diff(previous, current)    <- pure function, no IO
DeltaResult { created, updated, deleted }
    |
    v check_fail_safe(prev_count, deleted_count, threshold)
    |         |
    |     ABORT if deleted% > threshold
    |
    v upsert_batch() -> update snapshot in SurrealDB
    v delete_stale() -> remove rows from old cycle
    v sink.send_batch() -> deliver events
    |
    v log_cycle_finish() -> record result in cycle_log
```

## Key Components

### compute_diff (oversync-core/model.rs)

Pure function. Takes `HashMap<key, hash>` (previous state) and `Vec<RawRow>` (current). Returns `DeltaResult`:

- **Created** = key exists in current, missing from previous
- **Updated** = key exists in both, hash differs
- **Deleted** = key exists in previous, missing from current
- **Unchanged** = hash matches (ignored)

Hash = SHA-256 of `serde_json::to_string(row_data)`.

### DeltaEngine (oversync-delta/engine.rs)

SurrealDB wrapper. All queries live in `.surql` files (`surql/queries/delta/`):

| Method | File | Purpose |
|--------|------|---------|
| `read_snapshot_keys` | `read_snapshot_keys.surql` | All (key, hash) pairs for source+query |
| `upsert_batch` | `batch_upsert.surql` | UPSERT each row into snapshot |
| `delete_stale` | `delete_stale.surql` | DELETE WHERE cycle_id < N |
| `next_cycle_id` | `next_cycle_id.surql` | MAX(cycle_id) + 1 from cycle_log |
| `log_cycle_start` | `log_cycle_start.surql` | CREATE cycle_log with status=running |
| `log_cycle_finish` | `log_cycle_finish.surql` | UPDATE cycle_log with results |

### CycleRunner (src/cycle.rs)

Single-cycle orchestrator. Takes `DeltaEngine` + `SourceConnector` + `Vec<Sink>`. Manages lifecycle:

1. next_cycle_id
2. log_cycle_start
3. read_snapshot_keys
4. connector.fetch_all
5. compute_diff
6. check_fail_safe -> abort or continue
7. upsert_batch + delete_stale
8. sink.send_batch
9. log_cycle_finish (success/failed/aborted)

### Fail-Safe

If more than threshold% of previous rows are deleted, the cycle aborts. Snapshot is untouched, no events are sent.

Rationale: if the source temporarily returns empty results (network error, truncated response), without fail-safe we would generate thousands of false DELETE events.

Default: 30%. Configurable per-source.

### PostgresConnector (oversync-connectors/postgres.rs)

sqlx PgPool. `fetch_all(sql, key_column)` executes arbitrary SQL, row_key = key_column value, row_data = JSON with all columns.

Type-aware: TEXT->String, INT->Number, BOOL->Bool, JSON/JSONB->Value, FLOAT->Number, rest->String fallback.

### StdoutSink (oversync-sinks/stdout.rs)

Prints events as JSON to stdout. For development and debugging.

## Schema and Migrations (overshift)

Schema managed by **overshift** — shared migration engine:

- **Declarative schema** (`surql/schema/`) — `DEFINE ... OVERWRITE`, applied on every startup
- **Imperative migrations** (`surql/migrations/`) — one-shot, for data backfill
- **Distributed lock** — leader election, safe with multiple instances
- **Compile-time validation** — `surql_parser::build::validate_schema()` in build.rs
- **Manifest** — `surql/manifest.toml` (ns, db, system_db, modules)

Four schema domains:

| Domain | Tables |
|--------|--------|
| sync | snapshot, cycle_log, pending_event |
| config | source_config, query_config, sink_config |
| links | link_rule, resolved_link |
| plugin | plugin |

## State Storage (SurrealDB)

Two key tables:

**snapshot** — latest known state of each row:
```
source_id + query_id + row_key -> UNIQUE
row_data, row_hash, prev_hash, cycle_id, updated_at
```

**cycle_log** — audit trail for each cycle:
```
source_id + query_id + cycle_id -> UNIQUE
status (running/success/failed/aborted), rows_*, timing
```

## File Structure

```
surql/
  manifest.toml               — overshift manifest (ns, db, modules)
  schema/
    sync/tables.surql          — snapshot, cycle_log, pending_event
    config/tables.surql        — source/query/sink config
    links/tables.surql         — link_rule, resolved_link
    plugin/tables.surql        — plugin registry
  migrations/
    v001_schema.surql          — initial tables (IF NOT EXISTS)
    v002_prev_hash.surql       — prev_hash field
    v003_pending_events.surql  — outbox pattern
  queries/delta/               — 14 operational query files
  generated/                   — auto-generated schema snapshot

crates/
  oversync-core/       — types, traits, config, errors, compute_diff
  oversync-delta/      — DeltaEngine (SurrealDB operations)
  oversync-connectors/ — PostgresConnector
  oversync-sinks/      — KafkaSink, StdoutSink
  oversync-api/        — (stub) HTTP API
  oversync-links/      — (stub) entity linking

tests/
  common/surreal.rs    — shared SurrealDB container (OnceCell + overshift)
  common/postgres.rs   — shared PostgreSQL container (OnceCell)
  migration_tests.rs   — 15 tests (schema apply, idempotency, tables, indexes, OVERWRITE)
  delta_engine_tests.rs — 29 tests (CRUD, cycle log, fail-safe, isolation, batching)
  postgres_connector_tests.rs — 18 tests (fetch, types, streaming, errors)
  kafka_sink_tests.rs  — 6 tests (produce, batch, headers)
  cycle_e2e_tests.rs   — 5 tests (full pipeline PG->delta->sink)
  resilience_tests.rs  — 13 tests (outbox, recovery, failure handling)
  scheduler_tests.rs   — 7 tests (scheduling, multi-source, errors)
  stress_chaos_test.rs — 1 test (500K rows stress test)
```
