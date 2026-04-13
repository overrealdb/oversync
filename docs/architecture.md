# oversync Architecture

## Overview

Pipe-first poll-based delta engine: fetches data from origins, compares with previous state, generates events (created/updated/deleted), and sends them to sinks.

Alternative to Kafka Connect / Debezium for scenarios where WAL-based CDC is impossible or unnecessary (system catalogs, APIs, metadata).

## Data Flow

```
Pipe origin: PostgreSQL / MySQL / HTTP API / GraphQL / Trino / Kafka / SurrealDB / Flight SQL / ...
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

## Two Modes

### Embedded (library)

```rust
let engine = OversyncEngine::builder("ws://localhost:8000")
    .namespace("myapp")
    .credentials("root", "root")
    .build().await?;

engine.start(config).await?;
let router = engine.api_router(); // optional: mount in your axum app
```

The `OversyncEngine` encapsulates `DeltaEngine`, `LifecycleManager`, and `PluginRegistry`. It handles SurrealDB connection, schema application, and scheduler lifecycle.
For new deployments prefer `ws://` / `wss://` SurrealDB URLs; oversync upgrades legacy `http://` / `https://` SurrealDB URLs internally so existing configs keep working.

### Standalone (binary)

```bash
oversync --config oversync.toml --bind 0.0.0.0:4200
```

The binary is a thin wrapper (~100 lines) that parses CLI args, initializes tracing/OTel, builds the engine, and starts the control-plane server. That server now serves:

- embedded UI at `/`
- same-origin API at `/api/*`
- direct compatibility endpoints at root for scripts and CLI tooling

## Key Components

### OversyncEngine (src/engine.rs)

High-level facade. Builder pattern:
- Connects to SurrealDB (state + snapshot)
- Applies schema via overshift (if `schema` feature enabled)
- Creates `DeltaEngine` + `LifecycleManager`
- Registers all built-in connector/sink factories
- Exposes `start()`, `pause()`, `resume()`, `shutdown()`, `api_router()`

### LifecycleManager (src/lifecycle.rs)

Wraps the `Scheduler`. Enables config-on-the-fly changes by stop-and-restart:
- `start(config)` — stops existing scheduler, spawns new one
- `pause()` / `resume()` — stops scheduler, remembers config, restarts on resume
- `shutdown()` — stops scheduler, clears config

Config changes are rare (human-initiated). Restart takes <1s. Cycle state persists in SurrealDB — no data loss.

### Scheduler (src/scheduler.rs)

Spawns one tokio task per `(pipe, query)` pair. Each task:
1. Creates connector via `PluginRegistry`
2. Runs immediate first cycle
3. Polls on `interval_secs` timer
4. Retries on failure with exponential backoff

Shutdown via `watch::channel` — all tasks select on the channel and exit cleanly.

### CycleRunner (src/cycle.rs)

Single-cycle orchestrator. Takes `DeltaEngine` + `OriginConnector` + `Vec<Sink>`:

1. `deliver_pending()` — retry any queued events from outbox
2. `next_cycle_id()` + `log_cycle_start()`
3. `stream_and_upsert()` — fetch rows via connector, upsert into snapshot
4. `compute_diff()` or DB-side diff
5. `check_fail_safe()` — abort if too many deletes
6. `deliver_paged()` — outbox pattern: save to pending, deliver to sinks, delete pending
7. `log_cycle_finish()` — record result

Two diff modes:
- **Memory** (`DiffMode::Memory`) — HashMap-based, fast, O(keys) memory
- **Db** (`DiffMode::Db`) — SurrealQL-based, slower, low memory

### compute_diff (oversync-core/model.rs)

Pure function. Takes `HashMap<key, hash>` (previous) and `Vec<RawRow>` (current):

- **Created** = key in current, missing from previous
- **Updated** = key in both, hash differs
- **Deleted** = key in previous, missing from current

Hash = SHA-256 of `serde_json::to_string(row_data)`.

### DeltaEngine (oversync-delta/engine.rs)

SurrealDB wrapper. All queries live in `.surql` files (`surql/queries/delta/`):

| Method | File | Purpose |
|--------|------|---------|
| `read_snapshot_keys` | `read_snapshot_keys.surql` | All `(key, hash)` pairs for pipe+query |
| `upsert_batch` | `batch_upsert.surql` | UPSERT each row into snapshot |
| `delete_stale` | `delete_stale.surql` | DELETE WHERE cycle_id < N |
| `next_cycle_id` | `next_cycle_id.surql` | MAX(cycle_id) + 1 from cycle_log |
| `log_cycle_start` | `log_cycle_start.surql` | CREATE cycle_log with status=running |
| `log_cycle_finish` | `log_cycle_finish.surql` | UPDATE cycle_log with results |
| `save_pending` | `save_pending.surql` | Outbox: save events before delivery |
| `read_pending` | `read_pending.surql` | Outbox: read undelivered events |
| `delete_pending` | `delete_pending.surql` | Outbox: clear delivered events |

### PluginRegistry (src/registry.rs)

Factory pattern for connectors and sinks. Stores `Arc<dyn Factory>`, implements `Clone` for lifecycle restarts.

Built-in factories:

| Origins | Sinks |
|---------|-------|
| `PostgresOriginFactory` | `StdoutTargetFactory` |
| `MysqlOriginFactory` | `KafkaTargetFactory` |
| `ClickHouseOriginFactory` | `SurrealDbTargetFactory` |
| `HttpOriginFactory` | `HttpTargetFactory` |
| `GraphqlOriginFactory` | `PostgresTargetFactory` |
| `TrinoOriginFactory` | `MysqlTargetFactory` |
| `FlightSqlOriginFactory` | `ClickHouseTargetFactory` |
| `KafkaOriginFactory` | `McpTargetFactory` |
| `SurrealDbOriginFactory` | |
| `McpOriginFactory` | |

Custom factories can be registered via `engine.register_source()` / `engine.register_sink()`.

### Config DB (src/config_db.rs)

Loads config from SurrealDB tables (`pipe_config`, `query_config`, `sink_config`, `pipe_preset_config`) into `SyncConfig`. Used by the control-plane API for runtime CRUD operations and import/export.

SurrealQL queries in `surql/queries/config/`:
- `load_pipes.surql` — `SELECT * FROM pipe_config WHERE enabled = true`
- `load_queries.surql` — `SELECT * FROM query_config WHERE enabled = true`
- `load_sinks.surql` — `SELECT * FROM sink_config WHERE enabled = true`
- `load_pipe_presets.surql` — `SELECT * FROM pipe_preset_config`

## Source Connectors

### PostgreSQL / MySQL

sqlx connection pool. `fetch_all(sql, key_column)` executes arbitrary SQL, returns typed JSON.

Type mapping: TEXT/VARCHAR→String, INT→Number, BOOL→Bool, JSON/JSONB→Value, FLOAT→Number, DECIMAL→String.

### HTTP REST (http_source.rs)

GET requests with:
- Auth: Bearer, Basic, custom Header
- Pagination: offset-based or cursor-based
- Response path navigation: `data.items` → drills into nested JSON
- Custom headers

### GraphQL (graphql.rs)

POST `{"query": ..., "variables": {...}}` with:
- Relay cursor pagination: detects `pageInfo.hasNextPage` + `endCursor` as siblings of items array
- GraphQL error detection: checks `response.errors` array
- Shares `AuthConfig`, `navigate_path`, `extract_items` with HTTP source via `http_common.rs`

### Trino

REST protocol client with:
- Stateful query execution (POST → poll nextUri)
- Heartbeat keep-alive (HEAD every 30s)
- Retry on 502/503/504 with exponential backoff
- Async cancellation (DELETE on drop)

### Arrow Flight SQL

gRPC streaming via Arrow Flight protocol. Converts Arrow record batches to JSON rows.

### Kafka / SurrealDB origins

`kafka` consumes JSON messages from Kafka topics and can derive row keys either from the Kafka message key or a JSON field. `surrealdb` queries SurrealDB records directly and maps record ids or explicit key fields into `RawRow`.

## Sinks

### HTTP Webhook (http_sink.rs)

POST/PUT `EventEnvelope` as JSON. Features:
- Native batch: sends array of envelopes in single request
- Retry on 429/502/503/504 with exponential backoff (1s/2s/4s, capped at 60s)
- Auth: Bearer, Basic, custom Header
- Custom headers, configurable timeout

### Kafka

`rdkafka` FutureProducer. Message key = event row key, value = JSON envelope.

### SurrealDB

UPSERT with `_meta` nested object (source_id, query_id, op, hash, cycle_id, synced_at). Batch via SurrealQL FOR loop.

### PostgreSQL / MySQL / ClickHouse

Relational sinks upsert JSON payloads into target tables. PostgreSQL and MySQL use SQL upserts; ClickHouse writes JSONEachRow over HTTP.

### Stdout

Prints JSON to stdout. Optional `"pretty": true`. Records events internally for test assertions.

## REST API (oversync-api / oversync-client)

Axum router with OpenAPI 3.1 (utoipa). Two layers:

**Read routes** (from in-memory cache, updated after mutations):
- `GET /health`, `GET /pipes`, `GET /pipes/{name}`, `GET /pipe-presets`, `GET /sinks`

**Mutation routes** (write to SurrealDB, reload lifecycle):
- `POST /pipes`, `PUT /pipes/{name}`, `DELETE /pipes/{name}`
- `POST /pipe-presets`, `PUT /pipe-presets/{name}`, `DELETE /pipe-presets/{name}`
- `POST /sinks`, `PUT /sinks/{name}`, `DELETE /sinks/{name}`

**Operation routes**:
- `GET /pipes/{name}/resolve` — view the effective runtime pipe after recipe expansion
- `POST /pipes/dry-run` — run diff logic without mutating scheduler state
- `GET /config/export` / `POST /config/import` — round-trip startup configs through the control-plane DB
- `POST /pipes/{name}/run` — manual single-pipe execution
- `POST /sync/pause` / `POST /sync/resume` — lifecycle control
- `GET /sync/status` — running/paused state
- `GET /history` — last 100 cycle_log entries

Mutation flow: API → write to DB → `reload_config()` → `load_config_from_db()` → `lifecycle.start(new_config)` → refresh read cache.

The merged OpenAPI document is the SDK source of truth:

- `ui/openapi.json` feeds the generated TypeScript SDK in `ui/src/api/generated/*`
- `crates/oversync-client/openapi.json` feeds the generated Rust module in `oversync-client`
- `oversync-client::OversyncClient` stays as the stable handwritten facade above that contract

## Fail-Safe

If more than threshold% of previous rows are deleted, the cycle aborts. Snapshot is untouched, no events are sent.

Rationale: if the source temporarily returns empty results (network error, truncated response), without fail-safe we would generate thousands of false DELETE events.

Default: 30%. Configurable per pipe via `fail_safe_threshold`.

## Schema and Migrations (overshift)

Schema managed by **overshift** — shared migration engine:

- **Declarative schema** (`surql/schema/`) — `DEFINE ... OVERWRITE`, applied on every startup
- **Imperative migrations** (`surql/migrations/`) — one-shot, for data backfill
- **Distributed lock** — leader election, safe with multiple instances
- **Compile-time validation** — `surql_parser::build::validate_schema()` in build.rs
- **Manifest** — `surql/manifest.toml` (ns, db, system_db, modules)

Five schema domains:

| Domain | Tables |
|--------|--------|
| sync | snapshot, cycle_log, pending_event |
| config | pipe_config, query_config, pipe_preset_config, sink_config |
| links | link_rule, resolved_link |
| plugin | plugin |
| transforms | SMT functions |

## Feature Flags

| Feature | Dependencies | Purpose |
|---------|-------------|---------|
| (default) | none | Core engine, all connectors and sinks |
| `schema` | overshift | Schema apply via overshift on engine build |
| `api` | oversync-api, axum | Server-side REST API via `engine.api_router()` |
| `client` | oversync-client, reqwest | Consumer-safe Rust SDK and shared wire DTOs |
| `cli` | api + schema + clap + otel | Standalone binary with CLI and tracing |

## File Structure

```
src/
  engine.rs          — OversyncEngine builder and facade
  lifecycle.rs       — LifecycleManager (start/pause/resume/shutdown)
  scheduler.rs       — Per-query polling tasks with retry
  cycle.rs           — CycleRunner (single-cycle orchestrator)
  config.rs          — SyncConfig, PipeConfig, SinkDef, QueryDef
  config_db.rs       — Load config from SurrealDB tables
  registry.rs        — PluginRegistry (factory pattern, Clone via Arc)
  recipes.rs         — Runtime recipe expansion (`postgres_metadata`, `postgres_snapshot`)
  lib.rs             — Re-exports

crates/
  oversync-core/     — Types, traits, AuthConfig, compute_diff, errors
  oversync-delta/    — DeltaEngine (SurrealDB state operations)
  oversync-connectors/
    http_common.rs   — Shared: AuthConfig, navigate_path, extract_items
    http_source.rs   — HTTP REST connector
    graphql.rs       — GraphQL connector (Relay pagination)
    postgres.rs      — PostgreSQL connector
    mysql.rs         — MySQL connector
    trino.rs         — Trino REST protocol client
    flight_sql.rs    — Arrow Flight SQL connector
    kafka_source.rs  — Kafka connector
    surrealdb_source.rs — SurrealDB connector
    mcp.rs           — MCP connector
  oversync-sinks/
    http_sink.rs     — HTTP webhook sink (retry, auth)
    kafka.rs         — Kafka sink
    surrealdb_sink.rs — SurrealDB sink
    postgres_sink.rs — PostgreSQL sink
    mysql_sink.rs    — MySQL sink
    clickhouse_sink.rs — ClickHouse sink
    stdout.rs        — Stdout sink
  oversync-client/
    client.rs        — typed Rust SDK for the control-plane API
    types.rs         — consumer-safe wire DTOs with OpenAPI schemas
  oversync-api/
    handlers.rs      — GET read routes
    mutations.rs     — CRUD write routes
    operations.rs    — pause, resume, history, status, config import/export
    state.rs         — ApiState, LifecycleControl trait
    types.rs         — compatibility re-export of oversync-client DTOs

surql/
  manifest.toml
  schema/{sync,config,links,plugin,transforms}/
  migrations/v001_*, v002_*, v003_*
  queries/{delta,config,sink}/

bin/oversync.rs      — Standalone binary (~100 lines using OversyncEngine)

examples/
  basic_stdout.rs          — Minimal embedded engine
  custom_connector.rs      — Custom SourceFactory
  http_api.rs              — Engine with REST API
  postgres_to_surrealdb.rs — Production-like multi-query
  graphql_source.rs        — Relay cursor pagination
  http_to_webhook.rs       — REST polling -> webhook delivery
  multi_source_kafka.rs    — Multi-source with sink routing

tests/                — integration, cluster, recipe, soak, and throughput coverage
```
