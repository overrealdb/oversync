# oversync

[![CI](https://github.com/overrealdb/oversync/actions/workflows/ci.yml/badge.svg)](https://github.com/overrealdb/oversync/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/overrealdb/oversync/graph/badge.svg)](https://codecov.io/gh/overrealdb/oversync)
[![Crates.io](https://img.shields.io/crates/v/oversync.svg)](https://crates.io/crates/oversync)
[![docs.rs](https://docs.rs/oversync/badge.svg)](https://docs.rs/oversync)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Lightweight poll-based data sync engine: fetch from any source, detect changes via SHA-256 hash diff, transform in-flight, deliver to any target.

Alternative to Kafka Connect / Debezium when WAL-based CDC is impossible (system catalogs, APIs, cross-platform metadata).

## Features

- **Pipe-based architecture** — each pipeline is a first-class `PipeConfig`: origin + filters + delta + transforms + targets
- **10 origin connectors** — PostgreSQL, MySQL, ClickHouse, Trino, HTTP REST, GraphQL, Arrow Flight SQL, MCP, Kafka, SurrealDB, plus JDBC databases via Trino bridge
- **8 target types** — Kafka, HTTP webhook, SurrealDB, PostgreSQL, MySQL, ClickHouse, MCP, stdout
- **15 built-in transforms** — rename, set, upper/lower, remove, copy, default, filter, map_value, truncate, nest, flatten, hash, coalesce, schema_filter
- **WASM plugins** — extend with custom transform steps via WebAssembly (wasmtime)
- **Dry-run** — preview pipeline results without writing to targets or state (mock and live modes)
- **Encrypted credentials** — AES-256-GCM at rest, transient credentials for dry-run (never persisted)
- **Shared snapshot state by default** — when no separate snapshot DB is configured, delta baseline stays in the primary state DB instead of local memory
- **Trino bridge** — non-native databases (MSSQL, Oracle, Hive, Iceberg, Snowflake) auto-route through Trino with per-query credential passthrough
- **Pre-delta filters** — regex allow/deny patterns applied before delta detection (skip unwanted data at source)
- **Env var interpolation** — `${VAR}` and `${VAR:-default}` in TOML configs
- **Config validation** — errors block start, warnings logged
- **Config versioning** — auto-save snapshots with rollback support
- **Rate limiting** — token bucket per pipe to respect source API limits
- **Fail-safe** — aborts cycle if deletion exceeds threshold
- **Dual mode** — embeddable library (`cargo add oversync`) or standalone binary
- **Full REST API** — CRUD pipes, saved recipes, sinks, credentials, dry-run, manual pipe runs, lifecycle control, OpenAPI 3.1
- **Generated OpenAPI** — `utoipa`-driven base spec merged with engine-owned routes like dry-run, resolve, credentials, and config versions
- **Generated Rust SDK** — `oversync-client` provides consumer-safe wire DTOs and a typed `reqwest` client for the same control-plane surface
- **Generated UI SDK** — the React control plane can generate its TypeScript SDK directly from the merged OpenAPI spec

## Quick Start

### Docker

```bash
# DockerHub
docker pull 22fx/oversync:latest

# GitHub Container Registry
docker pull ghcr.io/overrealdb/oversync:latest

# Or run with docker compose
docker compose up -d
open http://localhost:4200/
curl http://localhost:4200/api/health
```

The image is distroless (no shell, nonroot uid 65534) — minimal attack surface. It now serves the embedded control-plane UI and the API from the same process:

- UI: `http://localhost:4200/`
- Same-origin API: `http://localhost:4200/api/*`
- Direct compatibility endpoints still exist at root for scripts and CLI tooling

### Embedded (library)

```toml
[dependencies]
oversync = "0.1"
```

```rust,no_run
use oversync::OversyncEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = OversyncEngine::builder("http://localhost:8000")
        .namespace("myapp")
        .credentials("root", "root")
        .build()
        .await?;

    engine.start_from_toml(std::path::Path::new("oversync.toml")).await?;
    // engine.pause().await;
    // engine.resume().await?;
    engine.shutdown().await;
    Ok(())
}
```

### Standalone binary

```bash
cargo install oversync --features cli
export OVERSYNC_CREDENTIAL_KEY='replace-with-a-strong-passphrase'
oversync --config oversync.toml --bind 0.0.0.0:4200
```

The standalone server exposes the embedded control-plane UI at `/` and the same-origin API at `/api`.

### OpenAPI and UI SDK

The standalone control plane exposes the merged OpenAPI 3.1 spec at:

- `GET /openapi.json`

You can also export the same merged spec from the CLI without starting the server:

```bash
oversync openapi > openapi.json
oversync openapi --file ui/openapi.json
```

The frontend SDK in `ui/` is generated from that spec with `@hey-api/openapi-ts`:

```bash
cd ui
npm run generate:api
```

This refreshes:

- `ui/openapi.json`
- `ui/src/api/generated/*`

The React control plane already uses those generated operations for its pipe, sink, history, sync, import/export, and saved-recipe API calls.

### Rust SDK

For external Rust consumers, depend on `oversync-client` instead of the server crate:

```toml
[dependencies]
oversync-client = "0.6.1"
```

```rust,no_run
use oversync_client::OversyncClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = OversyncClient::with_api_key(
        "http://localhost:4200/api",
        "replace-with-api-key",
    )?;

    let health = client.health().await?;
    println!("oversync {} {}", health.status, health.version);
    Ok(())
}
```

## Configuration

### Pipes (recommended)

```toml
[surrealdb]
url = "${SURREALDB_URL:-http://localhost:8000}"
username = "${SURREALDB_USER:-root}"
password = "${SURREALDB_PASS:-root}"

[[pipes]]
name = "catalog-sync"
targets = ["kafka-main"]

[pipes.origin]
connector = "postgres"
dsn = "${PG_DSN}"
# credential = "prod-pg"  # reference encrypted credential by name

[pipes.schedule]
interval_secs = 60
missed_tick_policy = "skip"  # or "burst"
# max_requests_per_minute = 30  # rate limiting

[pipes.delta]
diff_mode = "memory"  # or "db" (low memory)
fail_safe_threshold = 25.0

[[pipes.filters]]
type = "schema_filter"
field = "schema_name"
allow = ["^public$", "^analytics$"]
deny = ["^pg_catalog", "^information_schema"]

[[pipes.transforms]]
type = "rename"
from = "entity_id"
to = "id"

[[pipes.transforms]]
type = "upper"
field = "name"

[[pipes.transforms]]
type = "set"
field = "version"
value = 1

[[pipes.queries]]
id = "tables"
sql = "SELECT oid::text, relname, relkind FROM pg_class WHERE relnamespace = 2200"
key_column = "oid"

[[pipes.queries]]
id = "columns"
sql = "SELECT attrelid::text || '.' || attnum::text AS id, attname FROM pg_attribute"
key_column = "id"
sinks = ["kafka-main"]  # per-query sink routing

[[sinks]]
name = "kafka-main"
type = "kafka"

[sinks.config]
brokers = "kafka:9092"
topic = "sync-events"
# optional: emit {"entityId":"..."} as Kafka key and {meta:{dateTime,changeType},data:{...}} as value
# key_format = "json_object"
# key_field = "entityId"
# value_format = "compact"
# created_change_type = "updated"  # optional: map inserts to "updated" for snapshot-style consumers
```

Use `diff_mode = "db"` when downstream consumers need full `data` on delete events. `diff_mode = "memory"` only compares keys and hashes and emits `row_data = null`.

Legacy `[[sources]]` configs are no longer supported. Startup now fails fast and requires `[[pipes]]`.

### PostgreSQL metadata recipe

For metadata-catalog style pipelines you can let `oversync` generate the standard PostgreSQL entity + aspect-table queries:

```toml
[[pipes]]
name = "some-postgresql-source-catalog"
targets = ["datacat-kafka"]

[pipes.origin]
connector = "postgres"
dsn = "postgres://placeholder"
credential = "some-postgresql-source-postgres"

[pipes.schedule]
interval_secs = 900

[pipes.delta]
diff_mode = "db"

[pipes.recipe]
type = "postgres_metadata"
prefix = "some-postgresql-source"
schemas = ["showcase_stream"]
```

### PostgreSQL snapshot recipe

For generic `postgres -> kafka` onboarding you can let `oversync` introspect PostgreSQL at runtime and generate one query per table that has a primary key:

```toml
[[pipes]]
name = "billing-snapshot"
targets = ["kafka-main"]

[pipes.origin]
connector = "postgres"
dsn = "postgres://placeholder"
credential = "billing-postgres"

[pipes.schedule]
interval_secs = 300

[pipes.delta]
diff_mode = "db"

[pipes.recipe]
type = "postgres_snapshot"
prefix = "billing"
schemas = ["public"]
```

This recipe:
- discovers ordinary tables with primary keys
- generates stable query ids like `billing.public.accounts`
- supports composite primary keys through an internal synthetic key column
- omits the synthetic key from emitted row payloads

### Saved recipes

Reusable saved recipes can live inside the control-plane DB or a startup config and be materialized into concrete pipes later:

```toml
[[pipe_presets]]
name = "postgres-metadata-template"
description = "Reusable metadata onboarding template"

[pipe_presets.spec.origin]
connector = "postgres"
dsn = "postgres://placeholder"
credential = "{{credential_name}}"

[[pipe_presets.spec.parameters]]
name = "credential_name"
label = "Credential"
required = true
secret = false

[[pipe_presets.spec.parameters]]
name = "source_name"
label = "Source name"
required = true
secret = false

[pipe_presets.spec.schedule]
interval_secs = 900

[pipe_presets.spec.delta]
diff_mode = "db"

[pipe_presets.spec.recipe]
type = "postgres_metadata"
prefix = "{{source_name}}"
schemas = ["public"]

[pipe_presets.spec.retry]
max_retries = 3
retry_base_delay_secs = 5
```

The UI can author these saved recipes directly, prompt for parameter values, preview the materialized TOML/JSON draft, and then create a runnable pipe from that template.

### Non-native databases via Trino

```toml
[[pipes]]
name = "mssql-sync"

[pipes.origin]
connector = "mssql"         # auto-routes through Trino
dsn = "host:1433/mydb"
trino_url = "http://trino:8080"  # optional: use custom Trino instance
```

Oversync automatically creates a Trino catalog and routes queries through it. Supported via Trino: MSSQL, Oracle, Hive, Iceberg, Snowflake, Teradata, DB2, Greenplum, Redshift.

### Cluster notes

- By default, snapshot state is stored in the same SurrealDB as cycle logs and pending events. This makes restarts and failover reuse the existing diff baseline instead of re-emitting a full create wave.
- If you want a separate snapshot store, configure `[surrealdb.snapshot]` or `OversyncEngine::builder(...).snapshot_url(...)`.
- `OVERSYNC_INSTANCE_ID` is optional. If unset, each scheduler instance generates a unique process-scoped identity automatically. Set it only when you need an explicit stable identifier in logs or orchestration.
- Horizontal scale is currently per query, not within one query. Adding replicas helps many independent queries; it does not split one large table scan across workers.

## Architecture

```
PipeConfig
  │
  ├── Origin (fetch)
  │     Native: postgres, mysql, clickhouse, trino, http, graphql, flight_sql, mcp, kafka, surrealdb
  │     Via Trino: mssql, oracle, hive, iceberg, snowflake, teradata, db2, redshift, ...
  │
  ├── Pre-delta Filters (regex allow/deny on RawRow)
  │
  ├── Delta Engine (SurrealDB)
  │     SHA-256 hash comparison → created/updated/deleted events
  │     Per-pipeline isolated tables (snapshot, cycle_log, pending_event)
  │     Fail-safe: abort if deletion% > threshold
  │
  ├── Transform Chain (on EventEnvelope)
  │     15 built-in steps + WASM plugins + custom TransformHook
  │
  └── Targets (deliver)
        kafka, http, surrealdb, mcp, stdout
        Per-query sink routing: query.sinks > pipe.targets > all sinks
```

## Origin Connectors

| Connector | Type | Key Features |
|-----------|------|-------------|
| PostgreSQL | `postgres` | Type-aware decoding, streaming via sqlx |
| MySQL | `mysql` | Type-aware decoding, streaming via sqlx |
| ClickHouse | `clickhouse` | HTTP API, JSONEachRow format |
| Trino | `trino` | REST protocol, heartbeat, retry, per-query credentials |
| HTTP REST | `http` | Auth (Bearer/Basic/Header), pagination (offset/cursor), response path |
| GraphQL | `graphql` | Relay cursor pagination, error detection |
| Arrow Flight SQL | `flight-sql` | gRPC streaming, Arrow record batch conversion |
| MCP | `mcp` | JSON-RPC over stdio, tool call → data rows |
| Kafka | `kafka` | Consume JSON events from Kafka topics |
| SurrealDB | `surrealdb` | Query SurrealDB records directly |
| **Any JDBC** | via Trino | MSSQL, Oracle, Hive, Iceberg, Snowflake, Teradata, DB2, Redshift, ... |

## Target Connectors

| Target | Type | Key Features |
|--------|------|-------------|
| Kafka | `kafka` | Native batch produce, message key = row key |
| HTTP Webhook | `http` | POST/PUT, retry with exponential backoff, auth |
| SurrealDB | `surrealdb` | UPSERT with `_meta`, batch via FOR loop |
| PostgreSQL | `postgres` | UPSERT into relational tables with JSON payload column |
| MySQL | `mysql` | UPSERT into relational tables with JSON payload column |
| ClickHouse | `clickhouse` | HTTP insert, JSONEachRow payload |
| MCP | `mcp` | Deliver events via tool calls (JSON-RPC over stdio) |
| Stdout | `stdout` | JSON output, optional pretty-print |

## Transform Steps

| Step | Params | Description |
|------|--------|-------------|
| `rename` | from, to | Move field |
| `set` | field, value | Set constant value |
| `upper` | field | Uppercase string |
| `lower` | field | Lowercase string |
| `remove` | field | Delete field |
| `copy` | from, to | Duplicate value |
| `default` | field, value | Set if absent/null |
| `filter` | field, op, value | Drop records (eq/ne/gt/gte/lt/lte/contains/exists) |
| `map_value` | field, mapping | Replace via lookup table |
| `truncate` | field, max_len | Limit string length |
| `nest` | fields, into | Group fields into sub-object |
| `flatten` | field | Inline sub-object fields |
| `hash` | field | SHA-256 hash replacement |
| `coalesce` | fields, into | First non-null value |
| `schema_filter` | field, allow, deny | Regex allow/deny patterns |

## REST API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/pipes` | List pipes |
| GET | `/pipes/{name}` | Get pipe details |
| POST | `/pipes` | Create pipe |
| PUT | `/pipes/{name}` | Update pipe |
| DELETE | `/pipes/{name}` | Delete pipe |
| GET | `/pipes/{name}/resolve` | Resolve a pipe into effective runtime queries |
| POST | `/pipes/dry-run` | Preview pipeline (mock/live) |
| GET | `/config/export` | Export config from control-plane DB |
| POST | `/config/import` | Replace config from TOML/JSON |
| GET | `/pipe-presets` | List saved recipes |
| GET | `/pipe-presets/{name}` | Get saved recipe details |
| POST | `/pipe-presets` | Create saved recipe |
| PUT | `/pipe-presets/{name}` | Update saved recipe |
| DELETE | `/pipe-presets/{name}` | Delete saved recipe |
| GET | `/sinks` | List sinks |
| POST | `/sinks` | Create sink |
| GET | `/credentials` | List credentials (no secrets) |
| POST | `/credentials` | Store encrypted credential |
| DELETE | `/credentials/{name}` | Delete credential |
| GET | `/config/versions` | Config version history |
| POST | `/sync/pause` | Pause all sync |
| POST | `/sync/resume` | Resume sync |
| GET | `/sync/status` | Running/paused state |
| GET | `/history` | Last 100 cycle results |
| GET | `/openapi.json` | Merged OpenAPI 3.1 spec for control-plane + engine routes, generated via `utoipa` |

There are no legacy `/sources` routes anymore. The control plane is pipe-first end to end.

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `oversync` | Engine, config, scheduler, lifecycle, dry-run, credentials, rate limiting |
| `oversync-core` | Types (`RawRow`, `DeltaEvent`), traits (`OriginConnector`, `Sink`, `TransformHook`), errors |
| `oversync-connectors` | Origin implementations (Postgres, MySQL, HTTP, GraphQL, Trino, ClickHouse, FlightSQL, MCP) |
| `oversync-sinks` | Target implementations (Kafka, HTTP, SurrealDB, MCP, stdout) |
| `oversync-transforms` | Transform step library + WASM plugin support |
| `oversync-delta` | DeltaEngine (SurrealDB state operations) |
| `oversync-client` | Consumer-safe Rust SDK and wire DTOs for external services using the control-plane API |
| `oversync-api` | Server-side Axum REST API for shared control-plane routes and base OpenAPI document |
| `oversync-links` | Entity linking (stub) |

## Feature Flags

| Feature | What it enables |
|---------|----------------|
| (default) | Core engine + all connectors + all sinks + transforms |
| `schema` | Schema apply via overshift on engine build |
| `api` | REST API via `engine.api_router()` |
| `cli` | Standalone binary (api + schema + clap + otel) |
| `wasm` | WASM transform plugins via wasmtime |
| `parallel` | Rayon-based parallel row hashing (multi-core speedup) |

## Performance

The hot path (row hashing + diff detection) is optimized for throughput:

- **Hardware-accelerated SHA-256** via sha2 0.11 (SHA-NI on x86-64, SHA2 on Apple Silicon)
- **SIMD hex encoding** via const-hex
- **Optional parallel hashing** via rayon (`--features parallel`)

Benchmarks on Apple M4 (10 cores), realistic rows (15 fields, nested JSON):

| Operation | 1K rows | 10K rows | 100K rows |
|-----------|---------|----------|-----------|
| Hash (sequential) | 604 µs | 6.2 ms | 63 ms |
| Hash (parallel) | 183 µs | 1.7 ms | 15 ms |
| Diff no-change (seq) | 688 µs | 7.5 ms | 83 ms |
| Diff no-change (par) | 282 µs | 2.3 ms | 28 ms |

Run benchmarks:

```bash
cargo make bench                # all benchmarks
cargo make bench-parallel       # with parallel feature
cargo make bench-baseline       # save baseline
cargo make bench-compare        # compare against baseline
```

Run the system-level throughput harness against the shared Docker test stack:

```bash
THROUGHPUT_QUERIES=10 \
THROUGHPUT_ROWS=10000 \
THROUGHPUT_TIMEOUT_SECS=120 \
cargo make throughput
```

What it proves:
- first sync throughput across many independent queries
- steady-state no-change scan cost
- partial-update wave behavior where only half the queries change

This is intentionally query-level proof. It does not claim that one heavy query can be split across replicas.

Observed local baseline on the shared test stack:

| Shape | Result |
|-------|--------|
| `12 queries x 10k rows` | first sync `~8.7k rows/sec`, no-change `~8.1k scanned rows/sec`, partial-update `~10.0k scanned rows/sec` |
| `20 queries x 20k rows` | first sync `~7.4k rows/sec`, no-change `~9.0k scanned rows/sec`, partial-update `~8.8k scanned rows/sec` |

How to read this honestly:
- This is a local engineering baseline, not a production SLA.
- The limiting cost is full-scan work per query, especially on no-change cycles.
- “Millions of rows per day” is realistic across many independent queries at this scan rate, but it is still query-level scale-out, not parallelization of one giant table scan.
- If your workload is dominated by a few huge tables with frequent no-change polls, you should expect scan cost to dominate and tune schedule/query shape accordingly.

Run the rolling-restart cluster soak harness against the shared Docker test stack:

```bash
SOAK_WAVES=25 \
SOAK_TIMEOUT_SECS=180 \
cargo make cluster-soak
```

What it proves:
- repeated scheduler restarts keep `cycle_id` moving forward against shared state
- no duplicate `created` wave is emitted during leader handoff
- sink state remains reconciled with the source after many restart/mutation rounds

Current regression baseline:
- `SOAK_WAVES=25` passed on the shared test stack.
- This proves short rolling-restart handoff stability, but it is still a bounded soak campaign rather than a long-duration production burn-in.

## Development

```bash
cargo make test-stack-up     # one shared Postgres/MySQL/SurrealDB/Kafka/Trino stack
cargo make test-stack-wait   # wait for the stack to be ready
cargo make check             # compilation check
cargo make test              # unit + integration tests
cargo make ci                # full CI: fmt + clippy + coverage
cargo make test-stack-down   # tear the shared stack down
```

Tests default to the shared local endpoints below and can be overridden with env vars:

- `OVERSYNC_TEST_POSTGRES_DSN=postgres://postgres:postgres@127.0.0.1:55432/postgres`
- `OVERSYNC_TEST_MYSQL_DSN=mysql://root:root@127.0.0.1:53306/test`
- `OVERSYNC_TEST_SURREAL_URL=http://127.0.0.1:58000`
- `OVERSYNC_TEST_SURREAL_USERNAME=root`
- `OVERSYNC_TEST_SURREAL_PASSWORD=root`
- `OVERSYNC_TEST_KAFKA_BROKER=127.0.0.1:59092`
- `OVERSYNC_TEST_TRINO_URL=http://127.0.0.1:58080`
- `OVERSYNC_TEST_TRINO_USER=test`
- `OVERSYNC_TEST_TRINO_CATALOG=memory`

## License

Apache-2.0
