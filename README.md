# oversync

[![CI](https://github.com/overrealdb/oversync/actions/workflows/ci.yml/badge.svg)](https://github.com/overrealdb/oversync/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/overrealdb/oversync/graph/badge.svg)](https://codecov.io/gh/overrealdb/oversync)
[![Crates.io](https://img.shields.io/crates/v/oversync.svg)](https://crates.io/crates/oversync)
[![docs.rs](https://docs.rs/oversync/badge.svg)](https://docs.rs/oversync)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Lightweight data sync engine: poll sources, detect changes, deliver events.

Alternative to Kafka Connect / Debezium for scenarios where WAL-based CDC is impossible or unnecessary (system catalogs, APIs, metadata).

## Features

- **Poll-based delta detection** — SHA-256 hash comparison, no WAL access required
- **6 source connectors** — PostgreSQL, MySQL, HTTP REST, GraphQL (Relay pagination), Trino, Arrow Flight SQL
- **4 sink types** — Kafka, HTTP webhook, SurrealDB, stdout
- **SurrealDB state store** — snapshots, cycle logs, outbox pattern for crash-safe delivery
- **Fail-safe** — aborts cycle if deletion exceeds threshold (prevents false mass-deletes)
- **Dual mode** — embeddable library or standalone binary
- **Full Config API** — CRUD sources/sinks, pause/resume sync, cycle history via REST
- **Lifecycle management** — start, pause, resume, shutdown with hot config reload
- **Compile-time SurrealQL validation** — invalid `.surql` files break the build

## Quick Start

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
        .database("sync_state")
        .credentials("root", "root")
        .build()
        .await?;

    // Start from TOML config file
    engine.start_from_toml(std::path::Path::new("oversync.toml")).await?;

    // Or start from config stored in SurrealDB
    // engine.start_from_db().await?;

    // Optional: mount REST API into your axum app
    // let router = engine.api_router(); // requires "api" feature

    // Lifecycle control
    engine.pause().await;
    engine.resume().await?;
    engine.shutdown().await;
    Ok(())
}
```

### Standalone binary

```bash
cargo install oversync --features cli
oversync --config oversync.toml --bind 0.0.0.0:4200
```

### Configuration (oversync.toml)

```toml
[surrealdb]
url = "http://localhost:8000"
namespace = "oversync"
database = "sync"

[[sources]]
name = "pg-prod"
connector = "postgres"
dsn = "postgres://readonly:pass@db:5432/app"
interval_secs = 60

[[sources.queries]]
id = "users"
sql = "SELECT id::text, name, email FROM users"
key_column = "id"

[[sources]]
name = "catalog-api"
connector = "http"
dsn = "https://api.example.com"
interval_secs = 300

[sources.config]
response_path = "data.items"

[sources.config.auth]
type = "bearer"
token = "sk-api-key"

[sources.config.pagination]
type = "offset"
page_size = 100

[[sources.queries]]
id = "datasets"
sql = "/v1/datasets"
key_column = "id"

[[sinks]]
name = "webhook"
type = "http"

[sinks.config]
url = "https://app.example.com/webhooks/sync"
auth = { type = "bearer", token = "webhook-secret" }

[[sinks]]
name = "kafka"
type = "kafka"

[sinks.config]
brokers = "kafka:9092"
topic = "sync-events"
```

## Source Connectors

| Connector | Type | Key Features |
|-----------|------|-------------|
| PostgreSQL | `postgres` | Type-aware decoding (jsonb, bool, numeric), streaming via sqlx |
| MySQL | `mysql` | Type-aware decoding, streaming via sqlx |
| HTTP REST | `http` | Auth (Bearer/Basic/Header), offset & cursor pagination, response path navigation |
| GraphQL | `graphql` | Relay cursor pagination, GraphQL error detection, response path navigation |
| Trino | `trino` | REST protocol, query heartbeat, retry on 502/503/504 |
| Arrow Flight SQL | `flight-sql` | gRPC streaming, Arrow record batch conversion |

## Sinks

| Sink | Type | Key Features |
|------|------|-------------|
| HTTP Webhook | `http` | POST/PUT, retry on 429/5xx with exponential backoff (capped at 60s), auth, custom headers |
| Kafka | `kafka` | Native batch produce, message key = row key |
| SurrealDB | `surrealdb` | UPSERT with `_meta` object, batch via FOR loop |
| Stdout | `stdout` | JSON output for debugging, optional pretty-print |

## REST API

Available when using the standalone binary or `engine.api_router()` with the `api` feature.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/sources` | List configured sources |
| POST | `/sources` | Create source |
| PUT | `/sources/{name}` | Update source |
| DELETE | `/sources/{name}` | Delete source |
| GET | `/sinks` | List configured sinks |
| POST | `/sinks` | Create sink |
| PUT | `/sinks/{name}` | Update sink |
| DELETE | `/sinks/{name}` | Delete sink |
| POST | `/sources/{name}/trigger` | Trigger immediate sync |
| POST | `/sync/pause` | Pause all sync |
| POST | `/sync/resume` | Resume sync |
| GET | `/sync/status` | Running/paused state |
| GET | `/history` | Last 100 cycle results |
| GET | `/openapi.json` | OpenAPI 3.1 spec |

## Architecture

```
Source (PostgreSQL, API, GraphQL, ...)
    |
    v  fetch_all(sql, key_column) -> Vec<RawRow>
    |
    v  compute_diff(previous_hashes, current_rows) -> DeltaResult
    |        |
    |    ABORT if deleted% > fail_safe_threshold
    |
    v  EventEnvelope { meta, data }
    |
    +---> Kafka sink
    +---> HTTP webhook sink
    +---> SurrealDB sink
    +---> stdout sink
    |
    v  cycle_log (audit trail in SurrealDB)
```

See [docs/architecture.md](docs/architecture.md) for internals.

## Examples

```bash
cargo run --example basic_stdout              # Minimal embedded engine
cargo run --example custom_connector          # Custom SourceFactory registration
cargo run --example http_api --features api   # Engine with REST API
cargo run --example postgres_to_surrealdb     # Postgres -> SurrealDB (needs infra)
cargo run --example graphql_source            # GraphQL with Relay pagination
cargo run --example http_to_webhook           # REST API -> webhook delivery
cargo run --example multi_source_kafka        # Multi-source with sink routing
```

## Custom Connectors

Implement `SourceFactory` + `SourceConnector` and register with the engine:

```rust,no_run
use oversync::OversyncEngine;

let engine = OversyncEngine::builder("mem://")
    .register_source(Box::new(MyCustomSourceFactory))
    .register_sink(Box::new(MyCustomSinkFactory))
    .build()
    .await?;
```

See `examples/custom_connector.rs` for a complete example.

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `oversync` | Engine, config, scheduler, lifecycle, cycle runner |
| `oversync-core` | Types (`RawRow`, `DeltaEvent`, `AuthConfig`), traits (`SourceConnector`, `Sink`), errors |
| `oversync-connectors` | Source implementations (Postgres, MySQL, HTTP, GraphQL, Trino, FlightSQL) |
| `oversync-sinks` | Sink implementations (Kafka, HTTP, SurrealDB, stdout) |
| `oversync-delta` | DeltaEngine (SurrealDB state operations) |
| `oversync-api` | Axum REST API (CRUD, operations, OpenAPI) |
| `oversync-links` | Entity linking (stub) |

## License

Apache-2.0
