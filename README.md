# oversync

[![CI](https://github.com/overrealdb/oversync/actions/workflows/ci.yml/badge.svg)](https://github.com/overrealdb/oversync/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/overrealdb/oversync/graph/badge.svg)](https://codecov.io/gh/overrealdb/oversync)
[![Crates.io](https://img.shields.io/crates/v/oversync.svg)](https://crates.io/crates/oversync)
[![docs.rs](https://docs.rs/oversync/badge.svg)](https://docs.rs/oversync)
[![dependency status](https://deps.rs/repo/github/overrealdb/oversync/status.svg)](https://deps.rs/repo/github/overrealdb/oversync)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Lightweight data sync engine: poll sources, detect changes, deliver events.

Alternative to Kafka Connect / Debezium for scenarios where WAL-based CDC is impossible or unnecessary (system catalogs, APIs, metadata).

## Features

- **Poll-based delta detection** — SHA-256 hash comparison, no WAL access required
- **SurrealDB state store** — snapshots, cycle logs, outbox pattern
- **Fail-safe** — aborts cycle if deletion exceeds threshold (prevents false mass-deletes)
- **Dual mode** — embeddable library or standalone binary
- **Compile-time SurrealQL validation** — invalid `.surql` files break the build
- **Declarative schema** — `DEFINE ... OVERWRITE` via [overshift](https://github.com/overrealdb/overshift)

## Quick Start

### As a library

```toml
[dependencies]
oversync = "0.1"
```

```rust,no_run
use oversync::{Scheduler, SyncConfig, PluginRegistry};
```

### As a standalone binary

```bash
cargo install oversync --features cli
oversync --config oversync.toml
```

## Architecture

```
Source (PostgreSQL, API, ...) → poll → delta detect → events → Sink (Kafka, webhook, ...)
                                         ↕
                                    SurrealDB (state)
```

See [docs/architecture.md](docs/architecture.md) for details.

## License

Apache-2.0
