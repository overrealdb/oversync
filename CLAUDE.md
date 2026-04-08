# OverSync Development Standards

## What is OverSync?

Lightweight poll-based data sync engine: fetch from sources (PostgreSQL, MySQL, HTTP, GraphQL, Trino, FlightSQL), detect changes via SHA-256 hash diff, deliver events to sinks (Kafka, HTTP webhook, SurrealDB). Alternative to Kafka Connect/Debezium when WAL-based CDC is impossible.

## Architecture

```
src/engine.rs       — OversyncEngine builder (embedded + standalone facade)
src/lifecycle.rs    — LifecycleManager (start/pause/resume/shutdown)
src/scheduler.rs    — Per-(source,query) polling tasks with retry
src/cycle.rs        — CycleRunner (single-cycle: fetch → diff → deliver)
src/config.rs       — SyncConfig from TOML
src/config_db.rs    — SyncConfig from SurrealDB tables
src/registry.rs     — PluginRegistry (SourceFactory + SinkFactory, Clone via Arc)

crates/
  oversync-core/       — Types (RawRow, DeltaEvent, AuthConfig), traits (SourceConnector, Sink), errors
  oversync-connectors/ — Postgres, MySQL, HTTP, GraphQL, Trino, FlightSQL
  oversync-delta/      — DeltaEngine (SurrealDB state operations)
  oversync-sinks/      — Kafka, HTTP webhook, SurrealDB, stdout
  oversync-api/        — Axum REST API (CRUD, auth middleware, OpenAPI)
  oversync-links/      — Entity linking (stub)
  oversync-transforms/ — Built-in + WASM transform pipeline
  oversync-queries/    — All .surql files + typed query constants (crates.io-publishable)
```

Data flow: Source → `Vec<RawRow>` → `compute_diff(prev_hashes, current)` → `DeltaResult{created,updated,deleted}` → fail-safe check → `EventEnvelope` → Sink

## Development Setup

```bash
# Prerequisites: Rust 1.94+, Docker (for integration tests)
cargo make check      # quick compilation check
cargo make test       # unit + integration tests
cargo make cli        # build standalone binary
```

## Testing

- **EVERYTHING must be covered by tests. ALWAYS. No exceptions.**
- **TDD workflow**: write failing test → make it pass → refactor
- **Integration tests use `testcontainers`** — real SurrealDB in Docker, not `mem://`
- Test helper: `tests/common/surreal.rs` → `TestSurrealContainer::new()` (with schema) or `::new_raw()` (without)
- Shared container via `tokio::sync::OnceCell` — one Docker container per test binary, isolated ns/db per test
- Mock HTTP/GraphQL servers via axum for connector/sink tests

### Docker requirements

Docker required for integration tests. Testcontainers starts SurrealDB automatically — no manual setup needed.

## Running Tests

```bash
cargo make test           # all tests (requires Docker)
cargo make test-docker    # with validate-docker feature
cargo make ci             # full CI: check + test + clippy + fmt + coverage
cargo make ci-full        # CI + Docker validation + cargo-deny
```

## Code Style

- No inline SurrealQL in Rust code — use `include_str!()` from `.surql` files
- No unnecessary comments (code should be self-documenting)
- No over-engineering — solve the current problem, not hypothetical future ones

## Quality

- `cargo make ci` must pass before creating PR
- Zero clippy warnings
- Zero fmt diffs

## SurrealQL in Files (MANDATORY)

All SurrealQL code lives in `.surql` files under `crates/oversync-queries/surql/`.
Never embed SQL strings in Rust code.

- Schema (declarative): `surql/schema/{domain}/tables.surql` — uses `DEFINE ... OVERWRITE`
- Migrations (imperative): `surql/migrations/v001_*.surql`, `v002_*.surql`, ... — uses `IF NOT EXISTS`
- Functions: `surql/schema/{domain}/fn.surql`
- Queries: `surql/queries/{delta,config,sink,mutations}/*.surql`
- Manifest: `surql/manifest.toml` — overshift manifest (ns, db, modules)
- Generated: `surql/generated/current.surql` — auto-generated schema snapshot (do not edit)
- Bootstrap managed by **overshift** (distributed lock, migration tracking in `_system` DB)
- Compile-time validation via `surql_parser::build::validate_schema()` in `oversync-queries/build.rs`

### Query access pattern

Sub-crates import query constants from `oversync-queries`, not `include_str!` directly:
```rust
use oversync_queries::delta;
const SQL: &str = delta::BATCH_UPSERT;
```
This ensures all .surql files live inside `oversync-queries` (required for crates.io publishing).

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `oversync` | Engine, config, scheduler, lifecycle, cycle runner |
| `oversync-core` | Types (`RawRow`, `DeltaEvent`, `AuthConfig`), traits (`SourceConnector`, `Sink`), errors |
| `oversync-connectors` | Postgres, MySQL, HTTP, GraphQL, Trino, FlightSQL |
| `oversync-sinks` | Kafka, HTTP webhook, SurrealDB, stdout |
| `oversync-delta` | DeltaEngine (SurrealDB state operations) |
| `oversync-api` | Axum REST API (CRUD, auth, operations, OpenAPI) |
| `oversync-queries` | All `.surql` files as typed Rust constants |
| `oversync-links` | Entity linking (stub) |
| `oversync-transforms` | Built-in + WASM transform pipeline |
| External: `overshift` v0.2 | Migration engine (distributed lock, checksums, dry-run) |
| External: `surql-parser` v0.1.4 | Compile-time `.surql` validation |
| External: `surql-macros` v0.1.1 | `#[surql_function]`, `surql_check!`, `surql_query!` proc-macros |

## Feature Flags

| Feature | What it enables |
|---------|----------------|
| (default) | Core engine + all connectors + all sinks |
| `schema` | Schema apply via overshift on engine build |
| `api` | REST API via `engine.api_router()` |
| `cli` | Standalone binary (api + schema + clap + otel) |
| `parallel` | Rayon-based parallel row hashing (multi-core speedup) |

## Key Principles

1. **Library-first** — `cargo add oversync` gives the full engine; binary is a thin wrapper
2. **TDD** — tests before code, never commit red
3. **SurrealQL in files** — no SQL strings in Rust, compile-time validated
4. **Fail-safe** — abort cycle if too many deletes (prevents false mass-delete events)
5. **No slop** — reject wrapper functions, restating comments, dead code, premature abstractions

## Git Commits

Conventional commits (feat/fix/chore). Pre-commit hooks must pass. Do not use `--no-verify`.

## AI Slop Index (REJECT if any apply)

- [ ] Wrapper functions that just call another function
- [ ] Comments restating code
- [ ] Over-engineering — abstractions for one-time use
- [ ] Per-request resource creation
- [ ] Dead code left behind
- [ ] Copy-paste with minor changes
- [ ] Misleading test names
- [ ] Pointless intermediate variables
- [ ] Premature generalization

## Cleanup

- Before finishing work, run `cargo make docker-cleanup` to remove testcontainers started by this project
- Do NOT run `docker-cleanup-all` — it kills containers from other parallel agents
- If tests are still running or you need containers alive, skip cleanup and mention it
