# Bug: SurrealDB + TiKV hangs on DEFINE TABLE / schema operations

**Reported:** 2026-04-03
**Severity:** Critical (blocks all deployment on TiKV backend)
**Component:** SurrealDB v3 + TiKV v8.5.5

## Problem

When oversync connects to SurrealDB backed by TiKV (not in-memory or RocksDB), any `DEFINE TABLE` or schema operation hangs indefinitely. This affects:

1. **overshift schema apply** — `DEFINE TABLE`, `DEFINE INDEX`, `DEFINE FIELD` hang forever
2. **oversync cycle runner** — first cycle creates snapshot tables dynamically, hangs
3. **datacat migrations** — work fine (applied in ~200ms) because they run once on startup

## Reproduction

```bash
# SurrealDB + TiKV running
docker run -d --name surrealdb surrealdb/surrealdb:v3 start --path tikv://tikv-pd:2379

# This hangs forever:
echo "USE NS test DB test; DEFINE TABLE foo SCHEMALESS;" | \
  docker exec -i surrealdb /surreal sql --endpoint http://localhost:8000 --username root --password root
```

## What works

- `SELECT` / `UPSERT` / `CREATE` on existing tables — fast
- `DEFINE TABLE IF NOT EXISTS` via surreal CLI — sometimes works
- DataCat's migration runner — works (applies 25 migrations in <1s)
- SurrealDB with RocksDB or kv-mem backend — all operations fast

## What doesn't work

- `DEFINE TABLE` from WS connections (oversync uses WS for state_db)
- overshift `apply_schema()` with 5 modules — hangs after `starting apply`
- oversync cycle runner first cycle — hangs (likely DEFINE TABLE for snapshot)

## Workaround

- Pre-create all tables manually via `surreal sql` CLI
- Use `OVERSYNC_SURQL_DIR=/empty-dir/` to skip overshift schema apply
- Use SurrealDB with RocksDB backend instead of TiKV for development

## Root Cause

SurrealDB DDL (`DEFINE TABLE/FIELD/INDEX`) acquires a namespace-level write lock for schema mutations.
On TiKV, this is a distributed transaction requiring Raft consensus across regions.

Oversync's scheduler spawned **all pipe-query tasks concurrently** via `tokio::spawn`, and each
task called `ensure_tables()` which fires 3x DEFINE TABLE + 8+ DEFINE FIELD + 3 DEFINE INDEX.
With 4 polling tasks, that's 4 concurrent DDL batches fighting over the same distributed lock.

Additionally, `ensure_tables()` sent DDL to **both** `state_client` and `snapshot_client` — when
both point to the same SurrealDB instance, that doubles the contention.

DataCat migrations work because overshift applies them **sequentially** through a single connection
before any other clients connect.

## Fix (applied)

1. **Serialized DDL** — `ensure_tables()` now runs sequentially in `Scheduler::run()` and
   `run_all_pipes()` before spawning concurrent tasks (not inside each task).
2. **Skip DDL when tables exist** — `ensure_tables()` checks `INFO FOR DB` first and skips
   all DDL if the snapshot table already exists (the common restart case).

Files changed:
- `crates/oversync-delta/src/engine.rs` — added `tables_exist()`, early return in `ensure_tables()`
- `src/scheduler.rs` — moved `ensure_tables` to pre-spawn loop
- `src/embedded.rs` — same pattern for embedded engine

## Environment

- SurrealDB: v3.0.4 (aarch64, Docker)
- TiKV: v8.5.5
- TiKV PD: v8.5.5
- Host: Mac Mini M4 (arm64)
