# OverSync Development Standards

## TDD Workflow (MANDATORY)

1. **Write failing tests first** — before any feature code
2. **Run tests** — verify they fail for the right reason
3. **Write code** — minimum code to make tests pass
4. **Refactor** — clean up while tests stay green
5. **Never commit with failing tests**

## Testing Rules

- **EVERYTHING must be covered by tests. ALWAYS. No exceptions.**
- **Integration tests use `testcontainers`** — real SurrealDB in Docker, not `mem://`
- Test helper: `tests/common/surreal.rs` → `TestSurrealContainer::new()` (with migrations) or `::new_raw()` (without)
- Shared container via `tokio::sync::OnceCell` — one Docker container per test binary, isolated ns/db per test

## SurrealQL in Files (MANDATORY)

All SurrealQL code lives in `.surql` files. Never embed SQL strings in Rust code.

- Schema (declarative): `surql/schema/{domain}/tables.surql` — uses `DEFINE ... OVERWRITE`
- Migrations (imperative): `surql/migrations/v001_*.surql`, `v002_*.surql`, ... — uses `IF NOT EXISTS`
- Functions: `surql/schema/{domain}/fn.surql` (when added)
- Queries: `surql/queries/delta/*.surql` — operational queries, loaded via `include_str!()`
- Manifest: `surql/manifest.toml` — overshift manifest (ns, db, modules)
- Generated: `surql/generated/current.surql` — auto-generated schema snapshot (do not edit)
- Bootstrap managed by **overshift** (distributed lock, migration tracking in `_system` DB)
- Compile-time validation via `surql_parser::build::validate_schema()` in `build.rs`

## Crate Structure

- `oversync-core` — config, error types, domain model (`RawRow`, `DeltaEvent`, `OpType`), traits (`SourceConnector`, `Sink`)
- `oversync-connectors` — source connector implementations (Postgres, MySQL, etc.)
- `oversync-delta` — delta detection (snapshot compare, hash diff)
- `oversync-links` — entity linking / foreign key resolution
- `oversync-sinks` — sink implementations (SurrealDB, webhook, etc.)
- `oversync-api` — HTTP API (health, status, manual trigger)
- External deps: `overshift` (migration engine), `surql-parser` (compile-time validation), `surql-macros` (proc macros)

## Architecture Rules

- Config prefix: `OVERSYNC_` env vars
- SurrealDB is the state store (snapshots, cycle logs, configs)
- Library-first: `cargo add oversync` gives core types + traits
- Binary `oversync` is a thin CLI wrapper over the library

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
