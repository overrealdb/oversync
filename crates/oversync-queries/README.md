# oversync-queries

Embedded SurrealQL query constants for oversync.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **Compile-time validated SurrealQL** -- all `.surql` files are parsed at build time via `surql-parser`
- **`delta` module** -- snapshot upsert, stale deletion, created/updated/deleted detection, cycle logging
- **`config` module** -- load and cache sources, sinks, pipes from SurrealDB
- **`mutations` module** -- CRUD operations for sources, sinks, pipes, DLQ entries, locks
- **`credential` module** -- credential storage and lookup
- **`sink` module** -- event upsert queries for the SurrealDB sink

All queries are `&str` constants loaded via `include_str!` from `surql/queries/`.

## Usage

```rust
use oversync_queries::delta;

let sql = delta::BATCH_UPSERT;
// Pass to surrealdb client: db.query(sql).bind(("rows", rows)).await?
```

## License

Apache-2.0
