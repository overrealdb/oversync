# oversync-delta

Hash-based delta detection engine for oversync.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **`DeltaEngine`** -- compares fetched rows against stored snapshots in SurrealDB, producing `DeltaEvent`s for created, updated, and deleted rows
- **Hash-based change detection** -- each row is hashed; only rows with changed hashes produce events
- **Cycle management** -- tracks cycle IDs, logs start/finish, and maintains history
- **Pending event buffer** -- stores pending events in SurrealDB for crash recovery
- **`check_fail_safe`** -- prevents catastrophic deletes when the source returns unexpectedly few rows

## Usage

```rust
use oversync_delta::DeltaEngine;
use oversync_core::TableNames;

let tables = TableNames::for_query("pg-prod", "users");
let engine = DeltaEngine::new(db.clone(), tables);

// Compare fresh rows against stored snapshot
let result = engine.compute(rows, "pg-prod", "users").await?;
// result.events contains Created, Updated, Deleted events
// result.status contains cycle statistics
```

## License

Apache-2.0
