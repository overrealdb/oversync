# oversync v0.4.2

Short release focused on embedded startup reliability.

## What changed

- `DeltaEngine::tables_exist()` now times out `INFO FOR DB` after 5 seconds.
- On timeout, oversync assumes the per-pipe tables already exist instead of hanging startup on metadata reads.
- Embedded mode now logs when each pipe starts and finishes `ensure_tables()`.

## Why

This fixes a startup failure mode in embedded deployments where SurrealDB metadata checks could stall pipe initialization even though the tables were already present.

## Impact

- safer restart behavior for embedded pipelines
- better startup observability
- no config or API changes

## PR / Release text

`v0.4.2` is a small reliability release for embedded deployments. It adds a timeout around `INFO FOR DB` during per-pipe table checks and treats timeout as "tables already exist", which avoids startup stalls on slow SurrealDB metadata paths. It also adds explicit logs around `ensure_tables()` so embedded startup is easier to debug.
