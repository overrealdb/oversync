# oversync v0.4.6

Pipe-first release focused on control-plane maturity, cluster safety, and operational validation.

## Highlights

- Pipe-first control plane end to end:
  - legacy `/sources` routes are gone
  - startup rejects legacy `[[sources]]`
  - UI onboarding now flows through `pipes` and saved recipes
- Saved recipes are first-class:
  - create, edit, duplicate, parameterize, and materialize into runnable pipes
  - preview/export materialized TOML or JSON before creation
- Runtime recipes for PostgreSQL:
  - `postgres_metadata`
  - `postgres_snapshot`
- Dry-run is available across backend and UI, including live and mock flows.
- Config import/export is part of the control plane.
- Cluster behavior is materially safer:
  - shared snapshot baseline defaults to the primary state DB
  - distributed lock renewal is in place
  - scheduler instance IDs are unique by default
  - rolling-restart soak and failover proofs are covered by regression tests

## Important Breaking Changes

- Legacy `[[sources]]` config is no longer supported.
- Legacy `/sources` API endpoints are removed.
- Declarative config schema no longer includes `source_config`; upgrades rely on forward-only migrations.

## Operational Notes

- Horizontal scale is query-level, not intra-query parallelization.
- Measured local baseline on the shared test stack:
  - `12 queries x 10k rows`: first sync about `8.7k rows/sec`
  - `20 queries x 20k rows`: first sync about `7.4k rows/sec`
- Rolling-restart soak harness passed with `SOAK_WAVES=25`.

## Internal Fixes

- Config DB and API mutation write paths now check Surreal statement-level errors explicitly via `Response::check()`.
- Root `surql/schema/` assets were resynced with the canonical schema under `crates/oversync-queries/surql/`.
- Shared external test stack replaced per-test container spawning.
