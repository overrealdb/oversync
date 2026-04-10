# Oversync Progress

Updated: 2026-04-10

## Current Goal

Bring `oversync` to a state where:

1. the UI is connected to the real backend and can onboard runnable PostgreSQL syncs through `pipes`
2. cluster behavior is safer for multi-instance 24/7 deployments
3. the release-facing surface is pipe-first, documented, and scrubbed of private naming outside `secret/`

## Facts Locked In

- Shared external test stack is in place and replaces per-test `testcontainers`.
- UI shell was redesigned and is already running against the real API.
- Current `sources` UI is not a valid onboarding path for runnable PostgreSQL syncs.
- Real onboarding should move to `pipes`, especially for `postgres_snapshot` and `postgres_metadata`.
- The generated OpenAPI should describe the actual engine router, not only the base API crate routes.
- Cluster readiness is only partial right now:
  - shared snapshot state now defaults to the primary state DB when no separate snapshot DB is configured
  - scheduler instance identity no longer falls back to bare hostname; each scheduler gets a unique process-scoped identity unless `OVERSYNC_INSTANCE_ID` is explicitly set
  - high-volume behavior is promising but not yet proven for strict production claims

## Plan

### 1. Cluster Hardening

- [x] make scheduler fail closed on lock acquisition errors
- [x] add lock renewal / lease heartbeat while a cycle is running
- [x] add tests for renewed lease behavior
- [x] reuse the primary state DB for snapshot state by default instead of local `mem://`
- [x] generate unique scheduler instance IDs by default instead of hostname-only fallback
- [x] add regression tests for restart/failover baseline reuse and multi-instance lock exclusivity
- [x] add scheduler handoff proof: leader A -> leader B no duplicate create wave, then only post-failover updates
- [x] add a rolling-restart soak harness that repeatedly hands work between fresh scheduler instances

### 2. Recipe Support In DB/API

- [x] persist `pipe.recipe` in config DB
- [x] extend API request/response types to carry recipe fields
- [x] load recipe back into runtime config from DB
- [x] verify `postgres_metadata` and `postgres_snapshot` still expand correctly from DB-loaded config

### 3. Pipes UI

- [x] add `/pipes` route and navigation entry
- [x] list existing pipes from the real API
- [x] add create flow for PostgreSQL recipe-based pipes
- [x] add pipe detail/history surface backed by real `resolve + history` data
- [x] add in-place pipe edit flow for recipe-backed and manual pipes
- [ ] support at least:
  - [x] `postgres_metadata`
  - [x] `postgres_snapshot`
- [x] bind sink targets, schedule, diff mode, and DSN/credential fields

### 4. Cleanup / Follow-Ups

- [x] simplify/deprecate legacy `sources` create flow in the UI so new onboarding points to `pipes`
- [x] fix source read cache so source detail does not lie about queries
- [x] add a reproducible query-level throughput harness
- [x] benchmark / document realistic throughput claims for millions of rows per day
- [x] run larger cluster soak campaigns and document the honest operating envelope

### 5. Control Plane Maturity

- [x] add `config import` from `toml/json` into config DB with validation and replace semantics
- [x] expose `config export` and `config import` in UI
- [x] add UI support for backend `dry-run` flows
- [x] support manual/custom pipe definitions in UI, not only built-in recipe presets
- [x] persist reusable user-defined pipe presets and allow them to prefill manual pipe creation
- [x] keep preset export/import compatible across nested runtime shape and legacy flat UI/API shape
- [x] promote saved recipes into a first-class UI surface with create/edit/duplicate flows
- [x] share one spec editor between pipe creation and saved-recipe authoring so runtime defaults stay consistent
- [x] add parameterized saved recipes that materialize placeholders into concrete pipe drafts before creation

## Validation Completed

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo check --features 'api cli' --bin oversync`
- `cargo test --test scheduler_tests`
- `cargo test --test scheduler_tests scheduler_two_instances_do_not_double_process_same_query -- --exact --nocapture`
- `cargo test --test scheduler_tests scheduler_failover_reuses_baseline_and_emits_only_post_failover_updates -- --exact --nocapture`
- `cargo test --test scheduler_tests scheduler_rolling_restarts_keep_sink_state_reconciled -- --exact --nocapture`
- `SOAK_WAVES=12 SOAK_TIMEOUT_SECS=60 cargo test --test scheduler_tests scheduler_rolling_restart_soak_campaign -- --ignored --exact --nocapture`
- `SOAK_WAVES=25 SOAK_TIMEOUT_SECS=180 cargo test --test scheduler_tests scheduler_rolling_restart_soak_campaign -- --ignored --exact --nocapture`
- `cargo test --test distributed_lock_tests lock_renew_extends_lease -- --exact --nocapture`
- `cargo test --test api_mutation_tests create_pipe_stores_in_db -- --exact --nocapture`
- `cargo test --test api_mutation_tests update_pipe_modifies_db -- --exact --nocapture`
- `cargo test --test config_db_tests load_pipes_preserves_filters_transforms_and_links -- --exact --nocapture`
- `cargo test --test config_db_tests db_loaded_postgres_metadata_recipe_expands_into_queries -- --exact --nocapture`
- `cargo test --test config_db_tests db_loaded_postgres_snapshot_recipe_expands_at_runtime -- --exact --nocapture`
- `cargo test --test config_db_tests replace_and_load_pipe_presets_round_trip -- --exact --nocapture`
- `cargo test --test config_db_tests loaded_pipe_presets_export_to_toml -- --exact --nocapture`
- `cargo test --workspace --features validate-docker`
- `cargo test --test api_mutation_tests import_config_replaces_db_from_toml -- --exact --nocapture`
- `cargo test --test api_mutation_tests list_pipes_includes_disabled_entries -- --exact --nocapture`
- `cargo test --test api_mutation_tests update_pipe_can_clear_recipe_for_manual_mode -- --exact --nocapture`
- `cargo test --test api_mutation_tests source_detail_cache_reflects_query_summaries -- --exact --nocapture`
- `cargo test --test api_mutation_tests create_pipe_preset_stores_in_db_and_lists_in_api -- --exact --nocapture`
- `cargo test --test api_mutation_tests update_pipe_preset_rewrites_spec -- --exact --nocapture`
- `cargo test --features 'api cli' --test engine_tests engine_api_resolve_pipe_returns_effective_queries -- --exact --nocapture`
- `cargo test --test engine_tests engine_builder_without_snapshot_url_reuses_shared_state_snapshot -- --exact --nocapture`
- `cargo test --lib scheduler::tests::generated_instance_ids_are_unique_without_override -- --exact --nocapture`
- `THROUGHPUT_QUERIES=4 THROUGHPUT_ROWS=1000 THROUGHPUT_TIMEOUT_SECS=60 cargo test --test throughput_benchmark_tests throughput_parallel_queries_three_wave_harness -- --ignored --exact --nocapture`
- `THROUGHPUT_QUERIES=20 THROUGHPUT_ROWS=20000 THROUGHPUT_TIMEOUT_SECS=300 cargo test --test throughput_benchmark_tests throughput_parallel_queries_three_wave_harness -- --ignored --exact --nocapture`
- `npm run lint`
- `npm test`
- `npm run build`
- `rg -n -i "private|secret|internal-only" . --glob '!target' --glob '!storage' --glob '!secret'`
- live smoke: `/pipes` page opened against the real API and successfully created a `postgres_snapshot` pipe with persisted `recipe`
- live smoke: `/pipes/{name}/resolve` returned real runtime queries for a recipe-backed pipe
- live smoke: `/pipes/$name` now renders runtime shape, effective queries, and pipe-scoped cycle history from the real backend
- live smoke: UI dry-run dialog opened from `/pipes`, ran against `/pipes/dry-run`, and rendered `created=1 updated=0 deleted=0` without console errors
- live smoke: manual pipe creation through `/pipes` persisted a runnable PostgreSQL query and dry-run returned `created=2 updated=0 deleted=0`
- live smoke: legacy flat `pipe_preset_config.spec` exported successfully as nested TOML via `/config/export?format=toml`
- `cargo test --test config_db_tests legacy_flat_pipe_presets_export_to_toml -- --exact --nocapture`
- `cargo test --test engine_tests engine_start_from_db -- --exact --nocapture`
- `cargo test --test config_db_tests -- --nocapture`
- `cargo test --test migration_tests -- --nocapture`
- `cargo test --test api_mutation_tests -- --nocapture`

## Notes Locked In

- Forward-only migrations are required here. Editing an applied migration caused a real checksum mismatch on startup, so `pipe.recipe` now lives in `v005_pipe_recipe.surql` instead of being patched into `v001`.
- Dry-run UI depends on backend `resolve + dry-run` routes, not on parsing exported TOML in the browser.
- Pipe read cache must include disabled pipes, otherwise control-plane CRUD and dry-run paths lie about persisted state.
- Presets are a normal anti-corruption boundary: UI/API can stay flat for ergonomics, but persisted/exported runtime config must stay nested. Compatibility lives in the mapper, not by collapsing the runtime model.
- The dashboard is now fully pipe-centric: its primary counts come from `/pipes`, so the main overview no longer depends on an outdated onboarding abstraction.
- Saved recipes now have their own first-class `/recipes` surface in the UI, instead of living only as a secondary “save as preset” action inside pipe creation.
- Pipe authoring and saved-recipe authoring now share the same spec editor component, which reduces drift between runtime pipe payloads and reusable onboarding templates.
- Saved recipes now support parameter schemas and placeholder materialization, so one template can fan out into many concrete pipe drafts without mutating the stored recipe itself.
- Parameterized saved recipes can now preview and download materialized TOML or JSON pipe drafts before creation, which closes the gap between reusable templates and startup config workflows.
- `OversyncEngine::api_router()` now serves a merged `utoipa` OpenAPI document that includes engine-owned routes such as dry-run, resolve, credentials, and config version history.
- Multi-instance safety is better than before, but the honest scaling claim is still query-level parallelism. One heavy query does not get faster by adding replicas.
- There is now a reproducible query-level throughput harness. It measures first sync, no-change scan, and partial-update wave across many independent queries, but it is still a smoke/proof harness, not a full production benchmark campaign.
- Latest larger throughput baseline on the shared test stack: `20 queries x 20k rows = 400k rows/wave`, with first sync at about `7.4k rows/sec`, no-change scans at about `9.0k rows/sec`, and partial-update scans at about `8.8k rows/sec`.
- There is now a scheduler-level failover proof for the happy path: one leader can hand off to another without a duplicate create wave, and post-failover source mutations become only `updated` events.
- There is now an ignored rolling-restart soak harness under `tests/scheduler_tests.rs` that can run many restart/mutation rounds and verify the final sink state still matches the source.
- README now documents the measured local throughput and soak baselines explicitly, so “millions/day” is framed as an engineering envelope rather than an unqualified product promise.
- Legacy `source_config` is now removed from the live declarative schema and config DB load path; only forward-only migrations still mention it for historical upgrade compatibility.
- `replace_config_in_db()` and API mutation handlers now call `Surreal Response::check()`, because statement-level errors can otherwise be swallowed even when `.query(...).await` itself succeeds.
- Root `surql/schema/config/tables.surql` must stay byte-aligned with the canonical schema in `crates/oversync-queries/surql/schema/config/tables.surql`; drift there already caused real confusion once.

## Current Status

- Completed: cluster hardening, DB/API recipe persistence, DB-loaded recipe expansion coverage, config export/import, live `pipes` UI create flow, pipe detail/edit flow, manual/custom pipes, reusable presets, first-class saved-recipe UI flows, parameterized saved recipes, materialized recipe preview/export, UI dry-run flow, pipe-centric dashboard metrics, merged engine OpenAPI, private source-name sanitization, shared default snapshot state, scheduler instance identity hardening, legacy source runtime removal, and documented throughput/soak baselines
- Next: tag / release packaging

## Latest Cleanup Pass

- Removed live `source_config` usage from declarative config schema and added forward-only migration `v007_remove_source_config.surql`.
- Deleted unused legacy source/query SurrealQL assets from `oversync-queries`.
- Added regression coverage for `replace_config_in_db()` with real `pipes`, plus migration coverage that `source_config` is absent after the latest schema.
- Fixed silent Surreal mutation failures by checking `Response::check()` on config DB writes and API mutation handlers.
- Synced root `surql/schema/config/tables.surql` with the canonical `crates/oversync-queries` schema so local schema assets no longer drift from runtime reality.
- Final docs/examples pass is now aligned with the pipe-first model:
  - README connector/sink counts match the actual runtime
  - `/config/export` and `/config/import` are documented
  - legacy `/sources` compatibility text is gone
  - `oversync.example.toml` includes recipe and saved-recipe examples
