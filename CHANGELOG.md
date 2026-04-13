# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.3](https://github.com/overrealdb/oversync/compare/oversync-v0.6.1...oversync-v0.6.3) - 2026-04-13

### Other

- Stabilize Surreal transport tests in CI
- Relax heavy HTTP snapshot repro assertion
- Prefer WebSocket transport for SurrealDB clients
- Prefer WebSocket transport for SurrealDB clients
- Add snapshot payload reproduction harness
- Wrap OversyncClient around generated SDK
- Generate Rust SDK from OpenAPI snapshot
- Add manual pipe run control-plane endpoint
- Extract oversync-client from server API

## [0.6.2](https://github.com/overrealdb/oversync/compare/oversync-v0.6.1...oversync-v0.6.2) - 2026-04-13

### Other

- Prefer WebSocket transport for SurrealDB clients
- Add snapshot payload reproduction harness
- Wrap OversyncClient around generated SDK
- Generate Rust SDK from OpenAPI snapshot
- Add manual pipe run control-plane endpoint
- Extract oversync-client from server API

## [0.6.1](https://github.com/overrealdb/oversync/compare/oversync-v0.6.0...oversync-v0.6.1) - 2026-04-12

### Other

- Update rand to 0.9.3 for cargo-deny
- Ignore OpenAPI version churn in CI SDK check
- Remove local target dir from UI API generation
- Unignore tracked UI API generation script
- Track OpenAPI SDK generation script
- Fix duplicate host port allocation in CI test stack
- Generate UI SDK from OpenAPI and tighten schema types
- Move flaky resilient DB soak tests out of blocking CI
- Embed control plane UI into oversync server

## [0.6.0](https://github.com/overrealdb/oversync/compare/oversync-v0.5.1...oversync-v0.6.0) - 2026-04-10

### Other

- Fix pipe runtime read models for file-start mode

## [0.5.1](https://github.com/overrealdb/oversync/compare/oversync-v0.5.0...oversync-v0.5.1) - 2026-04-10

### Other

- Harden CI stack startup with isolated ports

## [0.5.0](https://github.com/overrealdb/oversync/compare/oversync-v0.4.6...oversync-v0.5.0) - 2026-04-10

### Other

- Upgrade wasmtime to resolve RustSec advisories
- Finalize pipe-first release pass
- Drop legacy sources and ship pipe-first control plane
- Ship pipes control plane and cluster proof
- Polish control plane UI
- Stabilize connectors and shared test stack

## [0.4.6](https://github.com/overrealdb/oversync/compare/oversync-v0.4.5...oversync-v0.4.6) - 2026-04-08

### Fixed

- fix cli schema loading without repo checkout

## [0.4.5](https://github.com/overrealdb/oversync/compare/oversync-v0.4.4...oversync-v0.4.5) - 2026-04-08

### Other

- Merge pull request #27 from overrealdb/feat/perf-benchmarks
- optimize hash hot path — sha2 0.11, const-hex, rayon parallel

## [0.4.4](https://github.com/overrealdb/oversync/compare/oversync-v0.4.3...oversync-v0.4.4) - 2026-04-06

### Fixed

- speed up docker release builds

## [0.4.3](https://github.com/overrealdb/oversync/compare/oversync-v0.4.2...oversync-v0.4.3) - 2026-04-06

### Other

- note docker tag release fix

### Fixed

- *(docker)* publish images when release-plz creates `oversync-core-v*` tags

## [0.4.2](https://github.com/overrealdb/oversync/compare/oversync-v0.4.1...oversync-v0.4.2) - 2026-04-05

### Fixed

- harden embedded startup table checks

### Fixed

- *(delta)* treat `INFO FOR DB` timeout as "tables already exist" during startup checks, avoiding embedded startup stalls on slow SurrealDB metadata paths
- *(embedded)* add pipe-level logs around `ensure_tables()` so startup hangs are visible in embedded mode

## [0.4.0](https://github.com/overrealdb/oversync/compare/oversync-v0.3.0...oversync-v0.4.0) - 2026-04-03

### Added

- *(sinks)* add document mode to SurrealDB sink

### Fixed

- serialize DDL to prevent TiKV lock contention on startup
- *(docker)* use /surreal isready for healthcheck in SurrealDB v3

### Other

- add TiKV DEFINE TABLE hang bug report
- *(deps)* update overshift to v0.2.2
- fix rustfmt formatting

## [0.3.0](https://github.com/overrealdb/oversync/compare/oversync-v0.2.1...oversync-v0.3.0) - 2026-04-02

### Added

- *(bench)* add Criterion benchmarks for delta and transforms (Tasks 5.1, 5.2)
- *(links)* integrate entity linking into CycleRunner (Task 4.4)
- *(links)* add link storage schema, CRUD queries, and integration tests

### Fixed

- *(links)* address slop-check findings in entity linking

### Other

- fix rustfmt in benches
- mark Phase 3 release pipeline complete

## [0.2.1](https://github.com/overrealdb/oversync/compare/oversync-v0.2.0...oversync-v0.2.1) - 2026-04-02

### Added

- *(ui)* add ESLint flat config, fix lint issues, add CI job
- add KafkaAuth support, integration tests, and examples for new connectors
- add PostgreSQL, MySQL, ClickHouse sinks + Kafka, SurrealDB sources
- add JavaScript transform step via rquickjs (QuickJS)

### Fixed

- *(ci)* graceful skip for FlightSQL stress tests without server
- use OVERSYNC_SURQL_DIR env for schema path in Docker

### Other

- mark Task 3.1 release prep done
- update dev plan — Phase 1 tests complete
- add property-based tests for transform steps
- add property-based tests for delta engine and fail-safe
- add 60+ unit tests for factories, model edge cases, and fix KafkaAuth default
- quality sweep — fix clippy warnings, slop, dead code
- optimize CI — eliminate double test run, move stress to main-only

## [0.2.0](https://github.com/overrealdb/oversync/compare/oversync-v0.1.1...oversync-v0.2.0) - 2026-04-01

### Other

- bump surql-macros 0.1.1 → 0.1.2 ([#18](https://github.com/overrealdb/oversync/pull/18))

## [0.1.1](https://github.com/overrealdb/oversync/compare/oversync-v0.1.0...oversync-v0.1.1) - 2026-03-28

### Other

- Update CLAUDE.md and README for v0.1.0 release
- Fix cargo-make coverage task conflict with built-in
- cargo fmt
- Fix Dockerfile: surql/ moved to crates/oversync-queries/surql/
