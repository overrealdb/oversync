# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
