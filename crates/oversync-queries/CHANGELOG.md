# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.4](https://github.com/overrealdb/oversync/compare/oversync-queries-v0.6.3...oversync-queries-v0.6.4) - 2026-04-14

### Other

- Handle NONE results in Surreal distributed locks

## [0.5.0](https://github.com/overrealdb/oversync/compare/oversync-queries-v0.4.6...oversync-queries-v0.5.0) - 2026-04-10

### Other

- Finalize pipe-first release pass
- Drop legacy sources and ship pipe-first control plane
- Ship pipes control plane and cluster proof
- Stabilize connectors and shared test stack

## [0.4.0](https://github.com/overrealdb/oversync/compare/oversync-queries-v0.3.0...oversync-queries-v0.4.0) - 2026-04-03

### Added

- *(sinks)* add document mode to SurrealDB sink

### Other

- fix rustfmt formatting

## [0.3.0](https://github.com/overrealdb/oversync/compare/oversync-queries-v0.2.1...oversync-queries-v0.3.0) - 2026-04-02

### Added

- *(links)* integrate entity linking into CycleRunner (Task 4.4)
- *(links)* add link storage schema, CRUD queries, and integration tests

### Fixed

- *(links)* address slop-check findings in entity linking

## [0.2.0](https://github.com/overrealdb/oversync/compare/oversync-queries-v0.1.1...oversync-queries-v0.2.0) - 2026-04-01

### Other

- bump surql-macros 0.1.1 → 0.1.2 ([#18](https://github.com/overrealdb/oversync/pull/18))
- Add README.md for all 8 workspace crates

## [0.1.1](https://github.com/overrealdb/oversync/compare/oversync-queries-v0.1.0...oversync-queries-v0.1.1) - 2026-03-28

### Other

- cargo fmt
