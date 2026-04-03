# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0](https://github.com/overrealdb/oversync/compare/oversync-delta-v0.3.0...oversync-delta-v0.4.0) - 2026-04-03

### Fixed

- serialize DDL to prevent TiKV lock contention on startup

## [0.3.0](https://github.com/overrealdb/oversync/compare/oversync-delta-v0.2.1...oversync-delta-v0.3.0) - 2026-04-02

### Added

- *(links)* integrate entity linking into CycleRunner (Task 4.4)

### Fixed

- *(links)* address slop-check findings in entity linking

## [0.2.1](https://github.com/overrealdb/oversync/compare/oversync-delta-v0.2.0...oversync-delta-v0.2.1) - 2026-04-02

### Other

- add property-based tests for delta engine and fail-safe

## [0.2.0](https://github.com/overrealdb/oversync/compare/oversync-delta-v0.1.1...oversync-delta-v0.2.0) - 2026-04-01

### Other

- Add README.md for all 8 workspace crates

## [0.1.1](https://github.com/overrealdb/oversync/compare/oversync-delta-v0.1.0...oversync-delta-v0.1.1) - 2026-03-28

### Other

- cargo fmt
