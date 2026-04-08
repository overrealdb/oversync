# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.5](https://github.com/overrealdb/oversync/compare/oversync-core-v0.4.4...oversync-core-v0.4.5) - 2026-04-08

### Other

- optimize hash hot path — sha2 0.11, const-hex, rayon parallel

## [0.2.1](https://github.com/overrealdb/oversync/compare/oversync-core-v0.2.0...oversync-core-v0.2.1) - 2026-04-02

### Added

- add KafkaAuth support, integration tests, and examples for new connectors

### Other

- update dev plan — Phase 1 tests complete
- add property-based tests for delta engine and fail-safe
- add 60+ unit tests for factories, model edge cases, and fix KafkaAuth default
- quality sweep — fix clippy warnings, slop, dead code

## [0.2.0](https://github.com/overrealdb/oversync/compare/oversync-core-v0.1.1...oversync-core-v0.2.0) - 2026-04-01

### Other

- Add README.md for all 8 workspace crates
