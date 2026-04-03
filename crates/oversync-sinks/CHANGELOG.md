# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0](https://github.com/overrealdb/oversync/compare/oversync-sinks-v0.3.0...oversync-sinks-v0.4.0) - 2026-04-03

### Added

- *(sinks)* add document mode to SurrealDB sink

### Fixed

- use config struct for SurrealDbSink to satisfy clippy too-many-arguments

### Other

- fix rustfmt formatting

## [0.2.1](https://github.com/overrealdb/oversync/compare/oversync-sinks-v0.2.0...oversync-sinks-v0.2.1) - 2026-04-02

### Added

- add PostgreSQL, MySQL, ClickHouse sinks + Kafka, SurrealDB sources

### Other

- add 60+ unit tests for factories, model edge cases, and fix KafkaAuth default
- quality sweep — fix clippy warnings, slop, dead code

## [0.2.0](https://github.com/overrealdb/oversync/compare/oversync-sinks-v0.1.1...oversync-sinks-v0.2.0) - 2026-04-01

### Other

- Add README.md for all 8 workspace crates

## [0.1.1](https://github.com/overrealdb/oversync/compare/oversync-sinks-v0.1.0...oversync-sinks-v0.1.1) - 2026-03-28

### Other

- cargo fmt
