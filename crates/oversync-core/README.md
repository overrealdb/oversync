# oversync-core

Core types, traits, config, and errors for the oversync data sync engine.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **`OriginConnector` / `Sink` traits** -- the two fundamental interfaces every connector and sink implements
- **`OriginFactory` / `TargetFactory`** -- factory traits for creating connectors and sinks from JSON config
- **`TransformHook` / `TransformPipeline`** -- composable event transform chain applied before sinks
- **`DeltaEvent` / `EventEnvelope` / `RawRow`** -- core data model for rows, delta events, and envelope metadata
- **`OversyncConfig` / `OversyncError`** -- configuration loading and unified error type

## Usage

```rust
use oversync_core::{OriginConnector, Sink, TransformHook};
use oversync_core::{OversyncConfig, OversyncError, RawRow, DeltaEvent};

// Implement a custom origin connector
#[async_trait::async_trait]
impl OriginConnector for MySource {
    fn name(&self) -> &str { "my-source" }

    async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
        // fetch rows from your data source
        todo!()
    }

    async fn test_connection(&self) -> Result<(), OversyncError> {
        todo!()
    }
}
```

## License

Apache-2.0
