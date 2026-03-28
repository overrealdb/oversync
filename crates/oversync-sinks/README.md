# oversync-sinks

Sink implementations for oversync.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **`StdoutSink`** -- prints delta events to stdout as JSON (useful for debugging)
- **`HttpSink`** -- delivers events via HTTP POST to a webhook endpoint
- **`KafkaSink`** -- produces events to a Kafka topic with native batching via rdkafka
- **`SurrealDbSink`** -- upserts events into SurrealDB tables
- **`McpSink`** -- delivers events via the MCP protocol

Each sink implements the `Sink` trait and has a corresponding `TargetFactory` for creation from JSON config.

## Usage

```rust
use oversync_sinks::{KafkaTargetFactory, HttpTargetFactory};
use oversync_core::traits::TargetFactory;

let factory = KafkaTargetFactory;
let sink = factory.create("kafka-main", &serde_json::json!({
    "brokers": "localhost:9092",
    "topic": "sync-events"
})).await?;

sink.send_batch(&envelopes).await?;
```

## License

Apache-2.0
