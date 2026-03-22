//! Sink implementations for oversync.

pub mod factory;
pub mod http_sink;
pub mod kafka;
pub mod mcp_sink;
pub mod stdout;
pub mod surrealdb_sink;

pub use factory::{
	HttpTargetFactory, KafkaTargetFactory, McpTargetFactory, StdoutTargetFactory,
	SurrealDbTargetFactory,
};
pub use http_sink::HttpSink;
pub use kafka::KafkaSink;
pub use mcp_sink::McpSink;
pub use oversync_core::traits::Sink;
pub use stdout::StdoutSink;
pub use surrealdb_sink::SurrealDbSink;
