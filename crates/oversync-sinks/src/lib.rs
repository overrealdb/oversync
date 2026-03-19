//! Sink implementations for oversync.

pub mod factory;
pub mod kafka;
pub mod stdout;
pub mod surrealdb_sink;

pub use factory::{KafkaSinkFactory, StdoutSinkFactory, SurrealDbSinkFactory};
pub use kafka::KafkaSink;
pub use oversync_core::traits::Sink;
pub use stdout::StdoutSink;
pub use surrealdb_sink::SurrealDbSink;
