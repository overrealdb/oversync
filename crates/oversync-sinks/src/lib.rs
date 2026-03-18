//! Sink implementations for oversync.

pub mod kafka;
pub mod stdout;

pub use kafka::KafkaSink;
pub use oversync_core::traits::Sink;
pub use stdout::StdoutSink;
