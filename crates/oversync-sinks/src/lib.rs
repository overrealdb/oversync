//! Sink implementations for oversync.

pub mod factory;
pub mod kafka;
pub mod stdout;

pub use factory::StdoutSinkFactory;
pub use kafka::KafkaSink;
pub use oversync_core::traits::Sink;
pub use stdout::StdoutSink;
