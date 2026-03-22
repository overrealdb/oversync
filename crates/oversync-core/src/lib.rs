//! Core types, traits, config, and errors for the oversync data sync engine.
//!
//! This crate is dependency-light and imported by every other `oversync-*`
//! crate. It avoids heavy runtime dependencies so it compiles fast.

pub mod config;
pub mod error;
pub mod model;
pub mod table_names;
pub mod traits;

pub use config::OversyncConfig;
pub use error::OversyncError;
pub use model::{
	AuthConfig, CycleStatus, DeltaEvent, DeltaResult, EventEnvelope, EventMeta, OpType, RawRow,
	compute_diff,
};
pub use table_names::TableNames;
pub use traits::{
	OriginConnector, OriginFactory, Sink, TargetFactory, TransformHook, TransformPipeline,
};
