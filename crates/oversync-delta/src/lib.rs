//! Hash-based delta detection engine for oversync.
//!
//! Compares fetched rows against stored snapshots in SurrealDB,
//! producing [`DeltaEvent`]s for created, updated, and deleted rows.

pub mod engine;

pub use engine::{DeltaEngine, check_fail_safe};
pub use oversync_core::model::{CycleStatus, DeltaEvent, DeltaResult, OpType, compute_diff};
