//! Hash-based delta detection engine for oversync.
//!
//! Compares fetched rows against stored snapshots in SurrealDB,
//! producing [`DeltaEvent`]s for created, updated, and deleted rows.

pub use oversync_core::model::{DeltaEvent, OpType};
