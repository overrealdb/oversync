//! `oversync` — a lightweight data sync engine: poll, delta, sink.
//!
//! # Quick Start
//!
//! ```toml
//! [dependencies]
//! oversync = "0.1"
//! ```
//!
//! ```rust,no_run
//! use oversync::{RawRow, DeltaEvent, OpType, SourceConnector, Sink};
//! ```

pub use oversync_core::*;
