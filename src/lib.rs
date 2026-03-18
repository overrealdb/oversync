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

pub mod config;
pub mod cycle;
pub mod scheduler;

pub use config::SyncConfig;
pub use cycle::{CycleConfig, CycleRunner};
pub use oversync_core::*;
pub use scheduler::Scheduler;

mod surql_functions {
	#![allow(dead_code, unused_imports)]
	include!(concat!(env!("OUT_DIR"), "/surql_functions.rs"));
}
#[allow(unused_imports)]
pub use surql_functions::*;
