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
pub mod config_db;
pub mod cycle;
pub mod lifecycle;
pub mod registry;
pub mod scheduler;

pub use config::SyncConfig;
pub use config_db::load_config_from_db;
pub use cycle::{CycleConfig, CycleRunner};
pub use lifecycle::LifecycleManager;
pub use oversync_core::*;
pub use registry::PluginRegistry;
pub use scheduler::Scheduler;

mod surql_functions {
	#![allow(dead_code, unused_imports)]
	include!(concat!(env!("OUT_DIR"), "/surql_functions.rs"));
}
#[allow(unused_imports)]
pub use surql_functions::*;
