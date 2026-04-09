//! `oversync` — a lightweight data sync engine: poll, delta, sink.
//!
//! # Quick Start (embedded)
//!
//! ```rust,no_run
//! use oversync::OversyncEngine;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = OversyncEngine::builder("mem://")
//!     .skip_schema(true)
//!     .build()
//!     .await?;
//! engine.start_from_toml(std::path::Path::new("oversync.toml")).await?;
//! // engine.shutdown().await;
//! # Ok(())
//! # }
//! ```

pub mod alerting;
pub mod config;
pub mod config_db;
pub mod config_version;
pub mod credential;
pub mod cycle;
pub mod distributed_lock;
pub mod dlq;
pub mod dry_run;
pub mod embedded;
pub mod engine;
pub mod lifecycle;
pub mod metrics;
pub mod pipe_status;
pub mod rate_limit;
pub mod recipes;
pub mod registry;
pub mod resilient_db;
pub mod scheduler;

pub use config::SyncConfig;
pub use config_db::load_config_from_db;
pub use cycle::{CycleConfig, CycleRunner};
pub use embedded::{EmbeddedSync, EmbeddedSyncBuilder};
pub use engine::OversyncEngine;
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

#[cfg(feature = "schema")]
pub(crate) mod embedded_surql {
	include!(concat!(env!("OUT_DIR"), "/embedded_surql.rs"));
}
