use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::types::{SinkInfo, SourceInfo, SourceStatus};

pub struct ApiState {
	pub sources: Arc<RwLock<Vec<SourceConfig>>>,
	pub sinks: Arc<RwLock<Vec<SinkConfig>>>,
	pub cycle_status: Arc<RwLock<HashMap<String, SourceStatus>>>,
	pub db_client: Option<surrealdb::Surreal<surrealdb::engine::any::Any>>,
	pub lifecycle: Option<Arc<dyn LifecycleControl>>,
}

/// Trait for lifecycle operations so the API crate doesn't depend on the root crate.
#[async_trait::async_trait]
pub trait LifecycleControl: Send + Sync {
	async fn restart_with_config_json(
		&self,
		db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	) -> Result<(), oversync_core::error::OversyncError>;
	async fn pause(&self);
	async fn resume(&self) -> Result<(), oversync_core::error::OversyncError>;
	async fn is_running(&self) -> bool;
	async fn is_paused(&self) -> bool;
}

pub struct SourceConfig {
	pub name: String,
	pub connector: String,
	pub interval_secs: u64,
	pub queries: Vec<QueryConfig>,
}

pub struct QueryConfig {
	pub id: String,
	pub key_column: String,
}

pub struct SinkConfig {
	pub name: String,
	pub sink_type: String,
}

impl ApiState {
	pub fn sources_info(&self) -> Vec<SourceInfo> {
		let sources = match self.sources.try_read() {
			Ok(s) => s,
			Err(_) => return vec![],
		};
		sources
			.iter()
			.map(|s| {
				let status = self
					.cycle_status
					.try_read()
					.ok()
					.and_then(|map| map.get(&s.name).cloned())
					.unwrap_or(SourceStatus {
						last_cycle: None,
						total_cycles: 0,
					});

				SourceInfo {
					name: s.name.clone(),
					connector: s.connector.clone(),
					interval_secs: s.interval_secs,
					queries: s
						.queries
						.iter()
						.map(|q| crate::types::QueryInfo {
							id: q.id.clone(),
							key_column: q.key_column.clone(),
						})
						.collect(),
					status,
				}
			})
			.collect()
	}

	pub fn sinks_info(&self) -> Vec<SinkInfo> {
		let sinks = match self.sinks.try_read() {
			Ok(s) => s,
			Err(_) => return vec![],
		};
		sinks
			.iter()
			.map(|s| SinkInfo {
				name: s.name.clone(),
				sink_type: s.sink_type.clone(),
			})
			.collect()
	}
}
