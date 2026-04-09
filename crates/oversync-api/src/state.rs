use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::types::{PipePresetInfo, SinkInfo, SourceInfo, SourceStatus};

pub struct ApiState {
	pub sources: Arc<RwLock<Vec<SourceConfig>>>,
	pub sinks: Arc<RwLock<Vec<SinkConfig>>>,
	pub pipes: Arc<RwLock<Vec<PipeConfigCache>>>,
	pub pipe_presets: Arc<RwLock<Vec<PipePresetCache>>>,
	pub cycle_status: Arc<RwLock<HashMap<String, SourceStatus>>>,
	pub db_client: Option<Arc<surrealdb::Surreal<surrealdb::engine::any::Any>>>,
	pub lifecycle: Option<Arc<dyn LifecycleControl>>,
	pub api_key: Option<String>,
}

/// Trait for lifecycle operations so the API crate doesn't depend on the root crate.
#[async_trait::async_trait]
pub trait LifecycleControl: Send + Sync {
	async fn restart_with_config_json(
		&self,
		db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	) -> Result<(), oversync_core::error::OversyncError>;
	async fn export_config(
		&self,
		db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
		format: crate::types::ExportConfigFormat,
	) -> Result<String, oversync_core::error::OversyncError>;
	async fn import_config(
		&self,
		db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
		format: crate::types::ExportConfigFormat,
		content: &str,
	) -> Result<Vec<String>, oversync_core::error::OversyncError>;
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
	pub config: Option<serde_json::Value>,
}

pub struct PipeConfigCache {
	pub name: String,
	pub origin_connector: String,
	pub origin_dsn: String,
	pub targets: Vec<String>,
	pub interval_secs: u64,
	pub recipe: Option<serde_json::Value>,
	pub enabled: bool,
}

pub struct PipePresetCache {
	pub name: String,
	pub description: Option<String>,
	pub spec: serde_json::Value,
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
				config: s.config.clone(),
			})
			.collect()
	}

	pub fn pipes_info(&self) -> Vec<crate::types::PipeInfo> {
		let pipes = match self.pipes.try_read() {
			Ok(p) => p,
			Err(_) => return vec![],
		};
		pipes
			.iter()
			.map(|p| crate::types::PipeInfo {
				name: p.name.clone(),
				origin_connector: p.origin_connector.clone(),
				origin_dsn: p.origin_dsn.clone(),
				targets: p.targets.clone(),
				interval_secs: p.interval_secs,
				recipe: p.recipe.clone(),
				enabled: p.enabled,
			})
			.collect()
	}

	pub fn pipe_presets_info(&self) -> Vec<PipePresetInfo> {
		let presets = match self.pipe_presets.try_read() {
			Ok(p) => p,
			Err(_) => return vec![],
		};
		presets
			.iter()
			.map(|preset| PipePresetInfo {
				name: preset.name.clone(),
				description: preset.description.clone(),
				spec: preset.spec.clone(),
			})
			.collect()
	}
}
