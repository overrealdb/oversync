use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::types::{CycleInfo, SinkInfo, SourceInfo, SourceStatus};

pub struct ApiState {
	pub sources: Vec<SourceConfig>,
	pub sinks: Vec<SinkConfig>,
	pub cycle_status: Arc<RwLock<HashMap<String, SourceStatus>>>,
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
		self.sources
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
		self.sinks
			.iter()
			.map(|s| SinkInfo {
				name: s.name.clone(),
				sink_type: s.sink_type.clone(),
			})
			.collect()
	}
}

impl Clone for SourceStatus {
	fn clone(&self) -> Self {
		Self {
			last_cycle: self.last_cycle.as_ref().map(|c| CycleInfo {
				cycle_id: c.cycle_id,
				status: c.status.clone(),
				started_at: c.started_at,
				finished_at: c.finished_at,
				rows_created: c.rows_created,
				rows_updated: c.rows_updated,
				rows_deleted: c.rows_deleted,
			}),
			total_cycles: self.total_cycles,
		}
	}
}
