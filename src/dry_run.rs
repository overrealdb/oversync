use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::info;

use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, RawRow, compute_diff};
use oversync_core::traits::{OriginConnector, TransformHook};

use crate::config::PipeConfig;
use crate::registry::PluginRegistry;

/// Dry-run execution mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DryRunMode {
	/// Use provided mock data, no real connection.
	Mock,
	/// Connect to real origin, fetch limited rows, no target writes.
	Live,
}

/// Request to execute a dry-run of a pipe.
#[derive(Debug, Clone, Deserialize)]
pub struct DryRunRequest {
	pub pipe: PipeConfig,
	pub query_id: String,
	#[serde(default = "default_mode")]
	pub mode: DryRunMode,
	#[serde(default)]
	pub mock_data: Vec<RawRow>,
	#[serde(default = "default_row_limit")]
	pub row_limit: usize,
	#[serde(default)]
	pub transforms: Vec<serde_json::Value>,
	/// When true, diff against current SurrealDB state instead of empty.
	#[serde(default)]
	pub use_existing_state: bool,
}

fn default_mode() -> DryRunMode {
	DryRunMode::Mock
}

fn default_row_limit() -> usize {
	100
}

/// Dry-run result showing data at each pipeline stage.
#[derive(Debug, Clone, Serialize)]
pub struct DryRunResult {
	pub input_rows: usize,
	pub input_sample: Vec<RawRow>,
	pub changes: DryRunChanges,
	pub after_transform: Vec<EventEnvelope>,
	pub stats: DryRunStats,
}

/// Delta changes discovered during dry-run.
#[derive(Debug, Clone, Serialize)]
pub struct DryRunChanges {
	pub created: usize,
	pub updated: usize,
	pub deleted: usize,
}

/// Summary statistics for the dry-run.
#[derive(Debug, Clone, Serialize)]
pub struct DryRunStats {
	pub rows_fetched: usize,
	pub events_before_transform: usize,
	pub events_after_transform: usize,
	pub events_filtered_out: usize,
}

/// Execute a dry-run: fetch → diff → transform, without writing to targets or state.
///
/// When `state_db` is provided and `use_existing_state` is true, diffs against
/// the current snapshot in SurrealDB. Otherwise diffs against empty state.
pub async fn execute_dry_run(
	req: &DryRunRequest,
	registry: &PluginRegistry,
	transform_hook: Option<Arc<dyn TransformHook>>,
	state_db: Option<&oversync_delta::DeltaEngine>,
) -> Result<DryRunResult, OversyncError> {
	let query = req
		.pipe
		.queries
		.iter()
		.find(|q| q.id == req.query_id)
		.ok_or_else(|| {
			OversyncError::Config(format!(
				"pipe '{}': unknown query '{}'",
				req.pipe.name, req.query_id
			))
		})?;

	// 1. Fetch rows (mock or live)
	let rows = match req.mode {
		DryRunMode::Mock => {
			if req.mock_data.is_empty() {
				return Err(OversyncError::Config(
					"mock mode requires non-empty mock_data".into(),
				));
			}
			let limit = req.row_limit.min(req.mock_data.len());
			req.mock_data[..limit].to_vec()
		}
		DryRunMode::Live => {
			let connector = create_connector(&req.pipe, registry).await?;
			let limited_sql = format!("{} LIMIT {}", query.sql.trim().trim_end_matches(';'), req.row_limit);
			connector
				.fetch_all(&limited_sql, &query.key_column)
				.await?
		}
	};

	let input_rows = rows.len();
	let input_sample = if rows.len() > 10 {
		rows[..10].to_vec()
	} else {
		rows.clone()
	};

	info!(
		pipe = %req.pipe.name,
		query = %req.query_id,
		rows = input_rows,
		mode = ?req.mode,
		"dry-run: fetched rows"
	);

	// 2. Compute diff
	let previous: HashMap<String, String> = if req.use_existing_state {
		if let Some(engine) = state_db {
			let pipe_engine = engine.for_source(&req.pipe.name);
			pipe_engine
				.read_snapshot_keys_paged(&req.pipe.name, &req.query_id)
				.await
				.unwrap_or_default()
		} else {
			HashMap::new()
		}
	} else {
		HashMap::new()
	};
	let diff = compute_diff(&previous, &rows, &req.pipe.name, &req.query_id, 0);

	let changes = DryRunChanges {
		created: diff.created.len(),
		updated: diff.updated.len(),
		deleted: diff.deleted.len(),
	};

	// 3. Build envelopes
	let mut envelopes: Vec<EventEnvelope> = diff
		.created
		.iter()
		.chain(diff.updated.iter())
		.chain(diff.deleted.iter())
		.map(EventEnvelope::from)
		.collect();

	let events_before = envelopes.len();

	// 4. Apply transforms (if any)
	if let Some(hook) = transform_hook {
		envelopes = hook.transform(envelopes).await?;
	}

	let events_after = envelopes.len();

	info!(
		pipe = %req.pipe.name,
		before = events_before,
		after = events_after,
		"dry-run: transforms applied"
	);

	Ok(DryRunResult {
		input_rows,
		input_sample,
		changes,
		after_transform: envelopes,
		stats: DryRunStats {
			rows_fetched: input_rows,
			events_before_transform: events_before,
			events_after_transform: events_after,
			events_filtered_out: events_before.saturating_sub(events_after),
		},
	})
}

async fn create_connector(
	pipe: &PipeConfig,
	registry: &PluginRegistry,
) -> Result<Box<dyn OriginConnector>, OversyncError> {
	let mut map = match &pipe.origin.config {
		serde_json::Value::Object(m) => m.clone(),
		_ => serde_json::Map::new(),
	};
	map.insert(
		"dsn".into(),
		serde_json::Value::String(pipe.origin.dsn.clone()),
	);
	let config = serde_json::Value::Object(map);
	registry
		.create_source(&pipe.origin.connector, &pipe.name, &config)
		.await
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::*;

	fn test_pipe() -> PipeConfig {
		PipeConfig {
			name: "test-pipe".into(),
			origin: OriginDef {
				connector: "mock".into(),
				dsn: "mock://".into(),
				credential: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "q1".into(),
				sql: "SELECT 1".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef::default(),
			delta: DeltaDef::default(),
			retry: RetryDef::default(),
			filters: vec![],
			transforms: vec![],
			enabled: true,
		}
	}

	fn mock_rows() -> Vec<RawRow> {
		vec![
			RawRow {
				row_key: "1".into(),
				row_data: serde_json::json!({"id": "1", "name": "alice"}),
			},
			RawRow {
				row_key: "2".into(),
				row_data: serde_json::json!({"id": "2", "name": "bob"}),
			},
			RawRow {
				row_key: "3".into(),
				row_data: serde_json::json!({"id": "3", "name": "charlie"}),
			},
		]
	}

	#[tokio::test]
	async fn mock_dry_run_all_created() {
		let req = DryRunRequest {
			pipe: test_pipe(),
			query_id: "q1".into(),
			mode: DryRunMode::Mock,
			mock_data: mock_rows(),
			row_limit: 100,
			transforms: vec![],
			use_existing_state: false,
		};

		let registry = PluginRegistry::new();
		let result = execute_dry_run(&req, &registry, None, None).await.unwrap();

		assert_eq!(result.input_rows, 3);
		assert_eq!(result.input_sample.len(), 3);
		assert_eq!(result.changes.created, 3);
		assert_eq!(result.changes.updated, 0);
		assert_eq!(result.changes.deleted, 0);
		assert_eq!(result.after_transform.len(), 3);
		assert_eq!(result.stats.rows_fetched, 3);
		assert_eq!(result.stats.events_before_transform, 3);
		assert_eq!(result.stats.events_after_transform, 3);
		assert_eq!(result.stats.events_filtered_out, 0);
	}

	#[tokio::test]
	async fn mock_dry_run_with_row_limit() {
		let req = DryRunRequest {
			pipe: test_pipe(),
			query_id: "q1".into(),
			mode: DryRunMode::Mock,
			mock_data: mock_rows(),
			row_limit: 2,
			transforms: vec![],
			use_existing_state: false,
		};

		let registry = PluginRegistry::new();
		let result = execute_dry_run(&req, &registry, None, None).await.unwrap();

		assert_eq!(result.input_rows, 2);
		assert_eq!(result.changes.created, 2);
	}

	#[tokio::test]
	async fn mock_dry_run_with_transform() {
		use oversync_transforms::{StepChain, steps};

		let chain = StepChain::new(vec![
			Box::new(steps::Upper { field: "name".into() }),
			Box::new(steps::Set {
				field: "version".into(),
				value: serde_json::json!(1),
			}),
		]);

		let req = DryRunRequest {
			pipe: test_pipe(),
			query_id: "q1".into(),
			mode: DryRunMode::Mock,
			mock_data: mock_rows(),
			row_limit: 100,
			transforms: vec![],
			use_existing_state: false,
		};

		let registry = PluginRegistry::new();
		let result = execute_dry_run(&req, &registry, Some(Arc::new(chain)), None)
			.await
			.unwrap();

		assert_eq!(result.after_transform.len(), 3);
		assert_eq!(result.after_transform[0].data["name"], "ALICE");
		assert_eq!(result.after_transform[0].data["version"], 1);
	}

	#[tokio::test]
	async fn mock_dry_run_with_filter() {
		use oversync_transforms::{StepChain, steps};

		let chain = StepChain::new(vec![Box::new(steps::Filter {
			field: "name".into(),
			op: steps::FilterOp::Eq,
			value: serde_json::json!("alice"),
		})]);

		let req = DryRunRequest {
			pipe: test_pipe(),
			query_id: "q1".into(),
			mode: DryRunMode::Mock,
			mock_data: mock_rows(),
			row_limit: 100,
			transforms: vec![],
			use_existing_state: false,
		};

		let registry = PluginRegistry::new();
		let result = execute_dry_run(&req, &registry, Some(Arc::new(chain)), None)
			.await
			.unwrap();

		assert_eq!(result.stats.events_before_transform, 3);
		assert_eq!(result.stats.events_after_transform, 1);
		assert_eq!(result.stats.events_filtered_out, 2);
		assert_eq!(result.after_transform[0].data["name"], "alice");
	}

	#[tokio::test]
	async fn mock_dry_run_empty_data_errors() {
		let req = DryRunRequest {
			pipe: test_pipe(),
			query_id: "q1".into(),
			mode: DryRunMode::Mock,
			mock_data: vec![],
			row_limit: 100,
			transforms: vec![],
			use_existing_state: false,
		};

		let registry = PluginRegistry::new();
		let err = execute_dry_run(&req, &registry, None, None).await.unwrap_err();
		assert!(err.to_string().contains("mock_data"));
	}

	#[tokio::test]
	async fn mock_dry_run_unknown_query_errors() {
		let req = DryRunRequest {
			pipe: test_pipe(),
			query_id: "nonexistent".into(),
			mode: DryRunMode::Mock,
			mock_data: mock_rows(),
			row_limit: 100,
			transforms: vec![],
			use_existing_state: false,
		};

		let registry = PluginRegistry::new();
		let err = execute_dry_run(&req, &registry, None, None).await.unwrap_err();
		assert!(err.to_string().contains("nonexistent"));
	}

	#[tokio::test]
	async fn mock_dry_run_input_sample_capped_at_10() {
		let big_data: Vec<RawRow> = (0..25)
			.map(|i| RawRow {
				row_key: i.to_string(),
				row_data: serde_json::json!({"id": i}),
			})
			.collect();

		let req = DryRunRequest {
			pipe: test_pipe(),
			query_id: "q1".into(),
			mode: DryRunMode::Mock,
			mock_data: big_data,
			row_limit: 25,
			transforms: vec![],
			use_existing_state: false,
		};

		let registry = PluginRegistry::new();
		let result = execute_dry_run(&req, &registry, None, None).await.unwrap();

		assert_eq!(result.input_rows, 25);
		assert_eq!(result.input_sample.len(), 10);
	}
}
