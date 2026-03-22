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

/// Transient credentials for dry-run — NEVER persisted, only in-memory.
#[derive(Debug, Clone, Deserialize)]
pub struct TransientCredentials {
	pub username: String,
	pub password: String,
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
	/// Transient credentials for the origin connection. Never stored.
	/// For Trino: sent as X-Trino-Extra-Credential header.
	/// For native (postgres/mysql): injected into DSN.
	#[serde(default)]
	pub credentials: Option<TransientCredentials>,
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
			let connector = create_connector(&req.pipe, registry, req.credentials.as_ref()).await?;
			let limited_sql = format!(
				"{} LIMIT {}",
				query.sql.trim().trim_end_matches(';'),
				req.row_limit
			);
			connector.fetch_all(&limited_sql, &query.key_column).await?
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
	credentials: Option<&TransientCredentials>,
) -> Result<Box<dyn OriginConnector>, OversyncError> {
	let mut map = match &pipe.origin.config {
		serde_json::Value::Object(m) => m.clone(),
		_ => serde_json::Map::new(),
	};
	map.insert(
		"dsn".into(),
		serde_json::Value::String(pipe.origin.dsn.clone()),
	);

	// Inject transient credentials into connector config
	if let Some(creds) = credentials {
		match pipe.origin.connector.as_str() {
			"trino" => {
				// Trino: pass as extra_credentials for X-Trino-Extra-Credential header
				let extra = serde_json::json!({
					"username": creds.username,
					"password": creds.password,
				});
				map.insert("extra_credentials".into(), extra);
			}
			"postgres" | "mysql" => {
				// Native: inject user:pass into DSN
				if let Some(dsn) = map.get("dsn").and_then(|v| v.as_str()) {
					let injected =
						inject_credentials_into_dsn(dsn, &creds.username, &creds.password);
					map.insert("dsn".into(), serde_json::Value::String(injected));
				}
			}
			_ => {
				// Generic: add as top-level config fields
				map.insert(
					"username".into(),
					serde_json::Value::String(creds.username.clone()),
				);
				map.insert(
					"password".into(),
					serde_json::Value::String(creds.password.clone()),
				);
			}
		}
	}

	// Auto-route non-native connectors through Trino
	let (effective_connector, config) = if !pipe.origin.needs_trino_bridge() {
		(
			pipe.origin.connector.as_str(),
			serde_json::Value::Object(map),
		)
	} else {
		let trino_url = pipe
			.origin
			.trino_url
			.as_deref()
			.unwrap_or("http://localhost:8080");
		// Merge credentials into Trino config
		if let Some(creds) = credentials {
			map.clear();
			map.insert(
				"dsn".into(),
				serde_json::Value::String(trino_url.to_string()),
			);
			map.insert(
				"catalog".into(),
				serde_json::Value::String(pipe.origin.connector.clone()),
			);
			let extra = serde_json::json!({"username": creds.username, "password": creds.password});
			map.insert("extra_credentials".into(), extra);
		} else {
			map.clear();
			map.insert(
				"dsn".into(),
				serde_json::Value::String(trino_url.to_string()),
			);
			map.insert(
				"catalog".into(),
				serde_json::Value::String(pipe.origin.connector.clone()),
			);
		}
		("trino", serde_json::Value::Object(map))
	};
	registry
		.create_source(effective_connector, &pipe.name, &config)
		.await
}

/// Inject username:password into a DSN URL (postgres://user:pass@host/db).
fn inject_credentials_into_dsn(dsn: &str, username: &str, password: &str) -> String {
	if let Some(rest) = dsn.strip_prefix("postgres://") {
		if let Some(at_pos) = rest.find('@') {
			return format!(
				"postgres://{}:{}@{}",
				username,
				password,
				&rest[at_pos + 1..]
			);
		}
		return format!("postgres://{}:{}@{}", username, password, rest);
	}
	if let Some(rest) = dsn.strip_prefix("mysql://") {
		if let Some(at_pos) = rest.find('@') {
			return format!("mysql://{}:{}@{}", username, password, &rest[at_pos + 1..]);
		}
		return format!("mysql://{}:{}@{}", username, password, rest);
	}
	dsn.to_string()
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
				trino_url: None,
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
			alert_webhook: None,
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
			credentials: None,
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
			credentials: None,
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
			Box::new(steps::Upper {
				field: "name".into(),
			}),
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
			credentials: None,
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
			credentials: None,
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
			credentials: None,
		};

		let registry = PluginRegistry::new();
		let err = execute_dry_run(&req, &registry, None, None)
			.await
			.unwrap_err();
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
			credentials: None,
		};

		let registry = PluginRegistry::new();
		let err = execute_dry_run(&req, &registry, None, None)
			.await
			.unwrap_err();
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
			credentials: None,
		};

		let registry = PluginRegistry::new();
		let result = execute_dry_run(&req, &registry, None, None).await.unwrap();

		assert_eq!(result.input_rows, 25);
		assert_eq!(result.input_sample.len(), 10);
	}

	// ── Credential injection tests ──────────────────────────────

	#[test]
	fn inject_postgres_dsn() {
		let dsn = "postgres://olduser:oldpass@db.prod:5432/app";
		let result = inject_credentials_into_dsn(dsn, "ivan", "s3cret");
		assert_eq!(result, "postgres://ivan:s3cret@db.prod:5432/app");
	}

	#[test]
	fn inject_postgres_dsn_no_existing_creds() {
		let dsn = "postgres://db.prod:5432/app";
		let result = inject_credentials_into_dsn(dsn, "ivan", "pass");
		assert_eq!(result, "postgres://ivan:pass@db.prod:5432/app");
	}

	#[test]
	fn inject_mysql_dsn() {
		let dsn = "mysql://olduser:oldpass@db.prod:3306/app";
		let result = inject_credentials_into_dsn(dsn, "ivan", "pass");
		assert_eq!(result, "mysql://ivan:pass@db.prod:3306/app");
	}

	#[test]
	fn inject_unknown_dsn_unchanged() {
		let dsn = "http://api.example.com";
		let result = inject_credentials_into_dsn(dsn, "user", "pass");
		assert_eq!(result, "http://api.example.com");
	}

	#[test]
	fn transient_credentials_not_in_response() {
		// DryRunResult has no credentials field — verify at type level
		let result = DryRunResult {
			input_rows: 0,
			input_sample: vec![],
			changes: DryRunChanges {
				created: 0,
				updated: 0,
				deleted: 0,
			},
			after_transform: vec![],
			stats: DryRunStats {
				rows_fetched: 0,
				events_before_transform: 0,
				events_after_transform: 0,
				events_filtered_out: 0,
			},
		};
		let json = serde_json::to_value(&result).unwrap();
		assert!(!json.as_object().unwrap().contains_key("credentials"));
		assert!(!json.as_object().unwrap().contains_key("password"));
		assert!(!json.as_object().unwrap().contains_key("username"));
	}

	#[tokio::test]
	async fn dry_run_non_native_routes_through_trino() {
		let mut pipe = test_pipe();
		pipe.origin.connector = "mssql".into();
		pipe.origin.dsn = "mssql://host:1433/db".into();
		pipe.origin.trino_url = Some("http://my-trino:8080".into());

		let req = DryRunRequest {
			pipe,
			query_id: "q1".into(),
			mode: DryRunMode::Live,
			mock_data: vec![],
			row_limit: 10,
			transforms: vec![],
			use_existing_state: false,
			credentials: None,
		};

		let registry = PluginRegistry::new();
		// Will fail because registry has no "trino" connector registered,
		// but the error proves routing happened: "unknown source type: trino"
		let err = execute_dry_run(&req, &registry, None, None)
			.await
			.unwrap_err();
		let msg = err.to_string();
		assert!(
			msg.contains("trino"),
			"expected error about trino connector, got: {msg}"
		);
	}

	#[tokio::test]
	async fn dry_run_native_does_not_route_through_trino() {
		let mut pipe = test_pipe();
		pipe.origin.connector = "postgres".into();
		pipe.origin.dsn = "postgres://localhost/db".into();

		let req = DryRunRequest {
			pipe,
			query_id: "q1".into(),
			mode: DryRunMode::Live,
			mock_data: vec![],
			row_limit: 10,
			transforms: vec![],
			use_existing_state: false,
			credentials: None,
		};

		let registry = PluginRegistry::new();
		let err = execute_dry_run(&req, &registry, None, None)
			.await
			.unwrap_err();
		let msg = err.to_string();
		// Should fail with "unknown source type: postgres" (not trino)
		assert!(
			msg.contains("postgres"),
			"expected error about postgres connector, got: {msg}"
		);
	}
}
