use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::warn;

use oversync_core::error::OversyncError;

use crate::config::{
	DeltaDef, DiffMode, MissedTickPolicy, OriginDef, PipeConfig, QueryDef, RetryDef, ScheduleDef,
	SinkDef, SourceDef, SurrealDbDef, SyncConfig,
};

const LOAD_SOURCES_SQL: &str = include_str!("../surql/queries/config/load_sources.surql");
const LOAD_QUERIES_SQL: &str = include_str!("../surql/queries/config/load_queries.surql");
const LOAD_SINKS_SQL: &str = include_str!("../surql/queries/config/load_sinks.surql");
const LOAD_PIPES_SQL: &str = include_str!("../surql/queries/config/load_pipes.surql");

pub async fn load_config_from_db(
	client: &Surreal<Any>,
	surreal_def: &SurrealDbDef,
) -> Result<SyncConfig, OversyncError> {
	let sources = load_sources(client).await?;
	let sinks = load_sinks(client).await?;
	let pipes = load_pipes(client).await?;
	Ok(SyncConfig {
		surrealdb: surreal_def.clone(),
		sources,
		sinks,
		pipes,
	})
}

async fn load_pipes(client: &Surreal<Any>) -> Result<Vec<PipeConfig>, OversyncError> {
	let mut response = client
		.query(LOAD_PIPES_SQL)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("load_pipes: {e}")))?;

	let rows: Vec<serde_json::Value> = response
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("load_pipes take: {e}")))?;

	let mut queries_response = client
		.query(LOAD_QUERIES_SQL)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("load_queries for pipes: {e}")))?;

	let query_rows: Vec<serde_json::Value> = queries_response
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("load_queries take: {e}")))?;

	let mut pipes = Vec::with_capacity(rows.len());
	for row in &rows {
		let name = str_field(row, "name")?;

		let origin_config = row
			.get("origin_config")
			.cloned()
			.unwrap_or(serde_json::Value::Null);

		let schedule = row
			.get("schedule")
			.cloned()
			.unwrap_or(serde_json::Value::Null);

		let delta = row
			.get("delta")
			.cloned()
			.unwrap_or(serde_json::Value::Null);

		let retry = row
			.get("retry")
			.cloned()
			.unwrap_or(serde_json::Value::Null);

		let targets = row
			.get("targets")
			.and_then(|v| v.as_array())
			.map(|arr| {
				arr.iter()
					.filter_map(|v| v.as_str().map(String::from))
					.collect()
			})
			.unwrap_or_default();

		let mut queries = Vec::new();
		for q in query_rows
			.iter()
			.filter(|q| q.get("origin_id").and_then(|v| v.as_str()) == Some(&name))
		{
			let sinks = q
				.get("sinks")
				.and_then(|v| v.as_array())
				.map(|arr| {
					arr.iter()
						.filter_map(|v| v.as_str().map(String::from))
						.collect()
				});
			queries.push(QueryDef {
				id: str_field(q, "name")?,
				sql: str_field(q, "query")?,
				key_column: str_field(q, "key_column")?,
				sinks,
				transform: q
					.get("transform")
					.and_then(|v| v.as_str())
					.map(String::from),
			});
		}

		let diff_mode_str = delta
			.get("diff_mode")
			.and_then(|v| v.as_str())
			.unwrap_or("db");
		let diff_mode = match diff_mode_str {
			"db" => DiffMode::Db,
			"memory" => DiffMode::Memory,
			other => {
				warn!(pipe = %name, diff_mode = %other, "unknown diff_mode, defaulting to 'db'");
				DiffMode::Db
			}
		};

		let missed_tick_str = schedule
			.get("missed_tick_policy")
			.and_then(|v| v.as_str())
			.unwrap_or("skip");
		let missed_tick_policy = match missed_tick_str {
			"skip" => MissedTickPolicy::Skip,
			"burst" => MissedTickPolicy::Burst,
			other => {
				warn!(pipe = %name, policy = %other, "unknown missed_tick_policy, defaulting to 'skip'");
				MissedTickPolicy::Skip
			}
		};

		let enabled = row
			.get("enabled")
			.and_then(|v| v.as_bool())
			.unwrap_or(true);

		pipes.push(PipeConfig {
			name,
			origin: OriginDef {
				connector: str_field(row, "origin_connector")?,
				dsn: str_field(row, "origin_dsn")?,
				config: origin_config,
			},
			targets,
			queries,
			schedule: ScheduleDef {
				interval_secs: schedule
					.get("interval_secs")
					.and_then(|v| v.as_u64())
					.unwrap_or(300),
				missed_tick_policy,
			},
			delta: DeltaDef {
				diff_mode,
				fail_safe_threshold: delta
					.get("fail_safe_threshold")
					.and_then(|v| v.as_f64())
					.unwrap_or(30.0),
			},
			retry: RetryDef {
				max_retries: retry
					.get("max_retries")
					.and_then(|v| v.as_u64())
					.unwrap_or(3) as u32,
				retry_base_delay_secs: retry
					.get("retry_base_delay_secs")
					.and_then(|v| v.as_u64())
					.unwrap_or(5),
			},
			enabled,
		});
	}

	Ok(pipes)
}

async fn load_sources(client: &Surreal<Any>) -> Result<Vec<SourceDef>, OversyncError> {
	let mut response = client
		.query(LOAD_SOURCES_SQL)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("load_sources: {e}")))?;

	let rows: Vec<serde_json::Value> = response
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("load_sources take: {e}")))?;

	let mut queries_response = client
		.query(LOAD_QUERIES_SQL)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("load_queries: {e}")))?;

	let query_rows: Vec<serde_json::Value> = queries_response
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("load_queries take: {e}")))?;

	let mut sources = Vec::with_capacity(rows.len());
	for row in &rows {
		let name = str_field(row, "name")?;
		let connector = str_field(row, "connector")?;
		let config = row
			.get("config")
			.cloned()
			.unwrap_or(serde_json::Value::Null);

		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.unwrap_or_default()
			.to_string();

		let interval_secs = config
			.get("interval_secs")
			.and_then(|v| v.as_u64())
			.unwrap_or(300);

		let mut queries = Vec::new();
		for q in query_rows
			.iter()
			.filter(|q| q.get("origin_id").and_then(|v| v.as_str()) == Some(&name))
		{
			let sinks = q
				.get("sinks")
				.and_then(|v| v.as_array())
				.map(|arr| {
					arr.iter()
						.filter_map(|v| v.as_str().map(String::from))
						.collect()
				});
			queries.push(QueryDef {
				id: str_field(q, "name")?,
				sql: str_field(q, "query")?,
				key_column: str_field(q, "key_column")?,
				sinks,
				transform: q
					.get("transform")
					.and_then(|v| v.as_str())
					.map(String::from),
			});
		}

		sources.push(SourceDef {
			name,
			connector,
			dsn,
			interval_secs,
			fail_safe_threshold: config
				.get("fail_safe_threshold")
				.and_then(|v| v.as_f64())
				.unwrap_or(30.0),
			max_retries: config
				.get("max_retries")
				.and_then(|v| v.as_u64())
				.unwrap_or(3) as u32,
			retry_base_delay_secs: config
				.get("retry_base_delay_secs")
				.and_then(|v| v.as_u64())
				.unwrap_or(5),
			diff_mode: DiffMode::default(),
			missed_tick_policy: Default::default(),
			config,
			queries,
		});
	}

	Ok(sources)
}

async fn load_sinks(client: &Surreal<Any>) -> Result<Vec<SinkDef>, OversyncError> {
	let mut response = client
		.query(LOAD_SINKS_SQL)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("load_sinks: {e}")))?;

	let rows: Vec<serde_json::Value> = response
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("load_sinks take: {e}")))?;

	let mut sinks = Vec::with_capacity(rows.len());
	for row in &rows {
		sinks.push(SinkDef {
			name: str_field(row, "name")?,
			sink_type: str_field(row, "sink_type")?,
			config: row
				.get("config")
				.cloned()
				.unwrap_or(serde_json::Value::Null),
		});
	}

	Ok(sinks)
}

fn str_field(row: &serde_json::Value, field: &str) -> Result<String, OversyncError> {
	row.get(field)
		.and_then(|v| v.as_str())
		.map(String::from)
		.ok_or_else(|| {
			OversyncError::SurrealDb(format!("missing or invalid field '{field}' in config row"))
		})
}
