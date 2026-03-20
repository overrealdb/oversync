use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use oversync_core::error::OversyncError;

use crate::config::{DiffMode, QueryDef, SinkDef, SourceDef, SurrealDbDef, SyncConfig};

const LOAD_SOURCES_SQL: &str = include_str!("../surql/queries/config/load_sources.surql");
const LOAD_QUERIES_SQL: &str = include_str!("../surql/queries/config/load_queries.surql");
const LOAD_SINKS_SQL: &str = include_str!("../surql/queries/config/load_sinks.surql");

pub async fn load_config_from_db(
	client: &Surreal<Any>,
	surreal_def: &SurrealDbDef,
) -> Result<SyncConfig, OversyncError> {
	let sources = load_sources(client).await?;
	let sinks = load_sinks(client).await?;
	Ok(SyncConfig {
		surrealdb: surreal_def.clone(),
		sources,
		sinks,
	})
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
