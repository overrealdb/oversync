use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::warn;

use oversync_core::error::OversyncError;

use crate::config::{
	DeltaDef, DiffMode, LinkDef, MissedTickPolicy, OriginDef, PipeConfig, PipePresetDef,
	PipePresetParameterDef, PipePresetSpec, PipeRecipeDef, QueryDef, RetryDef, ScheduleDef,
	SinkDef, SurrealDbDef, SyncConfig,
};

const LOAD_QUERIES_SQL: &str = oversync_queries::config::LOAD_QUERIES;
const LOAD_SINKS_SQL: &str = oversync_queries::config::LOAD_SINKS;
const LOAD_PIPES_SQL: &str = oversync_queries::config::LOAD_PIPES;
const LOAD_PIPE_PRESETS_SQL: &str = oversync_queries::config::LOAD_PIPE_PRESETS;

pub async fn load_config_from_db(
	client: &Surreal<Any>,
	surreal_def: &SurrealDbDef,
) -> Result<SyncConfig, OversyncError> {
	let sinks = load_sinks(client).await?;
	let pipes = load_pipes(client).await?;
	let pipe_presets = load_pipe_presets(client).await?;
	Ok(SyncConfig {
		surrealdb: surreal_def.clone(),
		sinks,
		pipes,
		pipe_presets,
	})
}

pub async fn replace_config_in_db(
	client: &Surreal<Any>,
	config: &SyncConfig,
) -> Result<(), OversyncError> {
	client
		.query(
			"DELETE query_config; DELETE pipe_preset_config; DELETE pipe_config; DELETE sink_config;",
		)
		.await
		.and_then(|response| response.check())
		.map_err(|e| OversyncError::SurrealDb(format!("clear config tables: {e}")))?;

	for sink in &config.sinks {
		let sink_config = if sink.config.is_null() {
			serde_json::json!({})
		} else {
			sink.config.clone()
		};
		client
			.query(oversync_queries::mutations::CREATE_SINK)
			.bind(("name", sink.name.clone()))
			.bind(("sink_type", sink.sink_type.clone()))
			.bind(("config", sink_config))
			.await
			.and_then(|response| response.check())
			.map_err(|e| OversyncError::SurrealDb(format!("create sink '{}': {e}", sink.name)))?;
	}

	for pipe in &config.pipes {
		let origin_config = if pipe.origin.config.is_null() {
			serde_json::json!({})
		} else {
			pipe.origin.config.clone()
		};
		let recipe = pipe
			.recipe
			.as_ref()
			.map(serde_json::to_value)
			.transpose()
			.map_err(|e| {
				OversyncError::Config(format!("serialize recipe for pipe '{}': {e}", pipe.name))
			})?;
		client
			.query(oversync_queries::mutations::CREATE_PIPE)
			.bind(("name", pipe.name.clone()))
			.bind(("origin_connector", pipe.origin.connector.clone()))
			.bind(("origin_dsn", pipe.origin.dsn.clone()))
			.bind(("origin_credential", pipe.origin.credential.clone()))
			.bind(("trino_url", pipe.origin.trino_url.clone()))
			.bind(("origin_config", origin_config))
			.bind(("targets", serde_json::json!(pipe.targets)))
			.bind((
				"schedule",
				serde_json::to_value(&pipe.schedule).unwrap_or(serde_json::json!({})),
			))
			.bind((
				"delta",
				serde_json::to_value(&pipe.delta).unwrap_or(serde_json::json!({})),
			))
			.bind((
				"retry",
				serde_json::to_value(&pipe.retry).unwrap_or(serde_json::json!({})),
			))
			.bind(("recipe", recipe))
			.bind(("filters", serde_json::json!(pipe.filters)))
			.bind(("transforms", serde_json::json!(pipe.transforms)))
			.bind((
				"links",
				serde_json::to_value(&pipe.links).unwrap_or(serde_json::json!([])),
			))
			.await
			.and_then(|response| response.check())
			.map_err(|e| OversyncError::SurrealDb(format!("create pipe '{}': {e}", pipe.name)))?;

		if !pipe.enabled {
			client
				.query(oversync_queries::mutations::UPDATE_PIPE_ENABLED)
				.bind(("name", pipe.name.clone()))
				.bind(("v", false))
				.await
				.and_then(|response| response.check())
				.map_err(|e| {
					OversyncError::SurrealDb(format!("disable pipe '{}': {e}", pipe.name))
				})?;
		}

		for query in &pipe.queries {
			create_query_record(client, &pipe.name, query).await?;
		}
	}

	for preset in &config.pipe_presets {
		create_pipe_preset_record(client, preset).await?;
	}

	Ok(())
}

async fn load_pipe_presets(client: &Surreal<Any>) -> Result<Vec<PipePresetDef>, OversyncError> {
	let rows = load_rows_or_empty(client, LOAD_PIPE_PRESETS_SQL, "load_pipe_presets").await?;

	let mut presets = Vec::with_capacity(rows.len());
	for row in &rows {
		let name = str_field(row, "name")?;
		let spec = row.get("spec").cloned().unwrap_or(serde_json::Value::Null);
		let spec = parse_pipe_preset_spec(&name, spec)?;
		presets.push(PipePresetDef {
			name,
			description: row
				.get("description")
				.and_then(|v| v.as_str())
				.map(String::from),
			spec,
		});
	}

	Ok(presets)
}

fn parse_pipe_preset_spec(
	name: &str,
	spec: serde_json::Value,
) -> Result<PipePresetSpec, OversyncError> {
	if spec.get("origin").is_some() {
		return serde_json::from_value(spec).map_err(|e| {
			OversyncError::SurrealDb(format!("invalid spec for pipe preset '{name}': {e}"))
		});
	}

	let origin = OriginDef {
		connector: json_string_or_default(&spec, "origin_connector"),
		dsn: json_string_or_default(&spec, "origin_dsn"),
		credential: json_optional_string(&spec, "origin_credential"),
		trino_url: json_optional_string(&spec, "trino_url"),
		config: spec
			.get("origin_config")
			.cloned()
			.unwrap_or_else(|| serde_json::json!({})),
	};

	let queries = parse_json_value::<Vec<QueryDef>>(
		spec.get("queries")
			.cloned()
			.unwrap_or_else(|| serde_json::json!([])),
		name,
		"queries",
	)?;
	let parameters = parse_json_value::<Vec<PipePresetParameterDef>>(
		spec.get("parameters")
			.cloned()
			.unwrap_or_else(|| serde_json::json!([])),
		name,
		"parameters",
	)?;
	let schedule = parse_json_value::<ScheduleDef>(
		spec.get("schedule")
			.cloned()
			.unwrap_or_else(|| serde_json::json!({})),
		name,
		"schedule",
	)?;
	let delta = parse_json_value::<DeltaDef>(
		spec.get("delta")
			.cloned()
			.unwrap_or_else(|| serde_json::json!({})),
		name,
		"delta",
	)?;
	let retry = parse_json_value::<RetryDef>(
		spec.get("retry")
			.cloned()
			.unwrap_or_else(|| serde_json::json!({})),
		name,
		"retry",
	)?;
	let recipe = match spec.get("recipe").cloned() {
		Some(value) if !value.is_null() => {
			Some(parse_json_value::<PipeRecipeDef>(value, name, "recipe")?)
		}
		_ => None,
	};
	let links = parse_json_value::<Vec<LinkDef>>(
		spec.get("links")
			.cloned()
			.unwrap_or_else(|| serde_json::json!([])),
		name,
		"links",
	)?;

	Ok(PipePresetSpec {
		origin,
		parameters,
		targets: spec
			.get("targets")
			.and_then(|value| value.as_array())
			.map(|values| {
				values
					.iter()
					.filter_map(|value| value.as_str().map(String::from))
					.collect()
			})
			.unwrap_or_default(),
		queries,
		schedule,
		delta,
		retry,
		recipe,
		filters: json_array_field(&spec, "filters"),
		transforms: json_array_field(&spec, "transforms"),
		links,
	})
}

fn parse_json_value<T>(
	value: serde_json::Value,
	name: &str,
	field: &str,
) -> Result<T, OversyncError>
where
	T: serde::de::DeserializeOwned,
{
	serde_json::from_value(value).map_err(|e| {
		OversyncError::SurrealDb(format!("invalid {field} for pipe preset '{name}': {e}"))
	})
}

fn json_string_or_default(row: &serde_json::Value, field: &str) -> String {
	row.get(field)
		.and_then(|value| value.as_str())
		.unwrap_or_default()
		.to_string()
}

fn json_optional_string(row: &serde_json::Value, field: &str) -> Option<String> {
	row.get(field)
		.and_then(|value| value.as_str())
		.map(String::from)
}

fn is_missing_table_error(error: &dyn std::fmt::Display) -> bool {
	let message = error.to_string();
	message.contains("does not exist") && message.contains("table")
}

async fn load_rows_or_empty(
	client: &Surreal<Any>,
	sql: &str,
	context: &str,
) -> Result<Vec<serde_json::Value>, OversyncError> {
	let mut response = match client.query(sql).await {
		Ok(response) => response,
		Err(error) if is_missing_table_error(&error) => return Ok(Vec::new()),
		Err(error) => return Err(OversyncError::SurrealDb(format!("{context}: {error}"))),
	};

	match response.take(0) {
		Ok(rows) => Ok(rows),
		Err(error) if is_missing_table_error(&error) => Ok(Vec::new()),
		Err(error) => Err(OversyncError::SurrealDb(format!("{context} take: {error}"))),
	}
}

async fn load_pipes(client: &Surreal<Any>) -> Result<Vec<PipeConfig>, OversyncError> {
	let rows = load_rows_or_empty(client, LOAD_PIPES_SQL, "load_pipes").await?;
	let query_rows = load_rows_or_empty(client, LOAD_QUERIES_SQL, "load_queries for pipes").await?;

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

		let delta = row.get("delta").cloned().unwrap_or(serde_json::Value::Null);

		let retry = row.get("retry").cloned().unwrap_or(serde_json::Value::Null);
		let recipe = parse_recipe(row, &name)?;
		let filters = json_array_field(row, "filters");
		let transforms = json_array_field(row, "transforms");
		let links = parse_links(row, &name)?;

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
			let sinks = q.get("sinks").and_then(|v| v.as_array()).map(|arr| {
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

		let enabled = row.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);

		pipes.push(PipeConfig {
			name,
			origin: OriginDef {
				connector: str_field(row, "origin_connector")?,
				dsn: str_field(row, "origin_dsn")?,
				credential: row
					.get("origin_credential")
					.and_then(|v| v.as_str())
					.map(String::from),
				trino_url: row
					.get("trino_url")
					.and_then(|v| v.as_str())
					.map(String::from),
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
				max_requests_per_minute: schedule
					.get("max_requests_per_minute")
					.and_then(|v| v.as_u64())
					.map(|v| v as u32),
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
			recipe,
			filters,
			transforms,
			links,
			alert_webhook: None,
			enabled,
		});
	}

	Ok(pipes)
}

fn parse_recipe(
	row: &serde_json::Value,
	pipe_name: &str,
) -> Result<Option<PipeRecipeDef>, OversyncError> {
	match row.get("recipe") {
		None | Some(serde_json::Value::Null) => Ok(None),
		Some(recipe) => serde_json::from_value(recipe.clone())
			.map(Some)
			.map_err(|e| {
				OversyncError::SurrealDb(format!("invalid recipe for pipe '{pipe_name}': {e}"))
			}),
	}
}

async fn create_query_record(
	client: &Surreal<Any>,
	origin_id: &str,
	query: &QueryDef,
) -> Result<(), OversyncError> {
	let sql = if query.sinks.is_some() {
		oversync_queries::mutations::CREATE_QUERY_WITH_SINKS
	} else {
		oversync_queries::mutations::CREATE_QUERY
	};

	client
		.query(sql)
		.bind(("source", origin_id.to_string()))
		.bind(("name", query.id.clone()))
		.bind(("query", query.sql.clone()))
		.bind(("key_column", query.key_column.clone()))
		.bind(("sinks", query.sinks.clone()))
		.bind(("transform", query.transform.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(|e| {
			OversyncError::SurrealDb(format!(
				"create query '{}' for origin '{}': {e}",
				query.id, origin_id
			))
		})?;

	Ok(())
}

async fn create_pipe_preset_record(
	client: &Surreal<Any>,
	preset: &PipePresetDef,
) -> Result<(), OversyncError> {
	client
		.query(oversync_queries::mutations::CREATE_PIPE_PRESET)
		.bind(("name", preset.name.clone()))
		.bind(("description", preset.description.clone()))
		.bind((
			"spec",
			serde_json::to_value(&preset.spec).map_err(|e| {
				OversyncError::Config(format!(
					"serialize spec for pipe preset '{}': {e}",
					preset.name
				))
			})?,
		))
		.await
		.and_then(|response| response.check())
		.map_err(|e| {
			OversyncError::SurrealDb(format!("create pipe preset '{}': {e}", preset.name))
		})?;

	Ok(())
}

async fn load_sinks(client: &Surreal<Any>) -> Result<Vec<SinkDef>, OversyncError> {
	let rows = load_rows_or_empty(client, LOAD_SINKS_SQL, "load_sinks").await?;

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

fn json_array_field(row: &serde_json::Value, field: &str) -> Vec<serde_json::Value> {
	row.get(field)
		.and_then(|v| v.as_array())
		.cloned()
		.unwrap_or_default()
}

fn parse_links(row: &serde_json::Value, pipe_name: &str) -> Result<Vec<LinkDef>, OversyncError> {
	match row.get("links") {
		None | Some(serde_json::Value::Null) => Ok(vec![]),
		Some(value) => serde_json::from_value(value.clone()).map_err(|e| {
			OversyncError::SurrealDb(format!("invalid links for pipe '{pipe_name}': {e}"))
		}),
	}
}
