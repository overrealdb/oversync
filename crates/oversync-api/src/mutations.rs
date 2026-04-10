use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use tracing::warn;

use crate::state::ApiState;
use crate::types::*;

use oversync_queries::mutations;

const SQL_DELETE_SINK: &str = mutations::DELETE_SINK;
const SQL_CREATE_SINK: &str = mutations::CREATE_SINK;
const SQL_UPDATE_SINK_TYPE: &str = mutations::UPDATE_SINK_TYPE;
const SQL_UPDATE_SINK_ENABLED: &str = mutations::UPDATE_SINK_ENABLED;
const SQL_UPDATE_SINK_CONFIG: &str = mutations::UPDATE_SINK_CONFIG;

const SQL_DELETE_PIPE: &str = mutations::DELETE_PIPE;
const SQL_DELETE_PIPE_PRESET: &str = mutations::DELETE_PIPE_PRESET;
const SQL_CREATE_PIPE: &str = mutations::CREATE_PIPE;
const SQL_CREATE_PIPE_PRESET: &str = mutations::CREATE_PIPE_PRESET;
const SQL_DELETE_PIPE_QUERIES: &str = mutations::DELETE_PIPE_QUERIES;
const SQL_CREATE_PIPE_QUERY: &str = mutations::CREATE_QUERY;
const SQL_CREATE_PIPE_QUERY_WITH_SINKS: &str = mutations::CREATE_QUERY_WITH_SINKS;
const SQL_UPDATE_PIPE_ORIGIN_CONNECTOR: &str = mutations::UPDATE_PIPE_ORIGIN_CONNECTOR;
const SQL_UPDATE_PIPE_ORIGIN_DSN: &str = mutations::UPDATE_PIPE_ORIGIN_DSN;
const SQL_UPDATE_PIPE_ORIGIN_CREDENTIAL: &str = mutations::UPDATE_PIPE_ORIGIN_CREDENTIAL;
const SQL_UPDATE_PIPE_TRINO_URL: &str = mutations::UPDATE_PIPE_TRINO_URL;
const SQL_UPDATE_PIPE_ORIGIN_CONFIG: &str = mutations::UPDATE_PIPE_ORIGIN_CONFIG;
const SQL_UPDATE_PIPE_TARGETS: &str = mutations::UPDATE_PIPE_TARGETS;
const SQL_UPDATE_PIPE_SCHEDULE: &str = mutations::UPDATE_PIPE_SCHEDULE;
const SQL_UPDATE_PIPE_DELTA: &str = mutations::UPDATE_PIPE_DELTA;
const SQL_UPDATE_PIPE_RETRY: &str = mutations::UPDATE_PIPE_RETRY;
const SQL_UPDATE_PIPE_RECIPE: &str = mutations::UPDATE_PIPE_RECIPE;
const SQL_UPDATE_PIPE_RECIPE_NONE: &str = mutations::UPDATE_PIPE_RECIPE_NONE;
const SQL_UPDATE_PIPE_PRESET: &str = mutations::UPDATE_PIPE_PRESET;
const SQL_UPDATE_PIPE_FILTERS: &str = mutations::UPDATE_PIPE_FILTERS;
const SQL_UPDATE_PIPE_TRANSFORMS: &str = mutations::UPDATE_PIPE_TRANSFORMS;
const SQL_UPDATE_PIPE_LINKS: &str = mutations::UPDATE_PIPE_LINKS;
const SQL_UPDATE_PIPE_ENABLED: &str = mutations::UPDATE_PIPE_ENABLED;

#[utoipa::path(
	post,
	path = "/sinks",
	request_body = CreateSinkRequest,
	responses(
		(status = 200, description = "Sink created", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn create_sink(
	State(state): State<Arc<ApiState>>,
	Json(req): Json<CreateSinkRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	let config_json = if req.config.is_null() {
		serde_json::json!({})
	} else {
		req.config
	};

	db.query(SQL_DELETE_SINK)
		.bind(("name", req.name.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	db.query(SQL_CREATE_SINK)
		.bind(("name", req.name.clone()))
		.bind(("sink_type", req.sink_type))
		.bind(("config", config_json))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("sink '{}' created", req.name),
	}))
}

#[utoipa::path(
	put,
	path = "/sinks/{name}",
	params(("name" = String, Path, description = "Sink name")),
	request_body = UpdateSinkRequest,
	responses(
		(status = 200, description = "Sink updated", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn update_sink(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
	Json(req): Json<UpdateSinkRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	if let Some(sink_type) = req.sink_type {
		db.query(SQL_UPDATE_SINK_TYPE)
			.bind(("name", name.clone()))
			.bind(("sink_type", sink_type))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query(SQL_UPDATE_SINK_ENABLED)
			.bind(("name", name.clone()))
			.bind(("enabled", enabled))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(config) = req.config {
		db.query(SQL_UPDATE_SINK_CONFIG)
			.bind(("name", name.clone()))
			.bind(("config", config))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("sink '{name}' updated"),
	}))
}

#[utoipa::path(
	delete,
	path = "/sinks/{name}",
	params(("name" = String, Path, description = "Sink name")),
	responses(
		(status = 200, description = "Sink deleted", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn delete_sink(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	db.query(SQL_DELETE_SINK)
		.bind(("name", name.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("sink '{name}' deleted"),
	}))
}

// ── Pipe CRUD ───────────────────────────────────────────────

#[utoipa::path(
	post,
	path = "/pipes",
	request_body = CreatePipeRequest,
	responses(
		(status = 200, description = "Pipe created", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn create_pipe(
	State(state): State<Arc<ApiState>>,
	Json(req): Json<CreatePipeRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;
	let queries = req.queries;

	let origin_config = if req.origin_config.is_null() {
		serde_json::json!({})
	} else {
		req.origin_config
	};
	let schedule = if req.schedule.is_null() {
		serde_json::json!({"interval_secs": 300, "missed_tick_policy": "skip"})
	} else {
		req.schedule
	};
	let delta = if req.delta.is_null() {
		serde_json::json!({"diff_mode": "db", "fail_safe_threshold": 30.0})
	} else {
		req.delta
	};
	let retry = if req.retry.is_null() {
		serde_json::json!({"max_retries": 3, "retry_base_delay_secs": 5})
	} else {
		req.retry
	};

	db.query(SQL_DELETE_PIPE)
		.bind(("name", req.name.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	db.query(SQL_CREATE_PIPE)
		.bind(("name", req.name.clone()))
		.bind(("origin_connector", req.origin_connector))
		.bind(("origin_dsn", req.origin_dsn))
		.bind(("origin_credential", req.origin_credential))
		.bind(("trino_url", req.trino_url))
		.bind(("origin_config", origin_config))
		.bind(("targets", req.targets))
		.bind(("schedule", schedule))
		.bind(("delta", delta))
		.bind(("retry", retry))
		.bind(("recipe", req.recipe))
		.bind(("filters", req.filters))
		.bind(("transforms", req.transforms))
		.bind(("links", req.links))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	for query in &queries {
		create_pipe_query_record(db, &req.name, query)
			.await
			.map_err(config_err)?;
	}

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("pipe '{}' created", req.name),
	}))
}

#[utoipa::path(
	put,
	path = "/pipes/{name}",
	params(("name" = String, Path, description = "Pipe name")),
	request_body = UpdatePipeRequest,
	responses(
		(status = 200, description = "Pipe updated", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn update_pipe(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
	Json(req): Json<UpdatePipeRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	if let Some(connector) = req.origin_connector {
		db.query(SQL_UPDATE_PIPE_ORIGIN_CONNECTOR)
			.bind(("name", name.clone()))
			.bind(("v", connector))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(dsn) = req.origin_dsn {
		db.query(SQL_UPDATE_PIPE_ORIGIN_DSN)
			.bind(("name", name.clone()))
			.bind(("v", dsn))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(credential) = req.origin_credential {
		db.query(SQL_UPDATE_PIPE_ORIGIN_CREDENTIAL)
			.bind(("name", name.clone()))
			.bind(("v", credential))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(trino_url) = req.trino_url {
		db.query(SQL_UPDATE_PIPE_TRINO_URL)
			.bind(("name", name.clone()))
			.bind(("v", trino_url))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(config) = req.origin_config {
		db.query(SQL_UPDATE_PIPE_ORIGIN_CONFIG)
			.bind(("name", name.clone()))
			.bind(("v", config))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(targets) = req.targets {
		db.query(SQL_UPDATE_PIPE_TARGETS)
			.bind(("name", name.clone()))
			.bind(("v", targets))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(schedule) = req.schedule {
		db.query(SQL_UPDATE_PIPE_SCHEDULE)
			.bind(("name", name.clone()))
			.bind(("v", schedule))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(delta) = req.delta {
		db.query(SQL_UPDATE_PIPE_DELTA)
			.bind(("name", name.clone()))
			.bind(("v", delta))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(retry) = req.retry {
		db.query(SQL_UPDATE_PIPE_RETRY)
			.bind(("name", name.clone()))
			.bind(("v", retry))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(recipe) = req.recipe {
		match recipe {
			Some(recipe) => {
				db.query(SQL_UPDATE_PIPE_RECIPE)
					.bind(("name", name.clone()))
					.bind(("v", recipe))
					.await
					.and_then(|response| response.check())
					.map_err(db_err)?;
			}
			None => {
				db.query(SQL_UPDATE_PIPE_RECIPE_NONE)
					.bind(("name", name.clone()))
					.await
					.and_then(|response| response.check())
					.map_err(db_err)?;
			}
		}
	}

	if let Some(filters) = req.filters {
		db.query(SQL_UPDATE_PIPE_FILTERS)
			.bind(("name", name.clone()))
			.bind(("v", filters))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(transforms) = req.transforms {
		db.query(SQL_UPDATE_PIPE_TRANSFORMS)
			.bind(("name", name.clone()))
			.bind(("v", transforms))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(links) = req.links {
		db.query(SQL_UPDATE_PIPE_LINKS)
			.bind(("name", name.clone()))
			.bind(("v", links))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query(SQL_UPDATE_PIPE_ENABLED)
			.bind(("name", name.clone()))
			.bind(("v", enabled))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;
	}

	if let Some(queries) = req.queries {
		db.query(SQL_DELETE_PIPE_QUERIES)
			.bind(("name", name.clone()))
			.await
			.and_then(|response| response.check())
			.map_err(db_err)?;

		for query in &queries {
			create_pipe_query_record(db, &name, query)
				.await
				.map_err(config_err)?;
		}
	}

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("pipe '{name}' updated"),
	}))
}

#[utoipa::path(
	delete,
	path = "/pipes/{name}",
	params(("name" = String, Path, description = "Pipe name")),
	responses(
		(status = 200, description = "Pipe deleted", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn delete_pipe(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	db.query(SQL_DELETE_PIPE)
		.bind(("name", name.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	db.query(SQL_DELETE_PIPE_QUERIES)
		.bind(("name", name.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("pipe '{name}' deleted"),
	}))
}

#[utoipa::path(
	post,
	path = "/pipe-presets",
	request_body = CreatePipePresetRequest,
	responses(
		(status = 200, description = "Pipe preset created", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn create_pipe_preset(
	State(state): State<Arc<ApiState>>,
	Json(req): Json<CreatePipePresetRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;
	let spec = storage_pipe_preset_spec_value(req.spec).map_err(json_serde_err)?;

	db.query(SQL_DELETE_PIPE_PRESET)
		.bind(("name", req.name.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	db.query(SQL_CREATE_PIPE_PRESET)
		.bind(("name", req.name.clone()))
		.bind(("description", req.description.clone()))
		.bind(("spec", spec))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	refresh_read_cache(state.as_ref())
		.await
		.map_err(config_err)?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("pipe preset '{}' created", req.name),
	}))
}

#[utoipa::path(
	put,
	path = "/pipe-presets/{name}",
	params(("name" = String, Path, description = "Pipe preset name")),
	request_body = UpdatePipePresetRequest,
	responses(
		(status = 200, description = "Pipe preset updated", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn update_pipe_preset(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
	Json(req): Json<UpdatePipePresetRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;
	let current = state
		.pipe_presets_info()
		.into_iter()
		.find(|preset| preset.name == name)
		.ok_or_else(|| {
			Json(ErrorResponse {
				error: format!("pipe preset not found: {name}"),
			})
		})?;

	let spec = match req.spec {
		Some(spec) => storage_pipe_preset_spec_value(spec).map_err(json_serde_err)?,
		None => storage_pipe_preset_spec_value(
			serde_json::from_value(current.spec).map_err(json_serde_err)?,
		)
		.map_err(json_serde_err)?,
	};

	db.query(SQL_UPDATE_PIPE_PRESET)
		.bind(("name", name.clone()))
		.bind(("description", req.description))
		.bind(("spec", spec))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	refresh_read_cache(state.as_ref())
		.await
		.map_err(config_err)?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("pipe preset '{name}' updated"),
	}))
}

#[utoipa::path(
	delete,
	path = "/pipe-presets/{name}",
	params(("name" = String, Path, description = "Pipe preset name")),
	responses(
		(status = 200, description = "Pipe preset deleted", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn delete_pipe_preset(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	db.query(SQL_DELETE_PIPE_PRESET)
		.bind(("name", name.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(db_err)?;

	refresh_read_cache(state.as_ref())
		.await
		.map_err(config_err)?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("pipe preset '{name}' deleted"),
	}))
}

fn require_db(
	state: &ApiState,
) -> Result<&surrealdb::Surreal<surrealdb::engine::any::Any>, Json<ErrorResponse>> {
	state.db_client.as_deref().ok_or_else(|| {
		Json(ErrorResponse {
			error: "database not configured".into(),
		})
	})
}

fn db_err(e: surrealdb::Error) -> Json<ErrorResponse> {
	Json(ErrorResponse {
		error: format!("db: {e}"),
	})
}

fn config_err(e: oversync_core::error::OversyncError) -> Json<ErrorResponse> {
	Json(ErrorResponse {
		error: e.to_string(),
	})
}

fn json_serde_err(e: serde_json::Error) -> Json<ErrorResponse> {
	Json(ErrorResponse {
		error: format!("json: {e}"),
	})
}

fn storage_pipe_preset_spec_value(
	spec: PipePresetSpecInput,
) -> Result<serde_json::Value, serde_json::Error> {
	let origin_config = if spec.origin_config.is_null() {
		serde_json::json!({})
	} else {
		spec.origin_config
	};
	let schedule = if spec.schedule.is_null() {
		serde_json::json!({})
	} else {
		spec.schedule
	};
	let delta = if spec.delta.is_null() {
		serde_json::json!({})
	} else {
		spec.delta
	};
	let retry = if spec.retry.is_null() {
		serde_json::json!({})
	} else {
		spec.retry
	};

	serde_json::to_value(serde_json::json!({
		"origin": {
			"connector": spec.origin_connector,
			"dsn": spec.origin_dsn,
			"credential": spec.origin_credential,
			"trino_url": spec.trino_url,
			"config": origin_config,
		},
		"parameters": spec.parameters,
		"targets": spec.targets,
		"queries": spec.queries,
		"schedule": schedule,
		"delta": delta,
		"retry": retry,
		"recipe": spec.recipe,
		"filters": spec.filters,
		"transforms": spec.transforms,
		"links": spec.links,
	}))
}

fn api_pipe_preset_spec_value(spec: &serde_json::Value) -> Option<serde_json::Value> {
	if spec.get("origin_connector").is_some() {
		return Some(serde_json::json!({
			"origin_connector": spec.get("origin_connector").cloned().unwrap_or(serde_json::Value::Null),
			"origin_dsn": spec.get("origin_dsn").cloned().unwrap_or(serde_json::Value::Null),
			"origin_credential": spec.get("origin_credential").cloned().unwrap_or(serde_json::Value::Null),
			"trino_url": spec.get("trino_url").cloned().unwrap_or(serde_json::Value::Null),
			"origin_config": spec.get("origin_config").cloned().unwrap_or_else(|| serde_json::json!({})),
			"parameters": spec.get("parameters").cloned().unwrap_or_else(|| serde_json::json!([])),
			"targets": spec.get("targets").cloned().unwrap_or_else(|| serde_json::json!([])),
			"queries": spec.get("queries").cloned().unwrap_or_else(|| serde_json::json!([])),
			"schedule": spec.get("schedule").cloned().unwrap_or_else(|| serde_json::json!({})),
			"delta": spec.get("delta").cloned().unwrap_or_else(|| serde_json::json!({})),
			"retry": spec.get("retry").cloned().unwrap_or_else(|| serde_json::json!({})),
			"recipe": spec.get("recipe").cloned().unwrap_or(serde_json::Value::Null),
			"filters": spec.get("filters").cloned().unwrap_or_else(|| serde_json::json!([])),
			"transforms": spec.get("transforms").cloned().unwrap_or_else(|| serde_json::json!([])),
			"links": spec.get("links").cloned().unwrap_or_else(|| serde_json::json!([])),
		}));
	}

	let origin = spec.get("origin")?;
	Some(serde_json::json!({
		"origin_connector": origin.get("connector").cloned().unwrap_or(serde_json::Value::Null),
		"origin_dsn": origin.get("dsn").cloned().unwrap_or(serde_json::Value::Null),
		"origin_credential": origin.get("credential").cloned().unwrap_or(serde_json::Value::Null),
		"trino_url": origin.get("trino_url").cloned().unwrap_or(serde_json::Value::Null),
		"origin_config": match origin.get("config") {
			Some(value) if !value.is_null() => value.clone(),
			_ => serde_json::json!({}),
		},
		"parameters": spec.get("parameters").cloned().unwrap_or_else(|| serde_json::json!([])),
		"targets": spec.get("targets").cloned().unwrap_or_else(|| serde_json::json!([])),
		"queries": spec.get("queries").cloned().unwrap_or_else(|| serde_json::json!([])),
		"schedule": match spec.get("schedule") {
			Some(value) if !value.is_null() => value.clone(),
			_ => serde_json::json!({}),
		},
		"delta": match spec.get("delta") {
			Some(value) if !value.is_null() => value.clone(),
			_ => serde_json::json!({}),
		},
		"retry": match spec.get("retry") {
			Some(value) if !value.is_null() => value.clone(),
			_ => serde_json::json!({}),
		},
		"recipe": spec.get("recipe").cloned().unwrap_or(serde_json::Value::Null),
		"filters": spec.get("filters").cloned().unwrap_or_else(|| serde_json::json!([])),
		"transforms": spec.get("transforms").cloned().unwrap_or_else(|| serde_json::json!([])),
		"links": spec.get("links").cloned().unwrap_or_else(|| serde_json::json!([])),
	}))
}

async fn create_pipe_query_record(
	db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	origin_id: &str,
	query: &PipeQueryInput,
) -> Result<(), oversync_core::error::OversyncError> {
	let sql = if query.sinks.is_some() {
		SQL_CREATE_PIPE_QUERY_WITH_SINKS
	} else {
		SQL_CREATE_PIPE_QUERY
	};

	db.query(sql)
		.bind(("source", origin_id.to_string()))
		.bind(("name", query.id.clone()))
		.bind(("query", query.sql.clone()))
		.bind(("key_column", query.key_column.clone()))
		.bind(("sinks", query.sinks.clone()))
		.bind(("transform", query.transform.clone()))
		.await
		.and_then(|response| response.check())
		.map_err(|e| {
			oversync_core::error::OversyncError::SurrealDb(format!(
				"create query '{}' for pipe '{}': {e}",
				query.id, origin_id
			))
		})?;

	Ok(())
}

pub(crate) async fn reload_config(state: &ApiState) -> Result<(), Json<ErrorResponse>> {
	if let (Some(lifecycle), Some(db)) = (&state.lifecycle, &state.db_client) {
		lifecycle.restart_with_config_json(db).await.map_err(|e| {
			Json(ErrorResponse {
				error: format!("reload: {e}"),
			})
		})?;
	}
	refresh_read_cache(state).await.map_err(|e| {
		Json(ErrorResponse {
			error: format!("refresh cache: {e}"),
		})
	})?;
	Ok(())
}

pub async fn refresh_read_cache(
	state: &ApiState,
) -> Result<(), oversync_core::error::OversyncError> {
	let Some(db) = &state.db_client else {
		return Ok(());
	};

	const SQL_LOAD_QUERIES: &str = oversync_queries::config::LOAD_QUERIES;
	const SQL_READ_SINKS_CACHE: &str = oversync_queries::config::READ_SINKS_CACHE;
	const SQL_READ_PIPES_CACHE: &str = oversync_queries::config::READ_PIPES_CACHE;
	const SQL_READ_PIPE_PRESETS_CACHE: &str = oversync_queries::config::READ_PIPE_PRESETS_CACHE;

	let query_rows =
		read_cache_rows_or_empty(db, SQL_LOAD_QUERIES, "refresh queries cache").await?;
	let mut query_counts_by_origin: std::collections::HashMap<String, usize> =
		std::collections::HashMap::new();
	for row in &query_rows {
		let Some(origin_id) = row.get("origin_id").and_then(|v| v.as_str()) else {
			continue;
		};
		*query_counts_by_origin
			.entry(origin_id.to_string())
			.or_default() += 1;
	}

	let sink_rows =
		read_cache_rows_or_empty(db, SQL_READ_SINKS_CACHE, "refresh sinks cache").await?;
	let mut sink_configs: Vec<crate::state::SinkConfig> = sink_rows
		.iter()
		.filter_map(|r| {
			Some(crate::state::SinkConfig {
				name: r.get("name")?.as_str()?.to_string(),
				sink_type: r.get("sink_type")?.as_str()?.to_string(),
				config: r.get("config").cloned(),
			})
		})
		.collect();

	let pipe_rows =
		read_cache_rows_or_empty(db, SQL_READ_PIPES_CACHE, "refresh pipes cache").await?;
	let mut pipe_configs: Vec<crate::state::PipeConfigCache> = pipe_rows
		.iter()
		.filter_map(|r| {
			let name = r.get("name")?.as_str()?.to_string();
			Some(crate::state::PipeConfigCache {
				name: name.clone(),
				origin_connector: r.get("origin_connector")?.as_str()?.to_string(),
				origin_dsn: r.get("origin_dsn")?.as_str()?.to_string(),
				targets: r
					.get("targets")
					.and_then(|v| v.as_array())
					.map(|arr| {
						arr.iter()
							.filter_map(|v| v.as_str().map(String::from))
							.collect()
					})
					.unwrap_or_default(),
				interval_secs: r
					.get("interval_secs")
					.and_then(|v| v.as_u64())
					.unwrap_or(300),
				query_count: *query_counts_by_origin.get(name.as_str()).unwrap_or(&0),
				recipe: r.get("recipe").cloned().filter(|v| !v.is_null()),
				enabled: r.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
			})
		})
		.collect();

	if let Some(lifecycle) = &state.lifecycle {
		match lifecycle.runtime_cache_snapshot().await {
			Ok(runtime) => merge_runtime_cache(&mut sink_configs, &mut pipe_configs, runtime),
			Err(error) => {
				warn!(error = %error, "failed to load runtime cache snapshot for API read model");
			}
		}
	}

	*state.sinks.write().await = sink_configs;
	*state.pipes.write().await = pipe_configs;

	let pipe_preset_rows = read_cache_rows_or_empty(
		db,
		SQL_READ_PIPE_PRESETS_CACHE,
		"refresh pipe presets cache",
	)
	.await?;
	let pipe_preset_configs: Vec<crate::state::PipePresetCache> = pipe_preset_rows
		.iter()
		.filter_map(|r| {
			Some(crate::state::PipePresetCache {
				name: r.get("name")?.as_str()?.to_string(),
				description: r
					.get("description")
					.and_then(|v| v.as_str())
					.map(String::from),
				spec: api_pipe_preset_spec_value(r.get("spec")?)?,
			})
		})
		.collect();
	*state.pipe_presets.write().await = pipe_preset_configs;

	Ok(())
}

fn merge_runtime_cache(
	sinks: &mut Vec<crate::state::SinkConfig>,
	pipes: &mut Vec<crate::state::PipeConfigCache>,
	runtime: crate::state::RuntimeCacheSnapshot,
) {
	for runtime_sink in runtime.sinks {
		match sinks.iter_mut().find(|sink| sink.name == runtime_sink.name) {
			Some(existing) => {
				if existing.config.is_none() && runtime_sink.config.is_some() {
					existing.config = runtime_sink.config;
				}
				if existing.sink_type.is_empty() {
					existing.sink_type = runtime_sink.sink_type;
				}
			}
			None => sinks.push(runtime_sink),
		}
	}

	for runtime_pipe in runtime.pipes {
		match pipes.iter_mut().find(|pipe| pipe.name == runtime_pipe.name) {
			Some(existing) => {
				if existing.query_count == 0 && runtime_pipe.query_count > 0 {
					existing.query_count = runtime_pipe.query_count;
				}
				if existing.recipe.is_none() && runtime_pipe.recipe.is_some() {
					existing.recipe = runtime_pipe.recipe;
				}
				if existing.targets.is_empty() && !runtime_pipe.targets.is_empty() {
					existing.targets = runtime_pipe.targets;
				}
				if existing.origin_connector.is_empty() {
					existing.origin_connector = runtime_pipe.origin_connector;
				}
				if existing.origin_dsn.is_empty() {
					existing.origin_dsn = runtime_pipe.origin_dsn;
				}
			}
			None => pipes.push(runtime_pipe),
		}
	}
}

fn is_missing_table_error(error: &dyn std::fmt::Display) -> bool {
	let message = error.to_string();
	message.contains("does not exist") && message.contains("table")
}

async fn read_cache_rows_or_empty(
	db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	sql: &str,
	context: &str,
) -> Result<Vec<serde_json::Value>, oversync_core::error::OversyncError> {
	let mut response = match db.query(sql).await {
		Ok(response) => response,
		Err(error) if is_missing_table_error(&error) => return Ok(Vec::new()),
		Err(error) => {
			return Err(oversync_core::error::OversyncError::SurrealDb(format!(
				"{context}: {error}"
			)));
		}
	};

	match response.take(0) {
		Ok(rows) => Ok(rows),
		Err(error) if is_missing_table_error(&error) => Ok(Vec::new()),
		Err(error) => Err(oversync_core::error::OversyncError::SurrealDb(format!(
			"{context} take: {error}"
		))),
	}
}

#[cfg(test)]
mod tests {
	use super::{api_pipe_preset_spec_value, is_missing_table_error};

	#[test]
	fn api_pipe_preset_spec_value_accepts_legacy_flat_shape() {
		let flat = serde_json::json!({
			"origin_connector": "postgres",
			"origin_dsn": "postgres://localhost/db",
			"origin_credential": null,
			"trino_url": null,
			"origin_config": { "sslmode": "require" },
			"targets": ["stdout"],
			"queries": [{
				"id": "aspect-columns",
				"sql": "SELECT id, payload FROM columns",
				"key_column": "id"
			}],
			"schedule": { "interval_secs": 900, "missed_tick_policy": "skip" },
			"delta": { "diff_mode": "db", "fail_safe_threshold": 30 },
			"retry": { "max_retries": 3, "retry_base_delay_secs": 5 },
			"recipe": null,
			"filters": [],
			"transforms": [],
			"links": []
		});

		let api = api_pipe_preset_spec_value(&flat).expect("legacy flat preset should map");
		assert_eq!(api["origin_connector"], "postgres");
		assert_eq!(api["origin_config"]["sslmode"], "require");
		assert_eq!(api["queries"][0]["id"], "aspect-columns");
	}

	#[test]
	fn missing_table_error_detection_matches_surreal_message_shape() {
		assert!(is_missing_table_error(
			&"The table 'pipe_config' does not exist"
		));
		assert!(!is_missing_table_error(&"permission denied"));
	}
}
