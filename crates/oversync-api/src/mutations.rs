use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};

use crate::state::ApiState;
use crate::types::*;

// ── Source mutations ────────────────────────────────────────
const SQL_DELETE_SOURCE: &str =
	include_str!("../../../surql/queries/mutations/delete_source.surql");
const SQL_CREATE_SOURCE: &str =
	include_str!("../../../surql/queries/mutations/create_source.surql");
const SQL_UPDATE_SOURCE_CONNECTOR: &str =
	include_str!("../../../surql/queries/mutations/update_source_connector.surql");
const SQL_UPDATE_SOURCE_ENABLED: &str =
	include_str!("../../../surql/queries/mutations/update_source_enabled.surql");
const SQL_UPDATE_SOURCE_CONFIG: &str =
	include_str!("../../../surql/queries/mutations/update_source_config.surql");
const SQL_DELETE_SOURCE_QUERIES: &str =
	include_str!("../../../surql/queries/mutations/delete_source_queries.surql");

// ── Sink mutations ──────────────────────────────────────────
const SQL_DELETE_SINK: &str = include_str!("../../../surql/queries/mutations/delete_sink.surql");
const SQL_CREATE_SINK: &str = include_str!("../../../surql/queries/mutations/create_sink.surql");
const SQL_UPDATE_SINK_TYPE: &str =
	include_str!("../../../surql/queries/mutations/update_sink_type.surql");
const SQL_UPDATE_SINK_ENABLED: &str =
	include_str!("../../../surql/queries/mutations/update_sink_enabled.surql");
const SQL_UPDATE_SINK_CONFIG: &str =
	include_str!("../../../surql/queries/mutations/update_sink_config.surql");

// ── Pipe mutations ──────────────────────────────────────────
const SQL_DELETE_PIPE: &str = include_str!("../../../surql/queries/mutations/delete_pipe.surql");
const SQL_CREATE_PIPE: &str = include_str!("../../../surql/queries/mutations/create_pipe.surql");
const SQL_DELETE_PIPE_QUERIES: &str =
	include_str!("../../../surql/queries/mutations/delete_pipe_queries.surql");
const SQL_UPDATE_PIPE_ORIGIN_CONNECTOR: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_origin_connector.surql");
const SQL_UPDATE_PIPE_ORIGIN_DSN: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_origin_dsn.surql");
const SQL_UPDATE_PIPE_ORIGIN_CONFIG: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_origin_config.surql");
const SQL_UPDATE_PIPE_TARGETS: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_targets.surql");
const SQL_UPDATE_PIPE_SCHEDULE: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_schedule.surql");
const SQL_UPDATE_PIPE_DELTA: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_delta.surql");
const SQL_UPDATE_PIPE_RETRY: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_retry.surql");
const SQL_UPDATE_PIPE_ENABLED: &str =
	include_str!("../../../surql/queries/mutations/update_pipe_enabled.surql");

#[utoipa::path(
	post,
	path = "/sources",
	request_body = CreateSourceRequest,
	responses(
		(status = 200, description = "Source created", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn create_source(
	State(state): State<Arc<ApiState>>,
	Json(req): Json<CreateSourceRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	let config_json = if req.config.is_null() {
		serde_json::json!({})
	} else {
		req.config
	};

	db.query(SQL_DELETE_SOURCE)
		.bind(("name", req.name.clone()))
		.await
		.map_err(db_err)?;

	db.query(SQL_CREATE_SOURCE)
		.bind(("name", req.name.clone()))
		.bind(("connector", req.connector))
		.bind(("config", config_json))
		.await
		.map_err(db_err)?;

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("source '{}' created", req.name),
	}))
}

#[utoipa::path(
	put,
	path = "/sources/{name}",
	params(("name" = String, Path, description = "Source name")),
	request_body = UpdateSourceRequest,
	responses(
		(status = 200, description = "Source updated", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn update_source(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
	Json(req): Json<UpdateSourceRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	if let Some(connector) = req.connector {
		db.query(SQL_UPDATE_SOURCE_CONNECTOR)
			.bind(("name", name.clone()))
			.bind(("connector", connector))
			.await
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query(SQL_UPDATE_SOURCE_ENABLED)
			.bind(("name", name.clone()))
			.bind(("enabled", enabled))
			.await
			.map_err(db_err)?;
	}

	if let Some(config) = req.config {
		db.query(SQL_UPDATE_SOURCE_CONFIG)
			.bind(("name", name.clone()))
			.bind(("config", config))
			.await
			.map_err(db_err)?;
	}

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("source '{name}' updated"),
	}))
}

#[utoipa::path(
	delete,
	path = "/sources/{name}",
	params(("name" = String, Path, description = "Source name")),
	responses(
		(status = 200, description = "Source deleted", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn delete_source(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	db.query(SQL_DELETE_SOURCE)
		.bind(("name", name.clone()))
		.await
		.map_err(db_err)?;

	db.query(SQL_DELETE_SOURCE_QUERIES)
		.bind(("name", name.clone()))
		.await
		.map_err(db_err)?;

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("source '{name}' deleted"),
	}))
}

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
		.map_err(db_err)?;

	db.query(SQL_CREATE_SINK)
		.bind(("name", req.name.clone()))
		.bind(("sink_type", req.sink_type))
		.bind(("config", config_json))
		.await
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
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query(SQL_UPDATE_SINK_ENABLED)
			.bind(("name", name.clone()))
			.bind(("enabled", enabled))
			.await
			.map_err(db_err)?;
	}

	if let Some(config) = req.config {
		db.query(SQL_UPDATE_SINK_CONFIG)
			.bind(("name", name.clone()))
			.bind(("config", config))
			.await
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
		.map_err(db_err)?;

	db.query(SQL_CREATE_PIPE)
		.bind(("name", req.name.clone()))
		.bind(("origin_connector", req.origin_connector))
		.bind(("origin_dsn", req.origin_dsn))
		.bind(("origin_config", origin_config))
		.bind(("targets", req.targets))
		.bind(("schedule", schedule))
		.bind(("delta", delta))
		.bind(("retry", retry))
		.await
		.map_err(db_err)?;

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
			.map_err(db_err)?;
	}

	if let Some(dsn) = req.origin_dsn {
		db.query(SQL_UPDATE_PIPE_ORIGIN_DSN)
			.bind(("name", name.clone()))
			.bind(("v", dsn))
			.await
			.map_err(db_err)?;
	}

	if let Some(config) = req.origin_config {
		db.query(SQL_UPDATE_PIPE_ORIGIN_CONFIG)
			.bind(("name", name.clone()))
			.bind(("v", config))
			.await
			.map_err(db_err)?;
	}

	if let Some(targets) = req.targets {
		db.query(SQL_UPDATE_PIPE_TARGETS)
			.bind(("name", name.clone()))
			.bind(("v", targets))
			.await
			.map_err(db_err)?;
	}

	if let Some(schedule) = req.schedule {
		db.query(SQL_UPDATE_PIPE_SCHEDULE)
			.bind(("name", name.clone()))
			.bind(("v", schedule))
			.await
			.map_err(db_err)?;
	}

	if let Some(delta) = req.delta {
		db.query(SQL_UPDATE_PIPE_DELTA)
			.bind(("name", name.clone()))
			.bind(("v", delta))
			.await
			.map_err(db_err)?;
	}

	if let Some(retry) = req.retry {
		db.query(SQL_UPDATE_PIPE_RETRY)
			.bind(("name", name.clone()))
			.bind(("v", retry))
			.await
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query(SQL_UPDATE_PIPE_ENABLED)
			.bind(("name", name.clone()))
			.bind(("v", enabled))
			.await
			.map_err(db_err)?;
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
		.map_err(db_err)?;

	db.query(SQL_DELETE_PIPE_QUERIES)
		.bind(("name", name.clone()))
		.await
		.map_err(db_err)?;

	reload_config(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("pipe '{name}' deleted"),
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

pub(crate) async fn reload_config_pub(state: &ApiState) -> Result<(), Json<ErrorResponse>> {
	reload_config(state).await
}

async fn reload_config(state: &ApiState) -> Result<(), Json<ErrorResponse>> {
	if let (Some(lifecycle), Some(db)) = (&state.lifecycle, &state.db_client) {
		lifecycle.restart_with_config_json(db).await.map_err(|e| {
			Json(ErrorResponse {
				error: format!("reload: {e}"),
			})
		})?;
	}
	refresh_read_cache(state).await;
	Ok(())
}

async fn refresh_read_cache(state: &ApiState) {
	let Some(db) = &state.db_client else { return };

	const SQL_READ_SOURCES_CACHE: &str =
		include_str!("../../../surql/queries/config/read_sources_cache.surql");
	const SQL_READ_SINKS_CACHE: &str =
		include_str!("../../../surql/queries/config/read_sinks_cache.surql");
	const SQL_READ_PIPES_CACHE: &str =
		include_str!("../../../surql/queries/config/read_pipes_cache.surql");

	if let Ok(mut resp) = db.query(SQL_READ_SOURCES_CACHE).await
		&& let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0)
	{
		let configs: Vec<crate::state::SourceConfig> = rows
			.iter()
			.filter_map(|r| {
				Some(crate::state::SourceConfig {
					name: r.get("name")?.as_str()?.to_string(),
					connector: r.get("connector")?.as_str()?.to_string(),
					interval_secs: r
						.get("interval_secs")
						.and_then(|v| v.as_u64())
						.unwrap_or(300),
					queries: vec![],
				})
			})
			.collect();
		*state.sources.write().await = configs;
	}

	if let Ok(mut resp) = db.query(SQL_READ_SINKS_CACHE).await
		&& let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0)
	{
		let configs: Vec<crate::state::SinkConfig> = rows
			.iter()
			.filter_map(|r| {
				Some(crate::state::SinkConfig {
					name: r.get("name")?.as_str()?.to_string(),
					sink_type: r.get("sink_type")?.as_str()?.to_string(),
				})
			})
			.collect();
		*state.sinks.write().await = configs;
	}

	if let Ok(mut resp) = db.query(SQL_READ_PIPES_CACHE).await
		&& let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0)
	{
		let configs: Vec<crate::state::PipeConfigCache> = rows
			.iter()
			.filter_map(|r| {
				Some(crate::state::PipeConfigCache {
					name: r.get("name")?.as_str()?.to_string(),
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
					enabled: r.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
				})
			})
			.collect();
		*state.pipes.write().await = configs;
	}
}
