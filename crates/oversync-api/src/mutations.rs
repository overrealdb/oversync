use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};

use crate::state::ApiState;
use crate::types::*;

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

	db.query("DELETE source_config WHERE name = $name")
		.bind(("name", req.name.clone()))
		.await
		.map_err(db_err)?;

	db.query("CREATE source_config SET name = $name, connector = $connector, config = $config, enabled = true, updated_at = time::now()")
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
		db.query("UPDATE source_config SET connector = $connector, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("connector", connector))
			.await
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query("UPDATE source_config SET enabled = $enabled, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("enabled", enabled))
			.await
			.map_err(db_err)?;
	}

	if let Some(config) = req.config {
		db.query("UPDATE source_config SET config = $config, updated_at = time::now() WHERE name = $name")
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

	db.query("DELETE source_config WHERE name = $name")
		.bind(("name", name.clone()))
		.await
		.map_err(db_err)?;

	db.query("DELETE query_config WHERE origin_id = $name")
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

	db.query("DELETE sink_config WHERE name = $name")
		.bind(("name", req.name.clone()))
		.await
		.map_err(db_err)?;

	db.query("CREATE sink_config SET name = $name, sink_type = $sink_type, config = $config, enabled = true, updated_at = time::now()")
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
		db.query("UPDATE sink_config SET sink_type = $sink_type, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("sink_type", sink_type))
			.await
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query("UPDATE sink_config SET enabled = $enabled, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("enabled", enabled))
			.await
			.map_err(db_err)?;
	}

	if let Some(config) = req.config {
		db.query("UPDATE sink_config SET config = $config, updated_at = time::now() WHERE name = $name")
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

	db.query("DELETE sink_config WHERE name = $name")
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

	db.query("DELETE pipe_config WHERE name = $name")
		.bind(("name", req.name.clone()))
		.await
		.map_err(db_err)?;

	db.query("CREATE pipe_config SET name = $name, origin_connector = $origin_connector, origin_dsn = $origin_dsn, origin_config = $origin_config, targets = $targets, schedule = $schedule, delta = $delta, retry = $retry, enabled = true, updated_at = time::now()")
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
		db.query("UPDATE pipe_config SET origin_connector = $v, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("v", connector))
			.await
			.map_err(db_err)?;
	}

	if let Some(dsn) = req.origin_dsn {
		db.query("UPDATE pipe_config SET origin_dsn = $v, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("v", dsn))
			.await
			.map_err(db_err)?;
	}

	if let Some(config) = req.origin_config {
		db.query("UPDATE pipe_config SET origin_config = $v, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("v", config))
			.await
			.map_err(db_err)?;
	}

	if let Some(targets) = req.targets {
		db.query("UPDATE pipe_config SET targets = $v, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("v", targets))
			.await
			.map_err(db_err)?;
	}

	if let Some(schedule) = req.schedule {
		db.query("UPDATE pipe_config SET schedule = $v, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("v", schedule))
			.await
			.map_err(db_err)?;
	}

	if let Some(delta) = req.delta {
		db.query("UPDATE pipe_config SET delta = $v, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("v", delta))
			.await
			.map_err(db_err)?;
	}

	if let Some(retry) = req.retry {
		db.query("UPDATE pipe_config SET retry = $v, updated_at = time::now() WHERE name = $name")
			.bind(("name", name.clone()))
			.bind(("v", retry))
			.await
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query("UPDATE pipe_config SET enabled = $v, updated_at = time::now() WHERE name = $name")
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

	db.query("DELETE pipe_config WHERE name = $name")
		.bind(("name", name.clone()))
		.await
		.map_err(db_err)?;

	db.query("DELETE query_config WHERE origin_id = $name")
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
	state.db_client.as_ref().ok_or_else(|| {
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
		lifecycle
			.restart_with_config_json(db)
			.await
			.map_err(|e| {
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

	if let Ok(mut resp) = db
		.query("SELECT name, connector, config.interval_secs AS interval_secs FROM source_config WHERE enabled = true")
		.await
	{
		if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0) {
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
	}

	if let Ok(mut resp) = db
		.query("SELECT name, sink_type FROM sink_config WHERE enabled = true")
		.await
	{
		if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0) {
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
	}

	if let Ok(mut resp) = db
		.query("SELECT name, origin_connector, origin_dsn, targets, schedule.interval_secs AS interval_secs, enabled FROM pipe_config WHERE enabled = true")
		.await
	{
		if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0) {
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
}
