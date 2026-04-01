use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};

use crate::state::ApiState;
use crate::types::*;

use oversync_queries::{mutations, query_config};

const SQL_LIST_QUERIES: &str = query_config::LIST_BY_SOURCE;
const SQL_DELETE_QUERY: &str = query_config::DELETE_ONE;
const SQL_CREATE_QUERY: &str = mutations::CREATE_QUERY;
const SQL_CREATE_QUERY_WITH_SINKS: &str = mutations::CREATE_QUERY_WITH_SINKS;
const SQL_UPDATE_QUERY_SQL: &str = query_config::UPDATE_QUERY;
const SQL_UPDATE_QUERY_KEY: &str = query_config::UPDATE_KEY_COLUMN;
const SQL_UPDATE_QUERY_SINKS: &str = query_config::UPDATE_SINKS;
const SQL_UPDATE_QUERY_ENABLED: &str = query_config::UPDATE_ENABLED;

fn db_err(e: surrealdb::Error) -> Json<ErrorResponse> {
	Json(ErrorResponse {
		error: format!("db: {e}"),
	})
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

#[utoipa::path(
	get,
	path = "/sources/{source}/queries",
	params(("source" = String, Path, description = "Source name")),
	responses(
		(status = 200, description = "List queries for source", body = QueryListResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn list_queries(
	State(state): State<Arc<ApiState>>,
	Path(source): Path<String>,
) -> Result<Json<QueryListResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	let mut resp = db
		.query(SQL_LIST_QUERIES)
		.bind(("source", source))
		.await
		.map_err(db_err)?;

	let rows: Vec<serde_json::Value> = resp.take(0).map_err(db_err)?;

	let queries = rows
		.iter()
		.filter_map(|r| {
			Some(QueryDetail {
				name: r.get("name")?.as_str()?.to_string(),
				query: r.get("query")?.as_str()?.to_string(),
				key_column: r.get("key_column")?.as_str()?.to_string(),
				sinks: r.get("sinks").and_then(|v| v.as_array()).map(|arr| {
					arr.iter()
						.filter_map(|v| v.as_str().map(String::from))
						.collect()
				}),
				enabled: r.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
			})
		})
		.collect();

	Ok(Json(QueryListResponse { queries }))
}

#[utoipa::path(
	post,
	path = "/sources/{source}/queries",
	params(("source" = String, Path, description = "Source name")),
	request_body = CreateQueryRequest,
	responses(
		(status = 200, description = "Query created", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn create_query(
	State(state): State<Arc<ApiState>>,
	Path(source): Path<String>,
	Json(req): Json<CreateQueryRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	db.query(SQL_DELETE_QUERY)
		.bind(("source", source.clone()))
		.bind(("name", req.name.clone()))
		.await
		.map_err(db_err)?;

	let sinks_val = req
		.sinks
		.map(|s| serde_json::Value::Array(s.into_iter().map(serde_json::Value::String).collect()));

	if let Some(sv) = sinks_val {
		db.query(SQL_CREATE_QUERY_WITH_SINKS)
			.bind(("source", source.clone()))
			.bind(("name", req.name.clone()))
			.bind(("query", req.query))
			.bind(("key_column", req.key_column))
			.bind(("sinks", sv))
			.await
			.map_err(db_err)?;
	} else {
		db.query(SQL_CREATE_QUERY)
			.bind(("source", source.clone()))
			.bind(("name", req.name.clone()))
			.bind(("query", req.query))
			.bind(("key_column", req.key_column))
			.await
			.map_err(db_err)?;
	}

	super::mutations::reload_config_pub(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("query '{}' created for source '{source}'", req.name),
	}))
}

#[utoipa::path(
	put,
	path = "/sources/{source}/queries/{name}",
	params(
		("source" = String, Path, description = "Source name"),
		("name" = String, Path, description = "Query name"),
	),
	request_body = UpdateQueryRequest,
	responses(
		(status = 200, description = "Query updated", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn update_query(
	State(state): State<Arc<ApiState>>,
	Path((source, name)): Path<(String, String)>,
	Json(req): Json<UpdateQueryRequest>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	if let Some(query) = req.query {
		db.query(SQL_UPDATE_QUERY_SQL)
			.bind(("source", source.clone()))
			.bind(("name", name.clone()))
			.bind(("query", query))
			.await
			.map_err(db_err)?;
	}

	if let Some(key_column) = req.key_column {
		db.query(SQL_UPDATE_QUERY_KEY)
			.bind(("source", source.clone()))
			.bind(("name", name.clone()))
			.bind(("key_column", key_column))
			.await
			.map_err(db_err)?;
	}

	if let Some(sinks) = req.sinks {
		let sinks_val =
			serde_json::Value::Array(sinks.into_iter().map(serde_json::Value::String).collect());
		db.query(SQL_UPDATE_QUERY_SINKS)
			.bind(("source", source.clone()))
			.bind(("name", name.clone()))
			.bind(("sinks", sinks_val))
			.await
			.map_err(db_err)?;
	}

	if let Some(enabled) = req.enabled {
		db.query(SQL_UPDATE_QUERY_ENABLED)
			.bind(("source", source.clone()))
			.bind(("name", name.clone()))
			.bind(("enabled", enabled))
			.await
			.map_err(db_err)?;
	}

	super::mutations::reload_config_pub(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("query '{name}' updated for source '{source}'"),
	}))
}

#[utoipa::path(
	delete,
	path = "/sources/{source}/queries/{name}",
	params(
		("source" = String, Path, description = "Source name"),
		("name" = String, Path, description = "Query name"),
	),
	responses(
		(status = 200, description = "Query deleted", body = MutationResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn delete_query(
	State(state): State<Arc<ApiState>>,
	Path((source, name)): Path<(String, String)>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	let db = require_db(&state)?;

	db.query(SQL_DELETE_QUERY)
		.bind(("source", source.clone()))
		.bind(("name", name.clone()))
		.await
		.map_err(db_err)?;

	super::mutations::reload_config_pub(&state).await?;

	Ok(Json(MutationResponse {
		ok: true,
		message: format!("query '{name}' deleted from source '{source}'"),
	}))
}
