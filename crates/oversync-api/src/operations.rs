use std::sync::Arc;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use tracing::warn;

use crate::state::ApiState;
use crate::types::*;

#[utoipa::path(
	post,
	path = "/sync/pause",
	responses(
		(status = 200, description = "Sync paused", body = MutationResponse)
	)
)]
pub async fn pause_sync(State(state): State<Arc<ApiState>>) -> Json<MutationResponse> {
	if let Some(ref lifecycle) = state.lifecycle {
		lifecycle.pause().await;
	}
	Json(MutationResponse {
		ok: true,
		message: "sync paused".into(),
	})
}

#[utoipa::path(
	post,
	path = "/sync/resume",
	responses(
		(status = 200, description = "Sync resumed", body = MutationResponse),
		(status = 400, description = "Resume failed", body = ErrorResponse)
	)
)]
pub async fn resume_sync(
	State(state): State<Arc<ApiState>>,
) -> Result<Json<MutationResponse>, Json<ErrorResponse>> {
	if let Some(ref lifecycle) = state.lifecycle {
		lifecycle.resume().await.map_err(|e| {
			Json(ErrorResponse {
				error: format!("resume: {e}"),
			})
		})?;
	}
	Ok(Json(MutationResponse {
		ok: true,
		message: "sync resumed".into(),
	}))
}

#[utoipa::path(
	get,
	path = "/history",
	responses(
		(status = 200, description = "Cycle history", body = HistoryResponse),
		(status = 400, description = "Database not configured", body = ErrorResponse)
	)
)]
pub async fn get_history(
	State(state): State<Arc<ApiState>>,
) -> Result<Json<HistoryResponse>, Json<ErrorResponse>> {
	let db = state.db_client.as_ref().ok_or_else(|| {
		Json(ErrorResponse {
			error: "database not configured".into(),
		})
	})?;

	const SQL_TEMPLATE: &str = oversync_queries::delta::LIST_CYCLE_HISTORY;

	// Collect per-pipe table names + legacy shared table
	let pipe_names: Vec<String> = state
		.pipes
		.read()
		.await
		.iter()
		.map(|p| p.name.clone())
		.collect();

	let mut all_tables: Vec<oversync_core::TableNames> = pipe_names
		.iter()
		.map(|name| oversync_core::TableNames::for_source(name))
		.collect();
	all_tables.push(oversync_core::TableNames::default_shared());
	all_tables.dedup_by(|a, b| a.cycle_log == b.cycle_log);

	// Query all per-pipe cycle_log tables concurrently
	let mut set = tokio::task::JoinSet::new();
	for tables in all_tables {
		let sql = tables.resolve_sql(SQL_TEMPLATE);
		let db = Arc::clone(db);
		set.spawn(async move {
			let mut resp = db.query(&sql).await.ok()?;
			resp.take::<Vec<serde_json::Value>>(0).ok()
		});
	}

	let mut all_rows: Vec<serde_json::Value> = Vec::new();
	while let Some(result) = set.join_next().await {
		if let Ok(Some(rows)) = result {
			all_rows.extend(rows);
		}
	}

	// Sort by started_at DESC, take 100
	all_rows.sort_by(|a, b| {
		let sa = a.get("started_at").and_then(|v| v.as_str()).unwrap_or("");
		let sb = b.get("started_at").and_then(|v| v.as_str()).unwrap_or("");
		sb.cmp(sa)
	});
	all_rows.truncate(100);

	let cycles = all_rows
		.iter()
		.filter_map(|r| {
			let cycle_id = r.get("cycle_id").and_then(|v| v.as_u64());
			let status = r.get("status").and_then(|v| v.as_str());
			if cycle_id.is_none() || status.is_none() {
				warn!(?r, "skipping cycle_log row with missing cycle_id or status");
				return None;
			}
			let started_at: DateTime<Utc> = r
				.get("started_at")
				.and_then(|v| v.as_str())
				.and_then(|s| s.parse().ok())
				.unwrap_or_default();
			let finished_at: Option<DateTime<Utc>> = r
				.get("finished_at")
				.and_then(|v| v.as_str())
				.and_then(|s| s.parse().ok());
			let duration_ms =
				finished_at.map(|f| (f - started_at).num_milliseconds().unsigned_abs());
			Some(CycleInfo {
				cycle_id: cycle_id?,
				source: r
					.get("origin_id")
					.and_then(|v| v.as_str())
					.unwrap_or("")
					.to_string(),
				query: r
					.get("query_id")
					.and_then(|v| v.as_str())
					.unwrap_or("")
					.to_string(),
				status: status?.to_string(),
				started_at,
				finished_at,
				rows_created: r.get("rows_created").and_then(|v| v.as_u64()).unwrap_or(0),
				rows_updated: r.get("rows_updated").and_then(|v| v.as_u64()).unwrap_or(0),
				rows_deleted: r.get("rows_deleted").and_then(|v| v.as_u64()).unwrap_or(0),
				duration_ms,
				error: r.get("error").and_then(|v| v.as_str()).map(String::from),
			})
		})
		.collect();

	Ok(Json(HistoryResponse { cycles }))
}

#[utoipa::path(
	get,
	path = "/sync/status",
	responses(
		(status = 200, description = "Sync status", body = StatusResponse)
	)
)]
pub async fn sync_status(State(state): State<Arc<ApiState>>) -> Json<StatusResponse> {
	let (running, paused) = match &state.lifecycle {
		Some(lc) => (lc.is_running().await, lc.is_paused().await),
		None => (false, false),
	};
	Json(StatusResponse { running, paused })
}

#[utoipa::path(
	get,
	path = "/config/export",
	params(
		("format" = Option<ExportConfigFormat>, Query, description = "Export format: toml or json")
	),
	responses(
		(status = 200, description = "Current persisted config export", body = ExportConfigResponse),
		(status = 400, description = "Config export unavailable", body = ErrorResponse)
	)
)]
pub async fn export_config(
	State(state): State<Arc<ApiState>>,
	Query(query): Query<ExportConfigQuery>,
) -> Result<Json<ExportConfigResponse>, (StatusCode, Json<ErrorResponse>)> {
	let db = state.db_client.as_ref().ok_or_else(|| {
		(
			StatusCode::BAD_REQUEST,
			Json(ErrorResponse {
				error: "database not configured".into(),
			}),
		)
	})?;
	let lifecycle = state.lifecycle.as_ref().ok_or_else(|| {
		(
			StatusCode::BAD_REQUEST,
			Json(ErrorResponse {
				error: "config export unavailable".into(),
			}),
		)
	})?;

	let format = query.format.unwrap_or(ExportConfigFormat::Toml);
	let content = lifecycle.export_config(db, format).await.map_err(|e| {
		(
			StatusCode::BAD_REQUEST,
			Json(ErrorResponse {
				error: format!("config export: {e}"),
			}),
		)
	})?;

	Ok(Json(ExportConfigResponse { format, content }))
}

#[utoipa::path(
	post,
	path = "/config/import",
	request_body = ImportConfigRequest,
	responses(
		(status = 200, description = "Config imported into the current control plane", body = ImportConfigResponse),
		(status = 400, description = "Config import failed", body = ErrorResponse)
	)
)]
pub async fn import_config(
	State(state): State<Arc<ApiState>>,
	Json(req): Json<ImportConfigRequest>,
) -> Result<Json<ImportConfigResponse>, (StatusCode, Json<ErrorResponse>)> {
	let db = state.db_client.as_ref().ok_or_else(|| {
		(
			StatusCode::BAD_REQUEST,
			Json(ErrorResponse {
				error: "database not configured".into(),
			}),
		)
	})?;
	let lifecycle = state.lifecycle.as_ref().ok_or_else(|| {
		(
			StatusCode::BAD_REQUEST,
			Json(ErrorResponse {
				error: "config import unavailable".into(),
			}),
		)
	})?;

	let warnings = lifecycle
		.import_config(db, req.format, &req.content)
		.await
		.map_err(|e| {
			(
				StatusCode::BAD_REQUEST,
				Json(ErrorResponse {
					error: format!("config import: {e}"),
				}),
			)
		})?;

	crate::mutations::refresh_read_cache(&state)
		.await
		.map_err(|e| {
			(
				StatusCode::BAD_REQUEST,
				Json(ErrorResponse {
					error: format!("refresh cache: {e}"),
				}),
			)
		})?;

	Ok(Json(ImportConfigResponse {
		ok: true,
		message: "config imported".into(),
		warnings,
	}))
}
