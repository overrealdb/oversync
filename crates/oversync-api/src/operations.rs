use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use tracing::warn;

use crate::state::ApiState;
use crate::types::*;

#[utoipa::path(
	post,
	path = "/sources/{name}/trigger",
	params(("name" = String, Path, description = "Source name to trigger")),
	responses(
		(status = 200, description = "Trigger initiated", body = TriggerResponse),
		(status = 400, description = "Bad request", body = ErrorResponse)
	)
)]
pub async fn trigger_source(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<TriggerResponse>, Json<ErrorResponse>> {
	let found = state.sources.read().await.iter().any(|s| s.name == name);
	if !found {
		return Err(Json(ErrorResponse {
			error: format!("source not found: {name}"),
		}));
	}

	// Trigger by restarting the lifecycle with current config
	if let Some(ref lifecycle) = state.lifecycle
		&& let Some(ref db) = state.db_client
	{
		lifecycle.restart_with_config_json(db).await.map_err(|e| {
			Json(ErrorResponse {
				error: format!("trigger: {e}"),
			})
		})?;
	}

	Ok(Json(TriggerResponse {
		source: name,
		message: "trigger initiated".into(),
	}))
}

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

	const SQL_CYCLE_HISTORY: &str = oversync_queries::delta::LIST_CYCLE_HISTORY;

	let mut response = db.query(SQL_CYCLE_HISTORY).await.map_err(|e| {
		Json(ErrorResponse {
			error: format!("db: {e}"),
		})
	})?;

	let rows: Vec<serde_json::Value> = response.take(0).map_err(|e| {
		Json(ErrorResponse {
			error: format!("db take: {e}"),
		})
	})?;

	let cycles = rows
		.iter()
		.filter_map(|r| {
			let cycle_id = r.get("cycle_id").and_then(|v| v.as_u64());
			let status = r.get("status").and_then(|v| v.as_str());
			if cycle_id.is_none() || status.is_none() {
				warn!(?r, "skipping cycle_log row with missing cycle_id or status");
				return None;
			}
			Some(CycleInfo {
				cycle_id: cycle_id?,
				status: status?.to_string(),
				started_at: r
					.get("started_at")
					.and_then(|v| v.as_str())
					.and_then(|s| s.parse().ok())
					.unwrap_or_default(),
				finished_at: r
					.get("finished_at")
					.and_then(|v| v.as_str())
					.and_then(|s| s.parse().ok()),
				rows_created: r.get("rows_created").and_then(|v| v.as_u64()).unwrap_or(0),
				rows_updated: r.get("rows_updated").and_then(|v| v.as_u64()).unwrap_or(0),
				rows_deleted: r.get("rows_deleted").and_then(|v| v.as_u64()).unwrap_or(0),
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
