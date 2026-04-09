use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::state::ApiState;
use crate::types::*;

#[utoipa::path(
	get,
	path = "/health",
	responses(
		(status = 200, description = "Service is healthy", body = HealthResponse)
	)
)]
pub async fn health() -> Json<HealthResponse> {
	Json(HealthResponse {
		status: "ok",
		version: env!("CARGO_PKG_VERSION"),
	})
}

#[utoipa::path(
	get,
	path = "/sinks",
	responses(
		(status = 200, description = "List configured sinks", body = SinkListResponse)
	)
)]
pub async fn list_sinks(State(state): State<Arc<ApiState>>) -> Json<SinkListResponse> {
	Json(SinkListResponse {
		sinks: state.sinks_info(),
	})
}

#[utoipa::path(
	get,
	path = "/pipes",
	responses(
		(status = 200, description = "List configured pipes", body = PipeListResponse)
	)
)]
pub async fn list_pipes(State(state): State<Arc<ApiState>>) -> Json<PipeListResponse> {
	Json(PipeListResponse {
		pipes: state.pipes_info(),
	})
}

#[utoipa::path(
	get,
	path = "/pipe-presets",
	responses(
		(status = 200, description = "List reusable pipe presets", body = PipePresetListResponse)
	)
)]
pub async fn list_pipe_presets(State(state): State<Arc<ApiState>>) -> Json<PipePresetListResponse> {
	Json(PipePresetListResponse {
		presets: state.pipe_presets_info(),
	})
}

#[utoipa::path(
	get,
	path = "/pipes/{name}",
	params(("name" = String, Path, description = "Pipe name")),
	responses(
		(status = 200, description = "Pipe details", body = PipeInfo),
		(status = 404, description = "Pipe not found", body = ErrorResponse)
	)
)]
pub async fn get_pipe(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<PipeInfo>, impl IntoResponse> {
	state
		.pipes_info()
		.into_iter()
		.find(|p| p.name == name)
		.map(Json)
		.ok_or_else(|| {
			(
				StatusCode::NOT_FOUND,
				Json(ErrorResponse {
					error: format!("pipe not found: {name}"),
				}),
			)
		})
}

#[utoipa::path(
	get,
	path = "/pipe-presets/{name}",
	params(("name" = String, Path, description = "Pipe preset name")),
	responses(
		(status = 200, description = "Pipe preset details", body = PipePresetInfo),
		(status = 404, description = "Pipe preset not found", body = ErrorResponse)
	)
)]
pub async fn get_pipe_preset(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<PipePresetInfo>, impl IntoResponse> {
	state
		.pipe_presets_info()
		.into_iter()
		.find(|preset| preset.name == name)
		.map(Json)
		.ok_or_else(|| {
			(
				StatusCode::NOT_FOUND,
				Json(ErrorResponse {
					error: format!("pipe preset not found: {name}"),
				}),
			)
		})
}
