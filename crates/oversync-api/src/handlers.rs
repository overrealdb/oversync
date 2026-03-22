use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};

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
	path = "/sources",
	responses(
		(status = 200, description = "List configured sources", body = SourceListResponse)
	)
)]
pub async fn list_sources(State(state): State<Arc<ApiState>>) -> Json<SourceListResponse> {
	Json(SourceListResponse {
		sources: state.sources_info(),
	})
}

#[utoipa::path(
	get,
	path = "/sources/{name}",
	params(("name" = String, Path, description = "Source name")),
	responses(
		(status = 200, description = "Source details", body = SourceInfo),
		(status = 404, description = "Source not found", body = ErrorResponse)
	)
)]
pub async fn get_source(
	State(state): State<Arc<ApiState>>,
	Path(name): Path<String>,
) -> Result<Json<SourceInfo>, Json<ErrorResponse>> {
	state
		.sources_info()
		.into_iter()
		.find(|s| s.name == name)
		.map(Json)
		.ok_or_else(|| {
			Json(ErrorResponse {
				error: format!("source not found: {name}"),
			})
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
) -> Result<Json<PipeInfo>, Json<ErrorResponse>> {
	state
		.pipes_info()
		.into_iter()
		.find(|p| p.name == name)
		.map(Json)
		.ok_or_else(|| {
			Json(ErrorResponse {
				error: format!("pipe not found: {name}"),
			})
		})
}
