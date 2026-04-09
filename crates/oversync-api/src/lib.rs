pub mod auth;
pub mod handlers;
pub mod mutations;
pub mod operations;
pub mod queries;
pub mod state;
pub mod types;

use std::sync::Arc;

use axum::Router;
use axum::middleware;
use axum::routing::{get, post, put};
use utoipa::OpenApi;

use crate::state::ApiState;

#[derive(OpenApi)]
#[openapi(
	paths(
		handlers::health,
		handlers::list_sources,
		handlers::get_source,
		handlers::list_sinks,
		handlers::list_pipes,
		handlers::list_pipe_presets,
		handlers::get_pipe,
		handlers::get_pipe_preset,
		mutations::create_source,
		mutations::update_source,
		mutations::delete_source,
		mutations::create_sink,
		mutations::update_sink,
		mutations::delete_sink,
		mutations::create_pipe,
		mutations::update_pipe,
		mutations::delete_pipe,
		mutations::create_pipe_preset,
		mutations::update_pipe_preset,
		mutations::delete_pipe_preset,
		operations::trigger_source,
		operations::pause_sync,
		operations::resume_sync,
		operations::export_config,
		operations::import_config,
		operations::get_history,
		operations::sync_status,
		queries::list_queries,
		queries::create_query,
		queries::update_query,
		queries::delete_query,
	),
	components(schemas(
		types::HealthResponse,
		types::SourceListResponse,
		types::SourceInfo,
		types::QueryInfo,
		types::SourceStatus,
		types::CycleInfo,
		types::SinkListResponse,
		types::SinkInfo,
		types::PipeListResponse,
		types::PipeInfo,
		types::PipePresetListResponse,
		types::PipePresetInfo,
		types::PipePresetSpecInput,
		types::PipeQueryInput,
		types::TriggerResponse,
		types::ErrorResponse,
		types::CreateSourceRequest,
		types::UpdateSourceRequest,
		types::CreateSinkRequest,
		types::UpdateSinkRequest,
		types::CreatePipeRequest,
		types::UpdatePipeRequest,
		types::CreatePipePresetRequest,
		types::UpdatePipePresetRequest,
		types::MutationResponse,
		types::HistoryResponse,
		types::StatusResponse,
		types::ExportConfigFormat,
		types::ExportConfigQuery,
		types::ExportConfigResponse,
		types::ImportConfigRequest,
		types::ImportConfigResponse,
		types::CreateQueryRequest,
		types::UpdateQueryRequest,
		types::QueryListResponse,
		types::QueryDetail,
	)),
	info(
		title = "oversync API",
		version = "0.1.0",
		description = "HTTP API for managing oversync sources, sinks, and sync status."
	)
)]
pub struct ApiDoc;

pub fn router(state: Arc<ApiState>) -> Router {
	// Protected routes — require API key when configured
	let protected = Router::new()
		.route(
			"/sources",
			get(handlers::list_sources).post(mutations::create_source),
		)
		.route(
			"/sources/{name}",
			get(handlers::get_source)
				.put(mutations::update_source)
				.delete(mutations::delete_source),
		)
		.route("/sources/{name}/trigger", post(operations::trigger_source))
		.route(
			"/sources/{source}/queries",
			get(queries::list_queries).post(queries::create_query),
		)
		.route(
			"/sources/{source}/queries/{name}",
			put(queries::update_query).delete(queries::delete_query),
		)
		.route(
			"/sinks",
			get(handlers::list_sinks).post(mutations::create_sink),
		)
		.route(
			"/sinks/{name}",
			put(mutations::update_sink).delete(mutations::delete_sink),
		)
		.route(
			"/pipes",
			get(handlers::list_pipes).post(mutations::create_pipe),
		)
		.route(
			"/pipe-presets",
			get(handlers::list_pipe_presets).post(mutations::create_pipe_preset),
		)
		.route(
			"/pipes/{name}",
			get(handlers::get_pipe)
				.put(mutations::update_pipe)
				.delete(mutations::delete_pipe),
		)
		.route(
			"/pipe-presets/{name}",
			get(handlers::get_pipe_preset)
				.put(mutations::update_pipe_preset)
				.delete(mutations::delete_pipe_preset),
		)
		.route("/sync/pause", post(operations::pause_sync))
		.route("/sync/resume", post(operations::resume_sync))
		.route("/sync/status", get(operations::sync_status))
		.route("/config/import", post(operations::import_config))
		.route("/config/export", get(operations::export_config))
		.route("/history", get(operations::get_history))
		.route_layer(middleware::from_fn_with_state(
			state.clone(),
			auth::require_api_key,
		));

	// Public routes — no auth required
	Router::new()
		.route("/health", get(handlers::health))
		.route(
			"/openapi.json",
			get(|| async { axum::Json(ApiDoc::openapi()) }),
		)
		.merge(protected)
		.with_state(state)
}

#[cfg(test)]
mod tests {
	use super::*;
	use axum::body::Body;
	use axum::http::{Request, StatusCode};
	use http_body_util::BodyExt;
	use state::*;
	use std::collections::HashMap;
	use tokio::sync::RwLock;
	use tower::ServiceExt;

	fn test_state() -> Arc<ApiState> {
		Arc::new(ApiState {
			sources: Arc::new(RwLock::new(vec![SourceConfig {
				name: "pg-prod".into(),
				connector: "postgres".into(),
				interval_secs: 300,
				queries: vec![QueryConfig {
					id: "users".into(),
					key_column: "id".into(),
				}],
			}])),
			sinks: Arc::new(RwLock::new(vec![SinkConfig {
				name: "stdout".into(),
				sink_type: "stdout".into(),
				config: None,
			}])),
			pipes: Arc::new(RwLock::new(vec![state::PipeConfigCache {
				name: "catalog-sync".into(),
				origin_connector: "postgres".into(),
				origin_dsn: "postgres://ro@pg1:5432/meta".into(),
				targets: vec!["kafka-main".into()],
				interval_secs: 60,
				recipe: None,
				enabled: true,
			}])),
			pipe_presets: Arc::new(RwLock::new(vec![])),
			cycle_status: Arc::new(RwLock::new(HashMap::new())),
			db_client: None,
			lifecycle: None,
			api_key: None,
		})
	}

	async fn get_json(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
		let req = Request::get(path).body(Body::empty()).unwrap();
		let resp = app.clone().oneshot(req).await.unwrap();
		let status = resp.status();
		let body = resp.into_body().collect().await.unwrap().to_bytes();
		let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
		(status, json)
	}

	#[tokio::test]
	async fn health_returns_ok() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/health").await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(json["status"], "ok");
		assert!(json["version"].is_string());
	}

	#[tokio::test]
	async fn list_sources_returns_configured() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/sources").await;
		assert_eq!(status, StatusCode::OK);
		let sources = json["sources"].as_array().unwrap();
		assert_eq!(sources.len(), 1);
		assert_eq!(sources[0]["name"], "pg-prod");
		assert_eq!(sources[0]["connector"], "postgres");
		assert_eq!(sources[0]["queries"][0]["id"], "users");
	}

	#[tokio::test]
	async fn get_source_by_name() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/sources/pg-prod").await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(json["name"], "pg-prod");
	}

	#[tokio::test]
	async fn get_source_not_found() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/sources/nonexistent").await;
		assert_eq!(status, StatusCode::NOT_FOUND);
		assert!(json["error"].as_str().unwrap().contains("not found"));
	}

	#[tokio::test]
	async fn list_sinks_returns_configured() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/sinks").await;
		assert_eq!(status, StatusCode::OK);
		let sinks = json["sinks"].as_array().unwrap();
		assert_eq!(sinks.len(), 1);
		assert_eq!(sinks[0]["name"], "stdout");
		assert_eq!(sinks[0]["sink_type"], "stdout");
	}

	#[tokio::test]
	async fn openapi_spec_is_valid_json() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/openapi.json").await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(json["openapi"], "3.1.0");
		assert_eq!(json["info"]["title"], "oversync API");
		assert!(json["paths"]["/health"].is_object());
		assert!(json["paths"]["/sources"].is_object());
		assert!(json["paths"]["/sinks"].is_object());
		assert!(json["paths"]["/pipes"].is_object());
		assert!(json["paths"]["/config/import"].is_object());
		assert!(json["paths"]["/config/export"].is_object());
		assert!(json["paths"]["/history"].is_object());
		assert!(json["paths"]["/sync/pause"].is_object());
		assert!(json["paths"]["/sync/resume"].is_object());
	}

	#[tokio::test]
	async fn sync_status_returns_default() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/sync/status").await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(json["running"], false);
	}

	#[tokio::test]
	async fn list_pipes_returns_configured() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/pipes").await;
		assert_eq!(status, StatusCode::OK);
		let pipes = json["pipes"].as_array().unwrap();
		assert_eq!(pipes.len(), 1);
		assert_eq!(pipes[0]["name"], "catalog-sync");
		assert_eq!(pipes[0]["origin_connector"], "postgres");
		assert_eq!(pipes[0]["targets"][0], "kafka-main");
		assert_eq!(pipes[0]["interval_secs"], 60);
		assert!(pipes[0]["enabled"].as_bool().unwrap());
	}

	#[tokio::test]
	async fn get_pipe_by_name() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/pipes/catalog-sync").await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(json["name"], "catalog-sync");
		assert_eq!(json["origin_connector"], "postgres");
	}

	#[tokio::test]
	async fn get_pipe_not_found() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/pipes/nonexistent").await;
		assert_eq!(status, StatusCode::NOT_FOUND);
		assert!(json["error"].as_str().unwrap().contains("not found"));
	}
}
