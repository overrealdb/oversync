pub mod auth;
pub mod handlers;
pub mod mutations;
pub mod operations;
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
		handlers::list_sinks,
		handlers::list_pipes,
		handlers::list_pipe_presets,
		handlers::get_pipe,
		handlers::get_pipe_preset,
		mutations::create_sink,
		mutations::update_sink,
		mutations::delete_sink,
		mutations::create_pipe,
		mutations::update_pipe,
		mutations::delete_pipe,
		mutations::create_pipe_preset,
		mutations::update_pipe_preset,
		mutations::delete_pipe_preset,
		operations::run_pipe,
		operations::pause_sync,
		operations::resume_sync,
		operations::export_config,
		operations::import_config,
		operations::get_history,
		operations::sync_status,
	),
	components(schemas(
		types::HealthResponse,
		types::CycleInfo,
		types::SinkListResponse,
		types::SinkInfo,
		types::PipeListResponse,
		types::PipeInfo,
		types::PipePresetListResponse,
		types::PipePresetInfo,
		types::PipePresetSpecInput,
		types::PipePresetParameterInput,
		types::PipeQueryInput,
		types::ErrorResponse,
		types::CreateSinkRequest,
		types::UpdateSinkRequest,
		types::CreatePipeRequest,
		types::UpdatePipeRequest,
		types::CreatePipePresetRequest,
		types::UpdatePipePresetRequest,
		types::MutationResponse,
		types::PipeRunQueryResult,
		types::PipeRunResponse,
		types::HistoryResponse,
		types::StatusResponse,
		types::ExportConfigFormat,
		types::ExportConfigQuery,
		types::ExportConfigResponse,
		types::ImportConfigRequest,
		types::ImportConfigResponse,
	)),
	info(
		title = "oversync API",
		version = env!("CARGO_PKG_VERSION"),
		description = "HTTP API for managing oversync pipes, sinks, recipes, and sync status."
	)
)]
pub struct ApiDoc;

pub fn router(state: Arc<ApiState>) -> Router {
	router_with_openapi(state, ApiDoc::openapi())
}

pub fn router_with_openapi(state: Arc<ApiState>, openapi: utoipa::openapi::OpenApi) -> Router {
	let openapi = Arc::new(openapi);

	let protected = Router::new()
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
		.route("/pipes/{name}/run", post(operations::run_pipe))
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

	Router::new()
		.route("/health", get(handlers::health))
		.route(
			"/openapi.json",
			get({
				let openapi = Arc::clone(&openapi);
				move || {
					let openapi = Arc::clone(&openapi);
					async move { axum::Json(openapi.as_ref().clone()) }
				}
			}),
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
	use tower::ServiceExt;

	struct TestLifecycle {
		run_results: Vec<types::PipeRunQueryResult>,
		run_error: Option<String>,
	}

	#[async_trait::async_trait]
	impl LifecycleControl for TestLifecycle {
		async fn restart_with_config_json(
			&self,
			_db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
		) -> Result<(), oversync_core::error::OversyncError> {
			Ok(())
		}

		async fn runtime_cache_snapshot(
			&self,
		) -> Result<RuntimeCacheSnapshot, oversync_core::error::OversyncError> {
			Ok(RuntimeCacheSnapshot::default())
		}

		async fn export_config(
			&self,
			_db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
			_format: types::ExportConfigFormat,
		) -> Result<String, oversync_core::error::OversyncError> {
			Ok(String::new())
		}

		async fn import_config(
			&self,
			_db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
			_format: types::ExportConfigFormat,
			_content: &str,
		) -> Result<Vec<String>, oversync_core::error::OversyncError> {
			Ok(vec![])
		}

		async fn run_pipe_once(
			&self,
			_pipe_name: &str,
		) -> Result<Vec<types::PipeRunQueryResult>, oversync_core::error::OversyncError> {
			match &self.run_error {
				Some(message) => Err(oversync_core::error::OversyncError::Config(message.clone())),
				None => Ok(self.run_results.clone()),
			}
		}

		async fn pause(&self) {}

		async fn resume(&self) -> Result<(), oversync_core::error::OversyncError> {
			Ok(())
		}

		async fn is_running(&self) -> bool {
			false
		}

		async fn is_paused(&self) -> bool {
			false
		}
	}

	fn test_state() -> Arc<ApiState> {
		Arc::new(ApiState {
			sinks: Arc::new(tokio::sync::RwLock::new(vec![SinkConfig {
				name: "stdout".into(),
				sink_type: "stdout".into(),
				config: None,
			}])),
			pipes: Arc::new(tokio::sync::RwLock::new(vec![state::PipeConfigCache {
				name: "catalog-sync".into(),
				origin_connector: "postgres".into(),
				origin_dsn: "postgres://ro@pg1:5432/meta".into(),
				targets: vec!["kafka-main".into()],
				interval_secs: 60,
				query_count: 2,
				recipe: None,
				enabled: true,
			}])),
			pipe_presets: Arc::new(tokio::sync::RwLock::new(vec![])),
			db_client: None,
			lifecycle: None,
			api_key: None,
		})
	}

	fn test_state_with_lifecycle(lifecycle: Arc<dyn LifecycleControl>) -> Arc<ApiState> {
		Arc::new(ApiState {
			sinks: Arc::new(tokio::sync::RwLock::new(vec![SinkConfig {
				name: "stdout".into(),
				sink_type: "stdout".into(),
				config: None,
			}])),
			pipes: Arc::new(tokio::sync::RwLock::new(vec![state::PipeConfigCache {
				name: "catalog-sync".into(),
				origin_connector: "postgres".into(),
				origin_dsn: "postgres://ro@pg1:5432/meta".into(),
				targets: vec!["kafka-main".into()],
				interval_secs: 60,
				query_count: 2,
				recipe: None,
				enabled: true,
			}])),
			pipe_presets: Arc::new(tokio::sync::RwLock::new(vec![])),
			db_client: None,
			lifecycle: Some(lifecycle),
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
		assert_eq!(pipes[0]["query_count"], 2);
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
	async fn run_pipe_returns_manual_run_results() {
		let app = router(test_state_with_lifecycle(Arc::new(TestLifecycle {
			run_results: vec![types::PipeRunQueryResult {
				query_id: "tables".into(),
				created: 2,
				updated: 1,
				deleted: 0,
			}],
			run_error: None,
		})));
		let req = Request::post("/pipes/catalog-sync/run")
			.body(Body::empty())
			.unwrap();
		let resp = app.clone().oneshot(req).await.unwrap();
		assert_eq!(resp.status(), StatusCode::OK);
		let body = resp.into_body().collect().await.unwrap().to_bytes();
		let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
		assert_eq!(json["ok"], true);
		assert_eq!(json["results"][0]["query_id"], "tables");
		assert_eq!(json["results"][0]["created"], 2);
	}

	#[tokio::test]
	async fn run_pipe_surfaces_manual_run_errors() {
		let app = router(test_state_with_lifecycle(Arc::new(TestLifecycle {
			run_results: vec![],
			run_error: Some("pipe is disabled".into()),
		})));
		let req = Request::post("/pipes/catalog-sync/run")
			.body(Body::empty())
			.unwrap();
		let resp = app.clone().oneshot(req).await.unwrap();
		assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
		let body = resp.into_body().collect().await.unwrap().to_bytes();
		let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
		assert!(json["error"].as_str().unwrap().contains("pipe is disabled"));
	}

	#[tokio::test]
	async fn get_pipe_not_found() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/pipes/nonexistent").await;
		assert_eq!(status, StatusCode::NOT_FOUND);
		assert!(json["error"].as_str().unwrap().contains("not found"));
	}

	#[tokio::test]
	async fn openapi_spec_is_valid_json() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/openapi.json").await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(json["openapi"], "3.1.0");
		assert_eq!(json["info"]["title"], "oversync API");
		assert!(json["paths"]["/health"].is_object());
		assert!(json["paths"]["/sinks"].is_object());
		assert!(json["paths"]["/pipes"].is_object());
		assert!(json["paths"]["/pipes/{name}/run"].is_object());
		assert!(json["paths"]["/pipe-presets"].is_object());
		assert!(json["paths"]["/config/import"].is_object());
		assert!(json["paths"]["/config/export"].is_object());
		assert!(json["paths"]["/history"].is_object());
		assert!(json["paths"]["/sync/pause"].is_object());
		assert!(json["paths"]["/sync/resume"].is_object());
		assert!(json["paths"]["/sources"].is_null());
	}

	#[tokio::test]
	async fn sync_status_returns_default() {
		let app = router(test_state());
		let (status, json) = get_json(&app, "/sync/status").await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(json["running"], false);
	}
}
