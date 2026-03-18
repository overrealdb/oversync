pub mod handlers;
pub mod state;
pub mod types;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use utoipa::OpenApi;

use crate::state::ApiState;

#[derive(OpenApi)]
#[openapi(
	paths(
		handlers::health,
		handlers::list_sources,
		handlers::get_source,
		handlers::list_sinks,
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
		types::TriggerResponse,
		types::ErrorResponse,
	)),
	info(
		title = "oversync API",
		version = "0.1.0",
		description = "HTTP API for managing oversync sources, sinks, and sync status."
	)
)]
pub struct ApiDoc;

pub fn router(state: Arc<ApiState>) -> Router {
	Router::new()
		.route("/health", get(handlers::health))
		.route("/sources", get(handlers::list_sources))
		.route("/sources/{name}", get(handlers::get_source))
		.route("/sinks", get(handlers::list_sinks))
		.route(
			"/openapi.json",
			get(|| async { axum::Json(ApiDoc::openapi()) }),
		)
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
			sources: vec![SourceConfig {
				name: "pg-prod".into(),
				connector: "postgres".into(),
				interval_secs: 300,
				queries: vec![QueryConfig {
					id: "users".into(),
					key_column: "id".into(),
				}],
			}],
			sinks: vec![SinkConfig {
				name: "stdout".into(),
				sink_type: "stdout".into(),
			}],
			cycle_status: Arc::new(RwLock::new(HashMap::new())),
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
		assert_eq!(status, StatusCode::OK); // TODO: proper 404 with IntoResponse
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
	}
}
