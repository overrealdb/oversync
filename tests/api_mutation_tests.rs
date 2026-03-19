mod common;

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::RwLock;
use tower::ServiceExt;

use common::surreal::TestSurrealContainer;
use oversync_api::state::*;

fn test_state_with_db(
	client: surrealdb::Surreal<surrealdb::engine::any::Any>,
) -> Arc<ApiState> {
	Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(client),
		lifecycle: None,
	})
}

fn test_state_no_db() -> Arc<ApiState> {
	Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: None,
		lifecycle: None,
	})
}

async fn post_json(
	app: &axum::Router,
	path: &str,
	body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
	let req = Request::post(path)
		.header("content-type", "application/json")
		.body(Body::from(serde_json::to_vec(&body).unwrap()))
		.unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	let status = resp.status();
	let bytes = resp.into_body().collect().await.unwrap().to_bytes();
	let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
	(status, json)
}

async fn put_json(
	app: &axum::Router,
	path: &str,
	body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
	let req = Request::put(path)
		.header("content-type", "application/json")
		.body(Body::from(serde_json::to_vec(&body).unwrap()))
		.unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	let status = resp.status();
	let bytes = resp.into_body().collect().await.unwrap().to_bytes();
	let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
	(status, json)
}

async fn delete_req(
	app: &axum::Router,
	path: &str,
) -> (StatusCode, serde_json::Value) {
	let req = Request::delete(path)
		.body(Body::empty())
		.unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	let status = resp.status();
	let bytes = resp.into_body().collect().await.unwrap().to_bytes();
	let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
	(status, json)
}

async fn get_json(
	app: &axum::Router,
	path: &str,
) -> (StatusCode, serde_json::Value) {
	let req = Request::get(path).body(Body::empty()).unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	let status = resp.status();
	let bytes = resp.into_body().collect().await.unwrap().to_bytes();
	let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
	(status, json)
}

// ── Source CRUD ─────────────────────────────────────────────

#[tokio::test]
async fn create_source_stores_in_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/sources",
		serde_json::json!({
			"name": "my-pg",
			"connector": "postgres",
			"config": {"dsn": "postgres://localhost/db"}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);
	assert!(json["message"].as_str().unwrap().contains("my-pg"));

	// Verify it's in the DB
	let mut resp = container
		.client
		.query("SELECT * FROM source_config WHERE name = 'my-pg'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["connector"], "postgres");
}

#[tokio::test]
async fn update_source_modifies_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	// Create first
	post_json(
		&app,
		"/sources",
		serde_json::json!({
			"name": "src1",
			"connector": "postgres",
			"config": {"dsn": "postgres://old"}
		}),
	)
	.await;

	// Update
	let (status, json) = put_json(
		&app,
		"/sources/src1",
		serde_json::json!({
			"config": {"dsn": "postgres://new"},
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	// Verify the DB was actually updated
	let mut resp = container
		.client
		.query("SELECT * FROM source_config WHERE name = 'src1'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["config"]["dsn"], "postgres://new");
}

#[tokio::test]
async fn delete_source_removes_from_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/sources",
		serde_json::json!({
			"name": "to-delete",
			"connector": "postgres",
		}),
	)
	.await;

	let (status, json) = delete_req(&app, "/sources/to-delete").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut resp = container
		.client
		.query("SELECT * FROM source_config WHERE name = 'to-delete'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(rows.is_empty());
}

// ── Sink CRUD ───────────────────────────────────────────────

#[tokio::test]
async fn create_sink_stores_in_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/sinks",
		serde_json::json!({
			"name": "wh-prod",
			"sink_type": "http",
			"config": {"url": "https://example.com/webhook"}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut resp = container
		.client
		.query("SELECT * FROM sink_config WHERE name = 'wh-prod'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["sink_type"], "http");
}

#[tokio::test]
async fn delete_sink_removes_from_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/sinks",
		serde_json::json!({
			"name": "to-del",
			"sink_type": "stdout",
		}),
	)
	.await;

	let (status, json) = delete_req(&app, "/sinks/to-del").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	// Verify the sink is gone from DB
	let mut resp = container
		.client
		.query("SELECT * FROM sink_config WHERE name = 'to-del'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(rows.is_empty());
}

// ── Operations ──────────────────────────────────────────────

#[tokio::test]
async fn pause_resume_without_lifecycle() {
	let state = test_state_no_db();
	let app = oversync_api::router(state);

	let (status, json) = post_json(&app, "/sync/pause", serde_json::json!({})).await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let (status, json) = post_json(&app, "/sync/resume", serde_json::json!({})).await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);
}

#[tokio::test]
async fn sync_status_returns_default() {
	let state = test_state_no_db();
	let app = oversync_api::router(state);

	let (status, json) = get_json(&app, "/sync/status").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["running"], false);
}

#[tokio::test]
async fn history_requires_db() {
	let state = test_state_no_db();
	let app = oversync_api::router(state);

	let (status, json) = get_json(&app, "/history").await;
	assert_eq!(status, StatusCode::OK); // Returns error json, not HTTP error
	assert!(json["error"].as_str().unwrap().contains("database"));
}

#[tokio::test]
async fn history_returns_empty_when_no_cycles() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = get_json(&app, "/history").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["cycles"].as_array().unwrap().len(), 0);
}

// ── Read-cache sync ─────────────────────────────────────────

#[tokio::test]
async fn get_sources_reflects_created_source() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	// Initially empty
	let (_, json) = get_json(&app, "/sources").await;
	assert_eq!(json["sources"].as_array().unwrap().len(), 0);

	// Create a source
	post_json(
		&app,
		"/sources",
		serde_json::json!({
			"name": "new-pg",
			"connector": "postgres",
		}),
	)
	.await;

	// GET /sources now shows it
	let (_, json) = get_json(&app, "/sources").await;
	let sources = json["sources"].as_array().unwrap();
	assert_eq!(sources.len(), 1);
	assert_eq!(sources[0]["name"], "new-pg");
	assert_eq!(sources[0]["connector"], "postgres");
}

#[tokio::test]
async fn get_sinks_reflects_created_sink() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/sinks",
		serde_json::json!({
			"name": "wh",
			"sink_type": "http",
		}),
	)
	.await;

	let (_, json) = get_json(&app, "/sinks").await;
	let sinks = json["sinks"].as_array().unwrap();
	assert_eq!(sinks.len(), 1);
	assert_eq!(sinks[0]["name"], "wh");
	assert_eq!(sinks[0]["sink_type"], "http");
}

// ── Error cases ─────────────────────────────────────────────

#[tokio::test]
async fn create_source_without_db_errors() {
	let state = test_state_no_db();
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/sources",
		serde_json::json!({
			"name": "x",
			"connector": "postgres",
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert!(json["error"].as_str().unwrap().contains("database"));
}
