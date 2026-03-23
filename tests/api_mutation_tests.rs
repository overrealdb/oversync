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

fn test_state_with_db(client: surrealdb::Surreal<surrealdb::engine::any::Any>) -> Arc<ApiState> {
	Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(client),
		lifecycle: None,
		api_key: None,
	})
}

fn test_state_no_db() -> Arc<ApiState> {
	Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: None,
		lifecycle: None,
		api_key: None,
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

async fn delete_req(app: &axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
	let req = Request::delete(path).body(Body::empty()).unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	let status = resp.status();
	let bytes = resp.into_body().collect().await.unwrap().to_bytes();
	let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
	(status, json)
}

async fn get_json(app: &axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
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

// ── Query CRUD ──────────────────────────────────────────────

#[tokio::test]
async fn create_and_list_queries() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	// Create a source first
	post_json(
		&app,
		"/sources",
		serde_json::json!({"name": "pg", "connector": "postgres"}),
	)
	.await;

	// Create a query
	let (status, json) = post_json(
		&app,
		"/sources/pg/queries",
		serde_json::json!({
			"name": "users",
			"query": "SELECT id, name FROM users",
			"key_column": "id",
		}),
	)
	.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	// List queries
	let (status, json) = get_json(&app, "/sources/pg/queries").await;
	assert_eq!(status, StatusCode::OK);
	let queries = json["queries"].as_array().unwrap();
	assert_eq!(queries.len(), 1);
	assert_eq!(queries[0]["name"], "users");
	assert_eq!(queries[0]["key_column"], "id");
}

#[tokio::test]
async fn update_query_changes_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/sources",
		serde_json::json!({"name": "pg", "connector": "postgres"}),
	)
	.await;
	post_json(
		&app,
		"/sources/pg/queries",
		serde_json::json!({
			"name": "q1",
			"query": "SELECT 1 AS id",
			"key_column": "id",
		}),
	)
	.await;

	let (status, json) = put_json(
		&app,
		"/sources/pg/queries/q1",
		serde_json::json!({"query": "SELECT 2 AS id"}),
	)
	.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	// Verify
	let mut resp = container
		.client
		.query("SELECT * FROM query_config WHERE origin_id = 'pg' AND name = 'q1'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(rows[0]["query"], "SELECT 2 AS id");
}

#[tokio::test]
async fn delete_query_removes_from_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/sources",
		serde_json::json!({"name": "pg", "connector": "postgres"}),
	)
	.await;
	post_json(
		&app,
		"/sources/pg/queries",
		serde_json::json!({
			"name": "to-del",
			"query": "SELECT 1 AS id",
			"key_column": "id",
		}),
	)
	.await;

	let (status, json) = delete_req(&app, "/sources/pg/queries/to-del").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut resp = container
		.client
		.query("SELECT * FROM query_config WHERE origin_id = 'pg' AND name = 'to-del'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(rows.is_empty());
}

// ── Auth middleware ─────────────────────────────────────────

#[tokio::test]
async fn auth_key_blocks_unauthorized_requests() {
	let container = TestSurrealContainer::new().await;
	let state = Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(container.client.clone()),
		lifecycle: None,
		api_key: Some("secret-key".into()),
	});
	let app = oversync_api::router(state);

	// Health is public — no auth needed
	let (status, _) = get_json(&app, "/health").await;
	assert_eq!(status, StatusCode::OK);

	// Protected route without key → 401
	let req = Request::get("/sources").body(Body::empty()).unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_key_allows_authorized_requests() {
	let container = TestSurrealContainer::new().await;
	let state = Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(container.client.clone()),
		lifecycle: None,
		api_key: Some("secret-key".into()),
	});
	let app = oversync_api::router(state);

	// Bearer auth
	let req = Request::get("/sources")
		.header("authorization", "Bearer secret-key")
		.body(Body::empty())
		.unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	assert_eq!(resp.status(), StatusCode::OK);

	// X-API-Key header
	let req = Request::get("/sinks")
		.header("x-api-key", "secret-key")
		.body(Body::empty())
		.unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn auth_key_wrong_key_returns_401() {
	let state = Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: None,
		lifecycle: None,
		api_key: Some("correct-key".into()),
	});
	let app = oversync_api::router(state);

	let req = Request::get("/sources")
		.header("authorization", "Bearer wrong-key")
		.body(Body::empty())
		.unwrap();
	let resp = app.clone().oneshot(req).await.unwrap();
	assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
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

// ── Pipe CRUD ───────────────────────────────────────────────

#[tokio::test]
async fn create_pipe_stores_in_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "catalog-sync",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://localhost/db",
			"targets": ["kafka"],
			"schedule": {"interval_secs": 120}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);
	assert!(json["message"].as_str().unwrap().contains("catalog-sync"));
}

#[tokio::test]
async fn create_and_list_pipes() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "pipe-a",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://a/db"
		}),
	)
	.await;

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "pipe-b",
			"origin_connector": "mysql",
			"origin_dsn": "mysql://b/db"
		}),
	)
	.await;

	let (status, json) = get_json(&app, "/pipes").await;
	assert_eq!(status, StatusCode::OK);
	let pipes = json["pipes"].as_array().unwrap();
	assert_eq!(pipes.len(), 2);
}

#[tokio::test]
async fn update_pipe_modifies_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "my-pipe",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://old"
		}),
	)
	.await;

	let (status, json) = put_json(
		&app,
		"/pipes/my-pipe",
		serde_json::json!({
			"origin_dsn": "postgres://new",
			"enabled": false
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);
}

#[tokio::test]
async fn delete_pipe_removes_from_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "to-delete",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://localhost/db"
		}),
	)
	.await;

	let (status, json) = delete_req(&app, "/pipes/to-delete").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	// Verify deleted
	let (_, list) = get_json(&app, "/pipes").await;
	let pipes = list["pipes"].as_array().unwrap();
	assert!(pipes.is_empty());
}

#[tokio::test]
async fn get_pipe_by_name() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "findme",
			"origin_connector": "http",
			"origin_dsn": "https://api.example.com",
			"targets": ["stdout"]
		}),
	)
	.await;

	let (status, json) = get_json(&app, "/pipes/findme").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["name"], "findme");
	assert_eq!(json["origin_connector"], "http");
	assert_eq!(json["targets"][0], "stdout");
}

#[tokio::test]
async fn create_pipe_replaces_existing() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "dup",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://v1"
		}),
	)
	.await;

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "dup",
			"origin_connector": "mysql",
			"origin_dsn": "mysql://v2"
		}),
	)
	.await;

	let (_, json) = get_json(&app, "/pipes").await;
	let pipes = json["pipes"].as_array().unwrap();
	assert_eq!(pipes.len(), 1);
	assert_eq!(pipes[0]["origin_connector"], "mysql");
}

// ── Sink update ─────────────────────────────────────────────

#[tokio::test]
async fn update_sink_modifies_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/sinks",
		serde_json::json!({
			"name": "my-sink",
			"sink_type": "stdout",
		}),
	)
	.await;

	let (status, json) = put_json(
		&app,
		"/sinks/my-sink",
		serde_json::json!({
			"sink_type": "kafka",
			"config": {"brokers": "kafka:9092", "topic": "test"}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);
}

// ── Trigger source ──────────────────────────────────────────

#[tokio::test]
async fn trigger_source_without_lifecycle() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/sources/nonexistent/trigger",
		serde_json::json!({}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert!(json["error"].as_str().is_some() || json["message"].as_str().is_some());
}

// ── Auth on write endpoints ─────────────────────────────────

#[tokio::test]
async fn auth_blocks_write_endpoints() {
	let container = TestSurrealContainer::new().await;
	let state = Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(container.client.clone()),
		lifecycle: None,
		api_key: Some("secret".into()),
	});
	let app = oversync_api::router(state);

	// Helper that doesn't require JSON body in response
	async fn status_of(app: &axum::Router, method: &str, path: &str) -> StatusCode {
		let body = serde_json::to_vec(&serde_json::json!({"name":"x","connector":"pg","origin_connector":"pg","origin_dsn":"pg://","sink_type":"stdout"})).unwrap();
		let req = match method {
			"POST" => Request::post(path).header("content-type", "application/json").body(Body::from(body)).unwrap(),
			"PUT" => Request::put(path).header("content-type", "application/json").body(Body::from(body)).unwrap(),
			"DELETE" => Request::delete(path).body(Body::empty()).unwrap(),
			_ => panic!("unsupported method"),
		};
		app.clone().oneshot(req).await.unwrap().status()
	}

	assert_eq!(status_of(&app, "POST", "/sources").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "PUT", "/sources/x").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "DELETE", "/sources/x").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "POST", "/sinks").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "PUT", "/sinks/x").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "DELETE", "/sinks/x").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "POST", "/pipes").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "PUT", "/pipes/x").await, StatusCode::UNAUTHORIZED);
	assert_eq!(status_of(&app, "DELETE", "/pipes/x").await, StatusCode::UNAUTHORIZED);
}
