mod common;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::RwLock;
use tower::ServiceExt;

use common::surreal::TestSurrealContainer;
use oversync::config::SurrealDbDef;
use oversync::config_db::{load_config_from_db, replace_config_in_db};
use oversync_api::state::*;

fn test_state_with_db(client: surrealdb::Surreal<surrealdb::engine::any::Any>) -> Arc<ApiState> {
	Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		pipe_presets: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(client.into()),
		lifecycle: None,
		api_key: None,
	})
}

fn test_state_no_db() -> Arc<ApiState> {
	Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		pipe_presets: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: None,
		lifecycle: None,
		api_key: None,
	})
}

async fn test_state_with_export_lifecycle(container: &TestSurrealContainer) -> Arc<ApiState> {
	let surreal_def = SurrealDbDef {
		url: TestSurrealContainer::url().await,
		username: "root".into(),
		password: "root".into(),
		namespace: container.ns.clone(),
		database: container.db.clone(),
		snapshot: None,
	};

	Arc::new(ApiState {
		sources: Arc::new(RwLock::new(vec![])),
		sinks: Arc::new(RwLock::new(vec![])),
		pipes: Arc::new(RwLock::new(vec![])),
		pipe_presets: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(container.client.clone().into()),
		lifecycle: Some(Arc::new(ExportLifecycle { surreal_def })),
		api_key: None,
	})
}

struct ExportLifecycle {
	surreal_def: SurrealDbDef,
}

#[async_trait]
impl LifecycleControl for ExportLifecycle {
	async fn restart_with_config_json(
		&self,
		_db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	) -> Result<(), oversync_core::error::OversyncError> {
		Ok(())
	}

	async fn export_config(
		&self,
		db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
		format: oversync_api::types::ExportConfigFormat,
	) -> Result<String, oversync_core::error::OversyncError> {
		let config = load_config_from_db(db, &self.surreal_def).await?;
		match format {
			oversync_api::types::ExportConfigFormat::Toml => toml::to_string_pretty(&config)
				.map_err(|e| {
					oversync_core::error::OversyncError::Config(format!(
						"serialize toml export: {e}"
					))
				}),
			oversync_api::types::ExportConfigFormat::Json => serde_json::to_string_pretty(&config)
				.map_err(|e| {
					oversync_core::error::OversyncError::Config(format!(
						"serialize json export: {e}"
					))
				}),
		}
	}

	async fn import_config(
		&self,
		db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
		format: oversync_api::types::ExportConfigFormat,
		content: &str,
	) -> Result<Vec<String>, oversync_core::error::OversyncError> {
		let mut config = match format {
			oversync_api::types::ExportConfigFormat::Toml => {
				oversync::config::SyncConfig::from_str(content)?
			}
			oversync_api::types::ExportConfigFormat::Json => serde_json::from_str(content)
				.map_err(|e| {
					oversync_core::error::OversyncError::Config(format!("parse JSON config: {e}"))
				})?,
		};

		let issues = oversync::config::validate_config(&config);
		let errors: Vec<String> = issues
			.iter()
			.filter(|issue| matches!(issue.severity, oversync::config::Severity::Error))
			.map(|issue| issue.message.clone())
			.collect();
		if !errors.is_empty() {
			return Err(oversync_core::error::OversyncError::Config(
				errors.join("; "),
			));
		}
		let warnings = issues
			.into_iter()
			.filter(|issue| matches!(issue.severity, oversync::config::Severity::Warning))
			.map(|issue| issue.message)
			.collect();

		config.surrealdb = self.surreal_def.clone();
		replace_config_in_db(db, &config).await?;
		Ok(warnings)
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
			"transform": "smt::normalize_users"
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
	assert_eq!(queries[0]["transform"], "smt::normalize_users");
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
			"transform": "smt::before"
		}),
	)
	.await;

	let (status, json) = put_json(
		&app,
		"/sources/pg/queries/q1",
		serde_json::json!({"query": "SELECT 2 AS id", "transform": "smt::after"}),
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
	assert_eq!(rows[0]["transform"], "smt::after");
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
		pipe_presets: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(container.client.clone().into()),
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
		pipe_presets: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(container.client.clone().into()),
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
		pipe_presets: Arc::new(RwLock::new(vec![])),
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
			"origin_credential": "catalog-cred",
			"trino_url": "http://trino:8080",
			"targets": ["kafka"],
			"schedule": {"interval_secs": 120},
			"recipe": {
				"type": "postgres_snapshot",
				"prefix": "some-postgresql-source",
				"entity_type_id": "postgres",
				"schema_id": "table",
				"schemas": ["public"]
			},
			"filters": [{"type": "keep", "field": "status", "equals": "active"}],
			"transforms": [{"type": "rename", "field": "legacy", "to": "current"}],
			"links": [{
				"name": "user-contact-email",
				"left_field": "email",
				"right_field": "email",
				"strategy": "exact",
				"target_origin": "contacts",
				"target_query": "emails"
			}]
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);
	assert!(json["message"].as_str().unwrap().contains("catalog-sync"));

	let mut res = container
		.client
		.query("SELECT * FROM pipe_config")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!rows.is_empty(),
		"pipe_config should contain at least one row after create, got: {rows:?}"
	);
	let debug_rows = rows.clone();
	let row = rows
		.into_iter()
		.find(|row| row["name"] == "catalog-sync")
		.unwrap_or_else(|| panic!("catalog-sync row should exist, got: {debug_rows:?}"));
	assert_eq!(row["origin_credential"], "catalog-cred");
	assert_eq!(row["trino_url"], "http://trino:8080");
	assert_eq!(row["recipe"]["type"], "postgres_snapshot");
	assert_eq!(row["recipe"]["prefix"], "some-postgresql-source");
	assert_eq!(row["filters"][0]["type"], "keep");
	assert_eq!(row["transforms"][0]["type"], "rename");
	assert_eq!(row["links"][0]["name"], "user-contact-email");
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
async fn create_manual_pipe_stores_queries_in_query_config() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "manual-pipe",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://localhost/db",
			"targets": ["stdout"],
			"queries": [{
				"id": "accounts",
				"sql": "SELECT id, name FROM accounts",
				"key_column": "id"
			}]
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut res = container
		.client
		.query("SELECT * FROM query_config WHERE origin_id = 'manual-pipe' AND name = 'accounts'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["query"], "SELECT id, name FROM accounts");
	assert_eq!(rows[0]["key_column"], "id");
}

#[tokio::test]
async fn update_pipe_replaces_manual_queries() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "manual-pipe",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://localhost/db",
			"queries": [{
				"id": "accounts",
				"sql": "SELECT id, name FROM accounts",
				"key_column": "id"
			}]
		}),
	)
	.await;

	let (status, json) = put_json(
		&app,
		"/pipes/manual-pipe",
		serde_json::json!({
			"queries": [{
				"id": "ledger",
				"sql": "SELECT ledger_id, balance FROM ledger",
				"key_column": "ledger_id"
			}]
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut res = container
		.client
		.query("SELECT name, query FROM query_config WHERE origin_id = 'manual-pipe' ORDER BY name")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["name"], "ledger");
	assert_eq!(rows[0]["query"], "SELECT ledger_id, balance FROM ledger");
}

#[tokio::test]
async fn create_pipe_preset_stores_in_db_and_lists_in_api() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/pipe-presets",
		serde_json::json!({
			"name": "columns-aspect",
			"description": "Reusable manual aspect preset",
			"spec": {
				"origin_connector": "postgres",
				"origin_dsn": "",
				"targets": ["stdout"],
				"queries": [{
					"id": "aspect-columns",
					"sql": "SELECT id, payload FROM columns",
					"key_column": "id"
				}],
				"schedule": { "interval_secs": 900, "missed_tick_policy": "skip" },
				"delta": { "diff_mode": "db", "fail_safe_threshold": 30 },
				"retry": { "max_retries": 3, "retry_base_delay_secs": 5 },
				"filters": [],
				"transforms": [],
				"links": []
			}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut res = container
		.client
		.query("SELECT * FROM pipe_preset_config WHERE name = 'columns-aspect'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["description"], "Reusable manual aspect preset");
	assert_eq!(
		rows[0]["spec"]["queries"][0]["name"],
		serde_json::Value::Null
	);
	assert_eq!(rows[0]["spec"]["queries"][0]["id"], "aspect-columns");

	let (status, json) = get_json(&app, "/pipe-presets").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["presets"][0]["name"], "columns-aspect");
	assert_eq!(
		json["presets"][0]["spec"]["queries"][0]["id"],
		"aspect-columns"
	);
}

#[tokio::test]
async fn update_pipe_preset_rewrites_spec() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipe-presets",
		serde_json::json!({
			"name": "catalog-template",
			"description": "Initial",
			"spec": {
				"origin_connector": "postgres",
				"origin_dsn": "",
				"targets": [],
				"queries": [],
				"schedule": { "interval_secs": 300, "missed_tick_policy": "skip" },
				"delta": { "diff_mode": "db", "fail_safe_threshold": 30 },
				"retry": { "max_retries": 3, "retry_base_delay_secs": 5 },
				"recipe": {
					"type": "postgres_metadata",
					"prefix": "some-postgresql-source",
					"entity_type_id": "postgres",
					"schema_id": "table",
					"schemas": ["public"]
				},
				"filters": [],
				"transforms": [],
				"links": []
			}
		}),
	)
	.await;

	let (status, json) = put_json(
		&app,
		"/pipe-presets/catalog-template",
		serde_json::json!({
			"description": "Updated metadata preset",
			"spec": {
				"origin_connector": "postgres",
				"origin_dsn": "",
				"targets": ["stdout"],
				"queries": [],
				"schedule": { "interval_secs": 600, "missed_tick_policy": "skip" },
				"delta": { "diff_mode": "memory", "fail_safe_threshold": 12.5 },
				"retry": { "max_retries": 7, "retry_base_delay_secs": 9 },
				"recipe": {
					"type": "postgres_snapshot",
					"prefix": "some-postgresql-source",
					"entity_type_id": "postgres",
					"schema_id": "table",
					"schemas": ["public", "analytics"]
				},
				"filters": [],
				"transforms": [],
				"links": []
			}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let (status, json) = get_json(&app, "/pipe-presets/catalog-template").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["description"], "Updated metadata preset");
	assert_eq!(json["spec"]["targets"][0], "stdout");
	assert_eq!(json["spec"]["delta"]["diff_mode"], "memory");
	assert_eq!(json["spec"]["recipe"]["type"], "postgres_snapshot");
	assert_eq!(json["spec"]["recipe"]["schemas"][1], "analytics");
}

#[tokio::test]
async fn create_pipe_preset_stores_in_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	let (status, json) = post_json(
		&app,
		"/pipe-presets",
		serde_json::json!({
			"name": "postgres-columns",
			"description": "Reusable manual aspect preset",
			"spec": {
				"origin_connector": "postgres",
				"origin_dsn": "postgres://postgres:postgres@127.0.0.1:55432/postgres",
				"origin_config": {},
				"targets": ["stdout"],
				"queries": [{
					"id": "aspect-columns",
					"sql": "SELECT id, payload FROM columns_view",
					"key_column": "id"
				}],
				"schedule": {"interval_secs": 600, "missed_tick_policy": "skip"},
				"delta": {"diff_mode": "db", "fail_safe_threshold": 30.0},
				"retry": {"max_retries": 3, "retry_base_delay_secs": 5},
				"filters": [],
				"transforms": [],
				"links": []
			}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut resp = container
		.client
		.query("SELECT * FROM pipe_preset_config WHERE name = 'postgres-columns'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["description"], "Reusable manual aspect preset");
	assert_eq!(rows[0]["spec"]["queries"][0]["id"], "aspect-columns");
}

#[tokio::test]
async fn update_pipe_preset_modifies_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipe-presets",
		serde_json::json!({
			"name": "postgres-columns",
			"description": "Initial preset",
			"spec": {
				"origin_connector": "postgres",
				"origin_dsn": "postgres://postgres:postgres@127.0.0.1:55432/postgres",
				"origin_config": {},
				"targets": ["stdout"],
				"queries": [{
					"id": "aspect-columns",
					"sql": "SELECT id, payload FROM columns_view",
					"key_column": "id"
				}],
				"schedule": {"interval_secs": 600},
				"delta": {"diff_mode": "db"},
				"retry": {},
				"filters": [],
				"transforms": [],
				"links": []
			}
		}),
	)
	.await;

	let (status, json) = put_json(
		&app,
		"/pipe-presets/postgres-columns",
		serde_json::json!({
			"description": "Updated preset",
			"spec": {
				"origin_connector": "postgres",
				"origin_dsn": "postgres://postgres:postgres@127.0.0.1:55432/postgres",
				"origin_config": {},
				"targets": ["stdout"],
				"queries": [{
					"id": "aspect-columns-v2",
					"sql": "SELECT id, payload, updated_at FROM columns_view",
					"key_column": "id"
				}],
				"schedule": {"interval_secs": 900},
				"delta": {"diff_mode": "memory", "fail_safe_threshold": 50.0},
				"retry": {"max_retries": 1},
				"filters": [],
				"transforms": [],
				"links": []
			}
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut resp = container
		.client
		.query("SELECT * FROM pipe_preset_config WHERE name = 'postgres-columns'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["description"], "Updated preset");
	assert_eq!(rows[0]["spec"]["queries"][0]["id"], "aspect-columns-v2");
	assert_eq!(rows[0]["spec"]["delta"]["diff_mode"], "memory");
}

#[tokio::test]
async fn export_config_returns_toml_from_db() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_export_lifecycle(&container).await;
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/sinks",
		serde_json::json!({
			"name": "stdout-main",
			"sink_type": "stdout",
			"config": {}
		}),
	)
	.await;

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "catalog-sync",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://localhost/db",
			"targets": ["stdout-main"],
			"schedule": {"interval_secs": 120},
			"delta": {"diff_mode": "db"},
			"recipe": {
				"type": "postgres_snapshot",
				"prefix": "some-postgresql-source",
				"entity_type_id": "postgres",
				"schema_id": "table",
				"schemas": ["public"]
			},
			"filters": [],
			"transforms": [],
			"links": []
		}),
	)
	.await;

	let (status, json) = get_json(&app, "/config/export?format=toml").await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["format"], "toml");

	let content = json["content"].as_str().unwrap();
	assert!(content.contains("[[sinks]]"));
	assert!(content.contains("name = \"catalog-sync\""));
	assert!(content.contains("type = \"postgres_snapshot\""));
	assert!(content.contains("prefix = \"some-postgresql-source\""));
	assert!(content.contains("targets = [\"stdout-main\"]"));
}

#[tokio::test]
async fn import_config_replaces_db_from_toml() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_export_lifecycle(&container).await;
	let app = oversync_api::router(state);

	let import_toml = r#"
[surrealdb]
url = "http://ignored-by-ui-import"
username = "ignored"
password = "ignored"
namespace = "ignored"
database = "ignored"

[[sinks]]
name = "stdout-main"
type = "stdout"

[[pipes]]
name = "catalog-sync"
targets = ["stdout-main"]

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[pipes.schedule]
interval_secs = 120

[pipes.delta]
diff_mode = "db"

[pipes.recipe]
type = "postgres_metadata"
prefix = "some-postgresql-source"
schemas = ["public"]
"#;

	let (status, json) = post_json(
		&app,
		"/config/import",
		serde_json::json!({
			"format": "toml",
			"content": import_toml
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);
	assert_eq!(json["warnings"], serde_json::json!([]));

	let mut sink_resp = container
		.client
		.query("SELECT * FROM sink_config WHERE name = 'stdout-main'")
		.await
		.unwrap();
	let sinks: Vec<serde_json::Value> = sink_resp.take(0).unwrap();
	assert_eq!(sinks.len(), 1);

	let mut pipe_resp = container
		.client
		.query("SELECT * FROM pipe_config WHERE name = 'catalog-sync'")
		.await
		.unwrap();
	let pipes: Vec<serde_json::Value> = pipe_resp.take(0).unwrap();
	assert_eq!(pipes.len(), 1);
	assert_eq!(pipes[0]["recipe"]["type"], "postgres_metadata");
	assert_eq!(pipes[0]["recipe"]["prefix"], "some-postgresql-source");
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
			"origin_credential": "rotated-cred",
			"trino_url": "http://new-trino:8080",
			"recipe": {
				"type": "postgres_metadata",
				"prefix": "datacat",
				"entity_type_id": "postgres",
				"schema_id": "table",
				"schemas": ["public", "analytics"]
			},
			"filters": [{"type": "drop", "field": "status", "equals": "deleted"}],
			"transforms": [{"type": "upper", "field": "name"}],
			"links": [{
				"name": "acct-owner",
				"left_field": "owner_id",
				"right_field": "id",
				"strategy": "exact",
				"target_origin": "owners",
				"target_query": "by-id"
			}],
			"enabled": false
		}),
	)
	.await;

	assert_eq!(status, StatusCode::OK);
	assert_eq!(json["ok"], true);

	let mut res = container
		.client
		.query("SELECT * FROM pipe_config")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!rows.is_empty(),
		"pipe_config should contain at least one row after update, got: {rows:?}"
	);
	let debug_rows = rows.clone();
	let row = rows
		.into_iter()
		.find(|row| row["name"] == "my-pipe")
		.unwrap_or_else(|| panic!("my-pipe row should exist, got: {debug_rows:?}"));
	assert_eq!(row["origin_dsn"], "postgres://new");
	assert_eq!(row["origin_credential"], "rotated-cred");
	assert_eq!(row["trino_url"], "http://new-trino:8080");
	assert_eq!(row["recipe"]["type"], "postgres_metadata");
	assert_eq!(row["recipe"]["schemas"][1], "analytics");
	assert_eq!(row["filters"][0]["type"], "drop");
	assert_eq!(row["transforms"][0]["type"], "upper");
	assert_eq!(row["links"][0]["target_origin"], "owners");
	assert_eq!(row["enabled"], false);
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
async fn list_pipes_includes_disabled_entries() {
	let container = TestSurrealContainer::new().await;
	let state = test_state_with_db(container.client.clone());
	let app = oversync_api::router(state);

	post_json(
		&app,
		"/pipes",
		serde_json::json!({
			"name": "disabled-pipe",
			"origin_connector": "postgres",
			"origin_dsn": "postgres://localhost/db"
		}),
	)
	.await;

	put_json(
		&app,
		"/pipes/disabled-pipe",
		serde_json::json!({
			"enabled": false
		}),
	)
	.await;

	let (status, json) = get_json(&app, "/pipes").await;
	assert_eq!(status, StatusCode::OK);
	let pipes = json["pipes"].as_array().unwrap();
	assert_eq!(pipes.len(), 1);
	assert_eq!(pipes[0]["name"], "disabled-pipe");
	assert_eq!(pipes[0]["enabled"], false);
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

	let (status, json) =
		post_json(&app, "/sources/nonexistent/trigger", serde_json::json!({})).await;

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
		pipe_presets: Arc::new(RwLock::new(vec![])),
		cycle_status: Arc::new(RwLock::new(HashMap::new())),
		db_client: Some(container.client.clone().into()),
		lifecycle: None,
		api_key: Some("secret".into()),
	});
	let app = oversync_api::router(state);

	// Helper that doesn't require JSON body in response
	async fn status_of(app: &axum::Router, method: &str, path: &str) -> StatusCode {
		let body = serde_json::to_vec(&serde_json::json!({"name":"x","connector":"pg","origin_connector":"pg","origin_dsn":"pg://","sink_type":"stdout"})).unwrap();
		let req = match method {
			"POST" => Request::post(path)
				.header("content-type", "application/json")
				.body(Body::from(body))
				.unwrap(),
			"PUT" => Request::put(path)
				.header("content-type", "application/json")
				.body(Body::from(body))
				.unwrap(),
			"DELETE" => Request::delete(path).body(Body::empty()).unwrap(),
			_ => panic!("unsupported method"),
		};
		app.clone().oneshot(req).await.unwrap().status()
	}

	assert_eq!(
		status_of(&app, "POST", "/sources").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "PUT", "/sources/x").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "DELETE", "/sources/x").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "POST", "/sinks").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "PUT", "/sinks/x").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "DELETE", "/sinks/x").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "POST", "/pipes").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "PUT", "/pipes/x").await,
		StatusCode::UNAUTHORIZED
	);
	assert_eq!(
		status_of(&app, "DELETE", "/pipes/x").await,
		StatusCode::UNAUTHORIZED
	);
}
