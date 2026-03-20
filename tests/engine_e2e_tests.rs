//! Engine end-to-end tests: full pipeline source → delta → sink
//! through OversyncEngine with real SurrealDB and mock HTTP servers.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::net::TcpListener;

use common::surreal::TestSurrealContainer;
use oversync::config::*;
use oversync::OversyncEngine;

async fn start_mock(app: Router) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	tokio::spawn(async move {
		axum::serve(listener, app).await.unwrap();
	});
	format!("http://{addr}")
}

/// Build engine with real SurrealDB container (state + snapshot both with schema).
async fn build_engine(
	state: &TestSurrealContainer,
	snap: &TestSurrealContainer,
) -> OversyncEngine {
	let url = TestSurrealContainer::url().await;
	OversyncEngine::builder(&url)
		.namespace(&state.ns)
		.database(&state.db)
		.snapshot_url(&url)
		.snapshot_namespace(&snap.ns)
		.snapshot_database(&snap.db)
		.skip_schema(true) // containers already applied schema
		.build()
		.await
		.unwrap()
}

fn make_config(
	state: &TestSurrealContainer,
	snap: &TestSurrealContainer,
	source: SourceDef,
	sinks: Vec<SinkDef>,
) -> SyncConfig {
	let url = "unused-engine-already-connected".into();
	SyncConfig {
		surrealdb: SurrealDbDef {
			url,
			username: "root".into(),
			password: "root".into(),
			namespace: state.ns.clone(),
			database: state.db.clone(),
			snapshot: Some(SnapshotDbDef {
				url: "unused".into(),
				username: "root".into(),
				password: "root".into(),
				namespace: snap.ns.clone(),
				database: snap.db.clone(),
			}),
		},
		sources: vec![source],
		sinks,
	}
}

// ── HTTP Source → Stdout Sink (full pipeline) ───────────────

#[tokio::test]
async fn engine_http_source_to_stdout_produces_cycle() {
	let state_db = TestSurrealContainer::new().await;
	let snap_db = TestSurrealContainer::new().await;

	let app = Router::new().route(
		"/items",
		get(|| async {
			Json(serde_json::json!([
				{"id": "1", "name": "alpha"},
				{"id": "2", "name": "beta"},
			]))
		}),
	);
	let source_url = start_mock(app).await;

	let engine = build_engine(&state_db, &snap_db).await;

	let source = SourceDef {
		name: "mock-http".into(),
		connector: "http".into(),
		dsn: source_url.clone(),
		interval_secs: 9999,
		fail_safe_threshold: 30.0,
		max_retries: 0,
		retry_base_delay_secs: 1,
		diff_mode: DiffMode::Memory,
			missed_tick_policy: Default::default(),
		config: serde_json::json!({"dsn": source_url}),
		queries: vec![QueryDef {
			id: "items".into(),
			sql: "/items".into(),
			key_column: "id".into(),
			sinks: None,
			transform: None,
		}],
	};
	let sinks = vec![SinkDef {
		name: "debug".into(),
		sink_type: "stdout".into(),
		config: serde_json::json!({}),
	}];

	engine
		.start(make_config(&state_db, &snap_db, source, sinks))
		.await
		.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;

	let mut resp = state_db
		.client
		.query("SELECT * FROM sync_mock_http_cycle_log WHERE source_id = 'mock-http'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(!rows.is_empty(), "expected at least one cycle_log entry");
	assert_eq!(rows[0]["status"], "success");
	assert_eq!(rows[0]["rows_created"], serde_json::json!(2));

	engine.shutdown().await;
}

// ── HTTP Source → HTTP Sink (webhook delivery) ──────────────

#[tokio::test]
async fn engine_http_source_to_http_sink_delivers_events() {
	let state_db = TestSurrealContainer::new().await;
	let snap_db = TestSurrealContainer::new().await;
	let received = Arc::new(AtomicUsize::new(0));

	let source_app = Router::new().route(
		"/data",
		get(|| async {
			Json(serde_json::json!([
				{"id": "a", "val": 1},
				{"id": "b", "val": 2},
				{"id": "c", "val": 3},
			]))
		}),
	);
	let source_url = start_mock(source_app).await;

	let recv = received.clone();
	let sink_app = Router::new().route(
		"/webhook",
		post(move |body: Json<serde_json::Value>| {
			let recv = recv.clone();
			async move {
				if let Some(arr) = body.as_array() {
					recv.fetch_add(arr.len(), Ordering::Relaxed);
				} else {
					recv.fetch_add(1, Ordering::Relaxed);
				}
				axum::http::StatusCode::OK
			}
		}),
	);
	let sink_url = start_mock(sink_app).await;

	let engine = build_engine(&state_db, &snap_db).await;

	let source = SourceDef {
		name: "src".into(),
		connector: "http".into(),
		dsn: source_url.clone(),
		interval_secs: 9999,
		fail_safe_threshold: 30.0,
		max_retries: 0,
		retry_base_delay_secs: 1,
		diff_mode: DiffMode::Memory,
			missed_tick_policy: Default::default(),
		config: serde_json::json!({"dsn": source_url}),
		queries: vec![QueryDef {
			id: "data".into(),
			sql: "/data".into(),
			key_column: "id".into(),
			sinks: None,
			transform: None,
		}],
	};
	let sinks = vec![SinkDef {
		name: "webhook".into(),
		sink_type: "http".into(),
		config: serde_json::json!({"url": format!("{sink_url}/webhook")}),
	}];

	engine
		.start(make_config(&state_db, &snap_db, source, sinks))
		.await
		.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;

	let count = received.load(Ordering::Relaxed);
	assert!(count >= 3, "expected >=3 events, got {count}");

	engine.shutdown().await;
}

// ── GraphQL Source (full pipeline) ──────────────────────────

#[tokio::test]
async fn engine_graphql_source_produces_cycle() {
	let state_db = TestSurrealContainer::new().await;
	let snap_db = TestSurrealContainer::new().await;

	let gql_app = Router::new().route(
		"/graphql",
		post(|| async {
			Json(serde_json::json!({
				"data": {
					"items": [
						{"id": "g1", "title": "first"},
						{"id": "g2", "title": "second"},
					]
				}
			}))
		}),
	);
	let gql_url = start_mock(gql_app).await;
	let gql_endpoint = format!("{gql_url}/graphql");

	let engine = build_engine(&state_db, &snap_db).await;

	let source = SourceDef {
		name: "gql-src".into(),
		connector: "graphql".into(),
		dsn: gql_endpoint.clone(),
		interval_secs: 9999,
		fail_safe_threshold: 30.0,
		max_retries: 0,
		retry_base_delay_secs: 1,
		diff_mode: DiffMode::Memory,
			missed_tick_policy: Default::default(),
		config: serde_json::json!({
			"dsn": gql_endpoint,
			"response_path": "data.items",
		}),
		queries: vec![QueryDef {
			id: "items".into(),
			sql: "{ items { id title } }".into(),
			key_column: "id".into(),
			sinks: None,
			transform: None,
		}],
	};

	engine
		.start(make_config(
			&state_db,
			&snap_db,
			source,
			vec![SinkDef {
				name: "debug".into(),
				sink_type: "stdout".into(),
				config: serde_json::json!({}),
			}],
		))
		.await
		.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;

	let mut resp = state_db
		.client
		.query("SELECT * FROM sync_gql_src_cycle_log WHERE source_id = 'gql-src'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(!rows.is_empty());
	assert_eq!(rows[0]["status"], "success");
	assert_eq!(rows[0]["rows_created"], serde_json::json!(2));

	engine.shutdown().await;
}

// ── Stable data: second cycle detects no changes ────────────

#[tokio::test]
async fn engine_second_cycle_detects_no_changes() {
	let state_db = TestSurrealContainer::new().await;
	let snap_db = TestSurrealContainer::new().await;

	let app = Router::new().route(
		"/items",
		get(|| async {
			Json(serde_json::json!([{"id": "1", "name": "stable"}]))
		}),
	);
	let source_url = start_mock(app).await;

	let engine = build_engine(&state_db, &snap_db).await;

	let source = SourceDef {
		name: "stable-src".into(),
		connector: "http".into(),
		dsn: source_url.clone(),
		interval_secs: 1,
		fail_safe_threshold: 30.0,
		max_retries: 0,
		retry_base_delay_secs: 1,
		diff_mode: DiffMode::Memory,
			missed_tick_policy: Default::default(),
		config: serde_json::json!({"dsn": source_url}),
		queries: vec![QueryDef {
			id: "items".into(),
			sql: "/items".into(),
			key_column: "id".into(),
			sinks: None,
			transform: None,
		}],
	};

	engine
		.start(make_config(
			&state_db,
			&snap_db,
			source,
			vec![SinkDef {
				name: "debug".into(),
				sink_type: "stdout".into(),
				config: serde_json::json!({}),
			}],
		))
		.await
		.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(4)).await;

	let mut resp = state_db
		.client
		.query("SELECT * FROM sync_stable_src_cycle_log WHERE source_id = 'stable-src' ORDER BY cycle_id ASC")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
	assert!(rows.len() >= 2, "expected >= 2 cycles, got {}", rows.len());

	// First cycle: 1 created
	assert_eq!(rows[0]["rows_created"], serde_json::json!(1));
	// Second cycle: no changes
	assert_eq!(rows[1]["rows_created"], serde_json::json!(0));
	assert_eq!(rows[1]["rows_updated"], serde_json::json!(0));
	assert_eq!(rows[1]["rows_deleted"], serde_json::json!(0));

	engine.shutdown().await;
}
