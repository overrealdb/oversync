mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{any, get, post};
use tokio::net::TcpListener;

use oversync_core::model::{EventEnvelope, EventMeta, OpType};
use oversync_core::traits::{Sink, TargetFactory};
use oversync_sinks::{ClickHouseSink, ClickHouseTargetFactory};

#[derive(Default, Clone)]
struct Captured {
	bodies: Arc<Mutex<Vec<String>>>,
	queries: Arc<Mutex<Vec<String>>>,
	headers: Arc<Mutex<Vec<HeaderMap>>>,
}

fn make_envelope(key: &str, op: OpType) -> EventEnvelope {
	EventEnvelope {
		meta: EventMeta {
			op,
			origin_id: "test-src".into(),
			query_id: "test-q".into(),
			key: key.into(),
			hash: format!("hash_{key}"),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({"key": key, "value": format!("data_{key}")}),
	}
}

async fn start_server(app: Router) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	tokio::spawn(async move {
		axum::serve(listener, app).await.unwrap();
	});
	format!("http://{addr}")
}

fn capture_handler() -> (Captured, Router) {
	let captured = Captured::default();
	let state = captured.clone();
	let app = Router::new()
		.route(
			"/",
			any(
				|State(cap): State<Captured>,
				 Query(params): Query<HashMap<String, String>>,
				 headers: HeaderMap,
				 body: Bytes| async move {
					let query = params.get("query").cloned().unwrap_or_default();
					cap.queries.lock().unwrap().push(query);
					cap.headers.lock().unwrap().push(headers);
					cap.bodies
						.lock()
						.unwrap()
						.push(String::from_utf8_lossy(&body).to_string());
					StatusCode::OK
				},
			),
		)
		.with_state(state);
	(captured, app)
}

fn make_sink(url: &str) -> ClickHouseSink {
	ClickHouseSink::new("test-ch", url, "events", None, "default".into(), None, 60).unwrap()
}

// ── Single event ───────────────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_sends_single_event() {
	let (captured, app) = capture_handler();
	let base = start_server(app).await;
	let sink = make_sink(&base);

	sink.send_event(&make_envelope("k1", OpType::Created))
		.await
		.unwrap();

	let bodies = captured.bodies.lock().unwrap();
	assert_eq!(bodies.len(), 1);
	let parsed: serde_json::Value = serde_json::from_str(&bodies[0]).unwrap();
	assert_eq!(parsed["key"], "k1");
	assert_eq!(parsed["op"], "created");
	assert_eq!(parsed["origin_id"], "test-src");
	assert_eq!(parsed["query_id"], "test-q");
	assert_eq!(parsed["hash"], "hash_k1");
	assert_eq!(parsed["cycle_id"], 1);
	assert!(parsed["data"].is_string());
	assert!(parsed["synced_at"].is_string());

	let queries = captured.queries.lock().unwrap();
	assert_eq!(queries[0], "INSERT INTO events FORMAT JSONEachRow");
}

// ── Batch ──────────────────────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_sends_batch() {
	let (captured, app) = capture_handler();
	let base = start_server(app).await;
	let sink = make_sink(&base);

	let envelopes = vec![
		make_envelope("a", OpType::Created),
		make_envelope("b", OpType::Updated),
		make_envelope("c", OpType::Deleted),
	];
	sink.send_batch(&envelopes).await.unwrap();

	let bodies = captured.bodies.lock().unwrap();
	assert_eq!(bodies.len(), 1);
	let lines: Vec<&str> = bodies[0].lines().collect();
	assert_eq!(lines.len(), 3);
	for line in &lines {
		let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
		assert!(parsed.is_object());
		assert!(parsed["key"].is_string());
	}
}

// ── Database qualification ─────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_with_database_qualification() {
	let (captured, app) = capture_handler();
	let base = start_server(app).await;
	let sink = ClickHouseSink::new(
		"test-ch",
		&base,
		"events",
		Some("analytics".into()),
		"default".into(),
		None,
		60,
	)
	.unwrap();

	sink.send_event(&make_envelope("k1", OpType::Created))
		.await
		.unwrap();

	let queries = captured.queries.lock().unwrap();
	assert_eq!(
		queries[0],
		"INSERT INTO analytics.events FORMAT JSONEachRow"
	);
}

// ── Auth: basic ────────────────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_auth_basic() {
	let (captured, app) = capture_handler();
	let base = start_server(app).await;
	let sink = ClickHouseSink::new(
		"test-ch",
		&base,
		"events",
		None,
		"admin".into(),
		Some("secret".into()),
		60,
	)
	.unwrap();

	sink.send_event(&make_envelope("k1", OpType::Created))
		.await
		.unwrap();

	let headers = captured.headers.lock().unwrap();
	let auth = headers[0]
		.get("authorization")
		.expect("Authorization header missing")
		.to_str()
		.unwrap();
	assert!(auth.starts_with("Basic "));
}

// ── Auth: header only ──────────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_auth_header_only() {
	let (captured, app) = capture_handler();
	let base = start_server(app).await;
	let sink =
		ClickHouseSink::new("test-ch", &base, "events", None, "myuser".into(), None, 60).unwrap();

	sink.send_event(&make_envelope("k1", OpType::Created))
		.await
		.unwrap();

	let headers = captured.headers.lock().unwrap();
	assert!(
		headers[0].get("authorization").is_none(),
		"should not have Authorization header"
	);
	let ch_user = headers[0]
		.get("x-clickhouse-user")
		.expect("X-ClickHouse-User header missing")
		.to_str()
		.unwrap();
	assert_eq!(ch_user, "myuser");
}

// ── test_connection ────────────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_test_connection() {
	let app = Router::new().route(
		"/",
		get(|Query(params): Query<HashMap<String, String>>| async move {
			if params.get("query").map(|q| q.as_str()) == Some("SELECT 1") {
				(StatusCode::OK, "1\n")
			} else {
				(StatusCode::BAD_REQUEST, "unexpected query")
			}
		}),
	);
	let base = start_server(app).await;
	let sink = make_sink(&base);

	sink.test_connection().await.unwrap();
}

// ── HTTP error ─────────────────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_http_error_returns_error() {
	let app = Router::new().route("/", post(|| async { StatusCode::INTERNAL_SERVER_ERROR }));
	let base = start_server(app).await;
	let sink = make_sink(&base);

	let result = sink.send_event(&make_envelope("k1", OpType::Created)).await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("500"));
}

// ── Factory ────────────────────────────────────────────────

#[tokio::test]
async fn clickhouse_sink_factory_creates_sink() {
	let (_, app) = capture_handler();
	let base = start_server(app).await;

	let factory = ClickHouseTargetFactory;
	assert_eq!(factory.sink_type(), "clickhouse");

	let config = serde_json::json!({
		"url": base,
		"table": "events",
		"user": "admin",
		"password": "secret",
	});
	let sink = factory.create("ch-factory", &config).await.unwrap();
	assert_eq!(sink.name(), "ch-factory");
	sink.send_event(&make_envelope("k1", OpType::Created))
		.await
		.unwrap();
}

#[tokio::test]
async fn clickhouse_sink_factory_missing_url_errors() {
	let factory = ClickHouseTargetFactory;
	let config = serde_json::json!({"table": "events"});
	let err = factory
		.create("test", &config)
		.await
		.err()
		.expect("should fail");
	assert!(err.to_string().contains("url"));
}

#[tokio::test]
async fn clickhouse_sink_factory_missing_table_errors() {
	let factory = ClickHouseTargetFactory;
	let config = serde_json::json!({"url": "http://localhost:8123"});
	let err = factory
		.create("test", &config)
		.await
		.err()
		.expect("should fail");
	assert!(err.to_string().contains("table"));
}
