mod common;

use axum::routing::{any, post, put};
use axum::{Json, Router};
use tokio::net::TcpListener;

use oversync_core::model::{AuthConfig, EventEnvelope, EventMeta, OpType};
use oversync_core::traits::{Sink, TargetFactory};
use oversync_sinks::http_sink::HttpSink;
use oversync_sinks::HttpTargetFactory;

async fn start_server(app: Router) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	tokio::spawn(async move {
		axum::serve(listener, app).await.unwrap();
	});
	format!("http://{addr}")
}

fn test_envelope(key: &str) -> EventEnvelope {
	EventEnvelope {
		meta: EventMeta {
			op: OpType::Created,
			origin_id: "test-src".into(),
			query_id: "test-q".into(),
			key: key.into(),
			hash: "abc123".into(),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({"id": key, "name": "test"}),
	}
}

fn make_sink(url: &str) -> HttpSink {
	HttpSink::new(
		"test",
		url,
		reqwest::Method::POST,
		Default::default(),
		None,
		30,
		0,
	)
	.unwrap()
}

// ── Single event ────────────────────────────────────────────

#[tokio::test]
async fn send_event_posts_json_body() {
	let app = Router::new().route(
		"/webhook",
		post(|body: Json<serde_json::Value>| async move {
			assert!(body.get("meta").is_some());
			assert!(body.get("data").is_some());
			assert_eq!(body["meta"]["key"], "k1");
			axum::http::StatusCode::OK
		}),
	);
	let base = start_server(app).await;
	let sink = make_sink(&format!("{base}/webhook"));

	sink.send_event(&test_envelope("k1")).await.unwrap();
}

#[tokio::test]
async fn send_event_forwards_auth_bearer() {
	let app = Router::new().route(
		"/secure",
		post(|headers: axum::http::HeaderMap| async move {
			let auth = headers
				.get("authorization")
				.and_then(|v| v.to_str().ok())
				.unwrap_or("");
			if auth == "Bearer webhook-secret" {
				axum::http::StatusCode::OK
			} else {
				axum::http::StatusCode::UNAUTHORIZED
			}
		}),
	);
	let base = start_server(app).await;
	let sink = HttpSink::new(
		"test",
		&format!("{base}/secure"),
		reqwest::Method::POST,
		Default::default(),
		Some(AuthConfig::Bearer {
			token: "webhook-secret".into(),
		}),
		30,
		0,
	)
	.unwrap();

	sink.send_event(&test_envelope("k1")).await.unwrap();
}

#[tokio::test]
async fn send_event_http_500_returns_error() {
	let app = Router::new().route(
		"/fail",
		post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
	);
	let base = start_server(app).await;
	let sink = make_sink(&format!("{base}/fail"));

	let result = sink.send_event(&test_envelope("k1")).await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("500"));
}

// ── Batch ───────────────────────────────────────────────────

#[tokio::test]
async fn send_batch_posts_array_body() {
	let app = Router::new().route(
		"/batch",
		post(|body: Json<Vec<serde_json::Value>>| async move {
			assert_eq!(body.len(), 3);
			assert!(body[0].get("meta").is_some());
			axum::http::StatusCode::OK
		}),
	);
	let base = start_server(app).await;
	let sink = make_sink(&format!("{base}/batch"));

	let envelopes = vec![
		test_envelope("k1"),
		test_envelope("k2"),
		test_envelope("k3"),
	];
	sink.send_batch(&envelopes).await.unwrap();
}

#[tokio::test]
async fn send_batch_empty_is_noop() {
	let sink = make_sink("http://127.0.0.1:1/never-called");
	sink.send_batch(&[]).await.unwrap();
}

// ── PUT method ──────────────────────────────────────────────

#[tokio::test]
async fn put_method_works() {
	let app = Router::new().route(
		"/webhook",
		put(|body: Json<serde_json::Value>| async move {
			assert!(body.get("meta").is_some());
			axum::http::StatusCode::OK
		}),
	);
	let base = start_server(app).await;
	let sink = HttpSink::new(
		"test",
		&format!("{base}/webhook"),
		reqwest::Method::PUT,
		Default::default(),
		None,
		30,
		0,
	)
	.unwrap();

	sink.send_event(&test_envelope("k1")).await.unwrap();
}

// ── Factory ─────────────────────────────────────────────────

#[tokio::test]
async fn factory_creates_http_sink() {
	let app = Router::new().route("/webhook", any(|| async { axum::http::StatusCode::OK }));
	let base = start_server(app).await;

	let factory = HttpTargetFactory;
	assert_eq!(factory.sink_type(), "http");
	let config = serde_json::json!({
		"url": format!("{base}/webhook"),
	});
	let sink = factory.create("test-http", &config).await.unwrap();
	assert_eq!(sink.name(), "test-http");
	sink.send_event(&test_envelope("k1")).await.unwrap();
}

#[tokio::test]
async fn factory_missing_url_errors() {
	let factory = HttpTargetFactory;
	let result = factory.create("test", &serde_json::json!({})).await;
	let err = result.err().expect("should be an error");
	assert!(err.to_string().contains("url"));
}

#[tokio::test]
async fn factory_with_put_method() {
	let app = Router::new().route("/wh", put(|| async { axum::http::StatusCode::OK }));
	let base = start_server(app).await;

	let factory = HttpTargetFactory;
	let config = serde_json::json!({
		"url": format!("{base}/wh"),
		"method": "PUT",
	});
	let sink = factory.create("put-sink", &config).await.unwrap();
	sink.send_event(&test_envelope("k1")).await.unwrap();
}

#[tokio::test]
async fn factory_with_auth_and_headers() {
	let app = Router::new().route(
		"/wh",
		post(|headers: axum::http::HeaderMap| async move {
			let auth = headers
				.get("authorization")
				.and_then(|v| v.to_str().ok())
				.unwrap_or("");
			let custom = headers
				.get("x-custom")
				.and_then(|v| v.to_str().ok())
				.unwrap_or("");
			if auth == "Bearer tok" && custom == "val" {
				axum::http::StatusCode::OK
			} else {
				axum::http::StatusCode::UNAUTHORIZED
			}
		}),
	);
	let base = start_server(app).await;

	let factory = HttpTargetFactory;
	let config = serde_json::json!({
		"url": format!("{base}/wh"),
		"auth": {"type": "bearer", "token": "tok"},
		"headers": {"X-Custom": "val"},
	});
	let sink = factory.create("auth-sink", &config).await.unwrap();
	sink.send_event(&test_envelope("k1")).await.unwrap();
}
