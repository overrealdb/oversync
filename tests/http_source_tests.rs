mod common;

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::Query;
use axum::routing::get;
use axum::{Json, Router};
use tokio::net::TcpListener;

use oversync_connectors::http_source::{AuthConfig, HttpSourceConfig, PaginationConfig};
use oversync_connectors::HttpSource;
use oversync_core::traits::OriginConnector;

async fn start_server(app: Router) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	tokio::spawn(async move {
		axum::serve(listener, app).await.unwrap();
	});
	format!("http://{addr}")
}

fn make_config(base_url: &str) -> HttpSourceConfig {
	serde_json::from_value(serde_json::json!({"dsn": base_url})).unwrap()
}

// ── Basic fetch ─────────────────────────────────────────────

#[tokio::test]
async fn fetch_top_level_array() {
	let app = Router::new().route(
		"/items",
		get(|| async {
			Json(serde_json::json!([
				{"id": "1", "name": "alpha"},
				{"id": "2", "name": "beta"},
				{"id": "3", "name": "gamma"},
			]))
		}),
	);
	let base = start_server(app).await;
	let source = HttpSource::new("test", make_config(&base)).unwrap();

	let rows = source.fetch_all("/items", "id").await.unwrap();
	assert_eq!(rows.len(), 3);
	assert_eq!(rows[0].row_key, "1");
	assert_eq!(rows[2].row_key, "3");
}

#[tokio::test]
async fn fetch_nested_response_path() {
	let app = Router::new().route(
		"/api",
		get(|| async {
			Json(serde_json::json!({
				"status": "ok",
				"data": {"items": [
					{"pk": "a", "val": 10},
					{"pk": "b", "val": 20},
				]}
			}))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&base);
	config.response_path = Some("data.items".into());
	let source = HttpSource::new("test", config).unwrap();

	let rows = source.fetch_all("/api", "pk").await.unwrap();
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0].row_key, "a");
}

// ── Auth ────────────────────────────────────────────────────

#[tokio::test]
async fn bearer_auth_sent() {
	let app = Router::new().route(
		"/secure",
		get(|headers: axum::http::HeaderMap| async move {
			let auth = headers
				.get("authorization")
				.and_then(|v| v.to_str().ok())
				.unwrap_or("");
			if auth == "Bearer my-secret-token" {
				Json(serde_json::json!([{"id": "ok"}]))
			} else {
				Json(serde_json::json!([]))
			}
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&base);
	config.auth = Some(AuthConfig::Bearer {
		token: "my-secret-token".into(),
	});
	let source = HttpSource::new("test", config).unwrap();

	let rows = source.fetch_all("/secure", "id").await.unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "ok");
}

#[tokio::test]
async fn custom_headers_sent() {
	let app = Router::new().route(
		"/headers",
		get(|headers: axum::http::HeaderMap| async move {
			let custom = headers
				.get("x-custom")
				.and_then(|v| v.to_str().ok())
				.unwrap_or("");
			Json(serde_json::json!([{"id": custom}]))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&base);
	config.headers.insert("X-Custom".into(), "hello".into());
	let source = HttpSource::new("test", config).unwrap();

	let rows = source.fetch_all("/headers", "id").await.unwrap();
	assert_eq!(rows[0].row_key, "hello");
}

// ── Offset pagination ───────────────────────────────────────

#[tokio::test]
async fn offset_pagination_fetches_all_pages() {
	let all_items: Arc<Vec<serde_json::Value>> = Arc::new(
		(0..25)
			.map(|i| serde_json::json!({"id": i.to_string(), "val": i}))
			.collect(),
	);

	let items = all_items.clone();
	let app = Router::new().route(
		"/paged",
		get(move |Query(params): Query<HashMap<String, String>>| {
			let items = items.clone();
			async move {
				let offset: usize = params
					.get("offset")
					.and_then(|s| s.parse().ok())
					.unwrap_or(0);
				let limit: usize = params
					.get("limit")
					.and_then(|s| s.parse().ok())
					.unwrap_or(10);
				let page: Vec<_> = items.iter().skip(offset).take(limit).cloned().collect();
				Json(serde_json::json!({"data": page}))
			}
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&base);
	config.response_path = Some("data".into());
	config.pagination = Some(PaginationConfig::Offset {
		page_size: 10,
		limit_param: "limit".into(),
		offset_param: "offset".into(),
	});
	let source = HttpSource::new("test", config).unwrap();

	let rows = source.fetch_all("/paged", "id").await.unwrap();
	assert_eq!(rows.len(), 25);
	assert_eq!(rows[0].row_key, "0");
	assert_eq!(rows[24].row_key, "24");
}

// ── Cursor pagination ───────────────────────────────────────

#[tokio::test]
async fn cursor_pagination_fetches_all_pages() {
	let all_items: Arc<Vec<serde_json::Value>> = Arc::new(
		(0..15)
			.map(|i| serde_json::json!({"id": i.to_string(), "val": i}))
			.collect(),
	);

	let items = all_items.clone();
	let app = Router::new().route(
		"/cursor",
		get(move |Query(params): Query<HashMap<String, String>>| {
			let items = items.clone();
			async move {
				let start: usize = params
					.get("after")
					.and_then(|s| s.parse().ok())
					.unwrap_or(0);
				let page_size = 5;
				let page: Vec<_> = items
					.iter()
					.skip(start)
					.take(page_size)
					.cloned()
					.collect();
				let next_cursor = if start + page_size < items.len() {
					serde_json::json!(start + page_size)
				} else {
					serde_json::Value::Null
				};
				Json(serde_json::json!({
					"items": page,
					"pagination": {"next_cursor": next_cursor}
				}))
			}
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&base);
	config.response_path = Some("items".into());
	config.pagination = Some(PaginationConfig::Cursor {
		page_size: 5,
		cursor_param: "after".into(),
		cursor_path: "pagination.next_cursor".into(),
	});
	let source = HttpSource::new("test", config).unwrap();

	let rows = source.fetch_all("/cursor", "id").await.unwrap();
	assert_eq!(rows.len(), 15);
	assert_eq!(rows[0].row_key, "0");
	assert_eq!(rows[14].row_key, "14");
}

// ── fetch_into streaming ────────────────────────────────────

#[tokio::test]
async fn fetch_into_streams_paginated_batches() {
	let all_items: Arc<Vec<serde_json::Value>> = Arc::new(
		(0..12)
			.map(|i| serde_json::json!({"id": i.to_string(), "v": i}))
			.collect(),
	);

	let items = all_items.clone();
	let app = Router::new().route(
		"/stream",
		get(move |Query(params): Query<HashMap<String, String>>| {
			let items = items.clone();
			async move {
				let offset: usize = params
					.get("offset")
					.and_then(|s| s.parse().ok())
					.unwrap_or(0);
				let limit: usize = params
					.get("limit")
					.and_then(|s| s.parse().ok())
					.unwrap_or(5);
				let page: Vec<_> = items.iter().skip(offset).take(limit).cloned().collect();
				Json(page)
			}
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&base);
	config.pagination = Some(PaginationConfig::Offset {
		page_size: 5,
		limit_param: "limit".into(),
		offset_param: "offset".into(),
	});
	let source = HttpSource::new("test", config).unwrap();

	let (tx, mut rx) = tokio::sync::mpsc::channel(10);
	let total = source.fetch_into("/stream", "id", 100, tx).await.unwrap();
	assert_eq!(total, 12);

	let mut batches = Vec::new();
	while let Ok(batch) = rx.try_recv() {
		batches.push(batch);
	}
	assert_eq!(batches.len(), 3); // 5 + 5 + 2
	assert_eq!(batches[0].len(), 5);
	assert_eq!(batches[1].len(), 5);
	assert_eq!(batches[2].len(), 2);
}

// ── Error handling ──────────────────────────────────────────

#[tokio::test]
async fn http_404_returns_error() {
	let app = Router::new();
	let base = start_server(app).await;
	let source = HttpSource::new("test", make_config(&base)).unwrap();

	let result = source.fetch_all("/nonexistent", "id").await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("404"));
}

#[tokio::test]
async fn http_500_returns_error() {
	let app = Router::new().route(
		"/fail",
		get(|| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
	);
	let base = start_server(app).await;
	let source = HttpSource::new("test", make_config(&base)).unwrap();

	let result = source.fetch_all("/fail", "id").await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("500"));
}

// ── Factory integration ─────────────────────────────────────

#[tokio::test]
async fn factory_creates_http_source() {
	use oversync_connectors::HttpOriginFactory;
	use oversync_core::traits::OriginFactory;

	let app = Router::new().route(
		"/test",
		get(|| async { Json(serde_json::json!([{"id": "x"}])) }),
	);
	let base = start_server(app).await;

	let factory = HttpOriginFactory;
	assert_eq!(factory.connector_type(), "http");
	let config = serde_json::json!({"dsn": base});
	let source = factory.create("test-http", &config).await.unwrap();
	assert_eq!(source.name(), "test-http");

	let rows = source.fetch_all("/test", "id").await.unwrap();
	assert_eq!(rows.len(), 1);
}
