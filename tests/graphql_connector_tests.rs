mod common;

use std::sync::Arc;

use axum::routing::post;
use axum::{Json, Router};
use tokio::net::TcpListener;

use oversync_connectors::GraphqlOriginFactory;
use oversync_connectors::graphql::{GraphqlConfig, GraphqlConnector, GraphqlPagination};
use oversync_connectors::http_common::AuthConfig;
use oversync_core::traits::{OriginConnector, OriginFactory};

async fn start_server(app: Router) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	tokio::spawn(async move {
		axum::serve(listener, app).await.unwrap();
	});
	format!("http://{addr}")
}

fn make_config(endpoint: &str) -> GraphqlConfig {
	serde_json::from_value(serde_json::json!({"dsn": endpoint})).unwrap()
}

// ── Basic query ─────────────────────────────────────────────

#[tokio::test]
async fn basic_query_returns_rows() {
	let app = Router::new().route(
		"/graphql",
		post(|body: Json<serde_json::Value>| async move {
			assert!(body.get("query").is_some());
			Json(serde_json::json!({
				"data": {
					"items": [
						{"id": "1", "name": "alpha"},
						{"id": "2", "name": "beta"},
					]
				}
			}))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.items".into());
	let connector = GraphqlConnector::new("test", config).unwrap();

	let rows = connector
		.fetch_all("{ items { id name } }", "id")
		.await
		.unwrap();
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0].row_key, "1");
	assert_eq!(rows[1].row_key, "2");
}

// ── Nested response_path ────────────────────────────────────

#[tokio::test]
async fn nested_response_path() {
	let app = Router::new().route(
		"/graphql",
		post(|| async {
			Json(serde_json::json!({
				"data": {
					"organization": {
						"repos": {
							"nodes": [
								{"id": "r1", "name": "repo-a"},
								{"id": "r2", "name": "repo-b"},
								{"id": "r3", "name": "repo-c"},
							]
						}
					}
				}
			}))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.organization.repos.nodes".into());
	let connector = GraphqlConnector::new("test", config).unwrap();

	let rows = connector
		.fetch_all(
			"query { organization { repos { nodes { id name } } } }",
			"id",
		)
		.await
		.unwrap();
	assert_eq!(rows.len(), 3);
	assert_eq!(rows[2].row_key, "r3");
}

// ── Relay cursor pagination ────────────────────────────────

#[tokio::test]
async fn relay_cursor_pagination_fetches_all_pages() {
	let all_items: Arc<Vec<serde_json::Value>> = Arc::new(
		(0..9)
			.map(|i| serde_json::json!({"id": format!("item-{i}"), "val": i}))
			.collect(),
	);

	let items = all_items.clone();
	let app = Router::new().route(
		"/graphql",
		post(move |body: Json<serde_json::Value>| {
			let items = items.clone();
			async move {
				let cursor = body["variables"]["cursor"]
					.as_str()
					.and_then(|s| s.parse::<usize>().ok())
					.unwrap_or(0);
				let page_size = 3;
				let page: Vec<_> = items.iter().skip(cursor).take(page_size).cloned().collect();
				let has_next = cursor + page_size < items.len();
				let end_cursor = if has_next {
					serde_json::json!((cursor + page_size).to_string())
				} else {
					serde_json::Value::Null
				};
				Json(serde_json::json!({
					"data": {
						"items": {
							"nodes": page,
							"pageInfo": {
								"hasNextPage": has_next,
								"endCursor": end_cursor,
							}
						}
					}
				}))
			}
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.items.nodes".into());
	config.pagination = Some(GraphqlPagination {
		cursor_variable: "cursor".into(),
		has_next_path: "pageInfo.hasNextPage".into(),
		end_cursor_path: "pageInfo.endCursor".into(),
	});
	let connector = GraphqlConnector::new("test", config).unwrap();

	let rows = connector
		.fetch_all("query($cursor: String) { items(after: $cursor) { nodes { id val } pageInfo { hasNextPage endCursor } } }", "id")
		.await
		.unwrap();
	assert_eq!(rows.len(), 9);
	assert_eq!(rows[0].row_key, "item-0");
	assert_eq!(rows[8].row_key, "item-8");
}

// ── Auth headers ────────────────────────────────────────────

#[tokio::test]
async fn auth_headers_forwarded() {
	let app = Router::new().route(
		"/graphql",
		post(|headers: axum::http::HeaderMap| async move {
			let auth = headers
				.get("authorization")
				.and_then(|v| v.to_str().ok())
				.unwrap_or("");
			if auth == "Bearer gql-secret" {
				Json(serde_json::json!({
					"data": {"items": [{"id": "ok"}]}
				}))
			} else {
				Json(serde_json::json!({
					"errors": [{"message": "unauthorized"}]
				}))
			}
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.items".into());
	config.auth = Some(AuthConfig::Bearer {
		token: "gql-secret".into(),
	});
	let connector = GraphqlConnector::new("test", config).unwrap();

	let rows = connector.fetch_all("{ items { id } }", "id").await.unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "ok");
}

// ── GraphQL errors ──────────────────────────────────────────

#[tokio::test]
async fn graphql_errors_returned_as_oversync_error() {
	let app = Router::new().route(
		"/graphql",
		post(|| async {
			Json(serde_json::json!({
				"data": null,
				"errors": [
					{"message": "Field 'bogus' not found"},
					{"message": "Syntax error"},
				]
			}))
		}),
	);
	let base = start_server(app).await;
	let config = make_config(&format!("{base}/graphql"));
	let connector = GraphqlConnector::new("test", config).unwrap();

	let result = connector.fetch_all("{ bogus }", "id").await;
	assert!(result.is_err());
	let err = result.unwrap_err().to_string();
	assert!(err.contains("Field 'bogus' not found"));
	assert!(err.contains("Syntax error"));
}

// ── Edge cases ──────────────────────────────────────────────

#[tokio::test]
async fn http_500_returns_error() {
	let app = Router::new().route(
		"/graphql",
		post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
	);
	let base = start_server(app).await;
	let config = make_config(&format!("{base}/graphql"));
	let connector = GraphqlConnector::new("test", config).unwrap();

	let result = connector.fetch_all("{ test }", "id").await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("500"));
}

#[tokio::test]
async fn empty_response_returns_empty_rows() {
	let app = Router::new().route(
		"/graphql",
		post(|| async {
			Json(serde_json::json!({
				"data": {"items": []}
			}))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.items".into());
	let connector = GraphqlConnector::new("test", config).unwrap();

	let rows = connector.fetch_all("{ items { id } }", "id").await.unwrap();
	assert!(rows.is_empty());
}

#[tokio::test]
async fn missing_key_column_errors() {
	let app = Router::new().route(
		"/graphql",
		post(|| async {
			Json(serde_json::json!({
				"data": {"items": [{"name": "no-id-field"}]}
			}))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.items".into());
	let connector = GraphqlConnector::new("test", config).unwrap();

	let result = connector.fetch_all("{ items { name } }", "id").await;
	assert!(result.is_err());
	assert!(
		result
			.unwrap_err()
			.to_string()
			.contains("missing key field")
	);
}

#[tokio::test]
async fn custom_headers_forwarded() {
	let app = Router::new().route(
		"/graphql",
		post(|headers: axum::http::HeaderMap| async move {
			let custom = headers
				.get("x-custom")
				.and_then(|v| v.to_str().ok())
				.unwrap_or("");
			Json(serde_json::json!({
				"data": {"items": [{"id": custom}]}
			}))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.items".into());
	config.headers.insert("X-Custom".into(), "test-val".into());
	let connector = GraphqlConnector::new("test", config).unwrap();

	let rows = connector.fetch_all("{ items { id } }", "id").await.unwrap();
	assert_eq!(rows[0].row_key, "test-val");
}

#[tokio::test]
async fn fetch_into_streams_pages() {
	let app = Router::new().route(
		"/graphql",
		post(|| async {
			Json(serde_json::json!({
				"data": {"items": [
					{"id": "1"}, {"id": "2"}, {"id": "3"},
					{"id": "4"}, {"id": "5"},
				]}
			}))
		}),
	);
	let base = start_server(app).await;
	let mut config = make_config(&format!("{base}/graphql"));
	config.response_path = Some("data.items".into());
	let connector = GraphqlConnector::new("test", config).unwrap();

	let (tx, mut rx) = tokio::sync::mpsc::channel(10);
	let total = connector
		.fetch_into("{ items { id } }", "id", 2, tx)
		.await
		.unwrap();
	assert_eq!(total, 5);

	let mut batches = vec![];
	while let Ok(batch) = rx.try_recv() {
		batches.push(batch);
	}
	assert_eq!(batches.len(), 3); // chunks of 2,2,1
	assert_eq!(batches[0].len(), 2);
	assert_eq!(batches[2].len(), 1);
}

// ── Factory ─────────────────────────────────────────────────

#[tokio::test]
async fn factory_creates_graphql_connector() {
	let app = Router::new().route(
		"/graphql",
		post(|| async {
			Json(serde_json::json!({
				"data": {"nodes": [{"id": "x"}]}
			}))
		}),
	);
	let base = start_server(app).await;

	let factory = GraphqlOriginFactory;
	assert_eq!(factory.connector_type(), "graphql");
	let config = serde_json::json!({
		"dsn": format!("{base}/graphql"),
		"response_path": "data.nodes",
	});
	let connector = factory.create("test-gql", &config).await.unwrap();
	assert_eq!(connector.name(), "test-gql");

	let rows = connector.fetch_all("{ nodes { id } }", "id").await.unwrap();
	assert_eq!(rows.len(), 1);
}
