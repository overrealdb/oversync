mod common;

use oversync_connectors::trino::{TrinoConfig, TrinoConnector};
use oversync_core::traits::OriginConnector;

use common::trino::TestTrino;

fn make_connector(trino: &TestTrino) -> TrinoConnector {
	let config: TrinoConfig = serde_json::from_value(trino.config()).unwrap();
	TrinoConnector::new("test", config).unwrap()
}

// ── Connection ──────────────────────────────────────────────

#[tokio::test]
async fn test_connection_succeeds() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);
	conn.test_connection().await.unwrap();
}

// ── Basic fetch ─────────────────────────────────────────────

#[tokio::test]
async fn fetch_all_system_table() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(
			"SELECT node_id, http_uri FROM system.runtime.nodes",
			"node_id",
		)
		.await
		.unwrap();

	assert!(!rows.is_empty());
	assert!(!rows[0].row_key.is_empty());
	assert!(rows[0].row_data.get("http_uri").is_some());
}

#[tokio::test]
async fn fetch_all_memory_table() {
	let trino = TestTrino::new().await;

	trino
		.run_sql("CREATE TABLE memory.default.items (id VARCHAR, name VARCHAR, value INTEGER)")
		.await;
	trino
		.run_sql("INSERT INTO memory.default.items VALUES ('a', 'alpha', 1), ('b', 'beta', 2), ('c', 'gamma', 3)")
		.await;

	let conn = make_connector(&trino);
	let rows = conn
		.fetch_all("SELECT id, name, value FROM memory.default.items", "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 3);
	let mut keys: Vec<&str> = rows.iter().map(|r| r.row_key.as_str()).collect();
	keys.sort();
	assert_eq!(keys, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn fetch_all_empty_table() {
	let trino = TestTrino::new().await;

	trino
		.run_sql("CREATE TABLE memory.default.empty_t (id VARCHAR)")
		.await;

	let conn = make_connector(&trino);
	let rows = conn
		.fetch_all("SELECT id FROM memory.default.empty_t", "id")
		.await
		.unwrap();
	assert!(rows.is_empty());
}

// ── Type mapping (aligned with trino-jdbc BaseTestJdbcResultSet) ─

#[tokio::test]
async fn fetch_all_boolean() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all("SELECT CAST('x' AS VARCHAR) AS id, true AS val", "id")
		.await
		.unwrap();
	assert_eq!(rows[0].row_data["val"], true);
}

#[tokio::test]
async fn fetch_all_integer_types() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(
			"SELECT 'k' AS id, CAST(1 AS TINYINT) AS t, CAST(2 AS SMALLINT) AS s, CAST(3 AS INTEGER) AS i, CAST(4 AS BIGINT) AS b",
			"id",
		)
		.await
		.unwrap();

	assert_eq!(rows[0].row_data["t"], 1);
	assert_eq!(rows[0].row_data["s"], 2);
	assert_eq!(rows[0].row_data["i"], 3);
	assert_eq!(rows[0].row_data["b"], 4);
}

#[tokio::test]
async fn fetch_all_real_double() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(
			"SELECT 'k' AS id, CAST(3.14 AS DOUBLE) AS dbl, CAST(2.5 AS REAL) AS rl",
			"id",
		)
		.await
		.unwrap();

	let dbl = rows[0].row_data["dbl"].as_f64().unwrap();
	assert!((dbl - 3.14).abs() < 0.001);
	let rl = rows[0].row_data["rl"].as_f64().unwrap();
	assert!((rl - 2.5).abs() < 0.1);
}

#[tokio::test]
async fn fetch_all_varchar_char() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(
			"SELECT 'k' AS id, CAST('hello' AS VARCHAR) AS v, CAST('ab' AS CHAR(5)) AS c",
			"id",
		)
		.await
		.unwrap();

	assert_eq!(rows[0].row_data["v"], "hello");
	// CHAR pads with spaces
	assert!(rows[0].row_data["c"].as_str().unwrap().starts_with("ab"));
}

#[tokio::test]
async fn fetch_all_decimal() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(
			"SELECT 'k' AS id, CAST(123.45 AS DECIMAL(10,2)) AS d",
			"id",
		)
		.await
		.unwrap();

	// Decimal comes as string to preserve precision
	let d = &rows[0].row_data["d"];
	assert!(d.is_string() || d.is_number(), "decimal should be string or number: {d}");
}

#[tokio::test]
async fn fetch_all_date() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all("SELECT 'k' AS id, DATE '2024-01-15' AS d", "id")
		.await
		.unwrap();

	let d = rows[0].row_data["d"].as_str().unwrap();
	assert!(d.contains("2024"), "date should contain year: {d}");
}

#[tokio::test]
async fn fetch_all_timestamp() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(
			"SELECT 'k' AS id, TIMESTAMP '2024-01-15 10:30:00' AS ts",
			"id",
		)
		.await
		.unwrap();

	let ts = rows[0].row_data["ts"].as_str().unwrap();
	assert!(ts.contains("2024"), "timestamp should contain year: {ts}");
}

#[tokio::test]
async fn fetch_all_null_values() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(
			"SELECT 'k' AS id, CAST(NULL AS VARCHAR) AS v, CAST(NULL AS INTEGER) AS i",
			"id",
		)
		.await
		.unwrap();

	assert!(rows[0].row_data["v"].is_null());
	assert!(rows[0].row_data["i"].is_null());
}

#[tokio::test]
async fn fetch_all_array_type() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all("SELECT 'k' AS id, ARRAY[1, 2, 3] AS arr", "id")
		.await
		.unwrap();

	let arr = rows[0].row_data["arr"].as_array().unwrap();
	assert_eq!(arr.len(), 3);
}

#[tokio::test]
async fn fetch_all_json_type() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let rows = conn
		.fetch_all(r#"SELECT 'k' AS id, JSON '{"key": "val"}' AS j"#, "id")
		.await
		.unwrap();

	// JSON comes as string in Trino REST protocol
	let j = &rows[0].row_data["j"];
	assert!(j.is_string() || j.is_object(), "json should be string or object: {j}");
}

// ── Large dataset ───────────────────────────────────────────

#[tokio::test]
async fn fetch_all_large_dataset() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	// Generate 10K rows via sequence
	let rows = conn
		.fetch_all(
			"SELECT CAST(x AS VARCHAR) AS id, x AS val FROM UNNEST(sequence(1, 10000)) AS t(x)",
			"id",
		)
		.await
		.unwrap();

	assert_eq!(rows.len(), 10000);
}

// ── Streaming ───────────────────────────────────────────────

#[tokio::test]
async fn fetch_into_streams_pages() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let (tx, mut rx) = tokio::sync::mpsc::channel(10);
	let total = conn
		.fetch_into(
			"SELECT CAST(x AS VARCHAR) AS id, x AS val FROM UNNEST(sequence(1, 500)) AS t(x)",
			"id",
			100,
			tx,
		)
		.await
		.unwrap();

	assert_eq!(total, 500);
	let mut count = 0;
	while let Some(batch) = rx.recv().await {
		assert!(!batch.is_empty());
		count += batch.len();
	}
	assert_eq!(count, 500);
}

#[tokio::test]
async fn fetch_into_matches_fetch_all() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let sql = "SELECT CAST(x AS VARCHAR) AS id, x AS val FROM UNNEST(sequence(1, 100)) AS t(x)";

	let all = conn.fetch_all(sql, "id").await.unwrap();

	let (tx, mut rx) = tokio::sync::mpsc::channel(10);
	conn.fetch_into(sql, "id", 100, tx).await.unwrap();

	let mut streamed = Vec::new();
	while let Some(batch) = rx.recv().await {
		streamed.extend(batch);
	}

	assert_eq!(all.len(), streamed.len());
	// Both should have same keys (order may differ due to Trino parallelism)
	let mut all_keys: Vec<&str> = all.iter().map(|r| r.row_key.as_str()).collect();
	let mut stream_keys: Vec<&str> = streamed.iter().map(|r| r.row_key.as_str()).collect();
	all_keys.sort();
	stream_keys.sort();
	assert_eq!(all_keys, stream_keys);
}

// ── Error handling ──────────────────────────────────────────

#[tokio::test]
async fn query_error_returns_error() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let result = conn
		.fetch_all("SELECT * FROM nonexistent_catalog.nonexistent_schema.nonexistent_table", "id")
		.await;

	assert!(result.is_err());
}

#[tokio::test]
async fn bad_key_column_errors() {
	let trino = TestTrino::new().await;
	let conn = make_connector(&trino);

	let result = conn
		.fetch_all("SELECT 'a' AS id, 1 AS val", "nonexistent")
		.await;

	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("nonexistent"));
}

// ── Factory ─────────────────────────────────────────────────

#[tokio::test]
async fn factory_creates_connector() {
	use oversync_connectors::TrinoOriginFactory;
	use oversync_core::traits::OriginFactory;

	let trino = TestTrino::new().await;
	let factory = TrinoOriginFactory;
	assert_eq!(factory.connector_type(), "trino");

	let source = factory.create("my-trino", &trino.config()).await.unwrap();
	assert_eq!(source.name(), "my-trino");
	source.test_connection().await.unwrap();
}
