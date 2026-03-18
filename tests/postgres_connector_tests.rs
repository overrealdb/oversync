mod common;

use oversync_connectors::PostgresConnector;
use oversync_core::traits::SourceConnector;

use common::postgres::TestPostgres;

#[tokio::test]
async fn test_connection_succeeds() {
	let pg = TestPostgres::new().await;
	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	conn.test_connection().await.unwrap();
}

#[tokio::test]
async fn fetch_all_returns_rows() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('c', 'gamma', 3)")
		.await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(
			&format!("SELECT id, name, value FROM {}.items", pg.schema),
			"id",
		)
		.await
		.unwrap();

	assert_eq!(rows.len(), 3);
	let keys: Vec<&str> = rows.iter().map(|r| r.row_key.as_str()).collect();
	assert!(keys.contains(&"a"));
	assert!(keys.contains(&"b"));
	assert!(keys.contains(&"c"));
}

#[tokio::test]
async fn fetch_all_row_data_contains_columns() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE users (uid TEXT PRIMARY KEY, email TEXT)")
		.await;
	pg.run_sql("INSERT INTO users VALUES ('u1', 'alice@example.com')")
		.await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(
			&format!("SELECT uid, email FROM {}.users", pg.schema),
			"uid",
		)
		.await
		.unwrap();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "u1");
	assert_eq!(rows[0].row_data["email"], "alice@example.com");
}

#[tokio::test]
async fn fetch_all_empty_table() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE empty_tbl (id TEXT PRIMARY KEY)")
		.await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(&format!("SELECT id FROM {}.empty_tbl", pg.schema), "id")
		.await
		.unwrap();

	assert!(rows.is_empty());
}

#[tokio::test]
async fn fetch_all_bad_key_column_errors() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE t (id TEXT PRIMARY KEY, val INT)")
		.await;
	pg.run_sql("INSERT INTO t VALUES ('x', 1)").await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let result = conn
		.fetch_all(
			&format!("SELECT id, val FROM {}.t", pg.schema),
			"nonexistent",
		)
		.await;

	assert!(result.is_err());
}

#[tokio::test]
async fn fetch_all_with_int_key() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE nums (oid TEXT PRIMARY KEY, pages INT)")
		.await;
	pg.run_sql("INSERT INTO nums VALUES ('100', 5)").await;
	pg.run_sql("INSERT INTO nums VALUES ('200', 10)").await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(&format!("SELECT oid, pages FROM {}.nums", pg.schema), "oid")
		.await
		.unwrap();

	assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn fetch_all_null_values() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE nulls (id TEXT PRIMARY KEY, val TEXT, num INT)")
		.await;
	pg.run_sql("INSERT INTO nulls (id) VALUES ('k1')").await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(
			&format!("SELECT id, val, num FROM {}.nulls", pg.schema),
			"id",
		)
		.await
		.unwrap();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_data["val"], serde_json::Value::Null);
	assert_eq!(rows[0].row_data["num"], serde_json::Value::Null);
}

#[tokio::test]
async fn fetch_all_boolean_column() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE flags (id TEXT PRIMARY KEY, active BOOL)")
		.await;
	pg.run_sql("INSERT INTO flags VALUES ('a', true)").await;
	pg.run_sql("INSERT INTO flags VALUES ('b', false)").await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(&format!("SELECT id, active FROM {}.flags", pg.schema), "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 2);
	let a = rows.iter().find(|r| r.row_key == "a").unwrap();
	let b = rows.iter().find(|r| r.row_key == "b").unwrap();
	assert_eq!(a.row_data["active"], true);
	assert_eq!(b.row_data["active"], false);
}

#[tokio::test]
async fn fetch_all_float_column() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE measures (id TEXT PRIMARY KEY, val FLOAT8)")
		.await;
	pg.run_sql("INSERT INTO measures VALUES ('m1', 3.14)").await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(&format!("SELECT id, val FROM {}.measures", pg.schema), "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 1);
	let val = rows[0].row_data["val"].as_f64().unwrap();
	assert!((val - 3.14).abs() < 0.001);
}

#[tokio::test]
async fn fetch_all_jsonb_column() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE docs (id TEXT PRIMARY KEY, meta JSONB)")
		.await;
	pg.run_sql("INSERT INTO docs VALUES ('d1', '{\"tags\": [\"a\", \"b\"]}')")
		.await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(&format!("SELECT id, meta FROM {}.docs", pg.schema), "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_data["meta"]["tags"][0], "a");
	assert_eq!(rows[0].row_data["meta"]["tags"][1], "b");
}

#[tokio::test]
async fn fetch_all_invalid_sql_errors() {
	let pg = TestPostgres::new().await;
	let conn = PostgresConnector::from_pool("test", pg.pool.clone());

	let result = conn.fetch_all("SELECT FROM NONEXISTENT", "id").await;
	assert!(result.is_err());
}

#[tokio::test]
async fn fetch_all_large_dataset() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE big (id TEXT PRIMARY KEY, v INT)")
		.await;
	for i in 0..500 {
		pg.run_sql(&format!("INSERT INTO big VALUES ('r{i}', {i})"))
			.await;
	}

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let rows = conn
		.fetch_all(&format!("SELECT id, v FROM {}.big", pg.schema), "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 500);
}

#[tokio::test]
async fn fetch_all_hash_stability() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE stable (id TEXT PRIMARY KEY, name TEXT, val INT)")
		.await;
	pg.run_sql("INSERT INTO stable VALUES ('k', 'hello', 42)")
		.await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let r1 = conn
		.fetch_all(
			&format!("SELECT id, name, val FROM {}.stable", pg.schema),
			"id",
		)
		.await
		.unwrap();
	let r2 = conn
		.fetch_all(
			&format!("SELECT id, name, val FROM {}.stable", pg.schema),
			"id",
		)
		.await
		.unwrap();

	assert_eq!(
		oversync_core::model::hash_row_data(&r1[0].row_data),
		oversync_core::model::hash_row_data(&r2[0].row_data),
	);
}

// --- fetch_into streaming tests ---

use tokio::sync::mpsc;

#[tokio::test]
async fn fetch_into_streams_all_rows() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE stream10 (id TEXT PRIMARY KEY, v INT)")
		.await;
	for i in 0..10 {
		pg.run_sql(&format!("INSERT INTO stream10 VALUES ('r{i}', {i})"))
			.await;
	}

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let sql = format!("SELECT id, v FROM {}.stream10", pg.schema);
	let (tx, mut rx) = mpsc::channel::<Vec<oversync_core::model::RawRow>>(2);

	let handle = tokio::spawn(async move { conn.fetch_into(&sql, "id", 3, tx).await.unwrap() });

	let mut batches = Vec::new();
	while let Some(batch) = rx.recv().await {
		batches.push(batch);
	}
	let total = handle.await.unwrap();

	assert_eq!(total, 10);
	assert_eq!(batches.len(), 4);
	assert_eq!(batches[0].len(), 3);
	assert_eq!(batches[1].len(), 3);
	assert_eq!(batches[2].len(), 3);
	assert_eq!(batches[3].len(), 1);
	let row_count: usize = batches.iter().map(|b| b.len()).sum();
	assert_eq!(row_count, 10);
}

#[tokio::test]
async fn fetch_into_single_batch() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE stream2 (id TEXT PRIMARY KEY, v INT)")
		.await;
	pg.run_sql("INSERT INTO stream2 VALUES ('a', 1)").await;
	pg.run_sql("INSERT INTO stream2 VALUES ('b', 2)").await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let sql = format!("SELECT id, v FROM {}.stream2", pg.schema);
	let (tx, mut rx) = mpsc::channel::<Vec<oversync_core::model::RawRow>>(2);

	let handle = tokio::spawn(async move { conn.fetch_into(&sql, "id", 100, tx).await.unwrap() });

	let mut batches = Vec::new();
	while let Some(batch) = rx.recv().await {
		batches.push(batch);
	}
	let total = handle.await.unwrap();

	assert_eq!(total, 2);
	assert_eq!(batches.len(), 1);
	assert_eq!(batches[0].len(), 2);
}

#[tokio::test]
async fn fetch_into_empty_table() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE stream_empty (id TEXT PRIMARY KEY)")
		.await;

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let sql = format!("SELECT id FROM {}.stream_empty", pg.schema);
	let (tx, mut rx) = mpsc::channel::<Vec<oversync_core::model::RawRow>>(2);

	let handle = tokio::spawn(async move { conn.fetch_into(&sql, "id", 10, tx).await.unwrap() });

	let mut batches = Vec::new();
	while let Some(batch) = rx.recv().await {
		batches.push(batch);
	}
	let total = handle.await.unwrap();

	assert_eq!(total, 0);
	assert_eq!(batches.len(), 0);
}

#[tokio::test]
async fn fetch_into_matches_fetch_all() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE stream_cmp (id TEXT PRIMARY KEY, name TEXT, val INT)")
		.await;
	for i in 0..7 {
		pg.run_sql(&format!(
			"INSERT INTO stream_cmp VALUES ('k{i}', 'name{i}', {i})"
		))
		.await;
	}

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let sql = format!(
		"SELECT id, name, val FROM {}.stream_cmp ORDER BY id",
		pg.schema
	);

	let all_rows = conn.fetch_all(&sql, "id").await.unwrap();

	let (tx, mut rx) = mpsc::channel::<Vec<oversync_core::model::RawRow>>(2);
	let sql2 = sql.clone();
	let conn2 = PostgresConnector::from_pool("test", pg.pool.clone());
	let handle = tokio::spawn(async move { conn2.fetch_into(&sql2, "id", 3, tx).await.unwrap() });

	let mut streamed_rows = Vec::new();
	while let Some(batch) = rx.recv().await {
		streamed_rows.extend(batch);
	}
	handle.await.unwrap();

	assert_eq!(all_rows.len(), streamed_rows.len());

	let mut all_keys: Vec<String> = all_rows.iter().map(|r| r.row_key.clone()).collect();
	let mut stream_keys: Vec<String> = streamed_rows.iter().map(|r| r.row_key.clone()).collect();
	all_keys.sort();
	stream_keys.sort();
	assert_eq!(all_keys, stream_keys);

	for key in &all_keys {
		let a = all_rows.iter().find(|r| &r.row_key == key).unwrap();
		let s = streamed_rows.iter().find(|r| &r.row_key == key).unwrap();
		assert_eq!(
			oversync_core::model::hash_row_data(&a.row_data),
			oversync_core::model::hash_row_data(&s.row_data),
		);
	}
}

#[tokio::test]
async fn fetch_into_large_streaming() {
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE stream1k (id TEXT PRIMARY KEY, v INT)")
		.await;
	for i in 0..1000 {
		pg.run_sql(&format!("INSERT INTO stream1k VALUES ('r{i}', {i})"))
			.await;
	}

	let conn = PostgresConnector::from_pool("test", pg.pool.clone());
	let sql = format!("SELECT id, v FROM {}.stream1k", pg.schema);
	let (tx, mut rx) = mpsc::channel::<Vec<oversync_core::model::RawRow>>(2);

	let handle = tokio::spawn(async move { conn.fetch_into(&sql, "id", 100, tx).await.unwrap() });

	let mut batches = Vec::new();
	while let Some(batch) = rx.recv().await {
		batches.push(batch);
	}
	let total = handle.await.unwrap();

	assert_eq!(total, 1000);
	assert_eq!(batches.len(), 10);
	let row_count: usize = batches.iter().map(|b| b.len()).sum();
	assert_eq!(row_count, 1000);
}
