mod common;

use oversync_connectors::MysqlConnector;
use oversync_core::traits::SourceConnector;

use common::mysql::TestMysql;

#[tokio::test]
async fn test_connection_succeeds() {
	let my = TestMysql::new().await;
	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	conn.test_connection().await.unwrap();
}

#[tokio::test]
async fn fetch_all_returns_rows() {
	let my = TestMysql::new().await;

	my.run_sql("CREATE TABLE items (id VARCHAR(64) PRIMARY KEY, name VARCHAR(255), value INT)")
		.await;
	my.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	my.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;
	my.run_sql("INSERT INTO items VALUES ('c', 'gamma', 3)")
		.await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let rows = conn.fetch_all("SELECT id, name, value FROM items", "id").await.unwrap();

	assert_eq!(rows.len(), 3);
	let keys: Vec<&str> = rows.iter().map(|r| r.row_key.as_str()).collect();
	assert!(keys.contains(&"a"));
	assert!(keys.contains(&"b"));
	assert!(keys.contains(&"c"));
}

#[tokio::test]
async fn fetch_all_column_data() {
	let my = TestMysql::new().await;

	my.run_sql("CREATE TABLE items (id VARCHAR(64) PRIMARY KEY, name VARCHAR(255), value INT)")
		.await;
	my.run_sql("INSERT INTO items VALUES ('a', 'alpha', 42)")
		.await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let rows = conn.fetch_all("SELECT id, name, value FROM items", "id").await.unwrap();

	assert_eq!(rows[0].row_data["name"], "alpha");
	assert_eq!(rows[0].row_data["value"], 42);
}

#[tokio::test]
async fn fetch_all_empty_table() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE empty_t (id VARCHAR(64) PRIMARY KEY)").await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let rows = conn.fetch_all("SELECT id FROM empty_t", "id").await.unwrap();
	assert!(rows.is_empty());
}

#[tokio::test]
async fn fetch_all_null_values() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE nullable (id VARCHAR(64) PRIMARY KEY, val VARCHAR(255))")
		.await;
	my.run_sql("INSERT INTO nullable VALUES ('a', NULL)").await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let rows = conn.fetch_all("SELECT id, val FROM nullable", "id").await.unwrap();
	assert!(rows[0].row_data["val"].is_null());
}

#[tokio::test]
async fn fetch_all_bad_key_column_errors() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE t (id VARCHAR(64) PRIMARY KEY)").await;
	my.run_sql("INSERT INTO t VALUES ('a')").await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let result = conn.fetch_all("SELECT id FROM t", "nonexistent").await;
	assert!(result.is_err());
}

#[tokio::test]
async fn fetch_all_float_column() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE floats (id VARCHAR(64) PRIMARY KEY, val DOUBLE)")
		.await;
	my.run_sql("INSERT INTO floats VALUES ('a', 3.14)").await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let rows = conn.fetch_all("SELECT id, val FROM floats", "id").await.unwrap();
	let val = rows[0].row_data["val"].as_f64().unwrap();
	assert!((val - 3.14).abs() < 0.001);
}

#[tokio::test]
async fn fetch_all_bigint_column() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE big (id VARCHAR(64) PRIMARY KEY, val BIGINT)")
		.await;
	my.run_sql("INSERT INTO big VALUES ('a', 9223372036854775807)")
		.await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let rows = conn.fetch_all("SELECT id, val FROM big", "id").await.unwrap();
	assert_eq!(rows[0].row_data["val"], 9223372036854775807_i64);
}

#[tokio::test]
async fn fetch_all_json_column() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE jsons (id VARCHAR(64) PRIMARY KEY, data JSON)")
		.await;
	my.run_sql(r#"INSERT INTO jsons VALUES ('a', '{"key": "val"}')"#)
		.await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let rows = conn.fetch_all("SELECT id, data FROM jsons", "id").await.unwrap();
	assert_eq!(rows[0].row_data["data"]["key"], "val");
}

#[tokio::test]
async fn fetch_into_streams_all_rows() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE stream_t (id VARCHAR(64) PRIMARY KEY, val INT)")
		.await;
	for i in 0..20 {
		my.run_sql(&format!("INSERT INTO stream_t VALUES ('r{i}', {i})"))
			.await;
	}

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let (tx, mut rx) = tokio::sync::mpsc::channel(10);
	let total = conn
		.fetch_into("SELECT id, val FROM stream_t", "id", 5, tx)
		.await
		.unwrap();

	assert_eq!(total, 20);
	let mut count = 0;
	while let Some(batch) = rx.recv().await {
		count += batch.len();
	}
	assert_eq!(count, 20);
}

#[tokio::test]
async fn fetch_into_matches_fetch_all() {
	let my = TestMysql::new().await;
	my.run_sql("CREATE TABLE match_t (id VARCHAR(64) PRIMARY KEY, name VARCHAR(255))")
		.await;
	my.run_sql("INSERT INTO match_t VALUES ('a', 'alpha')").await;
	my.run_sql("INSERT INTO match_t VALUES ('b', 'beta')").await;

	let conn = MysqlConnector::from_pool("test", my.pool.clone());
	let all = conn
		.fetch_all("SELECT id, name FROM match_t ORDER BY id", "id")
		.await
		.unwrap();

	let (tx, mut rx) = tokio::sync::mpsc::channel(10);
	conn.fetch_into("SELECT id, name FROM match_t ORDER BY id", "id", 100, tx)
		.await
		.unwrap();

	let mut streamed = Vec::new();
	while let Some(batch) = rx.recv().await {
		streamed.extend(batch);
	}

	assert_eq!(all.len(), streamed.len());
	for (a, b) in all.iter().zip(streamed.iter()) {
		assert_eq!(a.row_key, b.row_key);
		assert_eq!(a.row_data, b.row_data);
	}
}
