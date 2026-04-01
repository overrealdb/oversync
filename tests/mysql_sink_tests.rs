mod common;

use common::mysql::TestMysql;
use oversync_core::model::{EventEnvelope, EventMeta, OpType};
use oversync_core::traits::{Sink, TargetFactory};
use oversync_sinks::{MysqlSink, MysqlTargetFactory};
use sqlx::Row;

const CREATE_TABLE: &str = r#"
CREATE TABLE `events` (
    `key` VARCHAR(255) PRIMARY KEY,
    `data` TEXT,
    `op` VARCHAR(50),
    `origin_id` VARCHAR(255),
    `query_id` VARCHAR(255),
    `hash` VARCHAR(255),
    `cycle_id` BIGINT,
    `synced_at` DATETIME DEFAULT CURRENT_TIMESTAMP
)
"#;

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

#[tokio::test]
async fn mysql_sink_upserts_single_event() {
	let mysql = TestMysql::new().await;
	mysql.run_sql(CREATE_TABLE).await;

	let sink = MysqlSink::from_pool("test", mysql.pool.clone(), "events");
	let envelope = make_envelope("k1", OpType::Created);
	sink.send_event(&envelope).await.unwrap();

	let row = sqlx::query(
        "SELECT `key`, `data`, `op`, `origin_id`, `query_id`, `hash`, `cycle_id` FROM events WHERE `key` = ?",
    )
    .bind("k1")
    .fetch_one(&mysql.pool)
    .await
    .unwrap();

	assert_eq!(row.get::<String, _>("key"), "k1");
	let data: String = row.get("data");
	let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
	assert_eq!(parsed["key"], "k1");
	assert_eq!(parsed["value"], "data_k1");
	assert_eq!(row.get::<String, _>("op"), "created");
	assert_eq!(row.get::<String, _>("origin_id"), "test-src");
	assert_eq!(row.get::<String, _>("query_id"), "test-q");
	assert_eq!(row.get::<String, _>("hash"), "hash_k1");
	assert_eq!(row.get::<i64, _>("cycle_id"), 1);
}

#[tokio::test]
async fn mysql_sink_upserts_batch() {
	let mysql = TestMysql::new().await;
	mysql.run_sql(CREATE_TABLE).await;

	let sink = MysqlSink::from_pool("test", mysql.pool.clone(), "events");
	let envelopes = vec![
		make_envelope("b1", OpType::Created),
		make_envelope("b2", OpType::Updated),
		make_envelope("b3", OpType::Deleted),
	];
	sink.send_batch(&envelopes).await.unwrap();

	let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
		.fetch_one(&mysql.pool)
		.await
		.unwrap();
	assert_eq!(count, 3);

	for key in ["b1", "b2", "b3"] {
		let row = sqlx::query("SELECT `key` FROM events WHERE `key` = ?")
			.bind(key)
			.fetch_one(&mysql.pool)
			.await
			.unwrap();
		assert_eq!(row.get::<String, _>("key"), key);
	}
}

#[tokio::test]
async fn mysql_sink_upsert_overwrites_on_conflict() {
	let mysql = TestMysql::new().await;
	mysql.run_sql(CREATE_TABLE).await;

	let sink = MysqlSink::from_pool("test", mysql.pool.clone(), "events");

	let first = make_envelope("dup", OpType::Created);
	sink.send_event(&first).await.unwrap();

	let second = EventEnvelope {
		meta: EventMeta {
			op: OpType::Updated,
			origin_id: "test-src".into(),
			query_id: "test-q".into(),
			key: "dup".into(),
			hash: "hash_dup_v2".into(),
			cycle_id: 2,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({"key": "dup", "value": "updated_data"}),
	};
	sink.send_event(&second).await.unwrap();

	let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE `key` = ?")
		.bind("dup")
		.fetch_one(&mysql.pool)
		.await
		.unwrap();
	assert_eq!(count, 1);

	let row = sqlx::query("SELECT `data`, `op`, `hash`, `cycle_id` FROM events WHERE `key` = ?")
		.bind("dup")
		.fetch_one(&mysql.pool)
		.await
		.unwrap();

	assert_eq!(row.get::<String, _>("op"), "updated");
	assert_eq!(row.get::<String, _>("hash"), "hash_dup_v2");
	assert_eq!(row.get::<i64, _>("cycle_id"), 2);
	let data: String = row.get("data");
	let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
	assert_eq!(parsed["value"], "updated_data");
}

#[tokio::test]
async fn mysql_sink_test_connection() {
	let mysql = TestMysql::new().await;
	let sink = MysqlSink::from_pool("test", mysql.pool.clone(), "events");
	sink.test_connection().await.unwrap();
}

#[tokio::test]
async fn mysql_sink_name() {
	let mysql = TestMysql::new().await;
	let sink = MysqlSink::from_pool("my-mysql-sink", mysql.pool.clone(), "events");
	assert_eq!(sink.name(), "my-mysql-sink");
}

#[tokio::test]
async fn mysql_sink_factory_creates_working_sink() {
	let mysql = TestMysql::new().await;
	mysql.run_sql(CREATE_TABLE).await;

	let factory = MysqlTargetFactory;
	assert_eq!(factory.sink_type(), "mysql");

	let config = serde_json::json!({
		"dsn": mysql.dsn,
		"table": "events",
	});
	let sink = factory.create("factory-sink", &config).await.unwrap();
	assert_eq!(sink.name(), "factory-sink");

	let envelope = make_envelope("fk1", OpType::Created);
	sink.send_event(&envelope).await.unwrap();

	let row = sqlx::query("SELECT `key` FROM events WHERE `key` = ?")
		.bind("fk1")
		.fetch_one(&mysql.pool)
		.await
		.unwrap();
	assert_eq!(row.get::<String, _>("key"), "fk1");
}

#[tokio::test]
async fn mysql_sink_factory_missing_dsn_errors() {
	let factory = MysqlTargetFactory;
	let config = serde_json::json!({"table": "events"});
	let err = factory
		.create("test", &config)
		.await
		.err()
		.expect("should fail without dsn");
	assert!(err.to_string().contains("missing 'dsn'"));
}

#[tokio::test]
async fn mysql_sink_factory_missing_table_errors() {
	let factory = MysqlTargetFactory;
	let config = serde_json::json!({"dsn": "mysql://localhost/test"});
	let err = factory
		.create("test", &config)
		.await
		.err()
		.expect("should fail without table");
	assert!(err.to_string().contains("missing 'table'"));
}
