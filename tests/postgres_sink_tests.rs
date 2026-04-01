mod common;

use oversync_core::model::{EventEnvelope, EventMeta, OpType};
use oversync_core::traits::{Sink, TargetFactory};
use oversync_sinks::{PostgresSink, PostgresTargetFactory};

use common::postgres::TestPostgres;

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

const CREATE_TABLE: &str = r#"
CREATE TABLE events (
    key TEXT PRIMARY KEY,
    data JSONB,
    op TEXT,
    origin_id TEXT,
    query_id TEXT,
    hash TEXT,
    cycle_id BIGINT,
    synced_at TIMESTAMPTZ DEFAULT NOW()
)
"#;

#[tokio::test]
async fn postgres_sink_upserts_single_event() {
	let pg = TestPostgres::new().await;
	pg.run_sql(CREATE_TABLE).await;

	let sink = PostgresSink::from_pool("test", pg.pool.clone(), "events", &pg.schema);
	let envelope = make_envelope("row_1", OpType::Created);
	sink.send_event(&envelope).await.unwrap();

	let row = sqlx::query_as::<
		_,
		(
			String,
			serde_json::Value,
			String,
			String,
			String,
			String,
			i64,
		),
	>(&format!(
		"SELECT key, data, op, origin_id, query_id, hash, cycle_id FROM {}.events WHERE key = $1",
		pg.schema
	))
	.bind("row_1")
	.fetch_one(&pg.pool)
	.await
	.unwrap();

	assert_eq!(row.0, "row_1");
	assert_eq!(row.1["key"], "row_1");
	assert_eq!(row.1["value"], "data_row_1");
	assert_eq!(row.2, "created");
	assert_eq!(row.3, "test-src");
	assert_eq!(row.4, "test-q");
	assert_eq!(row.5, "hash_row_1");
	assert_eq!(row.6, 1);
}

#[tokio::test]
async fn postgres_sink_upserts_batch() {
	let pg = TestPostgres::new().await;
	pg.run_sql(CREATE_TABLE).await;

	let sink = PostgresSink::from_pool("test", pg.pool.clone(), "events", &pg.schema);
	let envelopes = vec![
		make_envelope("a", OpType::Created),
		make_envelope("b", OpType::Updated),
		make_envelope("c", OpType::Deleted),
	];
	sink.send_batch(&envelopes).await.unwrap();

	let rows = sqlx::query_as::<_, (String, String)>(&format!(
		"SELECT key, op FROM {}.events ORDER BY key",
		pg.schema
	))
	.fetch_all(&pg.pool)
	.await
	.unwrap();

	assert_eq!(rows.len(), 3);
	assert_eq!(rows[0], ("a".to_string(), "created".to_string()));
	assert_eq!(rows[1], ("b".to_string(), "updated".to_string()));
	assert_eq!(rows[2], ("c".to_string(), "deleted".to_string()));
}

#[tokio::test]
async fn postgres_sink_upsert_overwrites_on_conflict() {
	let pg = TestPostgres::new().await;
	pg.run_sql(CREATE_TABLE).await;

	let sink = PostgresSink::from_pool("test", pg.pool.clone(), "events", &pg.schema);

	let first = make_envelope("dup", OpType::Created);
	sink.send_event(&first).await.unwrap();

	let second = EventEnvelope {
		meta: EventMeta {
			op: OpType::Updated,
			origin_id: "test-src".into(),
			query_id: "test-q".into(),
			key: "dup".into(),
			hash: "hash_v2".into(),
			cycle_id: 2,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({"key": "dup", "value": "updated_data"}),
	};
	sink.send_event(&second).await.unwrap();

	let row = sqlx::query_as::<_, (String, serde_json::Value, String, String, i64)>(&format!(
		"SELECT key, data, op, hash, cycle_id FROM {}.events WHERE key = $1",
		pg.schema
	))
	.bind("dup")
	.fetch_one(&pg.pool)
	.await
	.unwrap();

	assert_eq!(row.0, "dup");
	assert_eq!(row.1["value"], "updated_data");
	assert_eq!(row.2, "updated");
	assert_eq!(row.3, "hash_v2");
	assert_eq!(row.4, 2);

	let count = sqlx::query_as::<_, (i64,)>(&format!(
		"SELECT COUNT(*) FROM {}.events WHERE key = $1",
		pg.schema
	))
	.bind("dup")
	.fetch_one(&pg.pool)
	.await
	.unwrap();

	assert_eq!(count.0, 1);
}

#[tokio::test]
async fn postgres_sink_test_connection() {
	let pg = TestPostgres::new().await;
	let sink = PostgresSink::from_pool("test", pg.pool.clone(), "events", &pg.schema);
	sink.test_connection().await.unwrap();
}

#[tokio::test]
async fn postgres_sink_name() {
	let pg = TestPostgres::new().await;
	let sink = PostgresSink::from_pool("my-pg-sink", pg.pool.clone(), "events", &pg.schema);
	assert_eq!(sink.name(), "my-pg-sink");
}

#[tokio::test]
async fn postgres_sink_factory_creates_working_sink() {
	let pg = TestPostgres::new().await;
	pg.run_sql(CREATE_TABLE).await;

	let factory = PostgresTargetFactory;
	assert_eq!(factory.sink_type(), "postgres");

	let config = serde_json::json!({
		"dsn": pg.dsn,
		"table": "events",
		"schema": pg.schema,
	});
	let sink = factory.create("factory-pg", &config).await.unwrap();

	sink.send_event(&make_envelope("fac_1", OpType::Created))
		.await
		.unwrap();

	let row = sqlx::query_as::<_, (String, String)>(&format!(
		"SELECT key, op FROM {}.events WHERE key = $1",
		pg.schema
	))
	.bind("fac_1")
	.fetch_one(&pg.pool)
	.await
	.unwrap();

	assert_eq!(row.0, "fac_1");
	assert_eq!(row.1, "created");
}

#[tokio::test]
async fn postgres_sink_factory_missing_dsn_errors() {
	let factory = PostgresTargetFactory;
	let config = serde_json::json!({"table": "events"});
	let result = factory.create("x", &config).await;
	let err = result.err().expect("should error");
	assert!(err.to_string().contains("dsn"));
}

#[tokio::test]
async fn postgres_sink_factory_missing_table_errors() {
	let factory = PostgresTargetFactory;
	let config = serde_json::json!({"dsn": "postgres://localhost/test"});
	let result = factory.create("x", &config).await;
	let err = result.err().expect("should error");
	assert!(err.to_string().contains("table"));
}
