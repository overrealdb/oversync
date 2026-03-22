mod common;

use std::sync::Arc;

use oversync::cycle::{CycleConfig, CycleRunner};
use oversync_connectors::PostgresConnector;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutSink;

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;

fn cycle_config(schema: &str, transform: Option<&str>) -> CycleConfig {
	CycleConfig {
		origin_id: "pg-test".into(),
		query_id: "items".into(),
		sql: format!("SELECT id, name, value FROM {schema}.items"),
		key_column: "id".into(),
		fail_safe_threshold: 30.0,
		diff_mode: oversync::config::DiffMode::Db,
		transform: transform.map(String::from),
	}
}

#[tokio::test]
async fn transform_none_passes_all_events() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sink = Arc::new(StdoutSink::new(false));
	let sinks: Vec<Arc<dyn Sink>> = vec![sink.clone()];

	let runner = CycleRunner::new(&engine, &connector, &sinks);
	let diff = runner.run(&cycle_config(&pg.schema, None)).await.unwrap();

	assert_eq!(diff.created.len(), 2);
	assert_eq!(sink.sent_events().len(), 2);
}

#[tokio::test]
async fn transform_identity_passes_all_events() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	surreal
		.client
		.query("DEFINE FUNCTION fn::smt::identity($events: array) { RETURN $events; }")
		.await
		.unwrap();

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sink = Arc::new(StdoutSink::new(false));
	let sinks: Vec<Arc<dyn Sink>> = vec![sink.clone()];

	let runner = CycleRunner::new(&engine, &connector, &sinks);
	let diff = runner
		.run(&cycle_config(&pg.schema, Some("smt::identity")))
		.await
		.unwrap();

	assert_eq!(diff.created.len(), 2);
	assert_eq!(sink.sent_events().len(), 2);
}

#[tokio::test]
async fn transform_filters_events_via_for_loop() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	surreal
		.client
		.query(
			r#"DEFINE FUNCTION fn::smt::drop_beta($events: array) {
				RETURN array::filter($events, |$e| $e.data.name != 'beta');
			}"#,
		)
		.await
		.unwrap();

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('c', 'gamma', 3)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sink = Arc::new(StdoutSink::new(false));
	let sinks: Vec<Arc<dyn Sink>> = vec![sink.clone()];

	let runner = CycleRunner::new(&engine, &connector, &sinks);
	let diff = runner
		.run(&cycle_config(&pg.schema, Some("smt::drop_beta")))
		.await
		.unwrap();

	assert_eq!(diff.created.len(), 3);
	let events = sink.sent_events();
	assert_eq!(events.len(), 2);
	let keys: Vec<&str> = events.iter().map(|e| e.meta.key.as_str()).collect();
	assert!(keys.contains(&"a"));
	assert!(keys.contains(&"c"));
	assert!(!keys.contains(&"b"));
}

#[tokio::test]
async fn transform_invalid_fn_name_rejected() {
	let engine = DeltaEngine::single(surrealdb::engine::any::connect("mem://").await.unwrap());

	let envelopes = vec![];
	let result = engine
		.apply_transform("'; DROP TABLE snapshot; --", envelopes)
		.await;
	assert!(result.is_err());
	assert!(
		result
			.unwrap_err()
			.to_string()
			.contains("invalid transform")
	);
}

// Direct test of apply_transform without full cycle pipeline
#[tokio::test]
async fn apply_transform_identity_roundtrip() {
	let db = surrealdb::engine::any::connect("mem://").await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();

	db.query("DEFINE FUNCTION fn::smt::passthrough($events: array) { RETURN $events; }")
		.await
		.unwrap();

	let engine = DeltaEngine::single(db);

	let envelope = EventEnvelope {
		meta: oversync_core::model::EventMeta {
			op: oversync_core::model::OpType::Created,
			origin_id: "src".into(),
			query_id: "q".into(),
			key: "k1".into(),
			hash: "h1".into(),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({"name": "alice", "age": 30}),
	};

	let result = engine
		.apply_transform("smt::passthrough", vec![envelope.clone()])
		.await
		.unwrap();

	assert_eq!(result.len(), 1);
	assert_eq!(result[0].meta.key, "k1");
	assert_eq!(result[0].meta.op, oversync_core::model::OpType::Created);
	assert_eq!(result[0].data["name"], "alice");
}
