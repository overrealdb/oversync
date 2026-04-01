mod common;

use std::sync::Arc;

use oversync::cycle::{CycleConfig, CycleRunner};
use oversync_connectors::PostgresConnector;
use oversync_core::traits::Sink;
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutSink;
use oversync_transforms::parse_steps;

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;

fn cycle_config(schema: &str, transform: Option<&str>) -> CycleConfig {
	CycleConfig {
		origin_id: "pg-test".into(),
		query_id: "items".into(),
		sql: format!("SELECT id, name, price, qty FROM {schema}.items"),
		key_column: "id".into(),
		fail_safe_threshold: 30.0,
		diff_mode: oversync::config::DiffMode::Db,
		transform: transform.map(String::from),
	}
}

#[tokio::test]
async fn js_transform_computes_fields_e2e() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, price INT, qty INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 10, 3)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 5, 2)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sink = Arc::new(StdoutSink::new(false));
	let sinks: Vec<Arc<dyn Sink>> = vec![sink.clone()];

	let chain = parse_steps(&[serde_json::json!({
		"type": "js",
		"function": "function transform(row) { row.total = row.price * row.qty; return row; }"
	})])
	.unwrap();

	let runner = CycleRunner::new(&engine, &connector, &sinks).with_pre_filter(Arc::new(chain));
	let diff = runner.run(&cycle_config(&pg.schema, None)).await.unwrap();

	assert_eq!(diff.created.len(), 2);

	let events = sink.sent_events();
	assert_eq!(events.len(), 2);
	for e in &events {
		assert!(e.data.get("total").is_some(), "total field should exist");
	}

	let alpha = events.iter().find(|e| e.meta.key == "a").unwrap();
	assert_eq!(alpha.data["total"], 30);

	let beta = events.iter().find(|e| e.meta.key == "b").unwrap();
	assert_eq!(beta.data["total"], 10);
}

#[tokio::test]
async fn js_transform_filters_rows_e2e() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, price INT, qty INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 10, 3)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 5, 0)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('c', 'gamma', 20, 2)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sink = Arc::new(StdoutSink::new(false));
	let sinks: Vec<Arc<dyn Sink>> = vec![sink.clone()];

	let chain = parse_steps(&[serde_json::json!({
		"type": "js",
		"function": "function transform(row) { return row.qty > 0 ? row : null; }"
	})])
	.unwrap();

	let runner = CycleRunner::new(&engine, &connector, &sinks).with_pre_filter(Arc::new(chain));
	let diff = runner.run(&cycle_config(&pg.schema, None)).await.unwrap();

	// pre_filter drops row "b" (qty=0) before upsert, so only 2 rows are diffed
	assert_eq!(diff.created.len(), 2);

	let events = sink.sent_events();
	assert_eq!(events.len(), 2);
	let keys: Vec<&str> = events.iter().map(|e| e.meta.key.as_str()).collect();
	assert!(keys.contains(&"a"));
	assert!(keys.contains(&"c"));
	assert!(!keys.contains(&"b"));
}

#[tokio::test]
async fn js_transform_chained_with_builtin_e2e() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, price INT, qty INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 10, 3)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sink = Arc::new(StdoutSink::new(false));
	let sinks: Vec<Arc<dyn Sink>> = vec![sink.clone()];

	// Chain: built-in upper(name) → JS compute total
	let chain = parse_steps(&[
		serde_json::json!({"type": "upper", "field": "name"}),
		serde_json::json!({
			"type": "js",
			"function": "function transform(row) { row.total = row.price * row.qty; return row; }"
		}),
	])
	.unwrap();

	let runner = CycleRunner::new(&engine, &connector, &sinks).with_pre_filter(Arc::new(chain));
	let diff = runner.run(&cycle_config(&pg.schema, None)).await.unwrap();

	assert_eq!(diff.created.len(), 1);

	let events = sink.sent_events();
	assert_eq!(events.len(), 1);
	assert_eq!(events[0].data["name"], "ALPHA");
	assert_eq!(events[0].data["total"], 30);
}
