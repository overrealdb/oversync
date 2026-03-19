mod common;

use oversync::cycle::{CycleConfig, CycleRunner};
use oversync_connectors::PostgresConnector;
use oversync_core::model::OpType;
use std::sync::Arc;
use oversync_core::traits::Sink;
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutSink;

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;

fn cycle_config(schema: &str) -> CycleConfig {
	CycleConfig {
		source_id: "pg-test".into(),
		query_id: "items".into(),
		sql: format!("SELECT id, name, value FROM {schema}.items"),
		key_column: "id".into(),
		fail_safe_threshold: 30.0,
		diff_mode: oversync::config::DiffMode::Db,
			missed_tick_policy: Default::default(),
		transform: None,
	}
}

#[tokio::test]
async fn e2e_first_cycle_all_created() {
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
	let sink = StdoutSink::new(false);
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(StdoutSink::new(false))];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	let config = cycle_config(&pg.schema);
	let diff = runner.run(&config).await.unwrap();

	assert_eq!(diff.created.len(), 2);
	assert!(diff.updated.is_empty());
	assert!(diff.deleted.is_empty());

	// Verify events have correct source/query
	assert!(diff.created.iter().all(|e| e.source_id == "pg-test"));
	assert!(diff.created.iter().all(|e| e.query_id == "items"));

	drop(sink);
}

#[tokio::test]
async fn e2e_second_cycle_detects_changes() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	// 5 rows so that 1 deletion = 20% < 30% default threshold
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('c', 'gamma', 3)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('e', 'epsilon', 5)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('f', 'zeta', 6)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(StdoutSink::new(false))];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	let config = cycle_config(&pg.schema);

	// Cycle 1: initial load
	let diff1 = runner.run(&config).await.unwrap();
	assert_eq!(diff1.created.len(), 5);

	// Mutate source: update b, delete c, add d (1 deletion / 5 previous = 20%)
	pg.run_sql("UPDATE items SET name = 'beta_v2' WHERE id = 'b'")
		.await;
	pg.run_sql("DELETE FROM items WHERE id = 'c'").await;
	pg.run_sql("INSERT INTO items VALUES ('d', 'delta', 4)")
		.await;

	// Cycle 2: detect changes
	let diff2 = runner.run(&config).await.unwrap();
	assert_eq!(diff2.created.len(), 1);
	assert_eq!(diff2.created[0].row_key, "d");
	assert_eq!(diff2.updated.len(), 1);
	assert_eq!(diff2.updated[0].row_key, "b");
	assert_eq!(diff2.deleted.len(), 1);
	assert_eq!(diff2.deleted[0].row_key, "c");
}

#[tokio::test]
async fn e2e_no_change_cycle_produces_empty_diff() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(StdoutSink::new(false))];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	let config = cycle_config(&pg.schema);

	runner.run(&config).await.unwrap();
	let diff2 = runner.run(&config).await.unwrap();

	assert!(diff2.is_empty());
}

#[tokio::test]
async fn e2e_fail_safe_aborts_on_mass_deletion() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	for i in 0..10 {
		pg.run_sql(&format!(
			"INSERT INTO items VALUES ('r{i}', 'row_{i}', {i})"
		))
		.await;
	}

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(StdoutSink::new(false))];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	let config = cycle_config(&pg.schema);

	// Cycle 1: load all 10
	runner.run(&config).await.unwrap();

	// Delete 8 of 10 rows (80% > 30% threshold)
	for i in 2..10 {
		pg.run_sql(&format!("DELETE FROM items WHERE id = 'r{i}'"))
			.await;
	}

	// Cycle 2: should fail with fail-safe
	let result = runner.run(&config).await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("fail-safe"));

	// Snapshot should still have all 10 rows (untouched)
	let keys = engine.read_snapshot_keys("pg-test", "items").await.unwrap();
	assert_eq!(keys.len(), 10);
}

#[tokio::test]
async fn e2e_sink_receives_all_events() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('x', 'x_val', 1)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());
	let sink = StdoutSink::new(false);
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(StdoutSink::new(false))];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	let config = cycle_config(&pg.schema);
	runner.run(&config).await.unwrap();

	// Second cycle: mutate and check sink receives events
	pg.run_sql("UPDATE items SET name = 'x_v2' WHERE id = 'x'")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('y', 'y_val', 2)")
		.await;

	let diff = runner.run(&config).await.unwrap();
	assert_eq!(diff.created.len(), 1);
	assert_eq!(diff.updated.len(), 1);

	// Verify event types
	assert_eq!(diff.created[0].op, OpType::Created);
	assert_eq!(diff.updated[0].op, OpType::Updated);

	drop(sink);
}
