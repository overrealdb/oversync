mod common;

use std::sync::Arc;

use oversync::cycle::{CycleConfig, CycleRunner};
use oversync_connectors::PostgresConnector;
use oversync_core::traits::Sink;
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutSink;

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;

fn cycle_config(schema: &str) -> CycleConfig {
	CycleConfig {
		origin_id: "pg-test".into(),
		query_id: "items".into(),
		sql: format!("SELECT id, name, value FROM {schema}.items"),
		key_column: "id".into(),
		fail_safe_threshold: 30.0,
		diff_mode: oversync::config::DiffMode::Db,
		transform: None,
		links: vec![],
	}
}

#[tokio::test]
async fn routing_broadcast_when_all_sinks() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = PostgresConnector::from_pool("pg-test", pg.pool.clone());

	let sink_a = Arc::new(StdoutSink::new(false));
	let sink_b = Arc::new(StdoutSink::new(false));
	let sinks: Vec<Arc<dyn Sink>> = vec![sink_a.clone(), sink_b.clone()];

	let runner = CycleRunner::new(&engine, &connector, &sinks);
	runner.run(&cycle_config(&pg.schema)).await.unwrap();

	// Both sinks received the event
	assert_eq!(sink_a.sent_events().len(), 1);
	assert_eq!(sink_b.sent_events().len(), 1);
}

#[tokio::test]
async fn routing_subset_of_sinks() {
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

	// Only give sink_a to the runner (simulating routing that filtered out sink_b)
	let sink_a = Arc::new(StdoutSink::new(false));
	let sink_b = Arc::new(StdoutSink::new(false));
	let routed_sinks: Vec<Arc<dyn Sink>> = vec![sink_a.clone()];

	let runner = CycleRunner::new(&engine, &connector, &routed_sinks);
	runner.run(&cycle_config(&pg.schema)).await.unwrap();

	// Only sink_a got events
	assert_eq!(sink_a.sent_events().len(), 2);
	assert_eq!(sink_b.sent_events().len(), 0);
}

#[tokio::test]
async fn routing_resolve_pipe_query_sinks_filters() {
	use oversync::config::{DeltaDef, OriginDef, PipeConfig, RetryDef, ScheduleDef};
	use oversync_core::traits::Sink;
	use std::collections::HashMap;

	let sink_a = Arc::new(StdoutSink::new(false));
	let sink_b = Arc::new(StdoutSink::new(false));
	let sink_c = Arc::new(StdoutSink::new(false));

	let mut named: HashMap<String, Arc<dyn Sink>> = HashMap::new();
	named.insert("alpha".into(), sink_a.clone());
	named.insert("beta".into(), sink_b.clone());
	named.insert("gamma".into(), sink_c.clone());

	let make_pipe = |targets: Vec<String>| PipeConfig {
		name: "test-pipe".into(),
		origin: OriginDef {
			connector: "postgres".into(),
			dsn: "postgres://localhost/db".into(),
			credential: None,
			trino_url: None,
			config: serde_json::Value::Null,
		},
		targets,
		queries: vec![],
		schedule: ScheduleDef::default(),
		delta: DeltaDef::default(),
		retry: RetryDef::default(),
		filters: vec![],
		transforms: vec![],
		links: vec![],
		enabled: true,
		alert_webhook: None,
	};

	let make_query = |sinks: Option<Vec<String>>| oversync::config::QueryDef {
		id: "q".into(),
		sql: "SELECT 1".into(),
		key_column: "id".into(),
		sinks,
		transform: None,
	};

	// No pipe targets, no query sinks → all sinks
	let pipe = make_pipe(vec![]);
	let query = make_query(None);
	let all = oversync::scheduler::resolve_pipe_query_sinks(&named, &pipe, &query).unwrap();
	assert_eq!(all.len(), 3);

	// Pipe targets set, no query sinks → pipe targets
	let pipe = make_pipe(vec!["alpha".into(), "gamma".into()]);
	let query = make_query(None);
	let filtered = oversync::scheduler::resolve_pipe_query_sinks(&named, &pipe, &query).unwrap();
	assert_eq!(filtered.len(), 2);

	// Query sinks override pipe targets
	let pipe = make_pipe(vec!["alpha".into(), "beta".into(), "gamma".into()]);
	let query = make_query(Some(vec!["beta".into()]));
	let override_sinks =
		oversync::scheduler::resolve_pipe_query_sinks(&named, &pipe, &query).unwrap();
	assert_eq!(override_sinks.len(), 1);

	// Unknown sink → error
	let pipe = make_pipe(vec![]);
	let query = make_query(Some(vec!["nonexistent".into()]));
	let err = oversync::scheduler::resolve_pipe_query_sinks(&named, &pipe, &query);
	assert!(err.is_err());
	let err_msg = err.err().expect("should error").to_string();
	assert!(err_msg.contains("nonexistent"));
}
