mod common;

use oversync::config::{QueryDef, SourceDef, SurrealDbDef, SyncConfig};
use oversync::registry::PluginRegistry;
use oversync::scheduler::Scheduler;
use oversync_connectors::PostgresSourceFactory;
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutSinkFactory;

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;

fn test_registry() -> PluginRegistry {
	let mut r = PluginRegistry::new();
	r.register_source(Box::new(PostgresSourceFactory));
	r.register_sink(Box::new(StdoutSinkFactory));
	r
}

fn make_config(pg: &TestPostgres, interval_secs: u64) -> SyncConfig {
	SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "test".into(),
			database: "test".into(),
			snapshot: None,
		},
		sources: vec![SourceDef {
			name: "pg-test".into(),
			connector: "postgres".into(),
			dsn: pg.dsn.clone(),
			interval_secs,
			fail_safe_threshold: 30.0,
			max_retries: 1,
			retry_base_delay_secs: 1,
			diff_mode: oversync::config::DiffMode::default(),
			queries: vec![QueryDef {
				id: "items".into(),
				sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
				key_column: "id".into(),
			}],
		}],
		sinks: vec![],
	}
}

#[tokio::test]
async fn scheduler_runs_first_cycle_immediately() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;

	// Get SurrealDB URL from shared container
	let engine = DeltaEngine::single(surreal.client.clone());

	let config = make_config(&pg, 3600);

	let scheduler = Scheduler::new(engine, config, test_registry());

	// Run scheduler in background, shut down after short delay
	let shutdown = scheduler.shutdown_tx_clone();
	let handle = tokio::spawn(async move {
		scheduler.run().await.unwrap();
	});

	// Wait a bit for first cycle to complete
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	let _ = shutdown.send(true);
	handle.await.unwrap();

	// Verify: cycle_log should have at least one entry
	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'pg-test'")
		.await
		.unwrap();
	let logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(!logs.is_empty(), "expected cycle_log entries");
	assert_eq!(logs[0]["status"], "success");
}

#[tokio::test]
async fn scheduler_runs_multiple_cycles() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('x', 'xray', 1)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());

	let config = make_config(&pg, 1);

	let scheduler = Scheduler::new(engine, config, test_registry());
	let shutdown = scheduler.shutdown_tx_clone();

	let handle = tokio::spawn(async move {
		scheduler.run().await.unwrap();
	});

	// Wait for ~3 cycles (first immediate + 2 interval ticks)
	tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
	let _ = shutdown.send(true);
	handle.await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'pg-test' ORDER BY cycle_id")
		.await
		.unwrap();
	let logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(logs.len() >= 2, "expected >=2 cycles, got {}", logs.len());
}

#[tokio::test]
async fn scheduler_handles_connector_error() {
	let surreal = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(surreal.client.clone());

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "test".into(),
			database: "test".into(),
			snapshot: None,
		},
		sources: vec![SourceDef {
			name: "bad-source".into(),
			connector: "postgres".into(),
			dsn: "postgres://nonexistent:5432/db".into(),
			interval_secs: 3600,
			fail_safe_threshold: 30.0,
			max_retries: 0,
			retry_base_delay_secs: 1,
			diff_mode: oversync::config::DiffMode::default(),
			queries: vec![QueryDef {
				id: "q".into(),
				sql: "SELECT 1 AS id".into(),
				key_column: "id".into(),
			}],
		}],
		sinks: vec![],
	};

	let scheduler = Scheduler::new(engine, config, test_registry());
	let shutdown = scheduler.shutdown_tx_clone();

	let handle = tokio::spawn(async move {
		scheduler.run().await.unwrap();
	});

	// Should not panic — just log error and exit task
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	let _ = shutdown.send(true);
	handle.await.unwrap();
}

#[tokio::test]
async fn scheduler_no_sources_exits_immediately() {
	let surreal = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(surreal.client.clone());

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "test".into(),
			database: "test".into(),
			snapshot: None,
		},
		sources: vec![],
		sinks: vec![],
	};

	let scheduler = Scheduler::new(engine, config, test_registry());
	// Should return immediately with no sources
	scheduler.run().await.unwrap();
}

#[tokio::test]
async fn scheduler_detects_data_changes() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('b', 'beta', 2)")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "test".into(),
			database: "test".into(),
			snapshot: None,
		},
		sources: vec![SourceDef {
			name: "pg-test".into(),
			connector: "postgres".into(),
			dsn: pg.dsn.clone(),
			interval_secs: 1,
			fail_safe_threshold: 50.0,
			max_retries: 1,
			retry_base_delay_secs: 1,
			diff_mode: oversync::config::DiffMode::default(),
			queries: vec![QueryDef {
				id: "items".into(),
				sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
				key_column: "id".into(),
			}],
		}],
		sinks: vec![],
	};

	let scheduler = Scheduler::new(engine, config, test_registry());
	let shutdown = scheduler.shutdown_tx_clone();

	let handle = tokio::spawn(async move {
		scheduler.run().await.unwrap();
	});

	// Wait for the first cycle to complete
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;

	// Mutate data: insert, update, delete
	pg.run_sql("INSERT INTO items VALUES ('c', 'gamma', 3)")
		.await;
	pg.run_sql("UPDATE items SET name = 'beta_v2' WHERE id = 'b'")
		.await;
	pg.run_sql("DELETE FROM items WHERE id = 'a'").await;

	// Wait for at least one more cycle to pick up changes
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	let _ = shutdown.send(true);
	handle.await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'pg-test' ORDER BY cycle_id")
		.await
		.unwrap();
	let logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		logs.len() >= 2,
		"expected >=2 cycle_log entries, got {}",
		logs.len()
	);

	let has_changes = logs.iter().any(|log| {
		let created = log["rows_created"].as_i64().unwrap_or(0);
		let updated = log["rows_updated"].as_i64().unwrap_or(0);
		let deleted = log["rows_deleted"].as_i64().unwrap_or(0);
		created > 0 || updated > 0 || deleted > 0
	});
	assert!(
		has_changes,
		"expected at least one cycle with data changes, logs: {logs:?}"
	);
}

#[tokio::test]
async fn scheduler_multiple_queries() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT)")
		.await;
	pg.run_sql("INSERT INTO users VALUES ('u1', 'alice@test.com')")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "test".into(),
			database: "test".into(),
			snapshot: None,
		},
		sources: vec![SourceDef {
			name: "pg-multi-q".into(),
			connector: "postgres".into(),
			dsn: pg.dsn.clone(),
			interval_secs: 3600,
			fail_safe_threshold: 50.0,
			max_retries: 1,
			retry_base_delay_secs: 1,
			diff_mode: oversync::config::DiffMode::default(),
			queries: vec![
				QueryDef {
					id: "items".into(),
					sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
					key_column: "id".into(),
				},
				QueryDef {
					id: "users".into(),
					sql: format!("SELECT id, email FROM {}.users", pg.schema),
					key_column: "id".into(),
				},
			],
		}],
		sinks: vec![],
	};

	let scheduler = Scheduler::new(engine, config, test_registry());
	let shutdown = scheduler.shutdown_tx_clone();

	let handle = tokio::spawn(async move {
		scheduler.run().await.unwrap();
	});

	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	let _ = shutdown.send(true);
	handle.await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'pg-multi-q' AND query_id = 'items'")
		.await
		.unwrap();
	let items_logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!items_logs.is_empty(),
		"expected cycle_log entries for query_id 'items'"
	);

	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'pg-multi-q' AND query_id = 'users'")
		.await
		.unwrap();
	let users_logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!users_logs.is_empty(),
		"expected cycle_log entries for query_id 'users'"
	);
}

#[tokio::test]
async fn scheduler_multiple_sources() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;

	pg.run_sql("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, value INT)")
		.await;
	pg.run_sql("INSERT INTO items VALUES ('a', 'alpha', 1)")
		.await;
	pg.run_sql("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT)")
		.await;
	pg.run_sql("INSERT INTO users VALUES ('u1', 'alice@test.com')")
		.await;

	let engine = DeltaEngine::single(surreal.client.clone());

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "test".into(),
			database: "test".into(),
			snapshot: None,
		},
		sources: vec![
			SourceDef {
				name: "source-alpha".into(),
				connector: "postgres".into(),
				dsn: pg.dsn.clone(),
				interval_secs: 3600,
				fail_safe_threshold: 50.0,
				max_retries: 1,
				retry_base_delay_secs: 1,
				diff_mode: oversync::config::DiffMode::default(),
				queries: vec![QueryDef {
					id: "items".into(),
					sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
					key_column: "id".into(),
				}],
			},
			SourceDef {
				name: "source-beta".into(),
				connector: "postgres".into(),
				dsn: pg.dsn.clone(),
				interval_secs: 3600,
				fail_safe_threshold: 50.0,
				max_retries: 1,
				retry_base_delay_secs: 1,
				diff_mode: oversync::config::DiffMode::default(),
				queries: vec![QueryDef {
					id: "users".into(),
					sql: format!("SELECT id, email FROM {}.users", pg.schema),
					key_column: "id".into(),
				}],
			},
		],
		sinks: vec![],
	};

	let scheduler = Scheduler::new(engine, config, test_registry());
	let shutdown = scheduler.shutdown_tx_clone();

	let handle = tokio::spawn(async move {
		scheduler.run().await.unwrap();
	});

	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	let _ = shutdown.send(true);
	handle.await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'source-alpha'")
		.await
		.unwrap();
	let alpha_logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!alpha_logs.is_empty(),
		"expected cycle_log entries for source_id 'source-alpha'"
	);

	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'source-beta'")
		.await
		.unwrap();
	let beta_logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!beta_logs.is_empty(),
		"expected cycle_log entries for source_id 'source-beta'"
	);
}
