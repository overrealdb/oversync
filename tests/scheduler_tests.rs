mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use oversync::config::{
	DeltaDef, DiffMode, OriginDef, PipeConfig, QueryDef, RetryDef, ScheduleDef, SinkDef,
	SurrealDbDef, SyncConfig,
};
use oversync::registry::PluginRegistry;
use oversync::scheduler::Scheduler;
use oversync_connectors::PostgresOriginFactory;
use oversync_core::TableNames;
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, OpType, RawRow};
use oversync_core::traits::{OriginConnector, OriginFactory, Sink, TargetFactory};
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutTargetFactory;
use surrealdb::opt::auth::Root;
use tokio::sync::{Mutex, RwLock};

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;

fn test_registry() -> PluginRegistry {
	let mut r = PluginRegistry::new();
	r.register_source(Box::new(PostgresOriginFactory));
	r.register_sink(Box::new(StdoutTargetFactory));
	r
}

fn stdout_sink() -> SinkDef {
	SinkDef {
		name: "stdout".into(),
		sink_type: "stdout".into(),
		config: serde_json::json!({}),
	}
}

fn make_pipe(
	name: &str,
	connector: &str,
	dsn: String,
	interval_secs: u64,
	fail_safe_threshold: f64,
	diff_mode: DiffMode,
	queries: Vec<QueryDef>,
) -> PipeConfig {
	PipeConfig {
		name: name.into(),
		origin: OriginDef {
			connector: connector.into(),
			dsn,
			credential: None,
			trino_url: None,
			config: serde_json::Value::Null,
		},
		targets: vec![],
		queries,
		schedule: ScheduleDef {
			interval_secs,
			missed_tick_policy: Default::default(),
			max_requests_per_minute: None,
		},
		delta: DeltaDef {
			diff_mode,
			fail_safe_threshold,
		},
		retry: RetryDef {
			max_retries: 1,
			retry_base_delay_secs: 1,
		},
		recipe: None,
		filters: vec![],
		transforms: vec![],
		links: vec![],
		alert_webhook: None,
		enabled: true,
	}
}

struct StaticCountingConnector {
	rows: Vec<RawRow>,
	delay: Duration,
	fetch_count: Arc<AtomicUsize>,
}

#[async_trait]
impl OriginConnector for StaticCountingConnector {
	fn name(&self) -> &str {
		"static"
	}

	async fn fetch_all(&self, _sql: &str, _key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		self.fetch_count.fetch_add(1, Ordering::SeqCst);
		tokio::time::sleep(self.delay).await;
		Ok(self.rows.clone())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct StaticCountingOriginFactory {
	rows: Vec<RawRow>,
	delay: Duration,
	fetch_count: Arc<AtomicUsize>,
}

#[async_trait]
impl OriginFactory for StaticCountingOriginFactory {
	fn connector_type(&self) -> &str {
		"static"
	}

	async fn create(
		&self,
		_name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		Ok(Box::new(StaticCountingConnector {
			rows: self.rows.clone(),
			delay: self.delay,
			fetch_count: Arc::clone(&self.fetch_count),
		}))
	}
}

struct MutableStaticConnector {
	rows: Arc<RwLock<Vec<RawRow>>>,
}

#[async_trait]
impl OriginConnector for MutableStaticConnector {
	fn name(&self) -> &str {
		"mutable-static"
	}

	async fn fetch_all(&self, _sql: &str, _key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		Ok(self.rows.read().await.clone())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct MutableStaticOriginFactory {
	rows: Arc<RwLock<Vec<RawRow>>>,
}

#[async_trait]
impl OriginFactory for MutableStaticOriginFactory {
	fn connector_type(&self) -> &str {
		"mutable-static"
	}

	async fn create(
		&self,
		_name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		Ok(Box::new(MutableStaticConnector {
			rows: Arc::clone(&self.rows),
		}))
	}
}

struct RecordingSink {
	events: Arc<Mutex<Vec<EventEnvelope>>>,
}

#[async_trait]
impl Sink for RecordingSink {
	fn name(&self) -> &str {
		"recorder"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.events.lock().await.push(envelope.clone());
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct RecordingTargetFactory {
	events: Arc<Mutex<Vec<EventEnvelope>>>,
}

#[async_trait]
impl TargetFactory for RecordingTargetFactory {
	fn sink_type(&self) -> &str {
		"recorder"
	}

	async fn create(
		&self,
		_name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		Ok(Box::new(RecordingSink {
			events: Arc::clone(&self.events),
		}))
	}
}

fn static_rows() -> Vec<RawRow> {
	vec![
		RawRow {
			row_key: "1".into(),
			row_data: serde_json::json!({"id": "1", "name": "alpha"}),
		},
		RawRow {
			row_key: "2".into(),
			row_data: serde_json::json!({"id": "2", "name": "beta"}),
		},
	]
}

fn failover_rows() -> Vec<RawRow> {
	(1..=20)
		.map(|id| RawRow {
			row_key: id.to_string(),
			row_data: serde_json::json!({
				"id": id.to_string(),
				"value": id,
				"version": 1,
			}),
		})
		.collect()
}

fn replay_sink_state(events: &[EventEnvelope]) -> HashMap<String, serde_json::Value> {
	let mut state = HashMap::new();
	for event in events {
		match event.meta.op {
			OpType::Created | OpType::Updated => {
				state.insert(event.meta.key.clone(), event.data.clone());
			}
			OpType::Deleted => {
				state.remove(&event.meta.key);
			}
		}
	}
	state
}

fn source_state(rows: &[RawRow]) -> HashMap<String, serde_json::Value> {
	rows.iter()
		.map(|row| (row.row_key.clone(), row.row_data.clone()))
		.collect()
}

fn apply_soak_mutation(rows: &mut Vec<RawRow>, wave: usize, next_id: &mut i64) -> (u64, u64, u64) {
	let stage = match wave {
		0 | 1 => return (0, 0, 0),
		_ => ((wave - 2) % 6) + 2,
	};

	match stage {
		2 => {
			let mut updated = 0u64;
			for row in rows.iter_mut().take(3) {
				let value = row.row_data["value"].as_i64().unwrap_or_default() + 10;
				let version = row.row_data["version"].as_i64().unwrap_or_default() + 1;
				row.row_data["value"] = serde_json::json!(value);
				row.row_data["version"] = serde_json::json!(version);
				updated += 1;
			}
			(0, updated, 0)
		}
		3 => {
			for _ in 0..2 {
				let id = *next_id;
				*next_id += 1;
				rows.push(RawRow {
					row_key: id.to_string(),
					row_data: serde_json::json!({
						"id": id.to_string(),
						"value": id * 10,
						"version": 1,
					}),
				});
			}
			(2, 0, 0)
		}
		4 => {
			rows.sort_by_key(|row| row.row_key.parse::<i64>().unwrap_or_default());
			let keys_to_delete: Vec<String> =
				rows.iter().take(2).map(|row| row.row_key.clone()).collect();
			let deleted = keys_to_delete.len() as u64;
			rows.retain(|row| !keys_to_delete.contains(&row.row_key));
			(0, 0, deleted)
		}
		5 => {
			rows.sort_by_key(|row| row.row_key.parse::<i64>().unwrap_or_default());
			let mut updated = 0u64;
			for row in rows.iter_mut().rev().take(2) {
				let value = row.row_data["value"].as_i64().unwrap_or_default() + 25;
				let version = row.row_data["version"].as_i64().unwrap_or_default() + 1;
				row.row_data["value"] = serde_json::json!(value);
				row.row_data["version"] = serde_json::json!(version);
				updated += 1;
			}
			(0, updated, 0)
		}
		6 => {
			let id = *next_id;
			*next_id += 1;
			rows.push(RawRow {
				row_key: id.to_string(),
				row_data: serde_json::json!({
					"id": id.to_string(),
					"value": id * 10,
					"version": 1,
				}),
			});
			(1, 0, 0)
		}
		7 => {
			rows.sort_by_key(|row| row.row_key.parse::<i64>().unwrap_or_default());
			if let Some(first_key) = rows.first().map(|row| row.row_key.clone()) {
				rows.retain(|row| row.row_key != first_key);
				(0, 0, 1)
			} else {
				(0, 0, 0)
			}
		}
		_ => (0, 0, 0),
	}
}

fn soak_wave_count() -> usize {
	std::env::var("SOAK_WAVES")
		.ok()
		.and_then(|value| value.parse().ok())
		.unwrap_or(25)
}

fn soak_timeout_secs() -> u64 {
	std::env::var("SOAK_TIMEOUT_SECS")
		.ok()
		.and_then(|value| value.parse().ok())
		.unwrap_or(180)
}

fn mutable_static_registry(
	rows: Arc<RwLock<Vec<RawRow>>>,
	events: Arc<Mutex<Vec<EventEnvelope>>>,
) -> PluginRegistry {
	let mut registry = PluginRegistry::new();
	registry.register_source(Box::new(MutableStaticOriginFactory { rows }));
	registry.register_sink(Box::new(RecordingTargetFactory { events }));
	registry
}

async fn connect_shared_surreal(
	ns: &str,
	db: &str,
) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
	let url = TestSurrealContainer::url().await;
	let client = surrealdb::engine::any::connect(&url).await.unwrap();
	client
		.signin(Root {
			username: "root".to_string(),
			password: "root".to_string(),
		})
		.await
		.unwrap();
	client.use_ns(ns).use_db(db).await.unwrap();
	client
}

async fn wait_for_success_cycles(
	client: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	cycle_log_table: &str,
	origin_id: &str,
	expected_rows: usize,
	timeout: Duration,
) -> Vec<serde_json::Value> {
	let deadline = tokio::time::Instant::now() + timeout;
	let mut last_count = 0usize;
	loop {
		let result = client
			.query(format!(
				"SELECT * FROM {cycle_log_table} WHERE origin_id = $origin_id AND status = 'success' ORDER BY cycle_id"
			))
			.bind(("origin_id", origin_id.to_string()))
			.await;
		if let Ok(mut response) = result
			&& let Ok(rows) = response.take::<Vec<serde_json::Value>>(0)
		{
			last_count = rows.len();
			if rows.len() >= expected_rows {
				return rows;
			}
		}

		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for {expected_rows} success cycles in {cycle_log_table}, got {last_count}"
		);
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
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
		sinks: vec![stdout_sink()],
		pipes: vec![make_pipe(
			"pg-test",
			"postgres",
			pg.dsn.clone(),
			interval_secs,
			30.0,
			DiffMode::default(),
			vec![QueryDef {
				id: "items".into(),
				sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
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
		.query("SELECT * FROM sync_pg_test_cycle_log WHERE origin_id = 'pg-test'")
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
		.query("SELECT * FROM sync_pg_test_cycle_log WHERE origin_id = 'pg-test' ORDER BY cycle_id")
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
		sinks: vec![stdout_sink()],
		pipes: vec![make_pipe(
			"bad-source",
			"postgres",
			"postgres://nonexistent:5432/db".into(),
			3600,
			30.0,
			DiffMode::default(),
			vec![QueryDef {
				id: "q".into(),
				sql: "SELECT 1 AS id".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
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
async fn scheduler_no_pipes_exits_immediately() {
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
		sinks: vec![],
		pipes: vec![],
		pipe_presets: vec![],
	};

	let scheduler = Scheduler::new(engine, config, test_registry());
	// Should return immediately with no runnable pipes.
	scheduler.run().await.unwrap();
}

#[tokio::test]
async fn scheduler_requires_explicit_sink_config() {
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
		sinks: vec![],
		pipes: vec![make_pipe(
			"pg-test",
			"postgres",
			"postgres://localhost/db".into(),
			3600,
			30.0,
			DiffMode::default(),
			vec![QueryDef {
				id: "items".into(),
				sql: "SELECT 1 AS id".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
	};

	let scheduler = Scheduler::new(engine, config, test_registry());
	let err = scheduler.run().await.unwrap_err();
	assert!(
		err.to_string().contains("no sinks configured"),
		"expected explicit sink config error, got: {err}"
	);
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
		sinks: vec![stdout_sink()],
		pipes: vec![make_pipe(
			"pg-test",
			"postgres",
			pg.dsn.clone(),
			1,
			50.0,
			DiffMode::default(),
			vec![QueryDef {
				id: "items".into(),
				sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
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
		.query("SELECT * FROM sync_pg_test_cycle_log WHERE origin_id = 'pg-test' ORDER BY cycle_id")
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
		sinks: vec![stdout_sink()],
		pipes: vec![make_pipe(
			"pg-multi-q",
			"postgres",
			pg.dsn.clone(),
			3600,
			50.0,
			DiffMode::default(),
			vec![
				QueryDef {
					id: "items".into(),
					sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
					key_column: "id".into(),
					sinks: None,
					transform: None,
				},
				QueryDef {
					id: "users".into(),
					sql: format!("SELECT id, email FROM {}.users", pg.schema),
					key_column: "id".into(),
					sinks: None,
					transform: None,
				},
			],
		)],
		pipe_presets: vec![],
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
		.query(
			"SELECT * FROM sync_pg_multi_q_cycle_log WHERE origin_id = 'pg-multi-q' AND query_id = 'items'",
		)
		.await
		.unwrap();
	let items_logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!items_logs.is_empty(),
		"expected cycle_log entries for query_id 'items'"
	);

	let mut res = surreal
		.client
		.query(
			"SELECT * FROM sync_pg_multi_q_cycle_log WHERE origin_id = 'pg-multi-q' AND query_id = 'users'",
		)
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
		sinks: vec![stdout_sink()],
		pipes: vec![
			make_pipe(
				"source-alpha",
				"postgres",
				pg.dsn.clone(),
				3600,
				50.0,
				DiffMode::default(),
				vec![QueryDef {
					id: "items".into(),
					sql: format!("SELECT id, name, value FROM {}.items", pg.schema),
					key_column: "id".into(),
					sinks: None,
					transform: None,
				}],
			),
			make_pipe(
				"source-beta",
				"postgres",
				pg.dsn.clone(),
				3600,
				50.0,
				DiffMode::default(),
				vec![QueryDef {
					id: "users".into(),
					sql: format!("SELECT id, email FROM {}.users", pg.schema),
					key_column: "id".into(),
					sinks: None,
					transform: None,
				}],
			),
		],
		pipe_presets: vec![],
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
		.query("SELECT * FROM sync_source_alpha_cycle_log WHERE origin_id = 'source-alpha'")
		.await
		.unwrap();
	let alpha_logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!alpha_logs.is_empty(),
		"expected cycle_log entries for origin_id 'source-alpha'"
	);

	let mut res = surreal
		.client
		.query("SELECT * FROM sync_source_beta_cycle_log WHERE origin_id = 'source-beta'")
		.await
		.unwrap();
	let beta_logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!beta_logs.is_empty(),
		"expected cycle_log entries for origin_id 'source-beta'"
	);
}

#[tokio::test]
async fn scheduler_two_instances_do_not_double_process_same_query() {
	let surreal = TestSurrealContainer::new().await;
	let engine = Arc::new(DeltaEngine::single(surreal.client.clone()));
	let fetch_count = Arc::new(AtomicUsize::new(0));
	let events = Arc::new(Mutex::new(Vec::new()));

	let mut registry_a = PluginRegistry::new();
	registry_a.register_source(Box::new(StaticCountingOriginFactory {
		rows: static_rows(),
		delay: Duration::from_millis(300),
		fetch_count: Arc::clone(&fetch_count),
	}));
	registry_a.register_sink(Box::new(RecordingTargetFactory {
		events: Arc::clone(&events),
	}));

	let mut registry_b = PluginRegistry::new();
	registry_b.register_source(Box::new(StaticCountingOriginFactory {
		rows: static_rows(),
		delay: Duration::from_millis(300),
		fetch_count: Arc::clone(&fetch_count),
	}));
	registry_b.register_sink(Box::new(RecordingTargetFactory {
		events: Arc::clone(&events),
	}));

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: surreal.ns.clone(),
			database: surreal.db.clone(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "recorder".into(),
			sink_type: "recorder".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![make_pipe(
			"static-shared",
			"static",
			"memory://".into(),
			3600,
			30.0,
			DiffMode::Db,
			vec![QueryDef {
				id: "items".into(),
				sql: "SELECT * FROM static_source".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
	};

	let scheduler_a = Scheduler::from_arc_engine_with_instance_id(
		Arc::clone(&engine),
		config.clone(),
		registry_a,
		"instance-a".into(),
	);
	let scheduler_b = Scheduler::from_arc_engine_with_instance_id(
		Arc::clone(&engine),
		config,
		registry_b,
		"instance-b".into(),
	);

	let shutdown_a = scheduler_a.shutdown_tx_clone();
	let shutdown_b = scheduler_b.shutdown_tx_clone();

	let handle_a = tokio::spawn(async move {
		scheduler_a.run().await.unwrap();
	});
	let handle_b = tokio::spawn(async move {
		scheduler_b.run().await.unwrap();
	});

	tokio::time::sleep(Duration::from_secs(1)).await;
	let _ = shutdown_a.send(true);
	let _ = shutdown_b.send(true);
	handle_a.await.unwrap();
	handle_b.await.unwrap();

	assert_eq!(
		fetch_count.load(Ordering::SeqCst),
		1,
		"only one scheduler instance should fetch the shared query"
	);
	assert_eq!(
		events.lock().await.len(),
		2,
		"only one create wave should be delivered across both schedulers"
	);

	let mut res = surreal
		.client
		.query(
			"SELECT * FROM sync_static_shared_cycle_log WHERE origin_id = 'static-shared' AND query_id = 'items' ORDER BY cycle_id",
		)
		.await
		.unwrap();
	let logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(logs.len(), 1, "expected exactly one successful cycle");
	assert_eq!(logs[0]["status"], "success");
}

#[tokio::test]
async fn scheduler_failover_reuses_baseline_and_emits_only_post_failover_updates() {
	let surreal = TestSurrealContainer::new().await;
	let shared_rows = Arc::new(RwLock::new(failover_rows()));
	let events = Arc::new(Mutex::new(Vec::new()));
	let cycle_log_table = TableNames::for_source("failover-source").cycle_log;
	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: surreal.ns.clone(),
			database: surreal.db.clone(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "recorder".into(),
			sink_type: "recorder".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![make_pipe(
			"failover-source",
			"mutable-static",
			"memory://".into(),
			3600,
			30.0,
			DiffMode::Db,
			vec![QueryDef {
				id: "items".into(),
				sql: "SELECT * FROM mutable_static_source".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
	};

	let client_a = connect_shared_surreal(&surreal.ns, &surreal.db).await;
	let scheduler_a = Scheduler::with_instance_id(
		DeltaEngine::single(client_a),
		config.clone(),
		mutable_static_registry(Arc::clone(&shared_rows), Arc::clone(&events)),
		"scheduler-a",
	);
	let shutdown_a = scheduler_a.shutdown_tx_clone();
	let handle_a = tokio::spawn(async move {
		scheduler_a.run().await.unwrap();
	});
	let first_wave = wait_for_success_cycles(
		&surreal.client,
		&cycle_log_table,
		"failover-source",
		1,
		Duration::from_secs(10),
	)
	.await;
	let _ = shutdown_a.send(true);
	handle_a.await.unwrap();

	assert_eq!(first_wave.len(), 1);
	assert_eq!(first_wave[0]["rows_created"].as_u64(), Some(20));
	assert_eq!(first_wave[0]["rows_updated"].as_u64(), Some(0));
	assert_eq!(first_wave[0]["rows_deleted"].as_u64(), Some(0));

	let events_after_first = events.lock().await.clone();
	assert_eq!(events_after_first.len(), 20);
	assert_eq!(
		events_after_first
			.iter()
			.filter(|event| event.meta.op == OpType::Created)
			.count(),
		20
	);

	let client_b = connect_shared_surreal(&surreal.ns, &surreal.db).await;
	let scheduler_b = Scheduler::with_instance_id(
		DeltaEngine::single(client_b),
		config.clone(),
		mutable_static_registry(Arc::clone(&shared_rows), Arc::clone(&events)),
		"scheduler-b",
	);
	let shutdown_b = scheduler_b.shutdown_tx_clone();
	let handle_b = tokio::spawn(async move {
		scheduler_b.run().await.unwrap();
	});
	let second_wave = wait_for_success_cycles(
		&surreal.client,
		&cycle_log_table,
		"failover-source",
		2,
		Duration::from_secs(10),
	)
	.await;
	let _ = shutdown_b.send(true);
	handle_b.await.unwrap();

	assert_eq!(second_wave.len(), 2);
	assert_eq!(second_wave[1]["rows_created"].as_u64(), Some(0));
	assert_eq!(second_wave[1]["rows_updated"].as_u64(), Some(0));
	assert_eq!(second_wave[1]["rows_deleted"].as_u64(), Some(0));
	assert_eq!(events.lock().await.len(), 20);

	{
		let mut rows = shared_rows.write().await;
		for row in rows.iter_mut().take(5) {
			let next_value = row.row_data["value"].as_i64().unwrap_or_default() + 100;
			row.row_data["value"] = serde_json::json!(next_value);
			row.row_data["version"] = serde_json::json!(2);
		}
	}

	let client_c = connect_shared_surreal(&surreal.ns, &surreal.db).await;
	let scheduler_c = Scheduler::with_instance_id(
		DeltaEngine::single(client_c),
		config,
		mutable_static_registry(Arc::clone(&shared_rows), Arc::clone(&events)),
		"scheduler-c",
	);
	let shutdown_c = scheduler_c.shutdown_tx_clone();
	let handle_c = tokio::spawn(async move {
		scheduler_c.run().await.unwrap();
	});
	let third_wave = wait_for_success_cycles(
		&surreal.client,
		&cycle_log_table,
		"failover-source",
		3,
		Duration::from_secs(10),
	)
	.await;
	let _ = shutdown_c.send(true);
	handle_c.await.unwrap();

	assert_eq!(third_wave.len(), 3);
	assert_eq!(third_wave[2]["rows_created"].as_u64(), Some(0));
	assert_eq!(third_wave[2]["rows_updated"].as_u64(), Some(5));
	assert_eq!(third_wave[2]["rows_deleted"].as_u64(), Some(0));

	let final_events = events.lock().await.clone();
	assert_eq!(final_events.len(), 25);
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Created)
			.count(),
		20
	);
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Updated)
			.count(),
		5
	);
}

#[tokio::test]
async fn scheduler_rolling_restarts_keep_sink_state_reconciled() {
	let surreal = TestSurrealContainer::new().await;
	let shared_rows = Arc::new(RwLock::new(failover_rows()));
	let events = Arc::new(Mutex::new(Vec::new()));
	let cycle_log_table = TableNames::for_source("soak-source").cycle_log;
	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: surreal.ns.clone(),
			database: surreal.db.clone(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "recorder".into(),
			sink_type: "recorder".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![make_pipe(
			"soak-source",
			"mutable-static",
			"memory://".into(),
			3600,
			30.0,
			DiffMode::Db,
			vec![QueryDef {
				id: "items".into(),
				sql: "SELECT * FROM mutable_static_source".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
	};

	let total_waves = 7usize;
	let mut next_id = 21i64;
	let mut expected_created = 20u64;
	let mut expected_updated = 0u64;
	let mut expected_deleted = 0u64;

	for wave in 1..=total_waves {
		if wave > 1 {
			let mut rows = shared_rows.write().await;
			let (created, updated, deleted) = apply_soak_mutation(&mut rows, wave, &mut next_id);
			expected_created += created;
			expected_updated += updated;
			expected_deleted += deleted;
		}

		let client = connect_shared_surreal(&surreal.ns, &surreal.db).await;
		let scheduler = Scheduler::with_instance_id(
			DeltaEngine::single(client),
			config.clone(),
			mutable_static_registry(Arc::clone(&shared_rows), Arc::clone(&events)),
			format!("soak-scheduler-{wave}"),
		);
		let shutdown = scheduler.shutdown_tx_clone();
		let handle = tokio::spawn(async move {
			scheduler.run().await.unwrap();
		});

		let success_cycles = wait_for_success_cycles(
			&surreal.client,
			&cycle_log_table,
			"soak-source",
			wave,
			Duration::from_secs(10),
		)
		.await;
		let cycle = success_cycles.last().unwrap();
		assert_eq!(cycle["cycle_id"].as_u64(), Some(wave as u64));

		match wave {
			1 => {
				assert_eq!(cycle["rows_created"].as_u64(), Some(20));
				assert_eq!(cycle["rows_updated"].as_u64(), Some(0));
				assert_eq!(cycle["rows_deleted"].as_u64(), Some(0));
			}
			2 => {
				assert_eq!(cycle["rows_created"].as_u64(), Some(0));
				assert_eq!(cycle["rows_updated"].as_u64(), Some(3));
				assert_eq!(cycle["rows_deleted"].as_u64(), Some(0));
			}
			3 => {
				assert_eq!(cycle["rows_created"].as_u64(), Some(2));
				assert_eq!(cycle["rows_updated"].as_u64(), Some(0));
				assert_eq!(cycle["rows_deleted"].as_u64(), Some(0));
			}
			4 => {
				assert_eq!(cycle["rows_created"].as_u64(), Some(0));
				assert_eq!(cycle["rows_updated"].as_u64(), Some(0));
				assert_eq!(cycle["rows_deleted"].as_u64(), Some(2));
			}
			5 => {
				assert_eq!(cycle["rows_created"].as_u64(), Some(0));
				assert_eq!(cycle["rows_updated"].as_u64(), Some(2));
				assert_eq!(cycle["rows_deleted"].as_u64(), Some(0));
			}
			6 => {
				assert_eq!(cycle["rows_created"].as_u64(), Some(1));
				assert_eq!(cycle["rows_updated"].as_u64(), Some(0));
				assert_eq!(cycle["rows_deleted"].as_u64(), Some(0));
			}
			7 => {
				assert_eq!(cycle["rows_created"].as_u64(), Some(0));
				assert_eq!(cycle["rows_updated"].as_u64(), Some(0));
				assert_eq!(cycle["rows_deleted"].as_u64(), Some(1));
			}
			_ => unreachable!(),
		}

		let _ = shutdown.send(true);
		handle.await.unwrap();
	}

	let final_events = events.lock().await.clone();
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Created)
			.count() as u64,
		expected_created
	);
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Updated)
			.count() as u64,
		expected_updated
	);
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Deleted)
			.count() as u64,
		expected_deleted
	);

	let mut create_counts: HashMap<String, usize> = HashMap::new();
	for event in &final_events {
		if event.meta.op == OpType::Created {
			*create_counts.entry(event.meta.key.clone()).or_default() += 1;
		}
	}
	assert!(
		create_counts.values().all(|count| *count == 1),
		"no key should receive duplicate created events across rolling restarts"
	);

	let sink_state = replay_sink_state(&final_events);
	let rows = shared_rows.read().await;
	assert_eq!(sink_state, source_state(&rows));
}

#[tokio::test]
#[ignore = "cluster soak harness — run explicitly against shared test stack"]
async fn scheduler_rolling_restart_soak_campaign() {
	let surreal = TestSurrealContainer::new().await;
	let shared_rows = Arc::new(RwLock::new(failover_rows()));
	let events = Arc::new(Mutex::new(Vec::new()));
	let cycle_log_table = TableNames::for_source("soak-source").cycle_log;
	let timeout = Duration::from_secs(soak_timeout_secs());
	let total_waves = soak_wave_count();
	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "unused".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: surreal.ns.clone(),
			database: surreal.db.clone(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "recorder".into(),
			sink_type: "recorder".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![make_pipe(
			"soak-source",
			"mutable-static",
			"memory://".into(),
			3600,
			30.0,
			DiffMode::Db,
			vec![QueryDef {
				id: "items".into(),
				sql: "SELECT * FROM mutable_static_source".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
	};

	let mut next_id = 21i64;
	let mut expected_created = 20u64;
	let mut expected_updated = 0u64;
	let mut expected_deleted = 0u64;

	for wave in 1..=total_waves {
		let (wave_created, wave_updated, wave_deleted) = if wave == 1 {
			(20, 0, 0)
		} else {
			let mut rows = shared_rows.write().await;
			apply_soak_mutation(&mut rows, wave, &mut next_id)
		};

		if wave > 1 {
			expected_created += wave_created;
			expected_updated += wave_updated;
			expected_deleted += wave_deleted;
		}

		let client = connect_shared_surreal(&surreal.ns, &surreal.db).await;
		let scheduler = Scheduler::with_instance_id(
			DeltaEngine::single(client),
			config.clone(),
			mutable_static_registry(Arc::clone(&shared_rows), Arc::clone(&events)),
			format!("soak-campaign-{wave}"),
		);
		let shutdown = scheduler.shutdown_tx_clone();
		let handle = tokio::spawn(async move {
			scheduler.run().await.unwrap();
		});

		let success_cycles = wait_for_success_cycles(
			&surreal.client,
			&cycle_log_table,
			"soak-source",
			wave,
			timeout,
		)
		.await;
		let cycle = success_cycles.last().unwrap();
		assert_eq!(cycle["cycle_id"].as_u64(), Some(wave as u64));
		assert_eq!(cycle["rows_created"].as_u64(), Some(wave_created));
		assert_eq!(cycle["rows_updated"].as_u64(), Some(wave_updated));
		assert_eq!(cycle["rows_deleted"].as_u64(), Some(wave_deleted));

		let _ = shutdown.send(true);
		handle.await.unwrap();
	}

	let final_events = events.lock().await.clone();
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Created)
			.count() as u64,
		expected_created
	);
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Updated)
			.count() as u64,
		expected_updated
	);
	assert_eq!(
		final_events
			.iter()
			.filter(|event| event.meta.op == OpType::Deleted)
			.count() as u64,
		expected_deleted
	);

	let mut create_counts: HashMap<String, usize> = HashMap::new();
	for event in &final_events {
		if event.meta.op == OpType::Created {
			*create_counts.entry(event.meta.key.clone()).or_default() += 1;
		}
	}
	assert!(
		create_counts.values().all(|count| *count == 1),
		"no key should receive duplicate created events during the soak campaign"
	);

	let sink_state = replay_sink_state(&final_events);
	let rows = shared_rows.read().await;
	assert_eq!(sink_state, source_state(&rows));
}
