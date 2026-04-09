mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;
use oversync::config::{
	DiffMode, MissedTickPolicy, QueryDef, SinkDef, SourceDef, SurrealDbDef, SyncConfig,
};
use oversync::registry::PluginRegistry;
use oversync::scheduler::Scheduler;
use oversync_connectors::PostgresOriginFactory;
use oversync_core::TableNames;
use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::{Sink, TargetFactory};
use oversync_delta::DeltaEngine;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

fn bench_query_count() -> usize {
	std::env::var("THROUGHPUT_QUERIES")
		.ok()
		.and_then(|value| value.parse().ok())
		.unwrap_or(10)
}

fn bench_rows_per_query() -> usize {
	std::env::var("THROUGHPUT_ROWS")
		.ok()
		.and_then(|value| value.parse().ok())
		.unwrap_or(10_000)
}

fn bench_timeout_secs() -> u64 {
	std::env::var("THROUGHPUT_TIMEOUT_SECS")
		.ok()
		.and_then(|value| value.parse().ok())
		.unwrap_or(120)
}

struct CountingSink {
	delivered: Arc<AtomicUsize>,
}

#[async_trait]
impl Sink for CountingSink {
	fn name(&self) -> &str {
		"bench-counter"
	}

	async fn send_event(&self, _envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.delivered.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}

	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		self.delivered.fetch_add(envelopes.len(), Ordering::SeqCst);
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct CountingTargetFactory {
	delivered: Arc<AtomicUsize>,
}

#[async_trait]
impl TargetFactory for CountingTargetFactory {
	fn sink_type(&self) -> &str {
		"bench-counter"
	}

	async fn create(
		&self,
		_name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		Ok(Box::new(CountingSink {
			delivered: Arc::clone(&self.delivered),
		}))
	}
}

fn throughput_config(pg: &TestPostgres, query_count: usize) -> SyncConfig {
	let queries = (0..query_count)
		.map(|index| {
			let table_name = format!("bench_{index:03}");
			QueryDef {
				id: table_name.clone(),
				sql: format!(
					"SELECT id::text AS id, value, updated_at FROM {}.{} ORDER BY id",
					pg.schema, table_name
				),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}
		})
		.collect();

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
			name: "throughput-bench".into(),
			connector: "postgres".into(),
			dsn: pg.dsn.clone(),
			interval_secs: 3600,
			fail_safe_threshold: 30.0,
			max_retries: 1,
			retry_base_delay_secs: 1,
			diff_mode: DiffMode::Db,
			missed_tick_policy: MissedTickPolicy::Skip,
			config: serde_json::json!({}),
			queries,
		}],
		sinks: vec![SinkDef {
			name: "bench-counter".into(),
			sink_type: "bench-counter".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![],
		pipe_presets: vec![],
	}
}

async fn setup_tables(pg: &TestPostgres, query_count: usize, rows_per_query: usize) {
	for index in 0..query_count {
		let table_name = format!("bench_{index:03}");
		pg.run_sql(&format!(
			"CREATE TABLE {table_name} (
				id BIGINT PRIMARY KEY,
				value BIGINT NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)"
		))
		.await;
		pg.run_sql(&format!(
			"INSERT INTO {table_name} (id, value)
			 SELECT g, g * 10 + {index}
			 FROM generate_series(1, {rows_per_query}) g"
		))
		.await;
	}
}

async fn mutate_half_the_tables(pg: &TestPostgres, query_count: usize) {
	for index in 0..(query_count / 2) {
		let table_name = format!("bench_{index:03}");
		pg.run_sql(&format!(
			"UPDATE {table_name}
			 SET value = value + 1, updated_at = now()
			 WHERE id = 1"
		))
		.await;
	}
}

async fn wait_for_cycle_rows(
	client: &Surreal<Any>,
	cycle_log_table: &str,
	expected_rows: usize,
	timeout: Duration,
) -> Vec<serde_json::Value> {
	let deadline = Instant::now() + timeout;
	let mut last_count = 0usize;
	loop {
		let query_result = client
			.query(format!(
				"SELECT * FROM {cycle_log_table} WHERE origin_id = 'throughput-bench' AND status = 'success' ORDER BY cycle_id, query_id"
			))
			.await;
		if let Ok(mut response) = query_result
			&& let Ok(rows) = response.take::<Vec<serde_json::Value>>(0)
		{
			last_count = rows.len();
			if rows.len() >= expected_rows {
				return rows;
			}
		}
		assert!(
			Instant::now() < deadline,
			"timed out waiting for {expected_rows} cycle rows, got {last_count}"
		);
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

struct SchedulerWave<'a> {
	engine: Arc<DeltaEngine>,
	config: SyncConfig,
	registry: PluginRegistry,
	instance_id: &'a str,
	cycle_log_table: &'a str,
	expected_rows: usize,
	client: &'a Surreal<Any>,
	timeout: Duration,
}

async fn run_one_scheduler_wave(wave: SchedulerWave<'_>) -> (Duration, Vec<serde_json::Value>) {
	let SchedulerWave {
		engine,
		config,
		registry,
		instance_id,
		cycle_log_table,
		expected_rows,
		client,
		timeout,
	} = wave;
	let scheduler = Scheduler::from_arc_engine_with_instance_id(
		engine,
		config,
		registry,
		instance_id.to_string(),
	);
	let shutdown = scheduler.shutdown_tx_clone();
	let started = Instant::now();
	let handle = tokio::spawn(async move {
		scheduler.run().await.unwrap();
	});

	let rows = wait_for_cycle_rows(client, cycle_log_table, expected_rows, timeout).await;
	let elapsed = started.elapsed();
	let _ = shutdown.send(true);
	handle.await.unwrap();
	(elapsed, rows)
}

#[tokio::test]
#[ignore = "throughput harness — run explicitly against shared test stack"]
async fn throughput_parallel_queries_three_wave_harness() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;
	let query_count = bench_query_count();
	let rows_per_query = bench_rows_per_query();
	let timeout = Duration::from_secs(bench_timeout_secs());
	let total_rows = query_count * rows_per_query;
	let changed_queries = query_count / 2;

	setup_tables(&pg, query_count, rows_per_query).await;

	let cycle_log_table = TableNames::for_source("throughput-bench").cycle_log;
	let engine = Arc::new(DeltaEngine::single(surreal.client.clone()));
	let delivered = Arc::new(AtomicUsize::new(0));

	let mut registry_first = PluginRegistry::new();
	registry_first.register_source(Box::new(PostgresOriginFactory));
	registry_first.register_sink(Box::new(CountingTargetFactory {
		delivered: Arc::clone(&delivered),
	}));

	let config = throughput_config(&pg, query_count);
	let (first_elapsed, first_rows) = run_one_scheduler_wave(SchedulerWave {
		engine: Arc::clone(&engine),
		config: config.clone(),
		registry: registry_first,
		instance_id: "throughput-wave-1",
		cycle_log_table: &cycle_log_table,
		expected_rows: query_count,
		client: &surreal.client,
		timeout,
	})
	.await;
	let first_delivered = delivered.load(Ordering::SeqCst);

	eprintln!(
		"throughput wave=first_sync queries={query_count} rows_per_query={rows_per_query} total_rows={total_rows} elapsed_secs={:.3} rows_per_sec={:.1}",
		first_elapsed.as_secs_f64(),
		total_rows as f64 / first_elapsed.as_secs_f64()
	);

	assert_eq!(first_rows.len(), query_count);
	assert!(
		first_rows
			.iter()
			.all(|row| row["rows_created"].as_u64() == Some(rows_per_query as u64))
	);
	assert_eq!(first_delivered, total_rows);

	let mut registry_second = PluginRegistry::new();
	registry_second.register_source(Box::new(PostgresOriginFactory));
	registry_second.register_sink(Box::new(CountingTargetFactory {
		delivered: Arc::clone(&delivered),
	}));

	let (second_elapsed, second_rows) = run_one_scheduler_wave(SchedulerWave {
		engine: Arc::clone(&engine),
		config: config.clone(),
		registry: registry_second,
		instance_id: "throughput-wave-2",
		cycle_log_table: &cycle_log_table,
		expected_rows: query_count * 2,
		client: &surreal.client,
		timeout,
	})
	.await;
	let second_delivered = delivered.load(Ordering::SeqCst);
	let second_wave = &second_rows[query_count..];

	eprintln!(
		"throughput wave=no_change queries={query_count} rows_per_query={rows_per_query} total_rows={total_rows} elapsed_secs={:.3} scanned_rows_per_sec={:.1}",
		second_elapsed.as_secs_f64(),
		total_rows as f64 / second_elapsed.as_secs_f64()
	);

	assert_eq!(second_wave.len(), query_count);
	assert!(second_wave.iter().all(|row| {
		row["rows_created"].as_u64() == Some(0)
			&& row["rows_updated"].as_u64() == Some(0)
			&& row["rows_deleted"].as_u64() == Some(0)
	}));
	assert_eq!(second_delivered, first_delivered);

	mutate_half_the_tables(&pg, query_count).await;

	let mut registry_third = PluginRegistry::new();
	registry_third.register_source(Box::new(PostgresOriginFactory));
	registry_third.register_sink(Box::new(CountingTargetFactory {
		delivered: Arc::clone(&delivered),
	}));

	let (third_elapsed, third_rows) = run_one_scheduler_wave(SchedulerWave {
		engine: Arc::clone(&engine),
		config,
		registry: registry_third,
		instance_id: "throughput-wave-3",
		cycle_log_table: &cycle_log_table,
		expected_rows: query_count * 3,
		client: &surreal.client,
		timeout,
	})
	.await;
	let third_delivered = delivered.load(Ordering::SeqCst);
	let third_wave = &third_rows[(query_count * 2)..];

	eprintln!(
		"throughput wave=partial_update queries={query_count} changed_queries={changed_queries} elapsed_secs={:.3} scanned_rows_per_sec={:.1}",
		third_elapsed.as_secs_f64(),
		total_rows as f64 / third_elapsed.as_secs_f64()
	);

	assert_eq!(third_wave.len(), query_count);
	for (index, row) in third_wave.iter().enumerate() {
		let updated = row["rows_updated"].as_u64().unwrap_or(0);
		let created = row["rows_created"].as_u64().unwrap_or(0);
		let deleted = row["rows_deleted"].as_u64().unwrap_or(0);
		if index < changed_queries {
			assert_eq!(
				updated, 1,
				"expected one updated row in changed query {index}"
			);
			assert_eq!(created, 0);
			assert_eq!(deleted, 0);
		} else {
			assert_eq!(updated, 0, "expected no updates in unchanged query {index}");
			assert_eq!(created, 0);
			assert_eq!(deleted, 0);
		}
	}
	assert_eq!(third_delivered - second_delivered, changed_queries);
}
