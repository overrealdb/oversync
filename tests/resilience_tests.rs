mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use oversync::cycle::{CycleConfig, CycleRunner};
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, RawRow};
use oversync_core::traits::{Sink, SourceConnector};
use oversync_delta::DeltaEngine;

use common::surreal::TestSurrealContainer;

// ── Mock connectors and sinks for testing failure scenarios ────

struct FailingConnector {
	fail_count: AtomicUsize,
	rows: Vec<RawRow>,
}

impl FailingConnector {
	fn new(fail_n_times: usize, rows: Vec<RawRow>) -> Self {
		Self {
			fail_count: AtomicUsize::new(fail_n_times),
			rows,
		}
	}
}

#[async_trait::async_trait]
impl SourceConnector for FailingConnector {
	fn name(&self) -> &str {
		"failing-connector"
	}

	async fn fetch_all(&self, _sql: &str, _key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		let remaining = self.fail_count.load(Ordering::SeqCst);
		if remaining > 0 {
			self.fail_count.fetch_sub(1, Ordering::SeqCst);
			Err(OversyncError::Connector("simulated source failure".into()))
		} else {
			Ok(self.rows.clone())
		}
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct FailingSink {
	fail_count: AtomicUsize,
	delivered: std::sync::Mutex<Vec<EventEnvelope>>,
}

impl FailingSink {
	fn new(fail_n_times: usize) -> Self {
		Self {
			fail_count: AtomicUsize::new(fail_n_times),
			delivered: std::sync::Mutex::new(Vec::new()),
		}
	}

	fn delivered_count(&self) -> usize {
		self.delivered.lock().unwrap().len()
	}
}

#[async_trait::async_trait]
impl Sink for FailingSink {
	fn name(&self) -> &str {
		"failing-sink"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let remaining = self.fail_count.load(Ordering::SeqCst);
		if remaining > 0 {
			self.fail_count.fetch_sub(1, Ordering::SeqCst);
			Err(OversyncError::Sink("simulated sink failure".into()))
		} else {
			self.delivered.lock().unwrap().push(envelope.clone());
			Ok(())
		}
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct CountingSink {
	count: AtomicUsize,
}

impl CountingSink {
	fn new() -> Self {
		Self {
			count: AtomicUsize::new(0),
		}
	}

	fn count(&self) -> usize {
		self.count.load(Ordering::SeqCst)
	}
}

#[async_trait::async_trait]
impl Sink for CountingSink {
	fn name(&self) -> &str {
		"counting-sink"
	}

	async fn send_event(&self, _envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.count.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

fn test_rows(n: usize) -> Vec<RawRow> {
	(0..n)
		.map(|i| RawRow {
			row_key: format!("r{i}"),
			row_data: serde_json::json!({"i": i}),
		})
		.collect()
}

fn cycle_config() -> CycleConfig {
	CycleConfig {
		source_id: "test".into(),
		query_id: "q".into(),
		sql: "unused".into(),
		key_column: "id".into(),
		fail_safe_threshold: 50.0,
		diff_mode: oversync::config::DiffMode::Db,
			missed_tick_policy: Default::default(),
		transform: None,
	}
}

// ── Source failure tests ──────────────────────────────────────

#[tokio::test]
async fn source_down_cycle_fails_gracefully() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let connector = FailingConnector::new(999, vec![]);
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	let result = runner.run(&cycle_config()).await;
	assert!(result.is_err());
	assert!(
		result
			.unwrap_err()
			.to_string()
			.contains("simulated source failure")
	);
}

#[tokio::test]
async fn source_recovers_after_failures() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let connector = FailingConnector::new(2, test_rows(3));
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	// First 2 attempts fail
	assert!(runner.run(&cycle_config()).await.is_err());
	assert!(runner.run(&cycle_config()).await.is_err());
	// Third succeeds
	let diff = runner.run(&cycle_config()).await.unwrap();
	assert_eq!(diff.created.len(), 3);
}

#[tokio::test]
async fn source_failure_does_not_corrupt_snapshot() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: succeeds with 3 rows
	let good_connector = FailingConnector::new(0, test_rows(3));
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner = CycleRunner::new(&engine, &good_connector, &sinks);
	runner.run(&cycle_config()).await.unwrap();

	// Cycle 2: source fails
	let bad_connector = FailingConnector::new(999, vec![]);
	let runner2 = CycleRunner::new(&engine, &bad_connector, &sinks);
	assert!(runner2.run(&cycle_config()).await.is_err());

	// Snapshot should still have 3 rows from cycle 1
	let keys = engine.read_snapshot_keys("test", "q").await.unwrap();
	assert_eq!(keys.len(), 3);
}

// ── Sink failure tests ───────────────────────────────────────

#[tokio::test]
async fn sink_down_events_saved_to_outbox() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let connector = FailingConnector::new(0, test_rows(3));
	let sink = Arc::new(FailingSink::new(999));
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(FailingSink::new(999))];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	// Cycle fails because sink is down
	let result = runner.run(&cycle_config()).await;
	assert!(result.is_err());

	// But events should be in outbox
	let pending = engine.read_pending_events("test", "q").await.unwrap();
	assert_eq!(pending.len(), 1, "events should be saved to outbox");
	assert_eq!(pending[0].1.len(), 3, "3 events pending");

	drop(sink);
}

#[tokio::test]
async fn sink_recovers_pending_events_delivered() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: sink fails → events go to outbox
	let connector = FailingConnector::new(0, test_rows(3));
	let sinks_fail: Vec<Arc<dyn Sink>> = vec![Arc::new(FailingSink::new(999))];
	let runner1 = CycleRunner::new(&engine, &connector, &sinks_fail);
	assert!(runner1.run(&cycle_config()).await.is_err());

	let pending = engine.read_pending_events("test", "q").await.unwrap();
	assert_eq!(pending.len(), 1);

	// Cycle 2: sink works → pending events delivered first, then new cycle
	let counting = Arc::new(CountingSink::new());
	let sinks_ok: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner2 = CycleRunner::new(&engine, &connector, &sinks_ok);

	// This should: 1) deliver pending events, 2) run new cycle
	runner2.run(&cycle_config()).await.unwrap();

	// Pending should be cleared
	let pending = engine.read_pending_events("test", "q").await.unwrap();
	assert!(
		pending.is_empty(),
		"outbox should be cleared after delivery"
	);

	drop(counting);
}

#[tokio::test]
async fn sink_failure_does_not_delete_stale_rows() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: 5 rows, succeeds
	let connector1 = FailingConnector::new(0, test_rows(5));
	let sinks_ok: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner1 = CycleRunner::new(&engine, &connector1, &sinks_ok);
	runner1.run(&cycle_config()).await.unwrap();

	// Cycle 2: only 3 rows (2 deleted), but sink fails
	let connector2 = FailingConnector::new(0, test_rows(3));
	let sinks_fail: Vec<Arc<dyn Sink>> = vec![Arc::new(FailingSink::new(999))];
	let runner2 = CycleRunner::new(&engine, &connector2, &sinks_fail);
	assert!(runner2.run(&cycle_config()).await.is_err());

	// Stale rows should NOT be deleted (sink didn't confirm)
	// Snapshot has: 3 updated rows (cycle 2) + 2 stale rows (cycle 1)
	let keys = engine.read_snapshot_keys("test", "q").await.unwrap();
	assert_eq!(keys.len(), 5, "stale rows should remain when sink fails");
}

// ── Outbox tests ─────────────────────────────────────────────

#[tokio::test]
async fn outbox_save_read_delete_roundtrip() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let events = vec![EventEnvelope {
		meta: oversync_core::model::EventMeta {
			op: oversync_core::model::OpType::Created,
			source_id: "s".into(),
			query_id: "q".into(),
			key: "k".into(),
			hash: "h".into(),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({"v": 1}),
	}];

	engine
		.save_pending_events("s", "q", 1, &events)
		.await
		.unwrap();

	let pending = engine.read_pending_events("s", "q").await.unwrap();
	assert_eq!(pending.len(), 1);
	assert_eq!(pending[0].0, 1);
	assert_eq!(pending[0].1.len(), 1);
	assert_eq!(pending[0].1[0].meta.key, "k");

	engine.delete_pending_events("s", "q", 1).await.unwrap();

	let pending = engine.read_pending_events("s", "q").await.unwrap();
	assert!(pending.is_empty());
}

#[tokio::test]
async fn outbox_multiple_cycles_accumulate() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let e1 = vec![EventEnvelope {
		meta: oversync_core::model::EventMeta {
			op: oversync_core::model::OpType::Created,
			source_id: "s".into(),
			query_id: "q".into(),
			key: "a".into(),
			hash: "h1".into(),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({}),
	}];
	let e2 = vec![EventEnvelope {
		meta: oversync_core::model::EventMeta {
			op: oversync_core::model::OpType::Updated,
			source_id: "s".into(),
			query_id: "q".into(),
			key: "b".into(),
			hash: "h2".into(),
			cycle_id: 2,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({}),
	}];

	engine.save_pending_events("s", "q", 1, &e1).await.unwrap();
	engine.save_pending_events("s", "q", 2, &e2).await.unwrap();

	let pending = engine.read_pending_events("s", "q").await.unwrap();
	assert_eq!(pending.len(), 2);
	assert_eq!(pending[0].0, 1);
	assert_eq!(pending[1].0, 2);

	// Delete up to cycle 1
	engine.delete_pending_events("s", "q", 1).await.unwrap();
	let pending = engine.read_pending_events("s", "q").await.unwrap();
	assert_eq!(pending.len(), 1);
	assert_eq!(pending[0].0, 2);
}

#[tokio::test]
async fn outbox_isolation_between_sources() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let event = vec![EventEnvelope {
		meta: oversync_core::model::EventMeta {
			op: oversync_core::model::OpType::Created,
			source_id: "s".into(),
			query_id: "q".into(),
			key: "k".into(),
			hash: "h".into(),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({}),
	}];

	engine
		.save_pending_events("src_a", "q1", 1, &event)
		.await
		.unwrap();
	engine
		.save_pending_events("src_b", "q1", 1, &event)
		.await
		.unwrap();

	let a = engine.read_pending_events("src_a", "q1").await.unwrap();
	let b = engine.read_pending_events("src_b", "q1").await.unwrap();
	let c = engine.read_pending_events("src_c", "q1").await.unwrap();

	assert_eq!(a.len(), 1);
	assert_eq!(b.len(), 1);
	assert!(c.is_empty());
}

// ── Multiple cycle recovery tests ────────────────────────────

#[tokio::test]
async fn three_cycles_with_middle_failure() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: 3 rows, succeeds
	let connector = FailingConnector::new(0, test_rows(3));
	let sinks_ok: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner = CycleRunner::new(&engine, &connector, &sinks_ok);
	let d1 = runner.run(&cycle_config()).await.unwrap();
	assert_eq!(d1.created.len(), 3);

	// Cycle 2: source fails
	let bad_connector = FailingConnector::new(999, vec![]);
	let runner2 = CycleRunner::new(&engine, &bad_connector, &sinks_ok);
	assert!(runner2.run(&cycle_config()).await.is_err());

	// Cycle 3: recovers with same 3 rows
	let d3 = runner.run(&cycle_config()).await.unwrap();
	// Should be no changes (same data as cycle 1)
	assert!(d3.is_empty(), "same data after recovery = no changes");
}

#[tokio::test]
async fn recovery_after_extended_downtime_with_changes() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: rows a, b, c
	let rows1 = vec![
		RawRow {
			row_key: "a".into(),
			row_data: serde_json::json!({"v": 1}),
		},
		RawRow {
			row_key: "b".into(),
			row_data: serde_json::json!({"v": 2}),
		},
		RawRow {
			row_key: "c".into(),
			row_data: serde_json::json!({"v": 3}),
		},
		RawRow {
			row_key: "d".into(),
			row_data: serde_json::json!({"v": 4}),
		},
		RawRow {
			row_key: "e".into(),
			row_data: serde_json::json!({"v": 5}),
		},
	];
	let conn1 = FailingConnector::new(0, rows1);
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner1 = CycleRunner::new(&engine, &conn1, &sinks);
	runner1.run(&cycle_config()).await.unwrap();

	// Cycles 2-4: source is down
	let bad = FailingConnector::new(999, vec![]);
	let runner_bad = CycleRunner::new(&engine, &bad, &sinks);
	assert!(runner_bad.run(&cycle_config()).await.is_err());
	assert!(runner_bad.run(&cycle_config()).await.is_err());
	assert!(runner_bad.run(&cycle_config()).await.is_err());

	// Cycle 5: source back with changes (b updated, c gone, f new)
	let rows5 = vec![
		RawRow {
			row_key: "a".into(),
			row_data: serde_json::json!({"v": 1}),
		},
		RawRow {
			row_key: "b".into(),
			row_data: serde_json::json!({"v": 999}),
		},
		RawRow {
			row_key: "d".into(),
			row_data: serde_json::json!({"v": 4}),
		},
		RawRow {
			row_key: "e".into(),
			row_data: serde_json::json!({"v": 5}),
		},
		RawRow {
			row_key: "f".into(),
			row_data: serde_json::json!({"v": 6}),
		},
	];
	let conn5 = FailingConnector::new(0, rows5);
	let runner5 = CycleRunner::new(&engine, &conn5, &sinks);
	let diff = runner5.run(&cycle_config()).await.unwrap();

	assert_eq!(diff.created.len(), 1, "f is new");
	assert_eq!(diff.updated.len(), 1, "b updated");
	assert_eq!(diff.deleted.len(), 1, "c gone");
}

#[tokio::test]
async fn cycle_log_tracks_failed_status() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let connector = FailingConnector::new(999, vec![]);
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	runner.run(&cycle_config()).await.ok();

	let mut res = t
		.client
		.query("SELECT status FROM cycle_log WHERE source_id = 'test'")
		.await
		.unwrap();
	let logs: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(!logs.is_empty());
	assert_eq!(logs[0]["status"], "failed");
}

#[tokio::test]
async fn empty_source_no_events_no_error() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let connector = FailingConnector::new(0, vec![]);
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(CountingSink::new())];
	let runner = CycleRunner::new(&engine, &connector, &sinks);

	let diff = runner.run(&cycle_config()).await.unwrap();
	assert!(diff.is_empty());
}
