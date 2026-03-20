mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use common::surreal::TestSurrealContainer;
use oversync::EmbeddedSync;
use oversync::config::{QueryDef, SourceDef};
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, RawRow};
use oversync_core::traits::{Sink, OriginConnector, OriginFactory, TransformHook};

// ── Test doubles ─────────────────────────────────────────────

struct StaticConnector {
	rows: Vec<RawRow>,
}

#[async_trait]
impl OriginConnector for StaticConnector {
	fn name(&self) -> &str {
		"static"
	}

	async fn fetch_all(&self, _sql: &str, _key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		Ok(self.rows.clone())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct StaticOriginFactory {
	rows: Vec<RawRow>,
}

#[async_trait]
impl OriginFactory for StaticOriginFactory {
	fn connector_type(&self) -> &str {
		"static"
	}

	async fn create(
		&self,
		_name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		Ok(Box::new(StaticConnector {
			rows: self.rows.clone(),
		}))
	}
}

struct RecordingSink {
	events: Arc<Mutex<Vec<EventEnvelope>>>,
}

#[async_trait]
impl Sink for RecordingSink {
	fn name(&self) -> &str {
		"recording"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.events.lock().unwrap().push(envelope.clone());
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct UppercaseTransform;

#[async_trait]
impl TransformHook for UppercaseTransform {
	async fn transform(
		&self,
		envelopes: Vec<EventEnvelope>,
	) -> Result<Vec<EventEnvelope>, OversyncError> {
		Ok(envelopes
			.into_iter()
			.map(|mut e| {
				e.meta.origin_id = e.meta.origin_id.to_uppercase();
				e
			})
			.collect())
	}
}

fn test_source_def() -> SourceDef {
	SourceDef {
		name: "test-src".into(),
		connector: "static".into(),
		dsn: "memory://".into(),
		interval_secs: 60,
		fail_safe_threshold: 50.0,
		max_retries: 0,
		retry_base_delay_secs: 1,
		diff_mode: oversync::config::DiffMode::Memory,
			missed_tick_policy: Default::default(),
		config: serde_json::Value::Null,
		queries: vec![QueryDef {
			id: "q1".into(),
			sql: "SELECT * FROM test".into(),
			key_column: "id".into(),
			sinks: None,
			transform: None,
		}],
	}
}

fn test_rows() -> Vec<RawRow> {
	vec![
		RawRow {
			row_key: "1".into(),
			row_data: serde_json::json!({"id": "1", "name": "alice"}),
		},
		RawRow {
			row_key: "2".into(),
			row_data: serde_json::json!({"id": "2", "name": "bob"}),
		},
	]
}

// ── Builder validation ───────────────────────────────────────

#[tokio::test]
async fn builder_rejects_missing_state_db() {
	let result = EmbeddedSync::builder().skip_schema().build().await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("state_db"));
}

#[tokio::test]
async fn builder_with_skip_schema_builds_ok() {
	let db = surrealdb::engine::any::connect("mem://").await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();

	let sync = EmbeddedSync::builder()
		.state_db(db)
		.skip_schema()
		.build()
		.await;
	assert!(sync.is_ok());
}

// ── run_once ─────────────────────────────────────────────────

#[tokio::test]
async fn run_once_delivers_events_to_custom_sink() {
	let container = TestSurrealContainer::new_raw().await;

	let events: Arc<Mutex<Vec<EventEnvelope>>> = Arc::new(Mutex::new(vec![]));
	let sink = Arc::new(RecordingSink {
		events: Arc::clone(&events),
	});

	let sync = EmbeddedSync::builder()
		.state_db(container.client.clone())
		.snapshot_db(container.client.clone())
		.skip_schema()
		.add_source(test_source_def())
		.add_sink("rec", sink)
		.register_source(Box::new(StaticOriginFactory {
			rows: test_rows(),
		}))
		.build()
		.await
		.unwrap();

	let result = sync.run_once("test-src", "q1").await.unwrap();
	assert_eq!(result.created.len(), 2);
	assert!(result.updated.is_empty());
	assert!(result.deleted.is_empty());

	let recorded = events.lock().unwrap();
	assert_eq!(recorded.len(), 2);
	assert!(recorded.iter().any(|e| e.meta.key == "1"));
	assert!(recorded.iter().any(|e| e.meta.key == "2"));
}

#[tokio::test]
async fn run_once_second_cycle_detects_no_changes() {
	let container = TestSurrealContainer::new_raw().await;

	let events: Arc<Mutex<Vec<EventEnvelope>>> = Arc::new(Mutex::new(vec![]));
	let sink = Arc::new(RecordingSink {
		events: Arc::clone(&events),
	});

	let sync = EmbeddedSync::builder()
		.state_db(container.client.clone())
		.snapshot_db(container.client.clone())
		.skip_schema()
		.add_source(test_source_def())
		.add_sink("rec", sink)
		.register_source(Box::new(StaticOriginFactory {
			rows: test_rows(),
		}))
		.build()
		.await
		.unwrap();

	// First cycle: all created
	let r1 = sync.run_once("test-src", "q1").await.unwrap();
	assert_eq!(r1.created.len(), 2);

	// Second cycle: same data → no changes
	let r2 = sync.run_once("test-src", "q1").await.unwrap();
	assert!(r2.is_empty());
}

#[tokio::test]
async fn run_once_unknown_source_errors() {
	let db = surrealdb::engine::any::connect("mem://").await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();

	let sync = EmbeddedSync::builder()
		.state_db(db)
		.skip_schema()
		.build()
		.await
		.unwrap();

	let result = sync.run_once("nonexistent", "q1").await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("nonexistent"));
}

#[tokio::test]
async fn run_once_unknown_query_errors() {
	let db = surrealdb::engine::any::connect("mem://").await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();

	let sync = EmbeddedSync::builder()
		.state_db(db)
		.skip_schema()
		.add_source(test_source_def())
		.register_source(Box::new(StaticOriginFactory {
			rows: test_rows(),
		}))
		.build()
		.await
		.unwrap();

	let result = sync.run_once("test-src", "nonexistent").await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("nonexistent"));
}

// ── TransformHook ────────────────────────────────────────────

#[tokio::test]
async fn transform_hook_modifies_events_before_sink() {
	let container = TestSurrealContainer::new_raw().await;

	let events: Arc<Mutex<Vec<EventEnvelope>>> = Arc::new(Mutex::new(vec![]));
	let sink = Arc::new(RecordingSink {
		events: Arc::clone(&events),
	});

	let mut source = test_source_def();
	source.queries[0].transform = Some("uppercase".into());

	let sync = EmbeddedSync::builder()
		.state_db(container.client.clone())
		.snapshot_db(container.client.clone())
		.skip_schema()
		.add_source(source)
		.add_sink("rec", sink)
		.add_transform("uppercase", Arc::new(UppercaseTransform))
		.register_source(Box::new(StaticOriginFactory {
			rows: test_rows(),
		}))
		.build()
		.await
		.unwrap();

	sync.run_once("test-src", "q1").await.unwrap();

	let recorded = events.lock().unwrap();
	assert_eq!(recorded.len(), 2);
	for event in recorded.iter() {
		assert_eq!(event.meta.origin_id, "TEST-SRC");
	}
}

// ── start / shutdown ─────────────────────────────────────────

#[tokio::test]
async fn start_spawns_polling_shutdown_stops() {
	let container = TestSurrealContainer::new_raw().await;

	let events: Arc<Mutex<Vec<EventEnvelope>>> = Arc::new(Mutex::new(vec![]));
	let sink = Arc::new(RecordingSink {
		events: Arc::clone(&events),
	});

	let mut source = test_source_def();
	source.interval_secs = 1;

	let sync = EmbeddedSync::builder()
		.state_db(container.client.clone())
		.snapshot_db(container.client.clone())
		.skip_schema()
		.add_source(source)
		.add_sink("rec", sink)
		.register_source(Box::new(StaticOriginFactory {
			rows: test_rows(),
		}))
		.build()
		.await
		.unwrap();

	sync.start().await.unwrap();

	// Wait for at least one cycle to complete
	tokio::time::sleep(std::time::Duration::from_millis(500)).await;

	let count = events.lock().unwrap().len();
	assert!(count >= 2, "expected at least 2 events, got {count}");

	sync.shutdown();
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;
}

#[tokio::test]
async fn shutdown_when_not_started_is_safe() {
	let db = surrealdb::engine::any::connect("mem://").await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();

	let sync = EmbeddedSync::builder()
		.state_db(db)
		.skip_schema()
		.build()
		.await
		.unwrap();

	// Must not panic
	sync.shutdown();
}

#[tokio::test]
async fn transform_hook_ignored_when_query_has_no_transform() {
	let container = TestSurrealContainer::new_raw().await;

	let events: Arc<Mutex<Vec<EventEnvelope>>> = Arc::new(Mutex::new(vec![]));
	let sink = Arc::new(RecordingSink {
		events: Arc::clone(&events),
	});

	// Source has no transform field (None), hook registered but should be ignored
	let sync = EmbeddedSync::builder()
		.state_db(container.client.clone())
		.snapshot_db(container.client.clone())
		.skip_schema()
		.add_source(test_source_def()) // query.transform = None
		.add_sink("rec", sink)
		.add_transform("uppercase", Arc::new(UppercaseTransform))
		.register_source(Box::new(StaticOriginFactory {
			rows: test_rows(),
		}))
		.build()
		.await
		.unwrap();

	sync.run_once("test-src", "q1").await.unwrap();

	let recorded = events.lock().unwrap();
	assert_eq!(recorded.len(), 2);
	// origin_id should NOT be uppercased — hook was not applied
	for event in recorded.iter() {
		assert_eq!(event.meta.origin_id, "test-src");
	}
}

#[tokio::test]
async fn start_twice_errors() {
	let db = surrealdb::engine::any::connect("mem://").await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();

	let sync = EmbeddedSync::builder()
		.state_db(db)
		.skip_schema()
		.build()
		.await
		.unwrap();

	sync.start().await.unwrap();
	let result = sync.start().await;
	assert!(result.is_err());
	assert!(result.unwrap_err().to_string().contains("already running"));
	sync.shutdown();
}
