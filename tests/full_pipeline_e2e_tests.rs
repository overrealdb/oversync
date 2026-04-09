//! Full pipeline e2e: origin → pre-delta filter → diff → transform → sink
//!
//! Tests the entire data flow with real SurrealDB.

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::surreal::TestSurrealContainer;
use oversync::config::*;
use oversync::embedded::EmbeddedSync;
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, RawRow};
use oversync_core::traits::{OriginConnector, OriginFactory, Sink};
use tokio::sync::Mutex;

// ── Static connector ────────────────────────────────────────

struct StaticConnector {
	rows: Vec<RawRow>,
}

#[async_trait]
impl OriginConnector for StaticConnector {
	fn name(&self) -> &str {
		"static"
	}
	async fn fetch_all(&self, _sql: &str, _key: &str) -> Result<Vec<RawRow>, OversyncError> {
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

// ── Recording sink ──────────────────────────────────────────

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

fn test_rows() -> Vec<RawRow> {
	vec![
		RawRow {
			row_key: "1".into(),
			row_data: serde_json::json!({"id": "1", "name": "alice", "schema": "public"}),
		},
		RawRow {
			row_key: "2".into(),
			row_data: serde_json::json!({"id": "2", "name": "bob", "schema": "internal"}),
		},
		RawRow {
			row_key: "3".into(),
			row_data: serde_json::json!({"id": "3", "name": "charlie", "schema": "public"}),
		},
	]
}

// ── Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn full_pipeline_origin_to_sink() {
	let surreal = TestSurrealContainer::new().await;
	let events = Arc::new(Mutex::new(Vec::new()));

	let sync = EmbeddedSync::builder()
		.state_db(surreal.client.clone())
		.skip_schema()
		.register_source(Box::new(StaticOriginFactory { rows: test_rows() }))
		.add_sink(
			"recorder",
			Arc::new(RecordingSink {
				events: events.clone(),
			}),
		)
		.add_pipe(PipeConfig {
			name: "e2e-pipe".into(),
			origin: OriginDef {
				connector: "static".into(),
				dsn: "memory://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "q1".into(),
				sql: "SELECT *".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef::default(),
			delta: DeltaDef::default(),
			retry: RetryDef::default(),
			recipe: None,
			filters: vec![],
			transforms: vec![],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		})
		.build()
		.await
		.unwrap();

	// First run: all 3 rows created
	sync.run_once("e2e-pipe", "q1").await.unwrap();
	let delivered = events.lock().await;
	assert_eq!(
		delivered.len(),
		3,
		"first run should deliver 3 created events"
	);
	assert!(
		delivered
			.iter()
			.all(|e| e.meta.op == oversync_core::model::OpType::Created)
	);
}

#[tokio::test]
async fn full_pipeline_with_transforms() {
	let surreal = TestSurrealContainer::new().await;
	let events = Arc::new(Mutex::new(Vec::new()));

	let sync = EmbeddedSync::builder()
		.state_db(surreal.client.clone())
		.skip_schema()
		.register_source(Box::new(StaticOriginFactory { rows: test_rows() }))
		.add_sink(
			"recorder",
			Arc::new(RecordingSink {
				events: events.clone(),
			}),
		)
		.add_pipe(PipeConfig {
			name: "transform-pipe".into(),
			origin: OriginDef {
				connector: "static".into(),
				dsn: "memory://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "q1".into(),
				sql: "SELECT *".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef::default(),
			delta: DeltaDef::default(),
			retry: RetryDef::default(),
			recipe: None,
			filters: vec![],
			transforms: vec![
				serde_json::json!({"type": "upper", "field": "name"}),
				serde_json::json!({"type": "set", "field": "version", "value": 1}),
			],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		})
		.build()
		.await
		.unwrap();

	sync.run_once("transform-pipe", "q1").await.unwrap();
	let delivered = events.lock().await;
	assert_eq!(delivered.len(), 3);
	assert_eq!(delivered[0].data["name"], "ALICE");
	assert_eq!(delivered[0].data["version"], 1);
	assert_eq!(delivered[1].data["name"], "BOB");
}

#[tokio::test]
async fn full_pipeline_with_pre_delta_filters() {
	let surreal = TestSurrealContainer::new().await;
	let events = Arc::new(Mutex::new(Vec::new()));

	let sync = EmbeddedSync::builder()
		.state_db(surreal.client.clone())
		.skip_schema()
		.register_source(Box::new(StaticOriginFactory { rows: test_rows() }))
		.add_sink(
			"recorder",
			Arc::new(RecordingSink {
				events: events.clone(),
			}),
		)
		.add_pipe(PipeConfig {
			name: "filter-pipe".into(),
			origin: OriginDef {
				connector: "static".into(),
				dsn: "memory://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "q1".into(),
				sql: "SELECT *".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef::default(),
			delta: DeltaDef::default(),
			retry: RetryDef::default(),
			recipe: None,
			filters: vec![serde_json::json!({
				"type": "filter",
				"field": "schema",
				"op": "eq",
				"value": "public"
			})],
			transforms: vec![],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		})
		.build()
		.await
		.unwrap();

	sync.run_once("filter-pipe", "q1").await.unwrap();
	let delivered = events.lock().await;
	// Only "public" rows: alice (id=1) and charlie (id=3)
	assert_eq!(
		delivered.len(),
		2,
		"filter should keep only public schema rows"
	);
	let keys: Vec<&str> = delivered.iter().map(|e| e.meta.key.as_str()).collect();
	assert!(keys.contains(&"1"));
	assert!(keys.contains(&"3"));
	assert!(!keys.contains(&"2")); // bob is internal
}

#[tokio::test]
async fn full_pipeline_second_run_no_changes() {
	let surreal = TestSurrealContainer::new().await;
	let events = Arc::new(Mutex::new(Vec::new()));

	let sync = EmbeddedSync::builder()
		.state_db(surreal.client.clone())
		.skip_schema()
		.register_source(Box::new(StaticOriginFactory { rows: test_rows() }))
		.add_sink(
			"recorder",
			Arc::new(RecordingSink {
				events: events.clone(),
			}),
		)
		.add_pipe(PipeConfig {
			name: "idempotent-pipe".into(),
			origin: OriginDef {
				connector: "static".into(),
				dsn: "memory://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "q1".into(),
				sql: "SELECT *".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef::default(),
			delta: DeltaDef::default(),
			retry: RetryDef::default(),
			recipe: None,
			filters: vec![],
			transforms: vec![],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		})
		.build()
		.await
		.unwrap();

	// First run: 3 created
	sync.run_once("idempotent-pipe", "q1").await.unwrap();
	assert_eq!(events.lock().await.len(), 3);

	// Second run: same data → no changes
	events.lock().await.clear();
	sync.run_once("idempotent-pipe", "q1").await.unwrap();
	assert_eq!(
		events.lock().await.len(),
		0,
		"same data should produce zero events"
	);
}

#[tokio::test]
async fn full_pipeline_filters_plus_transforms() {
	let surreal = TestSurrealContainer::new().await;
	let events = Arc::new(Mutex::new(Vec::new()));

	let sync = EmbeddedSync::builder()
		.state_db(surreal.client.clone())
		.skip_schema()
		.register_source(Box::new(StaticOriginFactory { rows: test_rows() }))
		.add_sink(
			"recorder",
			Arc::new(RecordingSink {
				events: events.clone(),
			}),
		)
		.add_pipe(PipeConfig {
			name: "full-pipe".into(),
			origin: OriginDef {
				connector: "static".into(),
				dsn: "memory://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "q1".into(),
				sql: "SELECT *".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef::default(),
			delta: DeltaDef::default(),
			retry: RetryDef::default(),
			recipe: None,
			filters: vec![
				serde_json::json!({"type": "filter", "field": "schema", "op": "eq", "value": "public"}),
			],
			transforms: vec![
				serde_json::json!({"type": "upper", "field": "name"}),
				serde_json::json!({"type": "remove", "field": "schema"}),
			],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		})
		.build()
		.await
		.unwrap();

	sync.run_once("full-pipe", "q1").await.unwrap();
	let delivered = events.lock().await;
	assert_eq!(delivered.len(), 2, "only public rows");
	// Transforms applied: name uppercased, schema removed
	assert_eq!(delivered[0].data["name"], "ALICE");
	assert!(
		delivered[0].data.get("schema").is_none(),
		"schema should be removed by transform"
	);
}
