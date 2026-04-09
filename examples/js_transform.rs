//! JS transform example: source → JavaScript transform → stdout sink.
//!
//! Run: `cargo run --example js_transform`
//!
//! Demonstrates the JS transform step that mutates or filters rows
//! using a user-supplied JavaScript function (QuickJS engine).

use std::sync::Arc;

use async_trait::async_trait;

use oversync::EmbeddedSync;
use oversync::config::{
	DeltaDef, DiffMode, OriginDef, PipeConfig, QueryDef, RetryDef, ScheduleDef,
};
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, RawRow};
use oversync_core::traits::{OriginConnector, OriginFactory, Sink, TransformHook};
use oversync_transforms::parse_steps;

struct InMemoryConnector {
	rows: Vec<RawRow>,
}

#[async_trait]
impl OriginConnector for InMemoryConnector {
	fn name(&self) -> &str {
		"in-memory"
	}

	async fn fetch_all(&self, _sql: &str, _key: &str) -> Result<Vec<RawRow>, OversyncError> {
		Ok(self.rows.clone())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct InMemoryFactory;

#[async_trait]
impl OriginFactory for InMemoryFactory {
	fn connector_type(&self) -> &str {
		"in-memory"
	}

	async fn create(
		&self,
		_name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		Ok(Box::new(InMemoryConnector {
			rows: vec![
				RawRow {
					row_key: "1".into(),
					row_data: serde_json::json!({"id": 1, "name": "alice", "price": 10, "qty": 3}),
				},
				RawRow {
					row_key: "2".into(),
					row_data: serde_json::json!({"id": 2, "name": "bob", "price": 5, "qty": 0}),
				},
				RawRow {
					row_key: "3".into(),
					row_data: serde_json::json!({"id": 3, "name": "carol", "price": 20, "qty": 2}),
				},
			],
		}))
	}
}

struct CollectSink {
	events: std::sync::Mutex<Vec<EventEnvelope>>,
}

#[async_trait]
impl Sink for CollectSink {
	fn name(&self) -> &str {
		"collect"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		println!(
			"  {} key={} data={}",
			envelope.meta.op, envelope.meta.key, envelope.data
		);
		self.events.lock().unwrap().push(envelope.clone());
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	tracing_subscriber::fmt::init();

	let db = surrealdb::engine::any::connect("mem://").await?;
	db.use_ns("demo").use_db("sync").await?;
	db.query(
		"DEFINE TABLE snapshot SCHEMAFULL;
		 DEFINE FIELD origin_id  ON snapshot TYPE string;
		 DEFINE FIELD query_id   ON snapshot TYPE string;
		 DEFINE FIELD row_key    ON snapshot TYPE string;
		 DEFINE FIELD row_data   ON snapshot TYPE object FLEXIBLE;
		 DEFINE FIELD row_hash   ON snapshot TYPE string;
		 DEFINE FIELD cycle_id   ON snapshot TYPE int;
		 DEFINE FIELD updated_at ON snapshot TYPE datetime DEFAULT time::now();
		 DEFINE FIELD prev_hash  ON snapshot TYPE option<string>;
		 DEFINE INDEX idx_snapshot_key ON snapshot FIELDS origin_id, query_id, row_key UNIQUE;
		 DEFINE INDEX idx_snapshot_cycle ON snapshot FIELDS origin_id, query_id, cycle_id;
		 DEFINE TABLE cycle_log SCHEMAFULL;
		 DEFINE FIELD origin_id    ON cycle_log TYPE string;
		 DEFINE FIELD query_id     ON cycle_log TYPE string;
		 DEFINE FIELD cycle_id     ON cycle_log TYPE int;
		 DEFINE FIELD started_at   ON cycle_log TYPE datetime;
		 DEFINE FIELD finished_at  ON cycle_log TYPE option<datetime>;
		 DEFINE FIELD status       ON cycle_log TYPE string DEFAULT 'running';
		 DEFINE FIELD rows_fetched ON cycle_log TYPE int DEFAULT 0;
		 DEFINE FIELD rows_created ON cycle_log TYPE int DEFAULT 0;
		 DEFINE FIELD rows_updated ON cycle_log TYPE int DEFAULT 0;
		 DEFINE FIELD rows_deleted ON cycle_log TYPE int DEFAULT 0;
		 DEFINE INDEX idx_cycle_source ON cycle_log FIELDS origin_id, query_id, cycle_id UNIQUE;
		 DEFINE TABLE pending_event SCHEMAFULL;
		 DEFINE FIELD origin_id   ON pending_event TYPE string;
		 DEFINE FIELD query_id    ON pending_event TYPE string;
		 DEFINE FIELD cycle_id    ON pending_event TYPE int;
		 DEFINE FIELD events_json ON pending_event TYPE string;
		 DEFINE FIELD created_at  ON pending_event TYPE datetime DEFAULT time::now();
		 DEFINE INDEX idx_pending_source ON pending_event FIELDS origin_id, query_id;",
	)
	.await?;

	// JS transform: compute total and filter out zero-qty rows
	let steps = parse_steps(&[serde_json::json!({
		"type": "js",
		"function": "function transform(row) { if (row.qty === 0) return null; row.total = row.price * row.qty; return row; }"
	})])?;
	let transform: Arc<dyn TransformHook> = Arc::new(steps);

	let sink = Arc::new(CollectSink {
		events: std::sync::Mutex::new(vec![]),
	});

	let sync = EmbeddedSync::builder()
		.state_db(db.clone())
		.snapshot_db(db)
		.skip_schema()
		.register_source(Box::new(InMemoryFactory))
		.add_pipe(PipeConfig {
			name: "demo".into(),
			origin: OriginDef {
				connector: "in-memory".into(),
				dsn: "memory://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "orders".into(),
				sql: "SELECT * FROM orders".into(),
				key_column: "id".into(),
				sinks: None,
				transform: Some("js-enrich".into()),
			}],
			schedule: ScheduleDef {
				interval_secs: 60,
				missed_tick_policy: Default::default(),
				max_requests_per_minute: None,
			},
			delta: DeltaDef {
				diff_mode: DiffMode::Memory,
				fail_safe_threshold: 50.0,
			},
			retry: RetryDef {
				max_retries: 0,
				retry_base_delay_secs: 1,
			},
			recipe: None,
			filters: vec![],
			transforms: vec![],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		})
		.add_transform("js-enrich", transform)
		.add_sink("collect", sink.clone())
		.build()
		.await?;

	println!("--- Sync with JS transform ---");
	println!("Input: 3 rows (alice qty=3, bob qty=0, carol qty=2)");
	println!("JS: filters qty=0, adds total=price*qty\n");

	let result = sync.run_once("demo", "orders").await?;
	println!(
		"\ncreated={} (bob filtered out by JS)",
		result.created.len()
	);

	let events = sink.events.lock().unwrap();
	assert_eq!(events.len(), 2, "bob should be filtered");
	for e in events.iter() {
		assert!(e.data.get("total").is_some(), "total field should exist");
	}

	Ok(())
}
