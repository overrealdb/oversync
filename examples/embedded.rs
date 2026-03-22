//! Embedded oversync example: programmatic DeltaEngine + custom sink.
//!
//! Run: `cargo run --example embedded`
//!
//! Demonstrates the library-mode API: bring your own SurrealDB connection,
//! register a custom source connector, add a pre-built sink, and run a
//! single sync cycle programmatically.
//!
//! Schema tables are created inline — in production you'd use overshift
//! migrations or import `surql/schema/sync/tables.surql`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use oversync::EmbeddedSync;
use oversync::config::{QueryDef, SourceDef};
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, RawRow};
use oversync_core::traits::{OriginConnector, OriginFactory, Sink};

struct InMemoryConnector {
	rows: Vec<RawRow>,
}

#[async_trait]
impl OriginConnector for InMemoryConnector {
	fn name(&self) -> &str {
		"in-memory"
	}

	async fn fetch_all(&self, _sql: &str, _key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		Ok(self.rows.clone())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct InMemoryOriginFactory;

#[async_trait]
impl OriginFactory for InMemoryOriginFactory {
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
					row_data: serde_json::json!({"id": 1, "name": "alice"}),
				},
				RawRow {
					row_key: "2".into(),
					row_data: serde_json::json!({"id": 2, "name": "bob"}),
				},
			],
		}))
	}
}

struct PrintSink {
	received: Arc<Mutex<Vec<EventEnvelope>>>,
}

#[async_trait]
impl Sink for PrintSink {
	fn name(&self) -> &str {
		"print"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		println!(
			"  {} {} key={}",
			envelope.meta.op, envelope.meta.origin_id, envelope.meta.key
		);
		self.received.lock().unwrap().push(envelope.clone());
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	tracing_subscriber::fmt::init();

	// 1. Connect to SurrealDB (in-memory for this example)
	let db = surrealdb::engine::any::connect("mem://").await?;
	db.use_ns("demo").use_db("sync").await?;

	// Apply minimal schema (snapshot, cycle_log, pending_event tables)
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

	// 2. Build EmbeddedSync with custom source + sink
	let received = Arc::new(Mutex::new(vec![]));
	let sink = Arc::new(PrintSink {
		received: Arc::clone(&received),
	});

	let sync = EmbeddedSync::builder()
		.state_db(db.clone())
		.snapshot_db(db)
		.skip_schema()
		.register_source(Box::new(InMemoryOriginFactory))
		.add_source(SourceDef {
			name: "demo".into(),
			connector: "in-memory".into(),
			dsn: "memory://".into(),
			interval_secs: 60,
			fail_safe_threshold: 50.0,
			max_retries: 0,
			retry_base_delay_secs: 1,
			diff_mode: oversync::config::DiffMode::Memory,
			missed_tick_policy: Default::default(),
			config: serde_json::Value::Null,
			queries: vec![QueryDef {
				id: "users".into(),
				sql: "SELECT * FROM users".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		})
		.add_sink("print", sink)
		.build()
		.await?;

	// 3. Run one cycle
	println!("--- Cycle 1 ---");
	let result = sync.run_once("demo", "users").await?;
	println!(
		"created={} updated={} deleted={}",
		result.created.len(),
		result.updated.len(),
		result.deleted.len()
	);

	// 4. Run again — same data → no changes
	println!("\n--- Cycle 2 (no changes) ---");
	let result = sync.run_once("demo", "users").await?;
	println!(
		"created={} updated={} deleted={}",
		result.created.len(),
		result.updated.len(),
		result.deleted.len()
	);

	let total = received.lock().unwrap().len();
	println!("\ntotal events received by sink: {total}");

	Ok(())
}
