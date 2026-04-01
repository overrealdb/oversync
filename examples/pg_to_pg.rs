//! PostgreSQL source → PostgreSQL sink example.
//!
//! Run: `cargo run --example pg_to_pg`
//!
//! Requires two PostgreSQL databases:
//!   - Source: PG_SOURCE_DSN (default: postgres://postgres:postgres@localhost:5432/source)
//!   - Sink:   PG_SINK_DSN   (default: postgres://postgres:postgres@localhost:5432/sink)
//!
//! Create the sink table first:
//!   CREATE TABLE synced_events (
//!       key TEXT PRIMARY KEY,
//!       data JSONB,
//!       op TEXT,
//!       origin_id TEXT,
//!       query_id TEXT,
//!       hash TEXT,
//!       cycle_id BIGINT,
//!       synced_at TIMESTAMPTZ DEFAULT NOW()
//!   );

use std::sync::Arc;

use oversync::EmbeddedSync;
use oversync::config::{QueryDef, SourceDef};
use oversync_connectors::PostgresOriginFactory;
use oversync_sinks::{PostgresSink, PostgresTargetFactory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	tracing_subscriber::fmt::init();

	let source_dsn = std::env::var("PG_SOURCE_DSN")
		.unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/source".into());
	let sink_dsn = std::env::var("PG_SINK_DSN")
		.unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/sink".into());

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

	let pg_sink = PostgresSink::new("pg-sink", &sink_dsn, "synced_events", "public").await?;

	let sync = EmbeddedSync::builder()
		.state_db(db.clone())
		.snapshot_db(db)
		.skip_schema()
		.register_source(Box::new(PostgresOriginFactory))
		.register_sink(Box::new(PostgresTargetFactory))
		.add_source(SourceDef {
			name: "pg-source".into(),
			connector: "postgres".into(),
			dsn: source_dsn.clone(),
			interval_secs: 10,
			fail_safe_threshold: 50.0,
			max_retries: 0,
			retry_base_delay_secs: 1,
			diff_mode: oversync::config::DiffMode::Memory,
			missed_tick_policy: Default::default(),
			config: serde_json::json!({"dsn": source_dsn}),
			queries: vec![QueryDef {
				id: "users".into(),
				sql: "SELECT id::text, name, email FROM users".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		})
		.add_sink("pg-sink", Arc::new(pg_sink))
		.build()
		.await?;

	println!("--- PG to PG sync ---");
	let result = sync.run_once("pg-source", "users").await?;
	println!(
		"created={} updated={} deleted={}",
		result.created.len(),
		result.updated.len(),
		result.deleted.len()
	);

	Ok(())
}
