//! Kafka source example: consume from Kafka topic → stdout sink.
//!
//! Run: `cargo run --example kafka_source`
//!
//! Requires a running Kafka broker at localhost:9092.
//! Produce test messages first:
//!   echo '{"id":"1","name":"alice"}' | kcat -P -b localhost:9092 -t demo-topic

use std::sync::Arc;

use oversync::EmbeddedSync;
use oversync::config::{
	DeltaDef, DiffMode, OriginDef, PipeConfig, QueryDef, RetryDef, ScheduleDef,
};
use oversync_connectors::KafkaOriginFactory;
use oversync_sinks::StdoutSink;

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

	let sync = EmbeddedSync::builder()
		.state_db(db.clone())
		.snapshot_db(db)
		.skip_schema()
		.register_source(Box::new(KafkaOriginFactory))
		.add_pipe(PipeConfig {
			name: "kafka-demo".into(),
			origin: OriginDef {
				connector: "kafka".into(),
				dsn: "localhost:9092".into(),
				credential: None,
				trino_url: None,
				config: serde_json::json!({
					"brokers": "localhost:9092",
					"topic": "demo-topic",
					"group_id": "oversync-demo"
				}),
			},
			targets: vec![],
			queries: vec![QueryDef {
				id: "events".into(),
				sql: "unused-for-kafka".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef {
				interval_secs: 5,
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
		.add_sink("stdout", Arc::new(StdoutSink::new(true)))
		.build()
		.await?;

	println!("--- Polling Kafka topic 'demo-topic' ---");
	let result = sync.run_once("kafka-demo", "events").await?;
	println!(
		"created={} updated={} deleted={}",
		result.created.len(),
		result.updated.len(),
		result.deleted.len()
	);

	Ok(())
}
