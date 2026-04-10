//! Multiple pipes → Kafka sink with query-level routing.
//!
//! Run: `cargo run --example multi_source_kafka`
//!
//! Shows: Two postgres-backed pipes, a Kafka sink and a stdout sink.
//! One query routes to both sinks, another only to Kafka.

use oversync::OversyncEngine;
use oversync::config::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	tracing_subscriber::fmt::init();

	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.build()
		.await?;

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "mem://".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "oversync".into(),
			database: "sync".into(),
			snapshot: None,
		},
		sinks: vec![
			SinkDef {
				name: "kafka-events".into(),
				sink_type: "kafka".into(),
				config: serde_json::json!({
					"brokers": "kafka:9092",
					"topic": "oversync-events",
				}),
			},
			SinkDef {
				name: "debug".into(),
				sink_type: "stdout".into(),
				config: serde_json::json!({}),
			},
		],
		pipes: vec![
			PipeConfig {
				name: "pg-users".into(),
				origin: OriginDef {
					connector: "postgres".into(),
					dsn: "postgres://ro:pass@pg1:5432/app".into(),
					credential: None,
					trino_url: None,
					config: serde_json::json!({}),
				},
				targets: vec!["kafka-events".into(), "debug".into()],
				queries: vec![QueryDef {
					id: "users".into(),
					sql: "SELECT id::text, name, email FROM users".into(),
					key_column: "id".into(),
					sinks: None, // broadcast to all sinks
					transform: None,
				}],
				schedule: ScheduleDef {
					interval_secs: 60,
					..ScheduleDef::default()
				},
				delta: DeltaDef {
					diff_mode: DiffMode::Memory,
					fail_safe_threshold: 30.0,
				},
				retry: RetryDef {
					max_retries: 3,
					retry_base_delay_secs: 5,
				},
				recipe: None,
				filters: vec![],
				transforms: vec![],
				links: vec![],
				alert_webhook: None,
				enabled: true,
			},
			PipeConfig {
				name: "pg-orders".into(),
				origin: OriginDef {
					connector: "postgres".into(),
					dsn: "postgres://ro:pass@pg2:5432/orders".into(),
					credential: None,
					trino_url: None,
					config: serde_json::json!({}),
				},
				targets: vec!["kafka-events".into(), "debug".into()],
				queries: vec![QueryDef {
					id: "recent-orders".into(),
					sql: "SELECT id::text, user_id, total, status FROM orders WHERE created_at > now() - interval '7 days'".into(),
					key_column: "id".into(),
					sinks: Some(vec!["kafka-events".into()]), // only kafka
					transform: None,
				}],
				schedule: ScheduleDef {
					interval_secs: 30,
					..ScheduleDef::default()
				},
				delta: DeltaDef {
					diff_mode: DiffMode::Db,
					fail_safe_threshold: 50.0,
				},
				retry: RetryDef {
					max_retries: 2,
					retry_base_delay_secs: 3,
				},
				recipe: None,
				filters: vec![],
				transforms: vec![],
				links: vec![],
				alert_webhook: None,
				enabled: true,
			},
		],
		pipe_presets: vec![],
	};

	engine.start(config).await?;
	println!("multi-source sync running (will fail without real databases)");

	tokio::time::sleep(std::time::Duration::from_secs(3)).await;
	engine.shutdown().await;
	Ok(())
}
