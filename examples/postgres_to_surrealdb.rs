//! Postgres source → SurrealDB sink: production-like configuration.
//!
//! Run: `cargo run --example postgres_to_surrealdb`
//!
//! Requires:
//!   - PostgreSQL at localhost:5432 with a "demo" database
//!   - SurrealDB at localhost:8000
//!
//! This shows a realistic setup: poll a postgres table every 60 seconds,
//! detect changes via hash diff, and upsert deltas into SurrealDB.

use oversync::OversyncEngine;
use oversync::config::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	tracing_subscriber::fmt::init();

	let engine = OversyncEngine::builder("http://localhost:8000")
		.namespace("catalog")
		.database("sync_state")
		.credentials("root", "root")
		.build()
		.await?;

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "http://localhost:8000".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "catalog".into(),
			database: "sync_state".into(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "sdb-events".into(),
			sink_type: "surrealdb".into(),
			config: serde_json::json!({
				"url": "http://localhost:8000",
				"namespace": "catalog",
				"database": "events",
				"table": "synced_data",
			}),
		}],
		pipes: vec![PipeConfig {
			name: "pg-prod".into(),
			origin: OriginDef {
				connector: "postgres".into(),
				dsn: "postgres://readonly:pass@localhost:5432/demo".into(),
				credential: None,
				trino_url: None,
				config: serde_json::json!({}),
			},
			targets: vec!["sdb-events".into()],
			queries: vec![
				QueryDef {
					id: "users".into(),
					sql: "SELECT id::text, name, email, updated_at FROM users".into(),
					key_column: "id".into(),
					sinks: None,
					transform: None,
				},
				QueryDef {
					id: "products".into(),
					sql: "SELECT id::text, name, price, category FROM products".into(),
					key_column: "id".into(),
					sinks: Some(vec!["sdb-events".into()]),
					transform: None,
				},
			],
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
		}],
		pipe_presets: vec![],
	};

	engine.start(config).await?;
	println!("syncing postgres → surrealdb every 60s, ctrl-c to stop");

	let e = engine.clone();
	tokio::spawn(async move {
		tokio::signal::ctrl_c().await.ok();
		e.shutdown().await;
	});

	// Keep running until shutdown
	while engine.is_running().await {
		tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	}
	Ok(())
}
