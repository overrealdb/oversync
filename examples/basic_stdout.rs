//! Minimal oversync example: embedded engine with stdout sink.
//!
//! Run: `cargo run --example basic_stdout`
//!
//! This starts an in-memory SurrealDB, creates a sync config with no sources
//! and a stdout sink, runs for 2 seconds, then shuts down. In a real app
//! you would configure actual sources (postgres, http, graphql, etc).

use oversync::config::{SinkDef, SyncConfig, SurrealDbDef};
use oversync::OversyncEngine;

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
		sources: vec![],
		sinks: vec![SinkDef {
			name: "console".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({"pretty": true}),
		}],
		pipes: vec![],
	};

	engine.start(config).await?;
	println!("engine running: {}", engine.is_running().await);

	tokio::time::sleep(std::time::Duration::from_secs(2)).await;

	engine.shutdown().await;
	println!("engine stopped");
	Ok(())
}
