//! Engine with HTTP API: shows how to embed oversync with the full REST API.
//!
//! Run: `cargo run --example http_api --features api`
//!
//! Endpoints available at http://localhost:4200:
//!   GET  /health
//!   GET  /sources
//!   POST /sources       (create)
//!   GET  /sinks
//!   POST /sinks         (create)
//!   POST /sync/pause
//!   POST /sync/resume
//!   GET  /sync/status
//!   GET  /history
//!   GET  /openapi.json

use oversync::OversyncEngine;
use oversync::config::{SinkDef, SurrealDbDef, SyncConfig};

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
			config: serde_json::json!({}),
		}],
		pipes: vec![],
	};

	engine.start(config).await?;

	let app = engine.api_router().await?;
	let listener = tokio::net::TcpListener::bind("0.0.0.0:4200").await?;
	println!("API server at http://localhost:4200");
	println!("Try: curl http://localhost:4200/health");

	let engine_shutdown = engine.clone();
	tokio::spawn(async move {
		tokio::signal::ctrl_c().await.ok();
		engine_shutdown.shutdown().await;
		std::process::exit(0);
	});

	axum::serve(listener, app).await?;
	Ok(())
}
