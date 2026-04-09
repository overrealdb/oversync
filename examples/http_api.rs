//! Engine with HTTP API: shows how to embed oversync with the full REST API.
//!
//! Run:
//!   export OVERSYNC_CREDENTIAL_KEY='replace-with-a-strong-passphrase'
//!   cargo run --example http_api --features api
//!
//! Endpoints available at http://localhost:4200:
//!   GET  /health
//!   GET  /pipes
//!   POST /pipes
//!   GET  /pipe-presets
//!   POST /pipe-presets
//!   GET  /pipes/{name}/resolve
//!   POST /pipes/dry-run
//!   GET  /sinks
//!   POST /sinks
//!   GET  /credentials
//!   POST /credentials
//!   GET  /config/versions
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

	let engine = OversyncEngine::builder("mem://").build().await?;

	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "mem://".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "oversync".into(),
			database: "sync".into(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "console".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![],
		pipe_presets: vec![],
	};

	engine.start(config).await?;

	let app = engine.api_router().await?;
	let listener = tokio::net::TcpListener::bind("0.0.0.0:4200").await?;
	println!("API server at http://localhost:4200");
	println!("Try: curl http://localhost:4200/health");
	println!("OpenAPI: http://localhost:4200/openapi.json");

	let engine_shutdown = engine.clone();
	tokio::spawn(async move {
		tokio::signal::ctrl_c().await.ok();
		engine_shutdown.shutdown().await;
		std::process::exit(0);
	});

	axum::serve(listener, app).await?;
	Ok(())
}
