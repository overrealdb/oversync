//! HTTP source → HTTP (webhook) sink: REST API polling with webhook delivery.
//!
//! Run: `cargo run --example http_to_webhook`
//!
//! Shows: HTTP source with Bearer auth and offset pagination delivering
//! delta events to a webhook endpoint via the HTTP sink.

use oversync::config::*;
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
		sources: vec![SourceDef {
			name: "catalog-api".into(),
			connector: "http".into(),
			dsn: "https://api.example.com".into(),
			interval_secs: 300,
			fail_safe_threshold: 30.0,
			max_retries: 3,
			retry_base_delay_secs: 5,
			diff_mode: DiffMode::default(),
			config: serde_json::json!({
				"dsn": "https://api.example.com",
				"auth": {"type": "bearer", "token": "sk-api-key"},
				"headers": {"Accept": "application/json"},
				"response_path": "data.items",
				"pagination": {
					"type": "offset",
					"page_size": 100,
					"limit_param": "limit",
					"offset_param": "offset"
				}
			}),
			queries: vec![QueryDef {
				id: "datasets".into(),
				sql: "/v1/datasets".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		}],
		sinks: vec![
			SinkDef {
				name: "my-webhook".into(),
				sink_type: "http".into(),
				config: serde_json::json!({
					"url": "https://example.com/api/webhooks/oversync",
					"auth": {"type": "bearer", "token": "webhook-secret"},
					"headers": {"X-Source": "oversync"},
					"retry_count": 3,
					"timeout_secs": 30,
				}),
			},
			SinkDef {
				name: "debug".into(),
				sink_type: "stdout".into(),
				config: serde_json::json!({"pretty": true}),
			},
		],
	};

	engine.start(config).await?;
	println!("http source → webhook sink running (will fail without real endpoints)");

	tokio::time::sleep(std::time::Duration::from_secs(3)).await;
	engine.shutdown().await;
	Ok(())
}
