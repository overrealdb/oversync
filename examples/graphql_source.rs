//! GraphQL source with Relay cursor pagination → stdout sink.
//!
//! Run: `cargo run --example graphql_source`
//!
//! Requires a GraphQL API at the configured endpoint. This shows how to
//! configure Relay-style cursor pagination with oversync.

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
		sources: vec![SourceDef {
			name: "github-repos".into(),
			connector: "graphql".into(),
			dsn: "https://api.github.com/graphql".into(),
			interval_secs: 3600,
			fail_safe_threshold: 30.0,
			max_retries: 2,
			retry_base_delay_secs: 10,
			diff_mode: DiffMode::default(),
			missed_tick_policy: Default::default(),
			config: serde_json::json!({
				"dsn": "https://api.github.com/graphql",
				"auth": {
					"type": "bearer",
					"token": "ghp_YOUR_TOKEN_HERE"
				},
				"response_path": "data.organization.repositories.nodes",
				"pagination": {
					"cursor_variable": "cursor",
					"has_next_path": "pageInfo.hasNextPage",
					"end_cursor_path": "pageInfo.endCursor"
				}
			}),
			queries: vec![QueryDef {
				id: "repos".into(),
				sql: r#"query($cursor: String) {
  organization(login: "rust-lang") {
    repositories(first: 50, after: $cursor) {
      nodes { id name description stargazerCount }
      pageInfo { hasNextPage endCursor }
    }
  }
}"#
				.into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		}],
		sinks: vec![SinkDef {
			name: "console".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({"pretty": true}),
		}],
		pipes: vec![],
		pipe_presets: vec![],
	};

	engine.start(config).await?;
	println!("graphql source running (will fail without valid token)");

	tokio::time::sleep(std::time::Duration::from_secs(5)).await;
	engine.shutdown().await;
	Ok(())
}
