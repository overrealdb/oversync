//! Dry-run with mock data: preview pipeline results without any connections.
//!
//! Run: `cargo run --example dry_run_mock`

use std::sync::Arc;

use oversync::PluginRegistry;
use oversync::config::*;
use oversync::dry_run::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let pipe = PipeConfig {
		name: "demo-pipe".into(),
		origin: OriginDef {
			connector: "mock".into(),
			dsn: "mock://".into(),
			credential: None,
			trino_url: None,
			config: serde_json::json!({}),
		},
		targets: vec![],
		queries: vec![QueryDef {
			id: "users".into(),
			sql: "SELECT * FROM users".into(),
			key_column: "id".into(),
			sinks: None,
			transform: None,
		}],
		schedule: ScheduleDef::default(),
		delta: DeltaDef::default(),
		retry: RetryDef::default(),
		filters: vec![],
		transforms: vec![],
		alert_webhook: None,
		enabled: true,
	};

	let mock_data = vec![
		oversync_core::model::RawRow {
			row_key: "1".into(),
			row_data: serde_json::json!({"id": "1", "name": "alice", "email": "alice@example.com"}),
		},
		oversync_core::model::RawRow {
			row_key: "2".into(),
			row_data: serde_json::json!({"id": "2", "name": "bob", "email": "bob@example.com"}),
		},
		oversync_core::model::RawRow {
			row_key: "3".into(),
			row_data: serde_json::json!({"id": "3", "name": "charlie", "email": "charlie@example.com"}),
		},
	];

	// Build transform chain: uppercase names + add version
	let chain = oversync_transforms::StepChain::new(vec![
		Box::new(oversync_transforms::steps::Upper {
			field: "name".into(),
		}),
		Box::new(oversync_transforms::steps::Set {
			field: "version".into(),
			value: serde_json::json!(1),
		}),
	]);

	let req = DryRunRequest {
		pipe,
		query_id: "users".into(),
		mode: DryRunMode::Mock,
		mock_data,
		row_limit: 100,
		transforms: vec![],
		use_existing_state: false,
		credentials: None,
	};

	let registry = PluginRegistry::new();
	let result = execute_dry_run(&req, &registry, Some(Arc::new(chain)), None).await?;

	println!("=== Dry-Run Results ===\n");
	println!("Rows fetched: {}", result.stats.rows_fetched);
	println!("Changes:");
	println!("  Created: {}", result.changes.created);
	println!("  Updated: {}", result.changes.updated);
	println!("  Deleted: {}", result.changes.deleted);
	println!(
		"\nAfter transforms ({} events):",
		result.after_transform.len()
	);
	for env in &result.after_transform {
		println!("  {} [{}] → {}", env.meta.key, env.meta.op, env.data);
	}

	Ok(())
}
