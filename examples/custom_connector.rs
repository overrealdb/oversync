//! Shows how to register a custom source connector with the engine.
//!
//! Run: `cargo run --example custom_connector`
//!
//! Implements a toy "csv" connector that returns hardcoded rows, registers
//! it via the builder, then runs a sync cycle.

use async_trait::async_trait;
use oversync::OversyncEngine;
use oversync::config::{
	DeltaDef, OriginDef, PipeConfig, QueryDef, RetryDef, ScheduleDef, SinkDef, SurrealDbDef,
	SyncConfig,
};
use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::{OriginConnector, OriginFactory};

struct CsvConnector {
	name: String,
}

#[async_trait]
impl OriginConnector for CsvConnector {
	fn name(&self) -> &str {
		&self.name
	}

	async fn fetch_all(&self, _sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		let rows = vec![
			serde_json::json!({"id": "1", "name": "Alice", "dept": "Engineering"}),
			serde_json::json!({"id": "2", "name": "Bob", "dept": "Marketing"}),
			serde_json::json!({"id": "3", "name": "Carol", "dept": "Engineering"}),
		];
		Ok(rows
			.into_iter()
			.map(|data| RawRow {
				row_key: data[key_column].as_str().unwrap().to_string(),
				row_data: data,
			})
			.collect())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct CsvOriginFactory;

#[async_trait]
impl OriginFactory for CsvOriginFactory {
	fn connector_type(&self) -> &str {
		"csv"
	}

	async fn create(
		&self,
		name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		Ok(Box::new(CsvConnector {
			name: name.to_string(),
		}))
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	tracing_subscriber::fmt::init();

	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.register_source(Box::new(CsvOriginFactory))
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
		sinks: vec![SinkDef {
			name: "console".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({"pretty": true}),
		}],
		pipes: vec![PipeConfig {
			name: "employees".into(),
			origin: OriginDef {
				connector: "csv".into(),
				dsn: "unused".into(),
				credential: None,
				trino_url: None,
				config: serde_json::json!({}),
			},
			targets: vec!["console".into()],
			queries: vec![QueryDef {
				id: "all-employees".into(),
				sql: "ignored-for-csv".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
			schedule: ScheduleDef {
				interval_secs: 10,
				..ScheduleDef::default()
			},
			delta: DeltaDef {
				fail_safe_threshold: 30.0,
				..DeltaDef::default()
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
		}],
		pipe_presets: vec![],
	};

	engine.start(config).await?;
	println!("custom csv connector running, will sync 3 rows to stdout");

	tokio::time::sleep(std::time::Duration::from_secs(3)).await;
	engine.shutdown().await;
	println!("done");
	Ok(())
}
