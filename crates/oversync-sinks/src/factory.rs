use async_trait::async_trait;

use oversync_core::error::OversyncError;
use oversync_core::traits::{Sink, SinkFactory};

use crate::kafka::KafkaSink;
use crate::stdout::StdoutSink;
use crate::surrealdb_sink::SurrealDbSink;

pub struct StdoutSinkFactory;

#[async_trait]
impl SinkFactory for StdoutSinkFactory {
	fn sink_type(&self) -> &str {
		"stdout"
	}

	async fn create(
		&self,
		_name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let pretty = config
			.get("pretty")
			.and_then(|v| v.as_bool())
			.unwrap_or(false);
		Ok(Box::new(StdoutSink::new(pretty)))
	}
}

pub struct KafkaSinkFactory;

#[async_trait]
impl SinkFactory for KafkaSinkFactory {
	fn sink_type(&self) -> &str {
		"kafka"
	}

	async fn create(
		&self,
		_name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let brokers = config
			.get("brokers")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("kafka: missing 'brokers'".into()))?;
		let topic = config
			.get("topic")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("kafka: missing 'topic'".into()))?;
		Ok(Box::new(KafkaSink::new(brokers, topic)?))
	}
}

pub struct SurrealDbSinkFactory;

#[async_trait]
impl SinkFactory for SurrealDbSinkFactory {
	fn sink_type(&self) -> &str {
		"surrealdb"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let url = config
			.get("url")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("surrealdb sink: missing 'url'".into()))?;
		let namespace = config
			.get("namespace")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("surrealdb sink: missing 'namespace'".into()))?;
		let database = config
			.get("database")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("surrealdb sink: missing 'database'".into()))?;
		let table = config
			.get("table")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("surrealdb sink: missing 'table'".into()))?;
		let username = config
			.get("username")
			.and_then(|v| v.as_str())
			.unwrap_or("root");
		let password = config
			.get("password")
			.and_then(|v| v.as_str())
			.unwrap_or("root");
		Ok(Box::new(
			SurrealDbSink::new(name, url, namespace, database, table, username, password).await?,
		))
	}
}
