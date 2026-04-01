use std::collections::HashMap;

use async_trait::async_trait;

use oversync_core::error::OversyncError;
use oversync_core::traits::{Sink, TargetFactory};

use crate::clickhouse_sink::ClickHouseSink;
use crate::http_sink::HttpSink;
use crate::kafka::KafkaSink;
use crate::mcp_sink::{McpSink, McpSinkConfig};
use crate::mysql_sink::MysqlSink;
use crate::postgres_sink::PostgresSink;
use crate::stdout::StdoutSink;
use crate::surrealdb_sink::SurrealDbSink;
use oversync_core::model::AuthConfig;

pub struct StdoutTargetFactory;

#[async_trait]
impl TargetFactory for StdoutTargetFactory {
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

pub struct KafkaTargetFactory;

#[async_trait]
impl TargetFactory for KafkaTargetFactory {
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

pub struct SurrealDbTargetFactory;

#[async_trait]
impl TargetFactory for SurrealDbTargetFactory {
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

pub struct McpTargetFactory;

#[async_trait]
impl TargetFactory for McpTargetFactory {
	fn sink_type(&self) -> &str {
		"mcp"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let mcp_config: McpSinkConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("mcp sink: {e}")))?;
		Ok(Box::new(McpSink::new(name, mcp_config)))
	}
}

pub struct MysqlTargetFactory;

#[async_trait]
impl TargetFactory for MysqlTargetFactory {
	fn sink_type(&self) -> &str {
		"mysql"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("mysql sink: missing 'dsn'".into()))?;
		let table = config
			.get("table")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("mysql sink: missing 'table'".into()))?;
		Ok(Box::new(MysqlSink::new(name, dsn, table).await?))
	}
}

pub struct PostgresTargetFactory;

#[async_trait]
impl TargetFactory for PostgresTargetFactory {
	fn sink_type(&self) -> &str {
		"postgres"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("postgres sink: missing 'dsn'".into()))?;
		let table = config
			.get("table")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("postgres sink: missing 'table'".into()))?;
		let schema = config
			.get("schema")
			.and_then(|v| v.as_str())
			.unwrap_or("public");
		Ok(Box::new(PostgresSink::new(name, dsn, table, schema).await?))
	}
}

pub struct ClickHouseTargetFactory;

#[async_trait]
impl TargetFactory for ClickHouseTargetFactory {
	fn sink_type(&self) -> &str {
		"clickhouse"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let url = config
			.get("url")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("clickhouse sink: missing 'url'".into()))?;
		let table = config
			.get("table")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("clickhouse sink: missing 'table'".into()))?;
		let database = config
			.get("database")
			.and_then(|v| v.as_str())
			.map(String::from);
		let user = config
			.get("user")
			.and_then(|v| v.as_str())
			.unwrap_or("default")
			.to_string();
		let password = config
			.get("password")
			.and_then(|v| v.as_str())
			.map(String::from);
		let timeout_secs = config
			.get("timeout_secs")
			.and_then(|v| v.as_u64())
			.unwrap_or(60);
		Ok(Box::new(ClickHouseSink::new(
			name,
			url,
			table,
			database,
			user,
			password,
			timeout_secs,
		)?))
	}
}

pub struct HttpTargetFactory;

#[async_trait]
impl TargetFactory for HttpTargetFactory {
	fn sink_type(&self) -> &str {
		"http"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let url = config
			.get("url")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("http sink: missing 'url'".into()))?;

		let method = match config
			.get("method")
			.and_then(|v| v.as_str())
			.unwrap_or("POST")
			.to_uppercase()
			.as_str()
		{
			"POST" => reqwest::Method::POST,
			"PUT" => reqwest::Method::PUT,
			other => {
				return Err(OversyncError::Config(format!(
					"http sink: unsupported method '{other}'"
				)));
			}
		};

		let headers: HashMap<String, String> = match config.get("headers") {
			Some(v) => serde_json::from_value(v.clone())
				.map_err(|e| OversyncError::Config(format!("http sink headers: {e}")))?,
			None => HashMap::new(),
		};

		let auth: Option<AuthConfig> = match config.get("auth") {
			Some(v) => Some(
				serde_json::from_value(v.clone())
					.map_err(|e| OversyncError::Config(format!("http sink auth: {e}")))?,
			),
			None => None,
		};

		let timeout_secs = config
			.get("timeout_secs")
			.and_then(|v| v.as_u64())
			.unwrap_or(30);

		let retry_count = config
			.get("retry_count")
			.and_then(|v| v.as_u64())
			.unwrap_or(3) as u32;

		Ok(Box::new(HttpSink::new(
			name,
			url,
			method,
			headers,
			auth,
			timeout_secs,
			retry_count,
		)?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn mysql_factory_sink_type() {
		assert_eq!(MysqlTargetFactory.sink_type(), "mysql");
	}

	#[tokio::test]
	async fn mysql_factory_missing_dsn() {
		let config = serde_json::json!({"table": "events"});
		let err = MysqlTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'dsn'"));
	}

	#[tokio::test]
	async fn mysql_factory_missing_table() {
		let config = serde_json::json!({"dsn": "mysql://localhost/test"});
		let err = MysqlTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'table'"));
	}

	#[tokio::test]
	async fn mysql_factory_missing_both() {
		let config = serde_json::json!({});
		let err = MysqlTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'dsn'"));
	}
}
