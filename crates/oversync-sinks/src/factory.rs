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
		let auth: Option<oversync_core::model::KafkaAuth> = config
			.get("auth")
			.map(|v| {
				serde_json::from_value(v.clone())
					.map_err(|e| OversyncError::Config(format!("kafka auth: {e}")))
			})
			.transpose()?;
		Ok(Box::new(KafkaSink::with_auth(
			brokers,
			topic,
			auth.as_ref(),
		)?))
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

	// ── Sink type identity ────────────────────────────────────────

	#[test]
	fn all_factories_report_correct_sink_type() {
		assert_eq!(StdoutTargetFactory.sink_type(), "stdout");
		assert_eq!(KafkaTargetFactory.sink_type(), "kafka");
		assert_eq!(SurrealDbTargetFactory.sink_type(), "surrealdb");
		assert_eq!(McpTargetFactory.sink_type(), "mcp");
		assert_eq!(MysqlTargetFactory.sink_type(), "mysql");
		assert_eq!(PostgresTargetFactory.sink_type(), "postgres");
		assert_eq!(ClickHouseTargetFactory.sink_type(), "clickhouse");
		assert_eq!(HttpTargetFactory.sink_type(), "http");
	}

	// ── Stdout factory ────────────────────────────────────────────

	#[tokio::test]
	async fn stdout_factory_creates_with_defaults() {
		let sink = StdoutTargetFactory
			.create("test", &serde_json::json!({}))
			.await
			.unwrap();
		assert_eq!(sink.name(), "stdout");
	}

	#[tokio::test]
	async fn stdout_factory_creates_with_pretty() {
		let sink = StdoutTargetFactory
			.create("test", &serde_json::json!({"pretty": true}))
			.await
			.unwrap();
		assert_eq!(sink.name(), "stdout");
	}

	// ── Kafka factory ─────────────────────────────────────────────

	#[tokio::test]
	async fn kafka_factory_missing_brokers() {
		let config = serde_json::json!({"topic": "t"});
		let err = KafkaTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'brokers'"));
	}

	#[tokio::test]
	async fn kafka_factory_missing_topic() {
		let config = serde_json::json!({"brokers": "localhost:9092"});
		let err = KafkaTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'topic'"));
	}

	// ── SurrealDB factory ─────────────────────────────────────────

	#[tokio::test]
	async fn surrealdb_factory_missing_url() {
		let config = serde_json::json!({"namespace": "ns", "database": "db", "table": "t"});
		let err = SurrealDbTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'url'"));
	}

	#[tokio::test]
	async fn surrealdb_factory_missing_namespace() {
		let config =
			serde_json::json!({"url": "ws://localhost:8000", "database": "db", "table": "t"});
		let err = SurrealDbTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'namespace'"));
	}

	#[tokio::test]
	async fn surrealdb_factory_missing_database() {
		let config =
			serde_json::json!({"url": "ws://localhost:8000", "namespace": "ns", "table": "t"});
		let err = SurrealDbTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'database'"));
	}

	#[tokio::test]
	async fn surrealdb_factory_missing_table() {
		let config =
			serde_json::json!({"url": "ws://localhost:8000", "namespace": "ns", "database": "db"});
		let err = SurrealDbTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'table'"));
	}

	// ── MySQL factory ─────────────────────────────────────────────

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

	// ── PostgreSQL factory ────────────────────────────────────────

	#[tokio::test]
	async fn postgres_factory_missing_dsn() {
		let config = serde_json::json!({"table": "events"});
		let err = PostgresTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'dsn'"));
	}

	#[tokio::test]
	async fn postgres_factory_missing_table() {
		let config = serde_json::json!({"dsn": "postgres://localhost/test"});
		let err = PostgresTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'table'"));
	}

	// ── ClickHouse factory ────────────────────────────────────────

	#[tokio::test]
	async fn clickhouse_factory_missing_url() {
		let config = serde_json::json!({"table": "events"});
		let err = ClickHouseTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'url'"));
	}

	#[tokio::test]
	async fn clickhouse_factory_missing_table() {
		let config = serde_json::json!({"url": "http://localhost:8123"});
		let err = ClickHouseTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'table'"));
	}

	#[tokio::test]
	async fn clickhouse_factory_creates_with_defaults() {
		let config = serde_json::json!({"url": "http://localhost:8123", "table": "events"});
		let sink = ClickHouseTargetFactory
			.create("ch-test", &config)
			.await
			.unwrap();
		assert_eq!(sink.name(), "ch-test");
	}

	#[tokio::test]
	async fn clickhouse_factory_creates_with_all_options() {
		let config = serde_json::json!({
			"url": "http://localhost:8123",
			"table": "events",
			"database": "analytics",
			"user": "admin",
			"password": "secret",
			"timeout_secs": 120
		});
		let sink = ClickHouseTargetFactory
			.create("ch-full", &config)
			.await
			.unwrap();
		assert_eq!(sink.name(), "ch-full");
	}

	// ── HTTP factory ──────────────────────────────────────────────

	#[tokio::test]
	async fn http_factory_missing_url() {
		let config = serde_json::json!({});
		let err = HttpTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'url'"));
	}

	#[tokio::test]
	async fn http_factory_creates_with_defaults() {
		let config = serde_json::json!({"url": "http://localhost:8080/webhook"});
		let sink = HttpTargetFactory
			.create("http-test", &config)
			.await
			.unwrap();
		assert_eq!(sink.name(), "http-test");
	}

	#[tokio::test]
	async fn http_factory_creates_with_put_method() {
		let config = serde_json::json!({"url": "http://localhost:8080", "method": "PUT"});
		let sink = HttpTargetFactory.create("put-test", &config).await.unwrap();
		assert_eq!(sink.name(), "put-test");
	}

	#[tokio::test]
	async fn http_factory_rejects_unsupported_method() {
		let config = serde_json::json!({"url": "http://localhost:8080", "method": "DELETE"});
		let err = HttpTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("unsupported method"));
	}

	#[tokio::test]
	async fn http_factory_creates_with_headers() {
		let config = serde_json::json!({
			"url": "http://localhost:8080",
			"headers": {"X-Custom": "value"}
		});
		let sink = HttpTargetFactory
			.create("headers-test", &config)
			.await
			.unwrap();
		assert_eq!(sink.name(), "headers-test");
	}

	#[tokio::test]
	async fn http_factory_creates_with_bearer_auth() {
		let config = serde_json::json!({
			"url": "http://localhost:8080",
			"auth": {"type": "bearer", "token": "tok123"}
		});
		let sink = HttpTargetFactory
			.create("auth-test", &config)
			.await
			.unwrap();
		assert_eq!(sink.name(), "auth-test");
	}

	#[tokio::test]
	async fn http_factory_invalid_headers_type() {
		let config = serde_json::json!({
			"url": "http://localhost:8080",
			"headers": "not-an-object"
		});
		let err = HttpTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("headers"));
	}

	// ── MCP factory ───────────────────────────────────────────────

	#[tokio::test]
	async fn mcp_factory_invalid_config() {
		let config = serde_json::json!("not-an-object");
		let err = McpTargetFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("mcp sink"));
	}
}
