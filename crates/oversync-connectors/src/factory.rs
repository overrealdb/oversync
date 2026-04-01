use async_trait::async_trait;

use oversync_core::error::OversyncError;
use oversync_core::traits::{OriginConnector, OriginFactory};

use crate::PostgresConnector;
use crate::clickhouse::{ClickHouseConfig, ClickHouseConnector};
use crate::flight_sql::FlightSqlConnector;
use crate::graphql::{GraphqlConfig, GraphqlConnector};
use crate::http_source::{HttpSource, HttpSourceConfig};
use crate::kafka_source::KafkaSourceConnector;
use crate::mcp::{McpConfig, McpOriginConnector};
use crate::mysql::MysqlConnector;
use crate::surrealdb_source::{SurrealDbConnector, SurrealDbLiveConnector};
use crate::trino::{TrinoConfig, TrinoConnector};

pub struct PostgresOriginFactory;

#[async_trait]
impl OriginFactory for PostgresOriginFactory {
	fn connector_type(&self) -> &str {
		"postgres"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("postgres: missing 'dsn'".into()))?;

		let connector = PostgresConnector::new(name, dsn).await?;
		Ok(Box::new(connector))
	}
}

pub struct HttpOriginFactory;

#[async_trait]
impl OriginFactory for HttpOriginFactory {
	fn connector_type(&self) -> &str {
		"http"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let http_config: HttpSourceConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("http source: {e}")))?;
		Ok(Box::new(HttpSource::new(name, http_config)?))
	}
}

pub struct MysqlOriginFactory;

#[async_trait]
impl OriginFactory for MysqlOriginFactory {
	fn connector_type(&self) -> &str {
		"mysql"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("mysql: missing 'dsn'".into()))?;

		let connector = MysqlConnector::new(name, dsn).await?;
		Ok(Box::new(connector))
	}
}

pub struct FlightSqlOriginFactory;

#[async_trait]
impl OriginFactory for FlightSqlOriginFactory {
	fn connector_type(&self) -> &str {
		"flight-sql"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("flight-sql: missing 'dsn'".into()))?;

		let connector = FlightSqlConnector::new(name, dsn)?;
		Ok(Box::new(connector))
	}
}

pub struct TrinoOriginFactory;

#[async_trait]
impl OriginFactory for TrinoOriginFactory {
	fn connector_type(&self) -> &str {
		"trino"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let trino_config: TrinoConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("trino: {e}")))?;
		let connector = TrinoConnector::new(name, trino_config)?;
		Ok(Box::new(connector))
	}
}

pub struct ClickHouseOriginFactory;

#[async_trait]
impl OriginFactory for ClickHouseOriginFactory {
	fn connector_type(&self) -> &str {
		"clickhouse"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let ch_config: ClickHouseConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("clickhouse: {e}")))?;
		let connector = ClickHouseConnector::new(name, ch_config)?;
		Ok(Box::new(connector))
	}
}

pub struct McpOriginFactory;

#[async_trait]
impl OriginFactory for McpOriginFactory {
	fn connector_type(&self) -> &str {
		"mcp"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let mcp_config: McpConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("mcp: {e}")))?;
		Ok(Box::new(McpOriginConnector::new(name, mcp_config)))
	}
}

pub struct GraphqlOriginFactory;

#[async_trait]
impl OriginFactory for GraphqlOriginFactory {
	fn connector_type(&self) -> &str {
		"graphql"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let gql_config: GraphqlConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("graphql: {e}")))?;
		let connector = GraphqlConnector::new(name, gql_config)?;
		Ok(Box::new(connector))
	}
}

pub struct KafkaOriginFactory;

#[async_trait]
impl OriginFactory for KafkaOriginFactory {
	fn connector_type(&self) -> &str {
		"kafka"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let brokers = config
			.get("brokers")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("kafka: missing 'brokers'".into()))?;
		let topic = config
			.get("topic")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("kafka: missing 'topic'".into()))?;
		let group_id = config
			.get("group_id")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("kafka: missing 'group_id'".into()))?;
		let auto_offset_reset = config.get("auto_offset_reset").and_then(|v| v.as_str());
		let auth: Option<oversync_core::model::KafkaAuth> = config
			.get("auth")
			.map(|v| {
				serde_json::from_value(v.clone())
					.map_err(|e| OversyncError::Config(format!("kafka auth: {e}")))
			})
			.transpose()?;
		let connector = KafkaSourceConnector::with_auth(
			name,
			brokers,
			topic,
			group_id,
			auto_offset_reset,
			auth.as_ref(),
		)?;
		Ok(Box::new(connector))
	}
}

pub struct SurrealDbOriginFactory;

#[async_trait]
impl OriginFactory for SurrealDbOriginFactory {
	fn connector_type(&self) -> &str {
		"surrealdb"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		let url = config
			.get("url")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("surrealdb: missing 'url'".into()))?;
		let namespace = config
			.get("namespace")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("surrealdb: missing 'namespace'".into()))?;
		let database = config
			.get("database")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("surrealdb: missing 'database'".into()))?;
		let username = config
			.get("username")
			.and_then(|v| v.as_str())
			.unwrap_or("root");
		let password = config
			.get("password")
			.and_then(|v| v.as_str())
			.unwrap_or("root");
		let live = config
			.get("live")
			.and_then(|v| v.as_bool())
			.unwrap_or(false);

		if live {
			let table = config
				.get("table")
				.and_then(|v| v.as_str())
				.ok_or_else(|| OversyncError::Config("surrealdb live: missing 'table'".into()))?;
			let key_column = config
				.get("key_column")
				.and_then(|v| v.as_str())
				.unwrap_or("id");
			let connector = SurrealDbLiveConnector::new(
				name, url, namespace, database, username, password, table, key_column,
			)
			.await?;
			Ok(Box::new(connector))
		} else {
			let connector =
				SurrealDbConnector::new(name, url, namespace, database, username, password).await?;
			Ok(Box::new(connector))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// ── Connector type identity ───────────────────────────────────

	#[test]
	fn all_factories_report_correct_connector_type() {
		assert_eq!(PostgresOriginFactory.connector_type(), "postgres");
		assert_eq!(HttpOriginFactory.connector_type(), "http");
		assert_eq!(MysqlOriginFactory.connector_type(), "mysql");
		assert_eq!(FlightSqlOriginFactory.connector_type(), "flight-sql");
		assert_eq!(TrinoOriginFactory.connector_type(), "trino");
		assert_eq!(ClickHouseOriginFactory.connector_type(), "clickhouse");
		assert_eq!(McpOriginFactory.connector_type(), "mcp");
		assert_eq!(GraphqlOriginFactory.connector_type(), "graphql");
		assert_eq!(KafkaOriginFactory.connector_type(), "kafka");
		assert_eq!(SurrealDbOriginFactory.connector_type(), "surrealdb");
	}

	// ── Postgres factory ──────────────────────────────────────────

	#[tokio::test]
	async fn postgres_factory_missing_dsn() {
		let config = serde_json::json!({});
		let err = PostgresOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'dsn'"));
	}

	// ── MySQL factory ─────────────────────────────────────────────

	#[tokio::test]
	async fn mysql_factory_missing_dsn() {
		let config = serde_json::json!({});
		let err = MysqlOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'dsn'"));
	}

	// ── FlightSQL factory ─────────────────────────────────────────

	#[tokio::test]
	async fn flight_sql_factory_missing_dsn() {
		let config = serde_json::json!({});
		let err = FlightSqlOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'dsn'"));
	}

	// ── HTTP factory ──────────────────────────────────────────────

	#[tokio::test]
	async fn http_factory_invalid_config() {
		let config = serde_json::json!("not-an-object");
		let err = HttpOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("http source"));
	}

	// ── Trino factory ─────────────────────────────────────────────

	#[tokio::test]
	async fn trino_factory_invalid_config() {
		let config = serde_json::json!("not-an-object");
		let err = TrinoOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("trino"));
	}

	// ── ClickHouse factory ────────────────────────────────────────

	#[tokio::test]
	async fn clickhouse_factory_invalid_config() {
		let config = serde_json::json!("not-an-object");
		let err = ClickHouseOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("clickhouse"));
	}

	// ── GraphQL factory ───────────────────────────────────────────

	#[tokio::test]
	async fn graphql_factory_invalid_config() {
		let config = serde_json::json!("not-an-object");
		let err = GraphqlOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("graphql"));
	}

	// ── Kafka factory ─────────────────────────────────────────────

	#[tokio::test]
	async fn kafka_factory_missing_brokers() {
		let config = serde_json::json!({"topic": "t", "group_id": "g"});
		let err = KafkaOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'brokers'"));
	}

	#[tokio::test]
	async fn kafka_factory_missing_topic() {
		let config = serde_json::json!({"brokers": "localhost:9092", "group_id": "g"});
		let err = KafkaOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'topic'"));
	}

	#[tokio::test]
	async fn kafka_factory_missing_group_id() {
		let config = serde_json::json!({"brokers": "localhost:9092", "topic": "t"});
		let err = KafkaOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'group_id'"));
	}

	#[tokio::test]
	async fn kafka_factory_invalid_auth() {
		let config = serde_json::json!({
			"brokers": "localhost:9092",
			"topic": "t",
			"group_id": "g",
			"auth": "not-an-object"
		});
		let err = KafkaOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("kafka auth"));
	}

	// ── SurrealDB factory ─────────────────────────────────────────

	#[tokio::test]
	async fn surrealdb_factory_missing_url() {
		let config = serde_json::json!({"namespace": "ns", "database": "db"});
		let err = SurrealDbOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'url'"));
	}

	#[tokio::test]
	async fn surrealdb_factory_missing_namespace() {
		let config = serde_json::json!({"url": "ws://localhost:8000", "database": "db"});
		let err = SurrealDbOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'namespace'"));
	}

	#[tokio::test]
	async fn surrealdb_factory_missing_database() {
		let config = serde_json::json!({"url": "ws://localhost:8000", "namespace": "ns"});
		let err = SurrealDbOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'database'"));
	}

	#[tokio::test]
	async fn surrealdb_live_factory_missing_table() {
		let config = serde_json::json!({
			"url": "ws://localhost:8000",
			"namespace": "ns",
			"database": "db",
			"live": true
		});
		let err = SurrealDbOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("missing 'table'"));
	}

	// ── MCP factory ───────────────────────────────────────────────

	#[tokio::test]
	async fn mcp_factory_creates_with_valid_config() {
		let config = serde_json::json!({"dsn": "npx -y @modelcontextprotocol/server-memory"});
		let connector = McpOriginFactory.create("mcp-test", &config).await.unwrap();
		assert_eq!(connector.name(), "mcp-test");
	}

	#[tokio::test]
	async fn mcp_factory_invalid_config() {
		let config = serde_json::json!("not-an-object");
		let err = McpOriginFactory
			.create("test", &config)
			.await
			.err()
			.expect("should fail");
		assert!(err.to_string().contains("mcp"));
	}
}
