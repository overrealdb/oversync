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
use crate::surrealdb_source::SurrealDbConnector;
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
		let connector =
			KafkaSourceConnector::new(name, brokers, topic, group_id, auto_offset_reset)?;
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
		let connector =
			SurrealDbConnector::new(name, url, namespace, database, username, password).await?;
		Ok(Box::new(connector))
	}
}
