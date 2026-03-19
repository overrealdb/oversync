use async_trait::async_trait;

use oversync_core::error::OversyncError;
use oversync_core::traits::{SourceConnector, SourceFactory};

use crate::flight_sql::FlightSqlConnector;
use crate::http_source::{HttpSource, HttpSourceConfig};
use crate::mysql::MysqlConnector;
use crate::trino::{TrinoConfig, TrinoConnector};
use crate::PostgresConnector;

pub struct PostgresSourceFactory;

#[async_trait]
impl SourceFactory for PostgresSourceFactory {
	fn connector_type(&self) -> &str {
		"postgres"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn SourceConnector>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("postgres: missing 'dsn'".into()))?;

		let connector = PostgresConnector::new(name, dsn).await?;
		Ok(Box::new(connector))
	}
}

pub struct HttpSourceFactory;

#[async_trait]
impl SourceFactory for HttpSourceFactory {
	fn connector_type(&self) -> &str {
		"http"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn SourceConnector>, OversyncError> {
		let http_config: HttpSourceConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("http source: {e}")))?;
		Ok(Box::new(HttpSource::new(name, http_config)?))
	}
}

pub struct MysqlSourceFactory;

#[async_trait]
impl SourceFactory for MysqlSourceFactory {
	fn connector_type(&self) -> &str {
		"mysql"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn SourceConnector>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("mysql: missing 'dsn'".into()))?;

		let connector = MysqlConnector::new(name, dsn).await?;
		Ok(Box::new(connector))
	}
}

pub struct FlightSqlSourceFactory;

#[async_trait]
impl SourceFactory for FlightSqlSourceFactory {
	fn connector_type(&self) -> &str {
		"flight-sql"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn SourceConnector>, OversyncError> {
		let dsn = config
			.get("dsn")
			.and_then(|v| v.as_str())
			.ok_or_else(|| OversyncError::Config("flight-sql: missing 'dsn'".into()))?;

		let connector = FlightSqlConnector::new(name, dsn)?;
		Ok(Box::new(connector))
	}
}

pub struct TrinoSourceFactory;

#[async_trait]
impl SourceFactory for TrinoSourceFactory {
	fn connector_type(&self) -> &str {
		"trino"
	}

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn SourceConnector>, OversyncError> {
		let trino_config: TrinoConfig = serde_json::from_value(config.clone())
			.map_err(|e| OversyncError::Config(format!("trino: {e}")))?;
		let connector = TrinoConnector::new(name, trino_config)?;
		Ok(Box::new(connector))
	}
}
