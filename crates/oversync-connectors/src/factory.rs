use async_trait::async_trait;

use oversync_core::error::OversyncError;
use oversync_core::traits::{SourceConnector, SourceFactory};

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
