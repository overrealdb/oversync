use chrono::{DateTime, Utc};
use serde::Serialize;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use oversync_core::error::OversyncError;

const SQL_NEXT_VERSION: &str = oversync_queries::config_version::NEXT_VERSION;
const SQL_CREATE_VERSION: &str = oversync_queries::config_version::CREATE_VERSION;
const SQL_LIST_VERSIONS: &str = oversync_queries::config_version::LIST_VERSIONS;
const SQL_GET_VERSION: &str = oversync_queries::config_version::GET_VERSION;

/// A saved config snapshot with version number.
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize)]
pub struct ConfigVersion {
	pub version: u64,
	pub config_json: serde_json::Value,
	pub created_at: DateTime<Utc>,
	pub description: String,
}

/// Save the current config as a new version.
pub async fn save_version(
	db: &Surreal<Any>,
	config: &crate::config::SyncConfig,
	description: &str,
) -> Result<u64, OversyncError> {
	let config_json = serde_json::to_value(config)
		.map_err(|e| OversyncError::Config(format!("serialize config: {e}")))?;

	// Get next version number
	let mut resp = db
		.query(SQL_NEXT_VERSION)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("config version query: {e}")))?;

	let rows: Vec<serde_json::Value> = resp
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("config version take: {e}")))?;

	let max_v = rows
		.first()
		.and_then(|r| r.get("version"))
		.and_then(|v| v.as_u64())
		.unwrap_or(0);

	let version = max_v + 1;

	db.query(SQL_CREATE_VERSION)
		.bind(("version", version as i64))
		.bind(("config", config_json))
		.bind(("desc", description.to_string()))
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("save config version: {e}")))?;

	Ok(version)
}

/// List all config versions (newest first).
pub async fn list_versions(db: &Surreal<Any>) -> Result<Vec<ConfigVersion>, OversyncError> {
	let mut resp = db
		.query(SQL_LIST_VERSIONS)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("list versions: {e}")))?;

	let rows: Vec<serde_json::Value> = resp
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("list versions take: {e}")))?;

	let versions = rows
		.iter()
		.filter_map(|r| {
			Some(ConfigVersion {
				version: r.get("version")?.as_u64()?,
				config_json: r.get("config_json")?.clone(),
				created_at: r
					.get("created_at")
					.and_then(|v| v.as_str())
					.and_then(|s| s.parse().ok())
					.unwrap_or_else(Utc::now),
				description: r
					.get("description")
					.and_then(|v| v.as_str())
					.unwrap_or("")
					.to_string(),
			})
		})
		.collect();

	Ok(versions)
}

/// Get a specific version's config.
pub async fn get_version(db: &Surreal<Any>, version: u64) -> Result<ConfigVersion, OversyncError> {
	let mut resp = db
		.query(SQL_GET_VERSION)
		.bind(("v", version as i64))
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("get version: {e}")))?;

	let rows: Vec<serde_json::Value> = resp
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("get version take: {e}")))?;

	let row = rows
		.first()
		.ok_or_else(|| OversyncError::Config(format!("config version {version} not found")))?;

	Ok(ConfigVersion {
		version: row
			.get("version")
			.and_then(|v| v.as_u64())
			.unwrap_or(version),
		config_json: row
			.get("config_json")
			.cloned()
			.unwrap_or(serde_json::Value::Null),
		created_at: row
			.get("created_at")
			.and_then(|v| v.as_str())
			.and_then(|s| s.parse().ok())
			.unwrap_or_else(Utc::now),
		description: row
			.get("description")
			.and_then(|v| v.as_str())
			.unwrap_or("")
			.to_string(),
	})
}
