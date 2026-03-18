use serde::Deserialize;
use std::path::Path;

use oversync_core::error::OversyncError;

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
	pub surrealdb: SurrealDbDef,
	#[serde(default)]
	pub sources: Vec<SourceDef>,
	#[serde(default)]
	pub sinks: Vec<SinkDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SurrealDbDef {
	pub url: String,
	#[serde(default = "default_user")]
	pub username: String,
	#[serde(default = "default_pass")]
	pub password: String,
	#[serde(default = "default_ns")]
	pub namespace: String,
	#[serde(default = "default_db")]
	pub database: String,
	#[serde(default)]
	pub snapshot: Option<SnapshotDbDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotDbDef {
	pub url: String,
	#[serde(default = "default_user")]
	pub username: String,
	#[serde(default = "default_pass")]
	pub password: String,
	#[serde(default = "default_ns")]
	pub namespace: String,
	#[serde(default = "default_db")]
	pub database: String,
}

fn default_user() -> String {
	"root".into()
}
fn default_pass() -> String {
	"root".into()
}
fn default_ns() -> String {
	"oversync".into()
}
fn default_db() -> String {
	"sync".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceDef {
	pub name: String,
	pub connector: String,
	pub dsn: String,
	#[serde(default = "default_interval")]
	pub interval_secs: u64,
	#[serde(default = "default_threshold")]
	pub fail_safe_threshold: f64,
	#[serde(default = "default_max_retries")]
	pub max_retries: u32,
	#[serde(default = "default_retry_delay")]
	pub retry_base_delay_secs: u64,
	#[serde(default)]
	pub diff_mode: DiffMode,
	#[serde(default)]
	pub queries: Vec<QueryDef>,
}

impl SourceDef {
	/// Build connector config JSON from source definition fields.
	pub fn connector_config(&self) -> serde_json::Value {
		serde_json::json!({ "dsn": self.dsn })
	}
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkDef {
	pub name: String,
	#[serde(rename = "type")]
	pub sink_type: String,
	#[serde(default)]
	pub config: serde_json::Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffMode {
	/// Compute diff via SurrealQL queries (prev_hash, paginated). Slower but low memory.
	#[default]
	Db,
	/// Compute diff in Rust via HashMap. Fast but needs O(keys) memory.
	Memory,
}

fn default_interval() -> u64 {
	300
}
fn default_threshold() -> f64 {
	30.0
}
fn default_max_retries() -> u32 {
	3
}
fn default_retry_delay() -> u64 {
	5
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryDef {
	pub id: String,
	pub sql: String,
	pub key_column: String,
}

impl SyncConfig {
	pub fn from_file(path: &Path) -> Result<Self, OversyncError> {
		let content = std::fs::read_to_string(path)
			.map_err(|e| OversyncError::Config(format!("read {}: {e}", path.display())))?;
		Self::from_str(&content)
	}

	pub fn from_str(toml_str: &str) -> Result<Self, OversyncError> {
		toml::from_str(toml_str).map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_minimal_config() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.surrealdb.url, "http://localhost:8000");
		assert_eq!(config.surrealdb.username, "root");
		assert_eq!(config.surrealdb.namespace, "oversync");
		assert!(config.sources.is_empty());
	}

	#[test]
	fn parse_full_config() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"
username = "admin"
password = "secret"
namespace = "prod"
database = "sync_state"

[[sources]]
name = "product_a"
connector = "postgres"
dsn = "postgres://ro@pg1:5432/meta"
interval_secs = 60
fail_safe_threshold = 25.0

[[sources.queries]]
id = "tables"
sql = "SELECT oid::text, relname FROM pg_class WHERE relnamespace = 2200"
key_column = "oid"

[[sources.queries]]
id = "columns"
sql = "SELECT attrelid::text || '.' || attnum::text AS id, attname FROM pg_attribute"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.surrealdb.username, "admin");
		assert_eq!(config.sources.len(), 1);
		assert_eq!(config.sources[0].name, "product_a");
		assert_eq!(config.sources[0].interval_secs, 60);
		assert_eq!(config.sources[0].queries.len(), 2);
		assert_eq!(config.sources[0].queries[0].id, "tables");
		assert_eq!(config.sources[0].queries[1].id, "columns");
	}

	#[test]
	fn parse_defaults() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "src"
connector = "postgres"
dsn = "postgres://localhost/db"

[[sources.queries]]
id = "q"
sql = "SELECT 1 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.sources[0].interval_secs, 300);
		assert_eq!(config.sources[0].fail_safe_threshold, 30.0);
	}

	#[test]
	fn parse_multiple_sources() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "pg_a"
connector = "postgres"
dsn = "postgres://a/db"

[[sources.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"

[[sources]]
name = "pg_b"
connector = "postgres"
dsn = "postgres://b/db"

[[sources.queries]]
id = "q2"
sql = "SELECT 2 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.sources.len(), 2);
		assert_eq!(config.sources[0].name, "pg_a");
		assert_eq!(config.sources[1].name, "pg_b");
	}

	#[test]
	fn invalid_toml_errors() {
		let result = SyncConfig::from_str("not valid toml {{{}}}");
		assert!(result.is_err());
	}

	#[test]
	fn missing_required_field_errors() {
		let result = SyncConfig::from_str("[surrealdb]\n");
		assert!(result.is_err());
	}

	#[test]
	fn from_file_reads_toml() {
		let dir = std::env::temp_dir().join("oversync_test_config");
		std::fs::create_dir_all(&dir).unwrap();
		let path = dir.join("test.toml");
		std::fs::write(
			&path,
			r#"
[surrealdb]
url = "http://localhost:8000"
"#,
		)
		.unwrap();
		let config = SyncConfig::from_file(&path).unwrap();
		assert_eq!(config.surrealdb.url, "http://localhost:8000");
		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn from_file_missing_file_errors() {
		let result = SyncConfig::from_file(std::path::Path::new("/tmp/nonexistent_oversync.toml"));
		assert!(result.is_err());
	}

	#[test]
	fn source_without_queries_is_valid() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "empty"
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(config.sources[0].queries.is_empty());
	}

	#[test]
	fn parse_minimal_config_snapshot_is_none() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(config.surrealdb.snapshot.is_none());
	}

	#[test]
	fn parse_snapshot_config() {
		let toml = r#"
[surrealdb]
url = "http://tikv-surreal:8000"

[surrealdb.snapshot]
url = "http://mem-surreal:8000"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.surrealdb.url, "http://tikv-surreal:8000");
		let snap = config.surrealdb.snapshot.unwrap();
		assert_eq!(snap.url, "http://mem-surreal:8000");
		assert_eq!(snap.username, "root");
		assert_eq!(snap.namespace, "oversync");
		assert_eq!(snap.database, "sync");
	}

	#[test]
	fn parse_snapshot_config_with_overrides() {
		let toml = r#"
[surrealdb]
url = "http://tikv-surreal:8000"
username = "admin"
password = "secret"

[surrealdb.snapshot]
url = "http://mem-surreal:8000"
username = "snap_user"
password = "snap_pass"
namespace = "snap_ns"
database = "snap_db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let snap = config.surrealdb.snapshot.unwrap();
		assert_eq!(snap.username, "snap_user");
		assert_eq!(snap.password, "snap_pass");
		assert_eq!(snap.namespace, "snap_ns");
		assert_eq!(snap.database, "snap_db");
	}
}
