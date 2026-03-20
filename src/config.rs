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
	#[serde(default)]
	pub pipes: Vec<PipeConfig>,
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
	pub missed_tick_policy: MissedTickPolicy,
	#[serde(default)]
	pub config: serde_json::Value,
	#[serde(default)]
	pub queries: Vec<QueryDef>,
}

/// What to do when a cycle takes longer than the polling interval.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedTickPolicy {
	/// Drop missed ticks — wait for the next interval boundary after the cycle finishes.
	#[default]
	Skip,
	/// Fire missed ticks immediately — run back-to-back until caught up.
	Burst,
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
fn default_true() -> bool {
	true
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryDef {
	pub id: String,
	pub sql: String,
	pub key_column: String,
	#[serde(default)]
	pub sinks: Option<Vec<String>>,
	#[serde(default)]
	pub transform: Option<String>,
}

/// A pipe is a complete data pipeline: origin → delta → transform → targets.
///
/// Replaces the flat `SourceDef` with structured sub-configs for origin,
/// schedule, delta, and retry. Targets are references to named sinks.
#[derive(Debug, Clone, Deserialize)]
pub struct PipeConfig {
	pub name: String,
	pub origin: OriginDef,
	#[serde(default)]
	pub targets: Vec<String>,
	#[serde(default)]
	pub queries: Vec<QueryDef>,
	#[serde(default)]
	pub schedule: ScheduleDef,
	#[serde(default)]
	pub delta: DeltaDef,
	#[serde(default)]
	pub retry: RetryDef,
	#[serde(default = "default_true")]
	pub enabled: bool,
}

/// Origin connector configuration within a pipe.
#[derive(Debug, Clone, Deserialize)]
pub struct OriginDef {
	pub connector: String,
	pub dsn: String,
	#[serde(default)]
	pub config: serde_json::Value,
}

/// Polling schedule for a pipe.
#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleDef {
	#[serde(default = "default_interval")]
	pub interval_secs: u64,
	#[serde(default)]
	pub missed_tick_policy: MissedTickPolicy,
}

impl Default for ScheduleDef {
	fn default() -> Self {
		Self {
			interval_secs: default_interval(),
			missed_tick_policy: MissedTickPolicy::default(),
		}
	}
}

/// Delta detection settings for a pipe.
#[derive(Debug, Clone, Deserialize)]
pub struct DeltaDef {
	#[serde(default)]
	pub diff_mode: DiffMode,
	#[serde(default = "default_threshold")]
	pub fail_safe_threshold: f64,
}

impl Default for DeltaDef {
	fn default() -> Self {
		Self {
			diff_mode: DiffMode::default(),
			fail_safe_threshold: default_threshold(),
		}
	}
}

/// Retry policy for failed cycles.
#[derive(Debug, Clone, Deserialize)]
pub struct RetryDef {
	#[serde(default = "default_max_retries")]
	pub max_retries: u32,
	#[serde(default = "default_retry_delay")]
	pub retry_base_delay_secs: u64,
}

impl Default for RetryDef {
	fn default() -> Self {
		Self {
			max_retries: default_max_retries(),
			retry_base_delay_secs: default_retry_delay(),
		}
	}
}

impl From<&SourceDef> for PipeConfig {
	fn from(src: &SourceDef) -> Self {
		Self {
			name: src.name.clone(),
			origin: OriginDef {
				connector: src.connector.clone(),
				dsn: src.dsn.clone(),
				config: src.config.clone(),
			},
			targets: vec![],
			queries: src.queries.clone(),
			schedule: ScheduleDef {
				interval_secs: src.interval_secs,
				missed_tick_policy: src.missed_tick_policy.clone(),
			},
			delta: DeltaDef {
				diff_mode: src.diff_mode.clone(),
				fail_safe_threshold: src.fail_safe_threshold,
			},
			retry: RetryDef {
				max_retries: src.max_retries,
				retry_base_delay_secs: src.retry_base_delay_secs,
			},
			enabled: true,
		}
	}
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

	/// Returns all pipes: explicit `[[pipes]]` entries plus auto-converted `[[sources]]`.
	///
	/// Legacy `[[sources]]` are converted to `PipeConfig` for backward compatibility.
	/// When both `pipes` and `sources` define the same name, explicit pipes take precedence.
	/// Duplicate names within `[[pipes]]` are deduplicated (last wins).
	pub fn effective_pipes(&self) -> Vec<PipeConfig> {
		let mut seen = std::collections::HashSet::new();
		let mut pipes: Vec<PipeConfig> = Vec::new();

		// Process in reverse so last definition wins, then reverse back.
		for pipe in self.pipes.iter().rev() {
			if seen.insert(pipe.name.clone()) {
				pipes.push(pipe.clone());
			}
		}
		pipes.reverse();

		for source in &self.sources {
			if seen.insert(source.name.clone()) {
				pipes.push(PipeConfig::from(source));
			}
		}
		pipes
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
	fn parse_http_source_with_config() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "github-repos"
connector = "http"
dsn = "https://api.github.com"
interval_secs = 3600

[sources.config]
headers = { "Accept" = "application/vnd.github+json" }
response_path = "items"

[sources.config.auth]
type = "bearer"
token = "ghp_test123"

[sources.config.pagination]
type = "offset"
page_size = 100
limit_param = "per_page"
offset_param = "page"

[[sources.queries]]
id = "repos"
sql = "/orgs/example/repos"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let src = &config.sources[0];
		assert_eq!(src.name, "github-repos");
		assert_eq!(src.connector, "http");
		assert_eq!(src.dsn, "https://api.github.com");
		assert_eq!(src.interval_secs, 3600);

		let cfg = src.config.as_object().unwrap();
		assert_eq!(cfg["response_path"], "items");
		assert_eq!(cfg["auth"]["type"], "bearer");
		assert_eq!(cfg["auth"]["token"], "ghp_test123");
		assert_eq!(cfg["pagination"]["type"], "offset");
		assert_eq!(cfg["pagination"]["page_size"], 100);
	}

	#[test]
	fn parse_query_with_sinks() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "pg"
connector = "postgres"
dsn = "postgres://localhost/db"

[[sources.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
sinks = ["kafka-main", "stdout-debug"]
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let sinks = config.sources[0].queries[0].sinks.as_ref().unwrap();
		assert_eq!(sinks, &["kafka-main", "stdout-debug"]);
	}

	#[test]
	fn parse_query_with_transform() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "pg"
connector = "postgres"
dsn = "postgres://localhost/db"

[[sources.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
transform = "smt::normalize_users"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(
			config.sources[0].queries[0].transform.as_deref(),
			Some("smt::normalize_users")
		);
	}

	#[test]
	fn parse_query_without_sinks_is_none() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "pg"
connector = "postgres"
dsn = "postgres://localhost/db"

[[sources.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(config.sources[0].queries[0].sinks.is_none());
	}

	#[test]
	fn source_config_defaults_to_null() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "pg"
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(config.sources[0].config.is_null());
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

	// ── PipeConfig tests ──────────────────────────────────────────

	#[test]
	fn parse_minimal_pipe() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "catalog-sync"

[pipes.origin]
connector = "postgres"
dsn = "postgres://ro@pg1:5432/meta"

[[pipes.queries]]
id = "tables"
sql = "SELECT oid::text, relname FROM pg_class"
key_column = "oid"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.pipes.len(), 1);
		let pipe = &config.pipes[0];
		assert_eq!(pipe.name, "catalog-sync");
		assert_eq!(pipe.origin.connector, "postgres");
		assert_eq!(pipe.origin.dsn, "postgres://ro@pg1:5432/meta");
		assert_eq!(pipe.queries.len(), 1);
		assert_eq!(pipe.queries[0].id, "tables");
		assert!(pipe.enabled);
	}

	#[test]
	fn pipe_defaults() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "p"

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let pipe = &config.pipes[0];
		assert_eq!(pipe.schedule.interval_secs, 300);
		assert_eq!(pipe.delta.fail_safe_threshold, 30.0);
		assert_eq!(pipe.retry.max_retries, 3);
		assert_eq!(pipe.retry.retry_base_delay_secs, 5);
		assert!(pipe.targets.is_empty());
		assert!(pipe.queries.is_empty());
		assert!(pipe.enabled);
	}

	#[test]
	fn parse_full_pipe() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "full-pipe"
targets = ["kafka-main", "stdout-debug"]
enabled = true

[pipes.origin]
connector = "postgres"
dsn = "postgres://ro@pg1:5432/meta"

[pipes.origin.config]
ssl_mode = "require"

[pipes.schedule]
interval_secs = 60
missed_tick_policy = "burst"

[pipes.delta]
diff_mode = "memory"
fail_safe_threshold = 25.0

[pipes.retry]
max_retries = 5
retry_base_delay_secs = 10

[[pipes.queries]]
id = "tables"
sql = "SELECT 1 AS id"
key_column = "id"
transform = "smt::normalize"

[[pipes.queries]]
id = "columns"
sql = "SELECT 2 AS id"
key_column = "id"
sinks = ["kafka-main"]
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let pipe = &config.pipes[0];
		assert_eq!(pipe.name, "full-pipe");
		assert_eq!(pipe.targets, vec!["kafka-main", "stdout-debug"]);
		assert_eq!(pipe.origin.connector, "postgres");
		assert_eq!(pipe.origin.config["ssl_mode"], "require");
		assert_eq!(pipe.schedule.interval_secs, 60);
		assert!(matches!(pipe.schedule.missed_tick_policy, MissedTickPolicy::Burst));
		assert!(matches!(pipe.delta.diff_mode, DiffMode::Memory));
		assert_eq!(pipe.delta.fail_safe_threshold, 25.0);
		assert_eq!(pipe.retry.max_retries, 5);
		assert_eq!(pipe.retry.retry_base_delay_secs, 10);
		assert_eq!(pipe.queries.len(), 2);
		assert_eq!(pipe.queries[0].transform.as_deref(), Some("smt::normalize"));
		assert_eq!(pipe.queries[1].sinks.as_deref(), Some(["kafka-main".to_string()].as_slice()));
	}

	#[test]
	fn parse_multiple_pipes() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "pipe-a"

[pipes.origin]
connector = "postgres"
dsn = "postgres://a/db"

[[pipes]]
name = "pipe-b"

[pipes.origin]
connector = "mysql"
dsn = "mysql://b/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.pipes.len(), 2);
		assert_eq!(config.pipes[0].name, "pipe-a");
		assert_eq!(config.pipes[0].origin.connector, "postgres");
		assert_eq!(config.pipes[1].name, "pipe-b");
		assert_eq!(config.pipes[1].origin.connector, "mysql");
	}

	#[test]
	fn effective_pipes_from_sources() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "pg"
connector = "postgres"
dsn = "postgres://localhost/db"
interval_secs = 120
fail_safe_threshold = 20.0

[[sources.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(config.pipes.is_empty());
		let pipes = config.effective_pipes();
		assert_eq!(pipes.len(), 1);
		let pipe = &pipes[0];
		assert_eq!(pipe.name, "pg");
		assert_eq!(pipe.origin.connector, "postgres");
		assert_eq!(pipe.origin.dsn, "postgres://localhost/db");
		assert_eq!(pipe.schedule.interval_secs, 120);
		assert_eq!(pipe.delta.fail_safe_threshold, 20.0);
		assert_eq!(pipe.queries.len(), 1);
	}

	#[test]
	fn effective_pipes_merges_both() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "legacy-src"
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes]]
name = "new-pipe"

[pipes.origin]
connector = "mysql"
dsn = "mysql://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let pipes = config.effective_pipes();
		assert_eq!(pipes.len(), 2);
		let names: Vec<&str> = pipes.iter().map(|p| p.name.as_str()).collect();
		assert!(names.contains(&"new-pipe"));
		assert!(names.contains(&"legacy-src"));
	}

	#[test]
	fn effective_pipes_explicit_takes_precedence() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "same-name"
connector = "postgres"
dsn = "postgres://old"

[[pipes]]
name = "same-name"

[pipes.origin]
connector = "mysql"
dsn = "mysql://new"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let pipes = config.effective_pipes();
		assert_eq!(pipes.len(), 1);
		assert_eq!(pipes[0].origin.connector, "mysql");
		assert_eq!(pipes[0].origin.dsn, "mysql://new");
	}

	#[test]
	fn source_def_converts_to_pipe() {
		let src = SourceDef {
			name: "pg".into(),
			connector: "postgres".into(),
			dsn: "postgres://localhost/db".into(),
			interval_secs: 120,
			fail_safe_threshold: 20.0,
			max_retries: 5,
			retry_base_delay_secs: 10,
			diff_mode: DiffMode::Memory,
			missed_tick_policy: MissedTickPolicy::Burst,
			config: serde_json::json!({"ssl": true}),
			queries: vec![QueryDef {
				id: "q1".into(),
				sql: "SELECT 1".into(),
				key_column: "id".into(),
				sinks: Some(vec!["kafka".into()]),
				transform: Some("smt::x".into()),
			}],
		};
		let pipe = PipeConfig::from(&src);
		assert_eq!(pipe.name, "pg");
		assert_eq!(pipe.origin.connector, "postgres");
		assert_eq!(pipe.origin.dsn, "postgres://localhost/db");
		assert_eq!(pipe.origin.config["ssl"], true);
		assert_eq!(pipe.schedule.interval_secs, 120);
		assert!(matches!(pipe.schedule.missed_tick_policy, MissedTickPolicy::Burst));
		assert!(matches!(pipe.delta.diff_mode, DiffMode::Memory));
		assert_eq!(pipe.delta.fail_safe_threshold, 20.0);
		assert_eq!(pipe.retry.max_retries, 5);
		assert_eq!(pipe.retry.retry_base_delay_secs, 10);
		assert_eq!(pipe.queries.len(), 1);
		assert_eq!(pipe.queries[0].sinks.as_deref(), Some(&["kafka".to_string()][..]));
		assert!(pipe.targets.is_empty());
		assert!(pipe.enabled);
	}

	#[test]
	fn effective_pipes_deduplicates_within_pipes() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "dup"

[pipes.origin]
connector = "postgres"
dsn = "postgres://first"

[[pipes]]
name = "dup"

[pipes.origin]
connector = "mysql"
dsn = "mysql://second"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let pipes = config.effective_pipes();
		assert_eq!(pipes.len(), 1);
		assert_eq!(pipes[0].origin.connector, "mysql");
	}

	#[test]
	fn pipe_disabled() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "disabled-pipe"
enabled = false

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(!config.pipes[0].enabled);
	}
}
