use std::path::Path;

use serde::{Deserialize, Serialize};

use oversync_core::error::OversyncError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
	pub surrealdb: SurrealDbDef,
	#[serde(default)]
	pub sources: Vec<SourceDef>,
	#[serde(default)]
	pub sinks: Vec<SinkDef>,
	#[serde(default)]
	pub pipes: Vec<PipeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedTickPolicy {
	/// Drop missed ticks — wait for the next interval boundary after the cycle finishes.
	#[default]
	Skip,
	/// Fire missed ticks immediately — run back-to-back until caught up.
	Burst,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkDef {
	pub name: String,
	#[serde(rename = "type")]
	pub sink_type: String,
	#[serde(default)]
	pub config: serde_json::Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
	#[serde(default)]
	pub filters: Vec<serde_json::Value>,
	#[serde(default)]
	pub transforms: Vec<serde_json::Value>,
	#[serde(default = "default_true")]
	pub enabled: bool,
}

/// Connector types that require Trino as a JDBC bridge.
const TRINO_BRIDGE_CONNECTORS: &[&str] = &[
	"mssql", "sqlserver", "oracle", "snowflake", "hive", "iceberg",
	"teradata", "db2", "sap_hana", "greenplum", "redshift",
];

/// Origin connector configuration within a pipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OriginDef {
	pub connector: String,
	pub dsn: String,
	#[serde(default)]
	pub credential: Option<String>,
	/// Trino URL for non-native connectors. When connector is e.g. "mssql",
	/// queries are routed through this Trino instance.
	/// If unset, falls back to engine-level default Trino URL.
	#[serde(default)]
	pub trino_url: Option<String>,
	#[serde(default)]
	pub config: serde_json::Value,
}

impl OriginDef {
	/// Returns true if the connector needs Trino as a JDBC bridge.
	/// Unknown connector types are assumed native (custom registered connectors).
	pub fn needs_trino_bridge(&self) -> bool {
		TRINO_BRIDGE_CONNECTORS.contains(&self.connector.as_str())
	}
}

/// Polling schedule for a pipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleDef {
	#[serde(default = "default_interval")]
	pub interval_secs: u64,
	#[serde(default)]
	pub missed_tick_policy: MissedTickPolicy,
	#[serde(default)]
	pub max_requests_per_minute: Option<u32>,
}

impl Default for ScheduleDef {
	fn default() -> Self {
		Self {
			interval_secs: default_interval(),
			missed_tick_policy: MissedTickPolicy::default(),
			max_requests_per_minute: None,
		}
	}
}

/// Delta detection settings for a pipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
				credential: None,
				trino_url: None,
				config: src.config.clone(),
			},
			targets: vec![],
			queries: src.queries.clone(),
			schedule: ScheduleDef {
				interval_secs: src.interval_secs,
				missed_tick_policy: src.missed_tick_policy.clone(),
				max_requests_per_minute: None,
			},
			delta: DeltaDef {
				diff_mode: src.diff_mode.clone(),
				fail_safe_threshold: src.fail_safe_threshold,
			},
			retry: RetryDef {
				max_retries: src.max_retries,
				retry_base_delay_secs: src.retry_base_delay_secs,
			},
			filters: vec![],
			transforms: vec![],
			enabled: true,
		}
	}
}

/// Severity of a config validation issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Severity {
	Error,
	Warning,
}

/// A single config validation issue.
#[derive(Debug, Clone)]
pub struct ConfigIssue {
	pub severity: Severity,
	pub message: String,
}

/// Validate a parsed config for common issues.
///
/// Returns a list of warnings and errors. An empty list means the config is valid.
pub fn validate_config(config: &SyncConfig) -> Vec<ConfigIssue> {
	let mut issues = Vec::new();

	let pipes = config.effective_pipes();

	let sink_names: std::collections::HashSet<&str> =
		config.sinks.iter().map(|s| s.name.as_str()).collect();

	for pipe in &pipes {
		if pipe.queries.is_empty() {
			issues.push(ConfigIssue {
				severity: Severity::Warning,
				message: format!("pipe '{}': no queries defined", pipe.name),
			});
		}

		if pipe.schedule.interval_secs == 0 {
			issues.push(ConfigIssue {
				severity: Severity::Error,
				message: format!(
					"pipe '{}': interval_secs is 0 (would busy-loop)",
					pipe.name
				),
			});
		}

		for target in &pipe.targets {
			if !sink_names.contains(target.as_str()) {
				issues.push(ConfigIssue {
					severity: Severity::Error,
					message: format!(
						"pipe '{}': target '{}' not found in sinks",
						pipe.name, target
					),
				});
			}
		}

		for query in &pipe.queries {
			if let Some(ref qs) = query.sinks {
				for s in qs {
					if !sink_names.contains(s.as_str()) {
						issues.push(ConfigIssue {
							severity: Severity::Error,
							message: format!(
								"pipe '{}' query '{}': sink '{}' not found",
								pipe.name, query.id, s
							),
						});
					}
				}
			}
		}

		if pipe.origin.dsn.is_empty() {
			issues.push(ConfigIssue {
				severity: Severity::Error,
				message: format!("pipe '{}': origin dsn is empty", pipe.name),
			});
		}
	}

	issues
}

/// Expand `${VAR}` and `${VAR:-default}` references in a string.
///
/// - `${VAR}` → value of env var `VAR`, error if unset
/// - `${VAR:-fallback}` → value of `VAR` if set, otherwise `fallback`
/// - Literal `$$` is escaped to `$`
pub fn expand_env_vars(input: &str) -> Result<String, OversyncError> {
	let mut result = String::with_capacity(input.len());
	let mut chars = input.chars().peekable();

	while let Some(ch) = chars.next() {
		if ch != '$' {
			result.push(ch);
			continue;
		}

		match chars.peek() {
			Some('$') => {
				chars.next();
				result.push('$');
			}
			Some('{') => {
				chars.next(); // consume '{'
				let mut var_expr = String::new();
				let mut found_close = false;
				for c in chars.by_ref() {
					if c == '}' {
						found_close = true;
						break;
					}
					var_expr.push(c);
				}
				if !found_close {
					return Err(OversyncError::Config(
						"unclosed ${...} in config".into(),
					));
				}

				let (var_name, default_val) = if let Some(pos) = var_expr.find(":-") {
					(&var_expr[..pos], Some(&var_expr[pos + 2..]))
				} else {
					(var_expr.as_str(), None)
				};

				if var_name.is_empty() {
					return Err(OversyncError::Config(
						"empty variable name in ${...}".into(),
					));
				}

				match std::env::var(var_name) {
					Ok(val) => result.push_str(&val),
					Err(_) => match default_val {
						Some(d) => result.push_str(d),
						None => {
							return Err(OversyncError::Config(format!(
								"env var '{var_name}' is not set (use ${{VAR:-default}} for fallback)"
							)));
						}
					},
				}
			}
			_ => {
				result.push('$');
			}
		}
	}

	Ok(result)
}

impl SyncConfig {
	/// Load config from a TOML file with `${VAR}` env var expansion.
	pub fn from_file(path: &Path) -> Result<Self, OversyncError> {
		let content = std::fs::read_to_string(path)
			.map_err(|e| OversyncError::Config(format!("read {}: {e}", path.display())))?;
		let expanded = expand_env_vars(&content)?;
		toml::from_str(&expanded).map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))
	}

	/// Parse config from a TOML string (no env var expansion).
	pub fn from_str(toml_str: &str) -> Result<Self, OversyncError> {
		toml::from_str(toml_str).map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))
	}

	/// Parse config from a TOML string with `${VAR}` env var expansion.
	pub fn from_str_with_env(toml_str: &str) -> Result<Self, OversyncError> {
		let expanded = expand_env_vars(toml_str)?;
		toml::from_str(&expanded).map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))
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

	#[test]
	fn origin_is_native() {
		let origin = OriginDef {
			connector: "postgres".into(),
			dsn: "postgres://localhost/db".into(),
			credential: None,
			trino_url: None,
			config: serde_json::Value::Null,
		};
		assert!(!origin.needs_trino_bridge());
	}

	#[test]
	fn origin_mssql_not_native() {
		let origin = OriginDef {
			connector: "mssql".into(),
			dsn: "mssql://host:1433/db".into(),
			credential: None,
			trino_url: Some("http://trino:8080".into()),
			config: serde_json::Value::Null,
		};
		assert!(origin.needs_trino_bridge());
	}

	#[test]
	fn parse_pipe_with_trino_url() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "mssql-pipe"

[pipes.origin]
connector = "mssql"
dsn = "host:1433/db"
trino_url = "http://domain-trino:8080"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let pipe = &config.pipes[0];
		assert_eq!(pipe.origin.trino_url.as_deref(), Some("http://domain-trino:8080"));
		assert!(pipe.origin.needs_trino_bridge());
	}

	#[test]
	fn parse_pipe_with_transforms() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "p1"

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes.transforms]]
type = "rename"
from = "old_name"
to = "new_name"

[[pipes.transforms]]
type = "upper"
field = "name"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.pipes[0].transforms.len(), 2);
		assert_eq!(config.pipes[0].transforms[0]["type"], "rename");
		assert_eq!(config.pipes[0].transforms[1]["type"], "upper");

		let chain = oversync_transforms::parse_steps(&config.pipes[0].transforms).unwrap();
		let mut data = serde_json::json!({"old_name": "alice", "name": "bob"});
		chain.apply_one(&mut data).unwrap();
		assert_eq!(data, serde_json::json!({"new_name": "alice", "name": "BOB"}));
	}

	// ── Env var expansion tests ──────────────────────────────────

	#[test]
	fn env_expand_basic() {
		unsafe { std::env::set_var("OVERSYNC_TEST_URL", "http://db:8000") };
		let result = expand_env_vars("url = \"${OVERSYNC_TEST_URL}\"").unwrap();
		assert_eq!(result, "url = \"http://db:8000\"");
		unsafe { std::env::remove_var("OVERSYNC_TEST_URL") };
	}

	#[test]
	fn env_expand_with_default() {
		unsafe { std::env::remove_var("OVERSYNC_MISSING_VAR") };
		let result = expand_env_vars("port = \"${OVERSYNC_MISSING_VAR:-5432}\"").unwrap();
		assert_eq!(result, "port = \"5432\"");
	}

	#[test]
	fn env_expand_default_not_used_when_set() {
		unsafe { std::env::set_var("OVERSYNC_TEST_PORT", "9999") };
		let result = expand_env_vars("port = \"${OVERSYNC_TEST_PORT:-5432}\"").unwrap();
		assert_eq!(result, "port = \"9999\"");
		unsafe { std::env::remove_var("OVERSYNC_TEST_PORT") };
	}

	#[test]
	fn env_expand_missing_var_errors() {
		unsafe { std::env::remove_var("OVERSYNC_UNSET_XYZ") };
		let err = expand_env_vars("dsn = \"${OVERSYNC_UNSET_XYZ}\"").unwrap_err();
		assert!(err.to_string().contains("OVERSYNC_UNSET_XYZ"));
		assert!(err.to_string().contains("not set"));
	}

	#[test]
	fn env_expand_escaped_dollar() {
		let result = expand_env_vars("price = \"$$100\"").unwrap();
		assert_eq!(result, "price = \"$100\"");
	}

	#[test]
	fn env_expand_no_vars_passthrough() {
		let input = "url = \"http://localhost:8000\"";
		assert_eq!(expand_env_vars(input).unwrap(), input);
	}

	#[test]
	fn env_expand_unclosed_brace_errors() {
		let err = expand_env_vars("x = \"${UNCLOSED\"").unwrap_err();
		assert!(err.to_string().contains("unclosed"));
	}

	#[test]
	fn env_expand_empty_var_name_errors() {
		let err = expand_env_vars("x = \"${:-default}\"").unwrap_err();
		assert!(err.to_string().contains("empty variable name"));
	}

	#[test]
	fn env_expand_in_full_toml() {
		unsafe { std::env::set_var("OVERSYNC_TEST_DB_URL", "http://prod:8000") };
		unsafe { std::env::set_var("OVERSYNC_TEST_DB_USER", "admin") };
		let toml = r#"
[surrealdb]
url = "${OVERSYNC_TEST_DB_URL}"
username = "${OVERSYNC_TEST_DB_USER}"
password = "${OVERSYNC_TEST_DB_PASS:-secret}"
"#;
		let config = SyncConfig::from_str_with_env(toml).unwrap();
		assert_eq!(config.surrealdb.url, "http://prod:8000");
		assert_eq!(config.surrealdb.username, "admin");
		assert_eq!(config.surrealdb.password, "secret");
		unsafe { std::env::remove_var("OVERSYNC_TEST_DB_URL") };
		unsafe { std::env::remove_var("OVERSYNC_TEST_DB_USER") };
	}

	#[test]
	fn env_expand_multiple_vars_in_one_line() {
		unsafe { std::env::set_var("OVERSYNC_TEST_HOST", "db.prod") };
		unsafe { std::env::set_var("OVERSYNC_TEST_DBPORT", "5432") };
		let result =
			expand_env_vars("dsn = \"postgres://${OVERSYNC_TEST_HOST}:${OVERSYNC_TEST_DBPORT}/app\"")
				.unwrap();
		assert_eq!(result, "dsn = \"postgres://db.prod:5432/app\"");
		unsafe { std::env::remove_var("OVERSYNC_TEST_HOST") };
		unsafe { std::env::remove_var("OVERSYNC_TEST_DBPORT") };
	}

	#[test]
	fn env_expand_bare_dollar_passthrough() {
		let result = expand_env_vars("price = $5").unwrap();
		assert_eq!(result, "price = $5");
	}

	// ── Config validation tests ─────────────────────────────────

	#[test]
	fn validate_valid_config() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sinks]]
name = "kafka"
type = "kafka"

[[pipes]]
name = "p1"
targets = ["kafka"]

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		assert!(issues.is_empty(), "expected no issues, got: {:?}", issues.iter().map(|i| &i.message).collect::<Vec<_>>());
	}

	#[test]
	fn validate_zero_interval() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "p1"

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[pipes.schedule]
interval_secs = 0
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		assert!(issues.iter().any(|i| i.severity == Severity::Error && i.message.contains("interval_secs is 0")));
	}

	#[test]
	fn validate_unknown_target() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "p1"
targets = ["nonexistent"]

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		assert!(issues.iter().any(|i| i.severity == Severity::Error && i.message.contains("nonexistent")));
	}

	#[test]
	fn validate_empty_dsn() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "p1"

[pipes.origin]
connector = "postgres"
dsn = ""
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		assert!(issues.iter().any(|i| i.severity == Severity::Error && i.message.contains("dsn is empty")));
	}

	#[test]
	fn validate_no_queries_warns() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "p1"

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		assert!(issues.iter().any(|i| i.severity == Severity::Warning && i.message.contains("no queries")));
	}

	#[test]
	fn validate_query_unknown_sink() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "p1"

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
sinks = ["missing-sink"]
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		assert!(issues.iter().any(|i| i.severity == Severity::Error && i.message.contains("missing-sink")));
	}

	// ── Auto-routing tests ──────────────────────────────────────

	#[test]
	fn native_connectors_recognized() {
		for connector in &["postgres", "mysql", "http", "graphql", "clickhouse", "flight_sql", "flight-sql", "mcp", "trino"] {
			let origin = OriginDef {
				connector: connector.to_string(),
				dsn: "test://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			};
			assert!(!origin.needs_trino_bridge(), "{connector} should be native");
		}
	}

	#[test]
	fn non_native_connectors_detected() {
		for connector in &["mssql", "oracle", "snowflake", "hive", "iceberg", "teradata", "db2"] {
			let origin = OriginDef {
				connector: connector.to_string(),
				dsn: "test://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			};
			assert!(origin.needs_trino_bridge(), "{connector} should need Trino bridge");
		}
	}

	#[test]
	fn custom_connector_not_routed_to_trino() {
		let origin = OriginDef {
			connector: "my_custom_source".into(),
			dsn: "custom://whatever".into(),
			credential: None,
			trino_url: None,
			config: serde_json::Value::Null,
		};
		assert!(!origin.needs_trino_bridge(), "unknown custom connectors should NOT need Trino bridge");
	}

	#[test]
	fn validate_config_blocks_zero_interval() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "bad"

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[pipes.schedule]
interval_secs = 0
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		let errors: Vec<_> = issues.iter().filter(|i| i.severity == Severity::Error).collect();
		assert!(!errors.is_empty(), "should have error for zero interval");
		assert!(errors[0].message.contains("interval_secs is 0"));
	}

	#[test]
	fn validate_config_warns_no_queries() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "empty"

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		let warnings: Vec<_> = issues.iter().filter(|i| i.severity == Severity::Warning).collect();
		assert!(!warnings.is_empty());
		// Warnings don't block — only errors do
		let errors: Vec<_> = issues.iter().filter(|i| i.severity == Severity::Error).collect();
		assert!(errors.is_empty());
	}
}
