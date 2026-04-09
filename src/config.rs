use std::path::Path;

use serde::{Deserialize, Serialize};

use oversync_core::error::OversyncError;

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
	pub surrealdb: SurrealDbDef,
	#[serde(default)]
	pub sinks: Vec<SinkDef>,
	#[serde(default)]
	pub pipes: Vec<PipeConfig>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub pipe_presets: Vec<PipePresetDef>,
}

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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
	String::new()
}
fn default_pass() -> String {
	String::new()
}
fn default_ns() -> String {
	"oversync".into()
}
fn default_db() -> String {
	"sync".into()
}

/// What to do when a cycle takes longer than the polling interval.
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedTickPolicy {
	/// Drop missed ticks — wait for the next interval boundary after the cycle finishes.
	#[default]
	Skip,
	/// Fire missed ticks immediately — run back-to-back until caught up.
	Burst,
}

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkDef {
	pub name: String,
	#[serde(rename = "type")]
	pub sink_type: String,
	#[serde(default)]
	pub config: serde_json::Value,
}

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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
/// Pipes are the only supported runtime onboarding surface. Targets are
/// references to named sinks.
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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
	pub recipe: Option<PipeRecipeDef>,
	#[serde(default)]
	pub filters: Vec<serde_json::Value>,
	#[serde(default)]
	pub transforms: Vec<serde_json::Value>,
	#[serde(default)]
	pub links: Vec<LinkDef>,
	#[serde(default)]
	pub alert_webhook: Option<String>,
	#[serde(default = "default_true")]
	pub enabled: bool,
}

/// Reusable control-plane preset for bootstrapping new pipes in UI/API.
/// Presets are not runnable by themselves; they store defaults for future pipes.
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipePresetDef {
	pub name: String,
	#[serde(default)]
	pub description: Option<String>,
	pub spec: PipePresetSpec,
}

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipePresetSpec {
	pub origin: OriginDef,
	#[serde(default)]
	pub parameters: Vec<PipePresetParameterDef>,
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
	pub recipe: Option<PipeRecipeDef>,
	#[serde(default)]
	pub filters: Vec<serde_json::Value>,
	#[serde(default)]
	pub transforms: Vec<serde_json::Value>,
	#[serde(default)]
	pub links: Vec<LinkDef>,
}

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipePresetParameterDef {
	pub name: String,
	#[serde(default)]
	pub label: Option<String>,
	#[serde(default)]
	pub description: Option<String>,
	#[serde(default)]
	pub default: Option<String>,
	#[serde(default = "default_true")]
	pub required: bool,
	#[serde(default)]
	pub secret: bool,
}

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipeRecipeDef {
	#[serde(rename = "type")]
	pub recipe_type: PipeRecipeType,
	pub prefix: String,
	#[serde(default)]
	pub entity_type_id: Option<String>,
	#[serde(default = "default_recipe_schema_id")]
	pub schema_id: String,
	#[serde(default)]
	pub schemas: Vec<String>,
}

#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipeRecipeType {
	PostgresMetadata,
	PostgresSnapshot,
}

fn default_recipe_schema_id() -> String {
	"table".into()
}

/// Matching strategy for entity linking rules.
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkStrategy {
	#[default]
	Exact,
	Normalized,
}

/// Cross-source entity linking rule in TOML config.
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkDef {
	pub name: String,
	pub left_field: String,
	pub right_field: String,
	#[serde(default)]
	pub strategy: LinkStrategy,
	pub target_origin: String,
	pub target_query: String,
}

/// Connector types that require Trino as a JDBC bridge.
const TRINO_BRIDGE_CONNECTORS: &[&str] = &[
	"mssql",
	"sqlserver",
	"oracle",
	"snowflake",
	"hive",
	"iceberg",
	"teradata",
	"db2",
	"sap_hana",
	"greenplum",
	"redshift",
];

/// Origin connector configuration within a pipe.
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "api", derive(utoipa::ToSchema))]
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
	let mut explicit_pipe_counts: std::collections::HashMap<&str, usize> =
		std::collections::HashMap::new();

	for pipe in &config.pipes {
		*explicit_pipe_counts.entry(pipe.name.as_str()).or_default() += 1;
	}

	for (name, count) in explicit_pipe_counts.iter().filter(|(_, count)| **count > 1) {
		issues.push(ConfigIssue {
			severity: Severity::Warning,
			message: format!(
				"pipe name '{name}' is defined {count} times in [[pipes]]; last definition wins"
			),
		});
	}

	for pipe in &config.pipes {
		if let Some(recipe) = &pipe.recipe {
			if !pipe.queries.is_empty() {
				issues.push(ConfigIssue {
					severity: Severity::Warning,
					message: format!(
						"pipe '{}': recipe is ignored because explicit queries are already defined",
						pipe.name
					),
				});
			}
			match recipe.recipe_type {
				PipeRecipeType::PostgresMetadata => {
					if pipe.origin.connector != "postgres" {
						issues.push(ConfigIssue {
							severity: Severity::Error,
							message: format!(
								"pipe '{}': recipe 'postgres_metadata' requires origin.connector = 'postgres'",
								pipe.name
							),
						});
					}
					if recipe.prefix.trim().is_empty() {
						issues.push(ConfigIssue {
							severity: Severity::Error,
							message: format!(
								"pipe '{}': recipe 'postgres_metadata' requires non-empty prefix",
								pipe.name
							),
						});
					}
				}
				PipeRecipeType::PostgresSnapshot => {
					if pipe.origin.connector != "postgres" {
						issues.push(ConfigIssue {
							severity: Severity::Error,
							message: format!(
								"pipe '{}': recipe 'postgres_snapshot' requires origin.connector = 'postgres'",
								pipe.name
							),
						});
					}
					if recipe.prefix.trim().is_empty() {
						issues.push(ConfigIssue {
							severity: Severity::Error,
							message: format!(
								"pipe '{}': recipe 'postgres_snapshot' requires non-empty prefix",
								pipe.name
							),
						});
					}
				}
			}
		}
	}

	let pipes = config.effective_pipes();

	let sink_names: std::collections::HashSet<&str> =
		config.sinks.iter().map(|s| s.name.as_str()).collect();

	for pipe in &pipes {
		if pipe.queries.is_empty() && pipe.recipe.is_none() {
			issues.push(ConfigIssue {
				severity: Severity::Warning,
				message: format!("pipe '{}': no queries defined", pipe.name),
			});
		}

		if matches!(pipe.delta.diff_mode, DiffMode::Memory) {
			issues.push(ConfigIssue {
				severity: Severity::Warning,
				message: format!(
					"pipe '{}': diff_mode 'memory' produces events without row_data (keys only). Use 'db' if sinks need full data.",
					pipe.name
				),
			});
		}

		if pipe.delta.fail_safe_threshold < 0.0 || pipe.delta.fail_safe_threshold > 100.0 {
			issues.push(ConfigIssue {
				severity: Severity::Error,
				message: format!(
					"pipe '{}': fail_safe_threshold must be 0-100, got {}",
					pipe.name, pipe.delta.fail_safe_threshold
				),
			});
		}

		if pipe.schedule.interval_secs == 0 {
			issues.push(ConfigIssue {
				severity: Severity::Error,
				message: format!("pipe '{}': interval_secs is 0 (would busy-loop)", pipe.name),
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
					return Err(OversyncError::Config("unclosed ${...} in config".into()));
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

fn parse_config_value(toml_str: &str) -> Result<toml::Value, OversyncError> {
	let value: toml::Value =
		toml::from_str(toml_str).map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))?;
	if value.get("sources").is_some() {
		return Err(OversyncError::Config(
			"legacy [[sources]] config is no longer supported; migrate to [[pipes]]".into(),
		));
	}
	Ok(value)
}

impl SyncConfig {
	/// Load config from a TOML file with `${VAR}` env var expansion.
	pub fn from_file(path: &Path) -> Result<Self, OversyncError> {
		let content = std::fs::read_to_string(path)
			.map_err(|e| OversyncError::Config(format!("read {}: {e}", path.display())))?;
		let expanded = expand_env_vars(&content)?;
		parse_config_value(&expanded)?
			.try_into()
			.map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))
	}

	/// Parse config from a TOML string (no env var expansion).
	#[allow(clippy::should_implement_trait)]
	pub fn from_str(toml_str: &str) -> Result<Self, OversyncError> {
		parse_config_value(toml_str)?
			.try_into()
			.map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))
	}

	/// Parse config from a TOML string with `${VAR}` env var expansion.
	pub fn from_str_with_env(toml_str: &str) -> Result<Self, OversyncError> {
		let expanded = expand_env_vars(toml_str)?;
		parse_config_value(&expanded)?
			.try_into()
			.map_err(|e| OversyncError::Config(format!("parse TOML: {e}")))
	}

	/// Returns the effective pipes after de-duplicating explicit `[[pipes]]` entries.
	///
	/// Duplicate names within `[[pipes]]` are deduplicated (last wins).
	pub fn effective_pipes(&self) -> Vec<PipeConfig> {
		let mut seen = std::collections::HashSet::new();
		let mut pipes: Vec<PipeConfig> = Vec::new();

		// Process in reverse so last definition wins, then reverse back.
		for pipe in self.pipes.iter().rev() {
			if seen.insert(pipe.name.clone()) {
				pipes.push(expand_pipe_recipes(pipe.clone()));
			}
		}
		pipes.reverse();
		pipes
	}
}

pub(crate) fn expand_pipe_recipes(mut pipe: PipeConfig) -> PipeConfig {
	if pipe.queries.is_empty()
		&& let Some(recipe) = &pipe.recipe
		&& matches!(recipe.recipe_type, PipeRecipeType::PostgresMetadata)
	{
		pipe.queries = postgres_metadata_recipe_queries(recipe);
		pipe.recipe = None;
	}
	pipe
}

fn postgres_metadata_recipe_queries(recipe: &PipeRecipeDef) -> Vec<QueryDef> {
	let prefix = sql_string_literal(&recipe.prefix);
	let entity_type_id = sql_string_literal(
		recipe
			.entity_type_id
			.as_deref()
			.unwrap_or(recipe.prefix.as_str()),
	);
	let schema_id = sql_string_literal(&recipe.schema_id);
	let schema_filter = if recipe.schemas.is_empty() {
		"n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_%'"
			.to_string()
	} else {
		format!("n.nspname IN ({})", sql_string_list(&recipe.schemas))
	};

	let entity_sql = format!(
		r#"SELECT
    '{prefix}.' || n.nspname || '.' || c.relname AS id,
    '{entity_type_id}' AS "entityTypeId",
    'entity' AS "type",
    n.nspname || '.' || c.relname AS name,
    COALESCE(d.description, '') AS description,
    0 AS version
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n
    ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_description d
    ON c.oid = d.objoid
   AND d.objsubid = 0
WHERE c.relkind IN ('r', 'v')
  AND {schema_filter}
ORDER BY n.nspname, c.relname"#,
	);

	let aspect_sql = format!(
		r#"SELECT
    '{prefix}.' || n.nspname || '.' || c.relname AS "entityId",
    '{schema_id}' AS "schemaId",
    '{entity_type_id}' AS "entityTypeId",
    'aspect' AS "type",
    'custom' AS "aspectType",
    jsonb_build_object(
        'name', n.nspname || '.' || c.relname,
        'description', COALESCE(table_desc.description, ''),
        'columns', jsonb_agg(
            jsonb_build_object(
                'ordinalPosition', a.attnum,
                'name', a.attname,
                'description', COALESCE(columns_desc.description, ''),
                'isKey', COALESCE(pk.is_pk, false),
                'type', pg_catalog.format_type(a.atttypid, a.atttypmod)
            )
            ORDER BY a.attnum
        )
    ) AS data,
    0 AS version
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n
    ON c.relnamespace = n.oid
JOIN pg_catalog.pg_attribute a
    ON c.oid = a.attrelid
LEFT JOIN pg_catalog.pg_description table_desc
    ON c.oid = table_desc.objoid
   AND table_desc.objsubid = 0
LEFT JOIN pg_catalog.pg_description columns_desc
    ON columns_desc.objoid = a.attrelid
   AND columns_desc.objsubid = a.attnum
LEFT JOIN (
    SELECT
        att.attrelid,
        att.attname,
        true AS is_pk
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_attribute att
        ON att.attrelid = con.conrelid
       AND att.attnum = ANY(con.conkey)
    WHERE con.contype = 'p'
) pk
    ON pk.attrelid = c.oid
   AND pk.attname = a.attname
WHERE c.relkind IN ('r', 'v')
  AND {schema_filter}
  AND a.attnum > 0
  AND NOT a.attisdropped
GROUP BY
    n.nspname,
    c.relname,
    table_desc.description
ORDER BY
    n.nspname,
    c.relname"#,
	);

	vec![
		QueryDef {
			id: "entity".into(),
			sql: entity_sql,
			key_column: "id".into(),
			sinks: None,
			transform: None,
		},
		QueryDef {
			id: "aspect-table".into(),
			sql: aspect_sql,
			key_column: "entityId".into(),
			sinks: None,
			transform: None,
		},
	]
}

fn sql_string_literal(value: &str) -> String {
	value.replace('\'', "''")
}

fn sql_string_list(values: &[String]) -> String {
	values
		.iter()
		.map(|value| format!("'{}'", sql_string_literal(value)))
		.collect::<Vec<_>>()
		.join(", ")
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
		assert!(config.surrealdb.username.is_empty());
		assert!(config.surrealdb.password.is_empty());
		assert_eq!(config.surrealdb.namespace, "oversync");
		assert!(config.pipes.is_empty());
	}

	#[test]
	fn parse_full_pipe_config() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"
username = "admin"
password = "secret"
namespace = "prod"
database = "sync_state"

[[pipes]]
name = "product_a"
[pipes.origin]
connector = "postgres"
dsn = "postgres://ro@pg1:5432/meta"

[pipes.schedule]
interval_secs = 60

[pipes.delta]
fail_safe_threshold = 25.0

[[pipes.queries]]
id = "tables"
sql = "SELECT oid::text, relname FROM pg_class WHERE relnamespace = 2200"
key_column = "oid"

[[pipes.queries]]
id = "columns"
sql = "SELECT attrelid::text || '.' || attnum::text AS id, attname FROM pg_attribute"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.surrealdb.username, "admin");
		assert_eq!(config.pipes.len(), 1);
		assert_eq!(config.pipes[0].name, "product_a");
		assert_eq!(config.pipes[0].schedule.interval_secs, 60);
		assert_eq!(config.pipes[0].queries.len(), 2);
		assert_eq!(config.pipes[0].queries[0].id, "tables");
		assert_eq!(config.pipes[0].queries[1].id, "columns");
	}

	#[test]
	fn parse_pipe_defaults() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "src"
[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes.queries]]
id = "q"
sql = "SELECT 1 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.pipes[0].schedule.interval_secs, 300);
		assert_eq!(config.pipes[0].delta.fail_safe_threshold, 30.0);
	}

	#[test]
	fn parse_two_minimal_pipes() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "pg_a"
[pipes.origin]
connector = "postgres"
dsn = "postgres://a/db"

[[pipes.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"

[[pipes]]
name = "pg_b"
[pipes.origin]
connector = "postgres"
dsn = "postgres://b/db"

[[pipes.queries]]
id = "q2"
sql = "SELECT 2 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(config.pipes.len(), 2);
		assert_eq!(config.pipes[0].name, "pg_a");
		assert_eq!(config.pipes[1].name, "pg_b");
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
	fn legacy_sources_are_rejected() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[sources]]
name = "empty"
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let error = SyncConfig::from_str(toml).unwrap_err();
		assert!(error.to_string().contains("legacy [[sources]]"));
	}

	#[test]
	fn parse_http_pipe_with_config() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "github-repos"
[pipes.origin]
connector = "http"
dsn = "https://api.github.com"

[pipes.origin.config]
headers = { "Accept" = "application/vnd.github+json" }
response_path = "items"

[pipes.origin.config.auth]
type = "bearer"
token = "ghp_test123"

[pipes.origin.config.pagination]
type = "offset"
page_size = 100
limit_param = "per_page"
offset_param = "page"

[[pipes.queries]]
id = "repos"
sql = "/orgs/example/repos"
key_column = "id"

[pipes.schedule]
interval_secs = 3600
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let pipe = &config.pipes[0];
		assert_eq!(pipe.name, "github-repos");
		assert_eq!(pipe.origin.connector, "http");
		assert_eq!(pipe.origin.dsn, "https://api.github.com");
		assert_eq!(pipe.schedule.interval_secs, 3600);

		let cfg = pipe.origin.config.as_object().unwrap();
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

[[pipes]]
name = "pg"
[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
sinks = ["kafka-main", "stdout-debug"]
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		let sinks = config.pipes[0].queries[0].sinks.as_ref().unwrap();
		assert_eq!(sinks, &["kafka-main", "stdout-debug"]);
	}

	#[test]
	fn parse_query_with_transform() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "pg"
[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
transform = "smt::normalize_users"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert_eq!(
			config.pipes[0].queries[0].transform.as_deref(),
			Some("smt::normalize_users")
		);
	}

	#[test]
	fn parse_query_without_sinks_is_none() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "pg"
[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[[pipes.queries]]
id = "q1"
sql = "SELECT 1 AS id"
key_column = "id"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(config.pipes[0].queries[0].sinks.is_none());
	}

	#[test]
	fn pipe_origin_config_defaults_to_null() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "pg"
[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"
"#;
		let config = SyncConfig::from_str(toml).unwrap();
		assert!(config.pipes[0].origin.config.is_null());
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
		assert!(snap.username.is_empty());
		assert!(snap.password.is_empty());
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
		assert!(matches!(
			pipe.schedule.missed_tick_policy,
			MissedTickPolicy::Burst
		));
		assert!(matches!(pipe.delta.diff_mode, DiffMode::Memory));
		assert_eq!(pipe.delta.fail_safe_threshold, 25.0);
		assert_eq!(pipe.retry.max_retries, 5);
		assert_eq!(pipe.retry.retry_base_delay_secs, 10);
		assert_eq!(pipe.queries.len(), 2);
		assert_eq!(pipe.queries[0].transform.as_deref(), Some("smt::normalize"));
		assert_eq!(
			pipe.queries[1].sinks.as_deref(),
			Some(["kafka-main".to_string()].as_slice())
		);
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
		assert_eq!(
			pipe.origin.trino_url.as_deref(),
			Some("http://domain-trino:8080")
		);
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
		assert_eq!(
			data,
			serde_json::json!({"new_name": "alice", "name": "BOB"})
		);
	}

	#[test]
	fn parse_pipe_with_postgres_metadata_recipe() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "catalog"
targets = ["kafka"]

[pipes.origin]
connector = "postgres"
dsn = "postgres://localhost/db"

[pipes.recipe]
type = "postgres_metadata"
prefix = "some-postgresql-source"
schemas = ["showcase_stream"]
"#;

		let config = SyncConfig::from_str(toml).unwrap();
		let pipes = config.effective_pipes();
		assert_eq!(pipes.len(), 1);
		assert_eq!(pipes[0].queries.len(), 2);
		assert_eq!(pipes[0].queries[0].id, "entity");
		assert_eq!(pipes[0].queries[1].id, "aspect-table");
		assert!(
			pipes[0].queries[0]
				.sql
				.contains("'some-postgresql-source.' || n.nspname || '.' || c.relname AS id")
		);
		assert!(
			pipes[0].queries[0]
				.sql
				.contains("n.nspname IN ('showcase_stream')")
		);
		assert!(pipes[0].queries[1].sql.contains("'table' AS \"schemaId\""));
	}

	#[test]
	fn validate_postgres_metadata_recipe_requires_postgres_connector() {
		let toml = r#"
[surrealdb]
url = "http://localhost:8000"

[[pipes]]
name = "catalog"

[pipes.origin]
connector = "mysql"
dsn = "mysql://localhost/db"

[pipes.recipe]
type = "postgres_metadata"
prefix = "some-postgresql-source"
"#;

		let config = SyncConfig::from_str(toml).unwrap();
		let issues = validate_config(&config);
		assert!(issues.iter().any(|issue| {
			issue.severity == Severity::Error
				&& issue
					.message
					.contains("recipe 'postgres_metadata' requires origin.connector = 'postgres'")
		}));
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
		let result = expand_env_vars(
			"dsn = \"postgres://${OVERSYNC_TEST_HOST}:${OVERSYNC_TEST_DBPORT}/app\"",
		)
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
		assert!(
			issues.is_empty(),
			"expected no issues, got: {:?}",
			issues.iter().map(|i| &i.message).collect::<Vec<_>>()
		);
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
		assert!(
			issues
				.iter()
				.any(|i| i.severity == Severity::Error && i.message.contains("interval_secs is 0"))
		);
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
		assert!(
			issues
				.iter()
				.any(|i| i.severity == Severity::Error && i.message.contains("nonexistent"))
		);
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
		assert!(
			issues
				.iter()
				.any(|i| i.severity == Severity::Error && i.message.contains("dsn is empty"))
		);
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
		assert!(
			issues
				.iter()
				.any(|i| i.severity == Severity::Warning && i.message.contains("no queries"))
		);
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
		assert!(
			issues
				.iter()
				.any(|i| i.severity == Severity::Error && i.message.contains("missing-sink"))
		);
	}

	#[test]
	fn validate_duplicate_pipe_names_warns() {
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
		let issues = validate_config(&config);
		assert!(issues.iter().any(|i| {
			i.severity == Severity::Warning
				&& i.message.contains("pipe name 'dup'")
				&& i.message.contains("last definition wins")
		}));
	}

	// ── Auto-routing tests ──────────────────────────────────────

	#[test]
	fn native_connectors_recognized() {
		for connector in &[
			"postgres",
			"mysql",
			"http",
			"graphql",
			"clickhouse",
			"flight_sql",
			"flight-sql",
			"mcp",
			"trino",
		] {
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
		for connector in &[
			"mssql",
			"oracle",
			"snowflake",
			"hive",
			"iceberg",
			"teradata",
			"db2",
		] {
			let origin = OriginDef {
				connector: connector.to_string(),
				dsn: "test://".into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			};
			assert!(
				origin.needs_trino_bridge(),
				"{connector} should need Trino bridge"
			);
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
		assert!(
			!origin.needs_trino_bridge(),
			"unknown custom connectors should NOT need Trino bridge"
		);
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
		let errors: Vec<_> = issues
			.iter()
			.filter(|i| i.severity == Severity::Error)
			.collect();
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
		let warnings: Vec<_> = issues
			.iter()
			.filter(|i| i.severity == Severity::Warning)
			.collect();
		assert!(!warnings.is_empty());
		// Warnings don't block — only errors do
		let errors: Vec<_> = issues
			.iter()
			.filter(|i| i.severity == Severity::Error)
			.collect();
		assert!(errors.is_empty());
	}
}
