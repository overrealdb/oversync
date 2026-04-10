use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

fn deserialize_optional_json_value<'de, D>(
	deserializer: D,
) -> Result<Option<Option<serde_json::Value>>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	Option::<serde_json::Value>::deserialize(deserializer).map(Some)
}

fn default_true() -> bool {
	true
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiMissedTickPolicy {
	Skip,
	Burst,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiScheduleDef {
	pub interval_secs: Option<u64>,
	pub missed_tick_policy: Option<ApiMissedTickPolicy>,
	pub max_requests_per_minute: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiDiffMode {
	Db,
	Memory,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiDeltaDef {
	pub diff_mode: Option<ApiDiffMode>,
	pub fail_safe_threshold: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiRetryDef {
	pub max_retries: Option<u32>,
	pub retry_base_delay_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiPipeRecipeType {
	PostgresMetadata,
	PostgresSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiPipeRecipeDef {
	#[serde(rename = "type")]
	pub recipe_type: ApiPipeRecipeType,
	pub prefix: String,
	#[serde(default)]
	pub entity_type_id: Option<String>,
	#[serde(default)]
	pub schema_id: Option<String>,
	#[serde(default)]
	pub schemas: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ApiLinkStrategy {
	Exact,
	Normalized,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiLinkDef {
	pub name: String,
	pub left_field: String,
	pub right_field: String,
	pub strategy: Option<ApiLinkStrategy>,
	pub target_origin: String,
	pub target_query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiAuthConfig {
	Bearer { token: String },
	Header { name: String, value: String },
	Basic { username: String, password: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiEmptyOriginConfig {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiTrinoExtraCredentials {
	pub username: String,
	pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiTrinoAuth {
	Bearer { token: String },
	Basic { username: String, password: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiTrinoOriginConfig {
	pub user: Option<String>,
	pub catalog: Option<String>,
	pub schema: Option<String>,
	pub timeout_secs: Option<u64>,
	pub auth: Option<ApiTrinoAuth>,
	pub extra_credentials: Option<ApiTrinoExtraCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiClickHouseOriginConfig {
	pub user: Option<String>,
	pub password: Option<String>,
	pub database: Option<String>,
	pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiHttpPaginationConfig {
	Offset {
		page_size: usize,
		limit_param: Option<String>,
		offset_param: Option<String>,
	},
	Cursor {
		page_size: usize,
		cursor_param: Option<String>,
		cursor_path: String,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiHttpOriginConfig {
	pub headers: Option<std::collections::HashMap<String, String>>,
	pub auth: Option<ApiAuthConfig>,
	pub pagination: Option<ApiHttpPaginationConfig>,
	pub response_path: Option<String>,
	pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiGraphqlPagination {
	pub cursor_variable: Option<String>,
	pub has_next_path: Option<String>,
	pub end_cursor_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiGraphqlOriginConfig {
	pub headers: Option<std::collections::HashMap<String, String>>,
	pub auth: Option<ApiAuthConfig>,
	pub response_path: Option<String>,
	pub timeout_secs: Option<u64>,
	pub pagination: Option<ApiGraphqlPagination>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiMcpOriginConfig {
	pub args: Option<Vec<String>>,
	pub key_field: Option<String>,
	pub response_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKafkaOriginConfig {
	pub brokers: String,
	pub topic: String,
	pub group_id: String,
	pub auto_offset_reset: Option<String>,
	pub auth: Option<ApiKafkaAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiSurrealDbOriginConfig {
	pub url: String,
	pub namespace: String,
	pub database: String,
	pub username: Option<String>,
	pub password: Option<String>,
	pub live: Option<bool>,
	pub table: Option<String>,
	pub key_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ApiOriginConfig {
	Empty(ApiEmptyOriginConfig),
	Trino(ApiTrinoOriginConfig),
	ClickHouse(ApiClickHouseOriginConfig),
	Http(ApiHttpOriginConfig),
	Graphql(ApiGraphqlOriginConfig),
	Mcp(ApiMcpOriginConfig),
	Kafka(ApiKafkaOriginConfig),
	SurrealDb(ApiSurrealDbOriginConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKafkaAuth {
	pub security_protocol: Option<String>,
	pub sasl_mechanism: Option<String>,
	pub sasl_username: Option<String>,
	pub sasl_password: Option<String>,
	pub sasl_kerberos_keytab: Option<String>,
	pub sasl_kerberos_principal: Option<String>,
	pub ssl_ca_location: Option<String>,
	pub ssl_certificate_location: Option<String>,
	pub ssl_key_location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiKafkaKeyFormat {
	String,
	JsonObject,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiKafkaValueFormat {
	Envelope,
	Compact,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiSurrealDbSinkMode {
	Envelope,
	Document,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiFilterOp {
	Eq,
	Ne,
	Gt,
	Gte,
	Lt,
	Lte,
	Contains,
	Exists,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiTransformStep {
	Rename {
		from: String,
		to: String,
	},
	Set {
		field: String,
		value: serde_json::Value,
	},
	Upper {
		field: String,
	},
	Lower {
		field: String,
	},
	Remove {
		field: String,
	},
	Copy {
		from: String,
		to: String,
	},
	Default {
		field: String,
		value: serde_json::Value,
	},
	Filter {
		field: String,
		op: ApiFilterOp,
		#[serde(default)]
		value: Option<serde_json::Value>,
	},
	MapValue {
		field: String,
		mapping: std::collections::HashMap<String, serde_json::Value>,
	},
	Truncate {
		field: String,
		max_len: usize,
	},
	Nest {
		fields: Vec<String>,
		into: String,
	},
	Flatten {
		field: String,
	},
	Hash {
		field: String,
	},
	Coalesce {
		fields: Vec<String>,
		into: String,
	},
	SchemaFilter {
		field: String,
		#[serde(default)]
		allow: Option<Vec<String>>,
		#[serde(default)]
		deny: Option<Vec<String>>,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiStdoutSinkConfig {
	pub pretty: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKafkaSinkConfig {
	pub brokers: String,
	pub topic: String,
	pub auth: Option<ApiKafkaAuth>,
	pub key_format: Option<ApiKafkaKeyFormat>,
	pub key_field: Option<String>,
	pub value_format: Option<ApiKafkaValueFormat>,
	pub created_change_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiSurrealDbSinkConfig {
	pub url: String,
	pub namespace: String,
	pub database: String,
	pub table: String,
	pub username: Option<String>,
	pub password: Option<String>,
	pub mode: Option<ApiSurrealDbSinkMode>,
	pub key_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiMcpSinkConfig {
	pub dsn: String,
	pub args: Option<Vec<String>>,
	pub tool_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiMysqlSinkConfig {
	pub dsn: String,
	pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiPostgresSinkConfig {
	pub dsn: String,
	pub table: String,
	pub schema: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiClickHouseSinkConfig {
	pub url: String,
	pub table: String,
	pub database: Option<String>,
	pub user: Option<String>,
	pub password: Option<String>,
	pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiHttpSinkConfig {
	pub url: String,
	pub method: Option<String>,
	pub headers: Option<std::collections::HashMap<String, String>>,
	pub auth: Option<ApiAuthConfig>,
	pub timeout_secs: Option<u64>,
	pub retry_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ApiSinkConfig {
	Stdout(ApiStdoutSinkConfig),
	Kafka(ApiKafkaSinkConfig),
	SurrealDb(ApiSurrealDbSinkConfig),
	Mcp(ApiMcpSinkConfig),
	Mysql(ApiMysqlSinkConfig),
	Postgres(ApiPostgresSinkConfig),
	ClickHouse(ApiClickHouseSinkConfig),
	Http(ApiHttpSinkConfig),
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
	pub status: &'static str,
	pub version: &'static str,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CycleInfo {
	pub cycle_id: u64,
	pub source: String,
	pub query: String,
	pub status: String,
	pub started_at: DateTime<Utc>,
	pub finished_at: Option<DateTime<Utc>>,
	pub rows_created: u64,
	pub rows_updated: u64,
	pub rows_deleted: u64,
	pub duration_ms: Option<u64>,
	pub error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SinkInfo {
	pub name: String,
	pub sink_type: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	#[schema(value_type = Option<ApiSinkConfig>)]
	pub config: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
	pub error: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SinkListResponse {
	pub sinks: Vec<SinkInfo>,
}

// ── Mutation request types ──────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSinkRequest {
	pub name: String,
	pub sink_type: String,
	#[serde(default)]
	#[schema(value_type = ApiSinkConfig)]
	pub config: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateSinkRequest {
	pub sink_type: Option<String>,
	#[schema(value_type = Option<ApiSinkConfig>)]
	pub config: Option<serde_json::Value>,
	pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MutationResponse {
	pub ok: bool,
	pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HistoryResponse {
	pub cycles: Vec<CycleInfo>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StatusResponse {
	pub running: bool,
	pub paused: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExportConfigFormat {
	Toml,
	Json,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ExportConfigQuery {
	pub format: Option<ExportConfigFormat>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ExportConfigResponse {
	pub format: ExportConfigFormat,
	pub content: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ImportConfigRequest {
	pub format: ExportConfigFormat,
	pub content: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ImportConfigResponse {
	pub ok: bool,
	pub message: String,
	pub warnings: Vec<String>,
}

// ── Pipe types ──────────────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
pub struct PipeListResponse {
	pub pipes: Vec<PipeInfo>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PipePresetListResponse {
	pub presets: Vec<PipePresetInfo>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PipeInfo {
	pub name: String,
	pub origin_connector: String,
	pub origin_dsn: String,
	pub targets: Vec<String>,
	pub interval_secs: u64,
	pub query_count: usize,
	#[serde(skip_serializing_if = "Option::is_none")]
	#[schema(value_type = Option<ApiPipeRecipeDef>)]
	pub recipe: Option<serde_json::Value>,
	pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PipePresetSpecInput {
	pub origin_connector: String,
	pub origin_dsn: String,
	#[serde(default)]
	pub origin_credential: Option<String>,
	#[serde(default)]
	pub trino_url: Option<String>,
	#[serde(default)]
	#[schema(value_type = ApiOriginConfig)]
	pub origin_config: serde_json::Value,
	#[serde(default)]
	pub parameters: Vec<PipePresetParameterInput>,
	#[serde(default)]
	pub targets: Vec<String>,
	#[serde(default)]
	pub queries: Vec<PipeQueryInput>,
	#[serde(default)]
	#[schema(value_type = ApiScheduleDef)]
	pub schedule: serde_json::Value,
	#[serde(default)]
	#[schema(value_type = ApiDeltaDef)]
	pub delta: serde_json::Value,
	#[serde(default)]
	#[schema(value_type = ApiRetryDef)]
	pub retry: serde_json::Value,
	#[serde(default)]
	#[schema(value_type = Option<ApiPipeRecipeDef>)]
	pub recipe: Option<serde_json::Value>,
	#[serde(default)]
	#[schema(value_type = Vec<ApiTransformStep>)]
	pub filters: Vec<serde_json::Value>,
	#[serde(default)]
	#[schema(value_type = Vec<ApiTransformStep>)]
	pub transforms: Vec<serde_json::Value>,
	#[serde(default)]
	#[schema(value_type = Vec<ApiLinkDef>)]
	pub links: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PipePresetParameterInput {
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

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PipePresetInfo {
	pub name: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub description: Option<String>,
	#[schema(value_type = PipePresetSpecInput)]
	pub spec: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct PipeQueryInput {
	pub id: String,
	pub sql: String,
	pub key_column: String,
	#[serde(default)]
	pub sinks: Option<Vec<String>>,
	#[serde(default)]
	pub transform: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePipeRequest {
	pub name: String,
	pub origin_connector: String,
	pub origin_dsn: String,
	#[serde(default)]
	pub origin_credential: Option<String>,
	#[serde(default)]
	pub trino_url: Option<String>,
	#[serde(default)]
	#[schema(value_type = ApiOriginConfig)]
	pub origin_config: serde_json::Value,
	#[serde(default)]
	pub targets: Vec<String>,
	#[serde(default)]
	#[schema(value_type = ApiScheduleDef)]
	pub schedule: serde_json::Value,
	#[serde(default)]
	#[schema(value_type = ApiDeltaDef)]
	pub delta: serde_json::Value,
	#[serde(default)]
	#[schema(value_type = ApiRetryDef)]
	pub retry: serde_json::Value,
	#[serde(default)]
	#[schema(value_type = Option<ApiPipeRecipeDef>)]
	pub recipe: Option<serde_json::Value>,
	#[serde(default)]
	#[schema(value_type = Vec<ApiTransformStep>)]
	pub filters: Vec<serde_json::Value>,
	#[serde(default)]
	#[schema(value_type = Vec<ApiTransformStep>)]
	pub transforms: Vec<serde_json::Value>,
	#[serde(default)]
	#[schema(value_type = Vec<ApiLinkDef>)]
	pub links: Vec<serde_json::Value>,
	#[serde(default)]
	pub queries: Vec<PipeQueryInput>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdatePipeRequest {
	pub origin_connector: Option<String>,
	pub origin_dsn: Option<String>,
	pub origin_credential: Option<String>,
	pub trino_url: Option<String>,
	#[schema(value_type = Option<ApiOriginConfig>)]
	pub origin_config: Option<serde_json::Value>,
	pub targets: Option<Vec<String>>,
	#[schema(value_type = Option<ApiScheduleDef>)]
	pub schedule: Option<serde_json::Value>,
	#[schema(value_type = Option<ApiDeltaDef>)]
	pub delta: Option<serde_json::Value>,
	#[schema(value_type = Option<ApiRetryDef>)]
	pub retry: Option<serde_json::Value>,
	#[serde(default, deserialize_with = "deserialize_optional_json_value")]
	#[schema(value_type = Option<ApiPipeRecipeDef>)]
	pub recipe: Option<Option<serde_json::Value>>,
	#[schema(value_type = Option<Vec<ApiTransformStep>>)]
	pub filters: Option<Vec<serde_json::Value>>,
	#[schema(value_type = Option<Vec<ApiTransformStep>>)]
	pub transforms: Option<Vec<serde_json::Value>>,
	#[schema(value_type = Option<Vec<ApiLinkDef>>)]
	pub links: Option<Vec<serde_json::Value>>,
	pub queries: Option<Vec<PipeQueryInput>>,
	pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePipePresetRequest {
	pub name: String,
	#[serde(default)]
	pub description: Option<String>,
	pub spec: PipePresetSpecInput,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdatePipePresetRequest {
	#[serde(default)]
	pub description: Option<String>,
	pub spec: Option<PipePresetSpecInput>,
}

// ── Credential types ────────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
pub struct CredentialListResponse {
	pub credentials: Vec<CredentialInfo>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CredentialInfo {
	pub name: String,
	pub credential_type: String,
	pub created_at: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateCredentialRequest {
	pub name: String,
	pub credential_type: String,
	pub secret: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateCredentialRequest {
	pub secret: Option<String>,
	pub credential_type: Option<String>,
}

#[cfg(test)]
mod tests {
	use super::UpdatePipeRequest;

	#[test]
	fn update_pipe_request_distinguishes_null_recipe_from_omitted() {
		let cleared: UpdatePipeRequest = serde_json::from_value(serde_json::json!({
			"recipe": null
		}))
		.expect("request with null recipe should deserialize");
		assert!(matches!(cleared.recipe, Some(None)));

		let set: UpdatePipeRequest = serde_json::from_value(serde_json::json!({
			"recipe": {"type": "postgres_snapshot", "prefix": "demo"}
		}))
		.expect("request with recipe object should deserialize");
		assert!(matches!(set.recipe, Some(Some(_))));

		let omitted: UpdatePipeRequest = serde_json::from_value(serde_json::json!({}))
			.expect("empty request should deserialize");
		assert!(omitted.recipe.is_none());
	}
}
