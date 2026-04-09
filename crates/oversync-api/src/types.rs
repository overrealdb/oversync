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
	pub config: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateSinkRequest {
	pub sink_type: Option<String>,
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
	pub origin_config: serde_json::Value,
	#[serde(default)]
	pub parameters: Vec<PipePresetParameterInput>,
	#[serde(default)]
	pub targets: Vec<String>,
	#[serde(default)]
	pub queries: Vec<PipeQueryInput>,
	#[serde(default)]
	pub schedule: serde_json::Value,
	#[serde(default)]
	pub delta: serde_json::Value,
	#[serde(default)]
	pub retry: serde_json::Value,
	#[serde(default)]
	pub recipe: Option<serde_json::Value>,
	#[serde(default)]
	pub filters: Vec<serde_json::Value>,
	#[serde(default)]
	pub transforms: Vec<serde_json::Value>,
	#[serde(default)]
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
	pub origin_config: serde_json::Value,
	#[serde(default)]
	pub targets: Vec<String>,
	#[serde(default)]
	pub schedule: serde_json::Value,
	#[serde(default)]
	pub delta: serde_json::Value,
	#[serde(default)]
	pub retry: serde_json::Value,
	#[serde(default)]
	pub recipe: Option<serde_json::Value>,
	#[serde(default)]
	pub filters: Vec<serde_json::Value>,
	#[serde(default)]
	pub transforms: Vec<serde_json::Value>,
	#[serde(default)]
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
	pub origin_config: Option<serde_json::Value>,
	pub targets: Option<Vec<String>>,
	pub schedule: Option<serde_json::Value>,
	pub delta: Option<serde_json::Value>,
	pub retry: Option<serde_json::Value>,
	#[serde(default, deserialize_with = "deserialize_optional_json_value")]
	pub recipe: Option<Option<serde_json::Value>>,
	pub filters: Option<Vec<serde_json::Value>>,
	pub transforms: Option<Vec<serde_json::Value>>,
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
