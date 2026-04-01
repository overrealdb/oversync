use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
	pub status: &'static str,
	pub version: &'static str,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SourceInfo {
	pub name: String,
	pub connector: String,
	pub interval_secs: u64,
	pub queries: Vec<QueryInfo>,
	pub status: SourceStatus,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QueryInfo {
	pub id: String,
	pub key_column: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SourceStatus {
	pub last_cycle: Option<CycleInfo>,
	pub total_cycles: u64,
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
pub struct TriggerResponse {
	pub source: String,
	pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
	pub error: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SourceListResponse {
	pub sources: Vec<SourceInfo>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SinkListResponse {
	pub sinks: Vec<SinkInfo>,
}

// ── Mutation request types ──────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSourceRequest {
	pub name: String,
	pub connector: String,
	#[serde(default)]
	pub config: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateSourceRequest {
	pub connector: Option<String>,
	pub config: Option<serde_json::Value>,
	pub enabled: Option<bool>,
}

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

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateQueryRequest {
	pub name: String,
	pub query: String,
	pub key_column: String,
	#[serde(default)]
	pub sinks: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateQueryRequest {
	pub query: Option<String>,
	pub key_column: Option<String>,
	pub sinks: Option<Vec<String>>,
	pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QueryListResponse {
	pub queries: Vec<QueryDetail>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QueryDetail {
	pub name: String,
	pub query: String,
	pub key_column: String,
	pub sinks: Option<Vec<String>>,
	pub enabled: bool,
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

// ── Pipe types ──────────────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
pub struct PipeListResponse {
	pub pipes: Vec<PipeInfo>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PipeInfo {
	pub name: String,
	pub origin_connector: String,
	pub origin_dsn: String,
	pub targets: Vec<String>,
	pub interval_secs: u64,
	pub enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePipeRequest {
	pub name: String,
	pub origin_connector: String,
	pub origin_dsn: String,
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
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdatePipeRequest {
	pub origin_connector: Option<String>,
	pub origin_dsn: Option<String>,
	pub origin_config: Option<serde_json::Value>,
	pub targets: Option<Vec<String>>,
	pub schedule: Option<serde_json::Value>,
	pub delta: Option<serde_json::Value>,
	pub retry: Option<serde_json::Value>,
	pub enabled: Option<bool>,
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
