use chrono::{DateTime, Utc};
use serde::Serialize;
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

#[derive(Debug, Serialize, ToSchema)]
pub struct SourceStatus {
	pub last_cycle: Option<CycleInfo>,
	pub total_cycles: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CycleInfo {
	pub cycle_id: u64,
	pub status: String,
	pub started_at: DateTime<Utc>,
	pub finished_at: Option<DateTime<Utc>>,
	pub rows_created: u64,
	pub rows_updated: u64,
	pub rows_deleted: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SinkInfo {
	pub name: String,
	pub sink_type: String,
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
