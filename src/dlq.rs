use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use serde::Serialize;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

const SQL_CREATE_DLQ: &str = include_str!("../surql/queries/mutations/create_dlq_entry.surql");
const SQL_LIST_DLQ: &str = include_str!("../surql/queries/mutations/list_dlq.surql");
const SQL_DELETE_DLQ: &str = include_str!("../surql/queries/mutations/delete_dlq_entry.surql");

#[derive(Debug, Clone, Serialize)]
pub struct DlqEntry {
	pub id: String,
	pub pipe: String,
	pub query: String,
	pub cycle_id: i64,
	pub events_count: usize,
	pub error: String,
	pub attempts: u32,
	pub created_at: String,
}

/// Send failed events to the dead letter queue.
pub async fn send_to_dlq(
	db: &Surreal<Any>,
	pipe: &str,
	query: &str,
	cycle_id: i64,
	events: &[EventEnvelope],
	error: &str,
	attempts: u32,
) -> Result<(), OversyncError> {
	let events_json = serde_json::to_string(events)
		.map_err(|e| OversyncError::Internal(format!("dlq serialize: {e}")))?;

	db.query(SQL_CREATE_DLQ)
		.bind(("pipe", pipe.to_string()))
		.bind(("query", query.to_string()))
		.bind(("cycle_id", cycle_id))
		.bind(("events_json", events_json))
		.bind(("error", error.to_string()))
		.bind(("attempts", attempts as i64))
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("dlq insert: {e}")))?;

	tracing::warn!(
		pipe = %pipe,
		query = %query,
		events = events.len(),
		error = %error,
		"events sent to dead letter queue"
	);

	crate::metrics::record_dlq_entry(pipe, query, events.len());
	Ok(())
}

/// List DLQ entries (newest first).
pub async fn list_dlq(db: &Surreal<Any>) -> Result<Vec<DlqEntry>, OversyncError> {
	let mut resp = db
		.query(SQL_LIST_DLQ)
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("dlq list: {e}")))?;

	let rows: Vec<serde_json::Value> = resp
		.take(0)
		.map_err(|e| OversyncError::SurrealDb(format!("dlq list take: {e}")))?;

	Ok(rows
		.iter()
		.filter_map(|r| {
			let events_json = r.get("events_json")?.as_str()?;
			let events_count = serde_json::from_str::<Vec<serde_json::Value>>(events_json)
				.map(|v| v.len())
				.unwrap_or(0);

			Some(DlqEntry {
				id: r
					.get("id")
					.map(|v| v.to_string().trim_matches('"').to_string())
					.unwrap_or_default(),
				pipe: r.get("pipe")?.as_str()?.to_string(),
				query: r.get("query")?.as_str()?.to_string(),
				cycle_id: r.get("cycle_id")?.as_i64()?,
				events_count,
				error: r.get("error")?.as_str()?.to_string(),
				attempts: r.get("attempts")?.as_u64()? as u32,
				created_at: r.get("created_at")?.as_str()?.to_string(),
			})
		})
		.collect())
}

/// Delete a DLQ entry by ID.
pub async fn delete_dlq_entry(db: &Surreal<Any>, id: &str) -> Result<(), OversyncError> {
	db.query(SQL_DELETE_DLQ)
		.bind(("id", id.to_string()))
		.await
		.map_err(|e| OversyncError::SurrealDb(format!("dlq delete: {e}")))?;
	Ok(())
}
