use async_trait::async_trait;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;

const UPSERT_EVENT_SQL: &str = include_str!("../../../surql/queries/sink/upsert_event.surql");
const BATCH_UPSERT_EVENTS_SQL: &str =
	include_str!("../../../surql/queries/sink/batch_upsert_events.surql");

pub struct SurrealDbSink {
	client: Surreal<Any>,
	table: String,
	sink_name: String,
}

impl SurrealDbSink {
	pub async fn new(
		name: &str,
		url: &str,
		namespace: &str,
		database: &str,
		table: &str,
		username: &str,
		password: &str,
	) -> Result<Self, OversyncError> {
		let client = surrealdb::engine::any::connect(url)
			.await
			.map_err(|e| OversyncError::Sink(format!("surrealdb connect: {e}")))?;

		client
			.signin(surrealdb::opt::auth::Root {
				username: username.to_string(),
				password: password.to_string(),
			})
			.await
			.map_err(|e| OversyncError::Sink(format!("surrealdb signin: {e}")))?;

		client
			.use_ns(namespace)
			.use_db(database)
			.await
			.map_err(|e| OversyncError::Sink(format!("surrealdb use ns/db: {e}")))?;

		Ok(Self {
			client,
			table: table.to_string(),
			sink_name: name.to_string(),
		})
	}

	/// Create from an existing connected client (for testing).
	pub fn from_client(name: &str, client: Surreal<Any>, table: &str) -> Self {
		Self {
			client,
			table: table.to_string(),
			sink_name: name.to_string(),
		}
	}
}

#[async_trait]
impl Sink for SurrealDbSink {
	fn name(&self) -> &str {
		&self.sink_name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.client
			.query(UPSERT_EVENT_SQL)
			.bind(("table", self.table.clone()))
			.bind(("key", envelope.meta.key.clone()))
			.bind(("data", envelope.data.clone()))
			.bind(("source_id", envelope.meta.source_id.clone()))
			.bind(("query_id", envelope.meta.query_id.clone()))
			.bind(("op", envelope.meta.op.to_string()))
			.bind(("hash", envelope.meta.hash.clone()))
			.bind(("cycle_id", envelope.meta.cycle_id))
			.await
			.map_err(|e| OversyncError::Sink(format!("surrealdb upsert: {e}")))?;

		debug!(table = %self.table, key = %envelope.meta.key, "upserted event");
		Ok(())
	}

	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		if envelopes.is_empty() {
			return Ok(());
		}

		let events: Vec<serde_json::Value> = envelopes
			.iter()
			.map(|e| {
				serde_json::json!({
					"table": self.table,
					"key": e.meta.key,
					"data": e.data,
					"source_id": e.meta.source_id,
					"query_id": e.meta.query_id,
					"op": e.meta.op.to_string(),
					"hash": e.meta.hash,
					"cycle_id": e.meta.cycle_id,
				})
			})
			.collect();

		self.client
			.query(BATCH_UPSERT_EVENTS_SQL)
			.bind(("events", events))
			.await
			.map_err(|e| OversyncError::Sink(format!("surrealdb batch upsert: {e}")))?;

		debug!(table = %self.table, count = envelopes.len(), "batch upserted events");
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.client
			.query("RETURN 1")
			.await
			.map_err(|e| OversyncError::Sink(format!("surrealdb test: {e}")))?;
		Ok(())
	}
}
