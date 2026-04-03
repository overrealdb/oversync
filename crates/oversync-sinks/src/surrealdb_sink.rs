use async_trait::async_trait;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::{debug, warn};

use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, EventMeta};
use oversync_core::traits::Sink;

fn build_document(data: &serde_json::Value, meta: &EventMeta) -> serde_json::Value {
	let mut map = match data {
		serde_json::Value::Object(m) => m.clone(),
		other => {
			warn!(kind = %other, "document mode: data is not an object, wrapping in payload");
			let mut m = serde_json::Map::new();
			m.insert("payload".into(), other.clone());
			m
		}
	};
	map.insert(
		"_meta".into(),
		serde_json::json!({
			"origin_id": meta.origin_id,
			"query_id": meta.query_id,
			"op": meta.op.to_string(),
			"hash": meta.hash,
			"cycle_id": meta.cycle_id,
		}),
	);
	serde_json::Value::Object(map)
}

/// How the SurrealDB sink writes records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
	/// Wrap payload inside a `data` field with `_meta` alongside (default).
	Envelope,
	/// Merge payload fields directly onto the record as top-level fields.
	Document,
}

pub struct SurrealDbSink {
	client: Surreal<Any>,
	table: String,
	sink_name: String,
	mode: SinkMode,
	key_field: Option<String>,
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
		Self::with_mode(
			name,
			url,
			namespace,
			database,
			table,
			username,
			password,
			SinkMode::Envelope,
			None,
		)
		.await
	}

	pub async fn with_mode(
		name: &str,
		url: &str,
		namespace: &str,
		database: &str,
		table: &str,
		username: &str,
		password: &str,
		mode: SinkMode,
		key_field: Option<String>,
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
			mode,
			key_field,
		})
	}

	/// Create from an existing connected client (for testing).
	pub fn from_client(
		name: &str,
		client: Surreal<Any>,
		table: &str,
		mode: SinkMode,
		key_field: Option<String>,
	) -> Self {
		Self {
			client,
			table: table.to_string(),
			sink_name: name.to_string(),
			mode,
			key_field,
		}
	}

	fn resolve_key(&self, envelope: &EventEnvelope) -> Result<String, OversyncError> {
		match &self.key_field {
			Some(field) => envelope
				.data
				.get(field)
				.and_then(|v| v.as_str())
				.map(String::from)
				.ok_or_else(|| {
					OversyncError::Sink(format!(
						"key_field '{field}' missing or not a string in data for key '{}'",
						envelope.meta.key,
					))
				}),
			None => Ok(envelope.meta.key.clone()),
		}
	}

	fn to_batch_entry(&self, envelope: &EventEnvelope) -> Result<serde_json::Value, OversyncError> {
		let key = self.resolve_key(envelope)?;
		Ok(match self.mode {
			SinkMode::Envelope => serde_json::json!({
				"table": self.table,
				"key": key,
				"data": envelope.data,
				"origin_id": envelope.meta.origin_id,
				"query_id": envelope.meta.query_id,
				"op": envelope.meta.op.to_string(),
				"hash": envelope.meta.hash,
				"cycle_id": envelope.meta.cycle_id,
			}),
			SinkMode::Document => serde_json::json!({
				"table": self.table,
				"key": key,
				"doc": build_document(&envelope.data, &envelope.meta),
			}),
		})
	}
}

#[async_trait]
impl Sink for SurrealDbSink {
	fn name(&self) -> &str {
		&self.sink_name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.send_batch(std::slice::from_ref(envelope)).await
	}

	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		if envelopes.is_empty() {
			return Ok(());
		}

		let events: Vec<serde_json::Value> = envelopes
			.iter()
			.map(|e| self.to_batch_entry(e))
			.collect::<Result<_, _>>()?;

		let sql = match self.mode {
			SinkMode::Envelope => oversync_queries::sink::BATCH_UPSERT_EVENTS,
			SinkMode::Document => oversync_queries::sink::BATCH_UPSERT_DOCUMENTS,
		};

		let response = self
			.client
			.query(sql)
			.bind(("events", events))
			.await
			.map_err(|e| OversyncError::Sink(format!("surrealdb batch upsert: {e}")))?;
		response
			.check()
			.map_err(|e| OversyncError::Sink(format!("surrealdb batch upsert check: {e}")))?;

		debug!(table = %self.table, count = envelopes.len(), mode = ?self.mode, "batch upserted events");
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
