use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;

pub struct ClickHouseSink {
	client: reqwest::Client,
	base_url: String,
	table: String,
	database: Option<String>,
	user: String,
	password: Option<String>,
	sink_name: String,
}

#[derive(Serialize)]
struct ClickHouseRow {
	key: String,
	data: String,
	op: String,
	origin_id: String,
	query_id: String,
	hash: String,
	cycle_id: u64,
	synced_at: String,
}

impl ClickHouseSink {
	pub fn new(
		name: &str,
		url: &str,
		table: &str,
		database: Option<String>,
		user: String,
		password: Option<String>,
		timeout_secs: u64,
	) -> Result<Self, OversyncError> {
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.timeout(Duration::from_secs(timeout_secs))
			.build()
			.map_err(|e| OversyncError::Sink(format!("clickhouse http client: {e}")))?;

		Ok(Self {
			client,
			base_url: url.trim_end_matches('/').to_string(),
			table: table.to_string(),
			database,
			user,
			password,
			sink_name: name.to_string(),
		})
	}

	fn insert_query(&self) -> String {
		let qualified_table = match &self.database {
			Some(db) => format!("{db}.{}", self.table),
			None => self.table.clone(),
		};
		format!("INSERT INTO {qualified_table} FORMAT JSONEachRow")
	}

	fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
		match &self.password {
			Some(pass) => req.basic_auth(&self.user, Some(pass)),
			None => req.header("X-ClickHouse-User", &self.user),
		}
	}

	async fn post_rows(&self, body: String) -> Result<(), OversyncError> {
		let req = self
			.client
			.post(&self.base_url)
			.query(&[("query", self.insert_query())])
			.header("Content-Type", "application/json")
			.body(body);
		let req = self.apply_auth(req);

		let resp = req
			.send()
			.await
			.map_err(|e| OversyncError::Sink(format!("clickhouse request: {e}")))?;

		let status = resp.status();
		let body = resp
			.text()
			.await
			.map_err(|e| OversyncError::Sink(format!("clickhouse response read: {e}")))?;

		if !status.is_success() {
			return Err(OversyncError::Sink(format!(
				"clickhouse HTTP {status}: {body}"
			)));
		}

		if body.starts_with("Code:") || body.contains("DB::Exception") {
			return Err(OversyncError::Sink(format!("clickhouse error: {body}")));
		}

		Ok(())
	}
}

fn envelope_to_row(envelope: &EventEnvelope) -> ClickHouseRow {
	ClickHouseRow {
		key: envelope.meta.key.clone(),
		data: serde_json::to_string(&envelope.data)
			.expect("serde_json::Value is always serializable"),
		op: envelope.meta.op.to_string(),
		origin_id: envelope.meta.origin_id.clone(),
		query_id: envelope.meta.query_id.clone(),
		hash: envelope.meta.hash.clone(),
		cycle_id: envelope.meta.cycle_id,
		synced_at: envelope
			.meta
			.timestamp
			.format("%Y-%m-%d %H:%M:%S")
			.to_string(),
	}
}

fn rows_to_jsonl(rows: &[ClickHouseRow]) -> Result<String, OversyncError> {
	let mut lines = Vec::with_capacity(rows.len());
	for row in rows {
		let line = serde_json::to_string(row)
			.map_err(|e| OversyncError::Sink(format!("serialize: {e}")))?;
		lines.push(line);
	}
	Ok(lines.join("\n"))
}

#[async_trait]
impl Sink for ClickHouseSink {
	fn name(&self) -> &str {
		&self.sink_name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let row = envelope_to_row(envelope);
		let body = serde_json::to_string(&row)
			.map_err(|e| OversyncError::Sink(format!("serialize: {e}")))?;
		debug!(sink = %self.sink_name, key = %envelope.meta.key, "inserting single row");
		self.post_rows(body).await
	}

	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		if envelopes.is_empty() {
			return Ok(());
		}
		let rows: Vec<ClickHouseRow> = envelopes.iter().map(envelope_to_row).collect();
		let body = rows_to_jsonl(&rows)?;
		debug!(sink = %self.sink_name, count = envelopes.len(), "inserting batch");
		self.post_rows(body).await
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		let req = self
			.client
			.get(&self.base_url)
			.query(&[("query", "SELECT 1")]);
		let req = self.apply_auth(req);

		let resp = req
			.send()
			.await
			.map_err(|e| OversyncError::Sink(format!("clickhouse test: {e}")))?;

		if !resp.status().is_success() {
			return Err(OversyncError::Sink(format!(
				"clickhouse test: status {}",
				resp.status()
			)));
		}

		debug!(sink = %self.sink_name, "clickhouse health check passed");
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::Utc;
	use oversync_core::model::{EventMeta, OpType};

	fn test_envelope() -> EventEnvelope {
		EventEnvelope {
			meta: EventMeta {
				op: OpType::Created,
				origin_id: "pg-prod".into(),
				query_id: "q1".into(),
				key: "pk-1".into(),
				hash: "abc123".into(),
				cycle_id: 42,
				timestamp: Utc::now(),
			},
			data: serde_json::json!({"name": "alice", "age": 30}),
		}
	}

	#[test]
	fn envelope_to_row_maps_fields() {
		let env = test_envelope();
		let row = envelope_to_row(&env);
		assert_eq!(row.key, "pk-1");
		assert_eq!(row.op, "created");
		assert_eq!(row.origin_id, "pg-prod");
		assert_eq!(row.query_id, "q1");
		assert_eq!(row.hash, "abc123");
		assert_eq!(row.cycle_id, 42);
		assert!(row.data.contains("alice"));
	}

	#[test]
	fn data_is_json_string_not_object() {
		let env = test_envelope();
		let row = envelope_to_row(&env);
		let serialized = serde_json::to_value(&row).unwrap();
		assert!(serialized["data"].is_string());
	}

	#[test]
	fn rows_to_jsonl_produces_newline_delimited() {
		let env = test_envelope();
		let rows = vec![envelope_to_row(&env), envelope_to_row(&env)];
		let body = rows_to_jsonl(&rows).unwrap();
		assert_eq!(body.lines().count(), 2);
		for line in body.lines() {
			let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
			assert!(parsed.is_object());
		}
	}

	#[test]
	fn insert_query_without_database() {
		let sink = ClickHouseSink::new(
			"test",
			"http://localhost:8123",
			"events",
			None,
			"default".into(),
			None,
			60,
		)
		.unwrap();
		assert_eq!(sink.insert_query(), "INSERT INTO events FORMAT JSONEachRow");
	}

	#[test]
	fn insert_query_with_database() {
		let sink = ClickHouseSink::new(
			"test",
			"http://localhost:8123",
			"events",
			Some("analytics".into()),
			"default".into(),
			None,
			60,
		)
		.unwrap();
		assert_eq!(
			sink.insert_query(),
			"INSERT INTO analytics.events FORMAT JSONEachRow"
		);
	}

	#[test]
	fn trailing_slash_trimmed() {
		let sink = ClickHouseSink::new(
			"test",
			"http://localhost:8123/",
			"events",
			None,
			"default".into(),
			None,
			60,
		)
		.unwrap();
		assert_eq!(sink.base_url, "http://localhost:8123");
	}

	#[test]
	fn deleted_event_has_null_data_serialized() {
		let env = EventEnvelope {
			meta: EventMeta {
				op: OpType::Deleted,
				origin_id: "s".into(),
				query_id: "q".into(),
				key: "k".into(),
				hash: "h".into(),
				cycle_id: 1,
				timestamp: Utc::now(),
			},
			data: serde_json::Value::Null,
		};
		let row = envelope_to_row(&env);
		assert_eq!(row.op, "deleted");
		assert_eq!(row.data, "null");
	}
}
