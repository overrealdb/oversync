use async_trait::async_trait;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;

pub struct PostgresSink {
	pool: PgPool,
	sink_name: String,
	table: String,
	schema: String,
}

impl PostgresSink {
	pub async fn new(
		name: &str,
		dsn: &str,
		table: &str,
		schema: &str,
	) -> Result<Self, OversyncError> {
		let pool = PgPoolOptions::new()
			.max_connections(5)
			.connect(dsn)
			.await
			.map_err(|e| OversyncError::Sink(format!("postgres connect: {e}")))?;

		Ok(Self {
			pool,
			sink_name: name.to_string(),
			table: table.to_string(),
			schema: schema.to_string(),
		})
	}

	pub fn from_pool(name: &str, pool: PgPool, table: &str, schema: &str) -> Self {
		Self {
			pool,
			sink_name: name.to_string(),
			table: table.to_string(),
			schema: schema.to_string(),
		}
	}

	fn upsert_sql(&self) -> String {
		format!(
			r#"INSERT INTO {schema}.{table} (key, data, op, origin_id, query_id, hash, cycle_id, synced_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
ON CONFLICT (key) DO UPDATE SET
  data = EXCLUDED.data,
  op = EXCLUDED.op,
  origin_id = EXCLUDED.origin_id,
  query_id = EXCLUDED.query_id,
  hash = EXCLUDED.hash,
  cycle_id = EXCLUDED.cycle_id,
  synced_at = NOW()"#,
			schema = self.schema,
			table = self.table,
		)
	}
}

#[async_trait]
impl Sink for PostgresSink {
	fn name(&self) -> &str {
		&self.sink_name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let sql = self.upsert_sql();
		sqlx::query(&sql)
			.bind(&envelope.meta.key)
			.bind(
				serde_json::to_value(&envelope.data)
					.map_err(|e| OversyncError::Sink(format!("serialize data: {e}")))?,
			)
			.bind(envelope.meta.op.to_string())
			.bind(&envelope.meta.origin_id)
			.bind(&envelope.meta.query_id)
			.bind(&envelope.meta.hash)
			.bind(envelope.meta.cycle_id as i64)
			.execute(&self.pool)
			.await
			.map_err(|e| OversyncError::Sink(format!("postgres upsert: {e}")))?;

		debug!(table = %self.table, key = %envelope.meta.key, "upserted event");
		Ok(())
	}

	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		if envelopes.is_empty() {
			return Ok(());
		}

		let sql = self.upsert_sql();
		let mut tx = self
			.pool
			.begin()
			.await
			.map_err(|e| OversyncError::Sink(format!("postgres begin tx: {e}")))?;

		for envelope in envelopes {
			sqlx::query(&sql)
				.bind(&envelope.meta.key)
				.bind(
					serde_json::to_value(&envelope.data)
						.map_err(|e| OversyncError::Sink(format!("serialize data: {e}")))?,
				)
				.bind(envelope.meta.op.to_string())
				.bind(&envelope.meta.origin_id)
				.bind(&envelope.meta.query_id)
				.bind(&envelope.meta.hash)
				.bind(envelope.meta.cycle_id as i64)
				.execute(&mut *tx)
				.await
				.map_err(|e| OversyncError::Sink(format!("postgres batch upsert: {e}")))?;
		}

		tx.commit()
			.await
			.map_err(|e| OversyncError::Sink(format!("postgres commit: {e}")))?;

		debug!(table = %self.table, count = envelopes.len(), "batch upserted events");
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		sqlx::query("SELECT 1")
			.execute(&self.pool)
			.await
			.map_err(|e| OversyncError::Sink(format!("postgres test: {e}")))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn upsert_sql_default_schema() {
		let sink = PostgresSink {
			pool: PgPool::connect_lazy("postgres://localhost/test").unwrap(),
			sink_name: "test".into(),
			table: "events".into(),
			schema: "public".into(),
		};
		let sql = sink.upsert_sql();
		assert!(sql.contains("public.events"));
		assert!(sql.contains("ON CONFLICT (key) DO UPDATE"));
		assert!(sql.contains("$1"));
		assert!(sql.contains("$7"));
	}

	#[tokio::test]
	async fn upsert_sql_custom_schema() {
		let sink = PostgresSink {
			pool: PgPool::connect_lazy("postgres://localhost/test").unwrap(),
			sink_name: "test".into(),
			table: "sync_events".into(),
			schema: "oversync".into(),
		};
		let sql = sink.upsert_sql();
		assert!(sql.contains("oversync.sync_events"));
	}

	#[tokio::test]
	async fn from_pool_sets_fields() {
		let pool = PgPool::connect_lazy("postgres://localhost/test").unwrap();
		let sink = PostgresSink::from_pool("my-sink", pool, "tbl", "myschema");
		assert_eq!(sink.name(), "my-sink");
		assert_eq!(sink.table, "tbl");
		assert_eq!(sink.schema, "myschema");
	}
}
