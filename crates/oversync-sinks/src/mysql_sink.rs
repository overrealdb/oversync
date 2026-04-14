use async_trait::async_trait;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;

pub struct MysqlSink {
	pool: MySqlPool,
	sink_name: String,
	table: String,
}

impl MysqlSink {
	pub async fn new(name: &str, dsn: &str, table: &str) -> Result<Self, OversyncError> {
		let pool = MySqlPoolOptions::new()
			.max_connections(5)
			.min_connections(0)
			.idle_timeout(std::time::Duration::from_secs(60))
			.max_lifetime(std::time::Duration::from_secs(30 * 60))
			.connect(dsn)
			.await
			.map_err(|e| OversyncError::Sink(format!("mysql connect: {e}")))?;

		Ok(Self {
			pool,
			sink_name: name.to_string(),
			table: table.to_string(),
		})
	}

	pub fn from_pool(name: &str, pool: MySqlPool, table: &str) -> Self {
		Self {
			pool,
			sink_name: name.to_string(),
			table: table.to_string(),
		}
	}

	fn upsert_sql(&self) -> String {
		format!(
			"INSERT INTO `{table}` (`key`, `data`, `op`, `origin_id`, `query_id`, `hash`, `cycle_id`, `synced_at`) \
			 VALUES (?, ?, ?, ?, ?, ?, ?, NOW()) \
			 ON DUPLICATE KEY UPDATE \
			 `data` = VALUES(`data`), \
			 `op` = VALUES(`op`), \
			 `origin_id` = VALUES(`origin_id`), \
			 `query_id` = VALUES(`query_id`), \
			 `hash` = VALUES(`hash`), \
			 `cycle_id` = VALUES(`cycle_id`), \
			 `synced_at` = NOW()",
			table = self.table,
		)
	}
}

#[async_trait]
impl Sink for MysqlSink {
	fn name(&self) -> &str {
		&self.sink_name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let sql = self.upsert_sql();
		sqlx::query(&sql)
			.bind(&envelope.meta.key)
			.bind(
				serde_json::to_string(&envelope.data)
					.map_err(|e| OversyncError::Sink(format!("serialize data: {e}")))?,
			)
			.bind(envelope.meta.op.to_string())
			.bind(&envelope.meta.origin_id)
			.bind(&envelope.meta.query_id)
			.bind(&envelope.meta.hash)
			.bind(envelope.meta.cycle_id as i64)
			.execute(&self.pool)
			.await
			.map_err(|e| OversyncError::Sink(format!("mysql upsert: {e}")))?;

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
			.map_err(|e| OversyncError::Sink(format!("mysql begin tx: {e}")))?;

		for envelope in envelopes {
			sqlx::query(&sql)
				.bind(&envelope.meta.key)
				.bind(
					serde_json::to_string(&envelope.data)
						.map_err(|e| OversyncError::Sink(format!("serialize data: {e}")))?,
				)
				.bind(envelope.meta.op.to_string())
				.bind(&envelope.meta.origin_id)
				.bind(&envelope.meta.query_id)
				.bind(&envelope.meta.hash)
				.bind(envelope.meta.cycle_id as i64)
				.execute(&mut *tx)
				.await
				.map_err(|e| OversyncError::Sink(format!("mysql batch upsert: {e}")))?;
		}

		tx.commit()
			.await
			.map_err(|e| OversyncError::Sink(format!("mysql commit: {e}")))?;

		debug!(table = %self.table, count = envelopes.len(), "batch upserted events");
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		sqlx::query("SELECT 1")
			.execute(&self.pool)
			.await
			.map_err(|e| OversyncError::Sink(format!("mysql test: {e}")))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn upsert_sql_uses_backtick_quoting() {
		let pool = MySqlPool::connect_lazy("mysql://localhost/test").unwrap();
		let sink = MysqlSink::from_pool("test", pool, "events");
		let sql = sink.upsert_sql();
		assert!(sql.contains("`events`"));
		assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
		assert!(sql.contains("VALUES("));
	}

	#[tokio::test]
	async fn upsert_sql_custom_table() {
		let pool = MySqlPool::connect_lazy("mysql://localhost/test").unwrap();
		let sink = MysqlSink::from_pool("test", pool, "sync_events");
		let sql = sink.upsert_sql();
		assert!(sql.contains("`sync_events`"));
	}

	#[tokio::test]
	async fn from_pool_sets_fields() {
		let pool = MySqlPool::connect_lazy("mysql://localhost/test").unwrap();
		let sink = MysqlSink::from_pool("my-sink", pool, "tbl");
		assert_eq!(sink.name(), "my-sink");
		assert_eq!(sink.table, "tbl");
	}
}
