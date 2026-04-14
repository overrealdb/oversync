use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Column, MySqlPool, Row, TypeInfo, ValueRef};
use tokio::sync::mpsc;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

pub struct MysqlConnector {
	pool: MySqlPool,
	source_name: String,
}

impl MysqlConnector {
	pub async fn new(name: &str, dsn: &str) -> Result<Self, OversyncError> {
		let pool = MySqlPoolOptions::new()
			.max_connections(5)
			.min_connections(0)
			.idle_timeout(std::time::Duration::from_secs(60))
			.max_lifetime(std::time::Duration::from_secs(30 * 60))
			.connect(dsn)
			.await
			.map_err(|e| OversyncError::Connector(format!("mysql connect: {e}")))?;

		Ok(Self {
			pool,
			source_name: name.to_string(),
		})
	}

	pub fn from_pool(name: &str, pool: MySqlPool) -> Self {
		Self {
			pool,
			source_name: name.to_string(),
		}
	}
}

#[async_trait]
impl OriginConnector for MysqlConnector {
	fn name(&self) -> &str {
		&self.source_name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		let rows = sqlx::query(sql)
			.fetch_all(&self.pool)
			.await
			.map_err(|e| OversyncError::Connector(format!("fetch_all: {e}")))?;

		let mut result = Vec::with_capacity(rows.len());

		for row in &rows {
			let key: String = row
				.try_get(key_column)
				.map_err(|e| OversyncError::Connector(format!("key column '{key_column}': {e}")))?;

			let columns = row.columns();
			let mut data = serde_json::Map::with_capacity(columns.len());

			for col in columns {
				let name = col.name();
				let raw = row.try_get_raw(name).ok();
				let val = match raw {
					Some(ref r) if !r.is_null() => {
						decode_mysql_value(row, name, col.type_info().name())
					}
					_ => serde_json::Value::Null,
				};
				data.insert(name.to_string(), val);
			}

			result.push(RawRow {
				row_key: key,
				row_data: serde_json::Value::Object(data),
			});
		}

		debug!(count = result.len(), "fetched rows from mysql");
		Ok(result)
	}

	async fn fetch_into(
		&self,
		sql: &str,
		key_column: &str,
		batch_size: usize,
		tx: mpsc::Sender<Vec<RawRow>>,
	) -> Result<usize, OversyncError> {
		let mut stream = sqlx::query(sql).fetch(&self.pool);
		let mut batch = Vec::with_capacity(batch_size);
		let mut total = 0;

		while let Some(row) = stream
			.try_next()
			.await
			.map_err(|e| OversyncError::Connector(format!("fetch_into stream: {e}")))?
		{
			let key: String = row
				.try_get(key_column)
				.map_err(|e| OversyncError::Connector(format!("key column '{key_column}': {e}")))?;

			let columns = row.columns();
			let mut data = serde_json::Map::with_capacity(columns.len());
			for col in columns {
				let name = col.name();
				let raw = row.try_get_raw(name).ok();
				let val = match raw {
					Some(ref r) if !r.is_null() => {
						decode_mysql_value(&row, name, col.type_info().name())
					}
					_ => serde_json::Value::Null,
				};
				data.insert(name.to_string(), val);
			}

			batch.push(RawRow {
				row_key: key,
				row_data: serde_json::Value::Object(data),
			});
			total += 1;

			if batch.len() >= batch_size {
				tx.send(std::mem::replace(
					&mut batch,
					Vec::with_capacity(batch_size),
				))
				.await
				.map_err(|_| OversyncError::Internal("channel closed".into()))?;
			}
		}

		if !batch.is_empty() {
			tx.send(batch)
				.await
				.map_err(|_| OversyncError::Internal("channel closed".into()))?;
		}

		debug!(total, "streamed rows from mysql");
		Ok(total)
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		sqlx::query("SELECT 1")
			.fetch_one(&self.pool)
			.await
			.map_err(|e| OversyncError::Connector(format!("test_connection: {e}")))?;
		Ok(())
	}
}

fn decode_mysql_value(
	row: &sqlx::mysql::MySqlRow,
	name: &str,
	type_name: &str,
) -> serde_json::Value {
	match type_name {
		"BOOLEAN" => row
			.try_get::<bool, _>(name)
			.map(serde_json::Value::Bool)
			.unwrap_or(serde_json::Value::Null),
		"TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" => row
			.try_get::<i32, _>(name)
			.map(|v| serde_json::json!(v))
			.unwrap_or(serde_json::Value::Null),
		"BIGINT" => row
			.try_get::<i64, _>(name)
			.map(|v| serde_json::json!(v))
			.unwrap_or(serde_json::Value::Null),
		"FLOAT" | "DOUBLE" => row
			.try_get::<f64, _>(name)
			.map(|v| serde_json::json!(v))
			.unwrap_or(serde_json::Value::Null),
		"DECIMAL" => row
			.try_get::<String, _>(name)
			.map(serde_json::Value::String)
			.unwrap_or(serde_json::Value::Null),
		"JSON" => row
			.try_get::<serde_json::Value, _>(name)
			.unwrap_or(serde_json::Value::Null),
		_ => row
			.try_get::<String, _>(name)
			.map(serde_json::Value::String)
			.unwrap_or(serde_json::Value::Null),
	}
}
