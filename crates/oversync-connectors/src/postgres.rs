use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, PgPool, Row, TypeInfo, ValueRef};
use tokio::sync::mpsc;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

const SYNTHETIC_KEY_COLUMN: &str = "__oversync_key";

pub struct PostgresConnector {
	pool: PgPool,
	source_name: String,
}

impl PostgresConnector {
	pub async fn new(name: &str, dsn: &str) -> Result<Self, OversyncError> {
		let pool = PgPoolOptions::new()
			.max_connections(5)
			.connect(dsn)
			.await
			.map_err(|e| OversyncError::Connector(format!("postgres connect: {e}")))?;

		Ok(Self {
			pool,
			source_name: name.to_string(),
		})
	}

	pub fn from_pool(name: &str, pool: PgPool) -> Self {
		Self {
			pool,
			source_name: name.to_string(),
		}
	}
}

#[async_trait]
impl OriginConnector for PostgresConnector {
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
			let key = decode_pg_key(row, key_column)?;

			let columns = row.columns();
			let mut data = serde_json::Map::with_capacity(columns.len());

			for col in columns {
				let name = col.name();
				if name == SYNTHETIC_KEY_COLUMN {
					continue;
				}
				let raw = row.try_get_raw(name).ok();
				let val = match raw {
					Some(ref r) if !r.is_null() => {
						decode_pg_value(row, name, col.type_info().name())
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

		debug!(count = result.len(), "fetched rows from postgres");
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
			let key = decode_pg_key(&row, key_column)?;

			let columns = row.columns();
			let mut data = serde_json::Map::with_capacity(columns.len());
			for col in columns {
				let name = col.name();
				if name == SYNTHETIC_KEY_COLUMN {
					continue;
				}
				let raw = row.try_get_raw(name).ok();
				let val = match raw {
					Some(ref r) if !r.is_null() => {
						decode_pg_value(&row, name, col.type_info().name())
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

		debug!(total, "streamed rows from postgres");
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

fn decode_pg_key(row: &sqlx::postgres::PgRow, key_column: &str) -> Result<String, OversyncError> {
	let key_col = row
		.columns()
		.iter()
		.find(|col| col.name() == key_column)
		.ok_or_else(|| OversyncError::Connector(format!("key column '{key_column}' not found")))?;
	let raw = row
		.try_get_raw(key_column)
		.map_err(|e| OversyncError::Connector(format!("key column '{key_column}': {e}")))?;
	if raw.is_null() {
		return Err(OversyncError::Connector(format!(
			"key column '{key_column}' is NULL"
		)));
	}
	let value = decode_pg_value(row, key_column, key_col.type_info().name());
	match value {
		serde_json::Value::String(s) => Ok(s),
		serde_json::Value::Null => Err(OversyncError::Connector(format!(
			"key column '{key_column}' is NULL"
		))),
		other => Ok(other.to_string()),
	}
}

fn decode_pg_value(row: &sqlx::postgres::PgRow, name: &str, type_name: &str) -> serde_json::Value {
	match type_name {
		"BOOL" => row
			.try_get::<bool, _>(name)
			.map(serde_json::Value::Bool)
			.unwrap_or(serde_json::Value::Null),
		"INT2" | "INT4" => row
			.try_get::<i32, _>(name)
			.map(|v| serde_json::json!(v))
			.unwrap_or(serde_json::Value::Null),
		"INT8" | "OID" => row
			.try_get::<i64, _>(name)
			.map(|v| serde_json::json!(v))
			.unwrap_or(serde_json::Value::Null),
		"FLOAT4" | "FLOAT8" | "NUMERIC" => row
			.try_get::<f64, _>(name)
			.map(|v| serde_json::json!(v))
			.unwrap_or(serde_json::Value::Null),
		"JSON" | "JSONB" => row
			.try_get::<serde_json::Value, _>(name)
			.unwrap_or(serde_json::Value::Null),
		_ => row
			.try_get::<String, _>(name)
			.map(serde_json::Value::String)
			.unwrap_or(serde_json::Value::Null),
	}
}
