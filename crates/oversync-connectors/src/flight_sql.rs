use std::sync::Arc;
use std::time::Duration;

use arrow::array::{self as arrow_array, Array, AsArray};
use arrow::datatypes::{DataType, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use futures::TryStreamExt;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

pub struct FlightSqlConnector {
	endpoint: String,
	source_name: String,
}

impl FlightSqlConnector {
	pub fn new(name: &str, endpoint: &str) -> Result<Self, OversyncError> {
		Ok(Self {
			endpoint: endpoint.to_string(),
			source_name: name.to_string(),
		})
	}

	async fn connect(&self) -> Result<FlightSqlServiceClient<Channel>, OversyncError> {
		let endpoint = Endpoint::new(self.endpoint.clone())
			.map_err(|e| OversyncError::Connector(format!("flight_sql endpoint: {e}")))?
			.connect_timeout(Duration::from_secs(20))
			.timeout(Duration::from_secs(60));

		let channel = endpoint
			.connect()
			.await
			.map_err(|e| OversyncError::Connector(format!("flight_sql connect: {e}")))?;

		let inner = FlightServiceClient::new(channel);
		Ok(FlightSqlServiceClient::new_from_inner(inner))
	}

	async fn execute_query(
		&self,
		sql: &str,
		key_column: &str,
	) -> Result<Vec<RawRow>, OversyncError> {
		let mut client = self.connect().await?;

		let flight_info = client
			.execute(sql.to_string(), None)
			.await
			.map_err(|e| OversyncError::Connector(format!("flight_sql execute: {e}")))?;

		let schema = Arc::new(
			Schema::try_from(flight_info.clone())
				.map_err(|e| OversyncError::Connector(format!("flight_sql schema: {e}")))?,
		);

		let mut all_rows = Vec::new();

		for endpoint in &flight_info.endpoint {
			let ticket = endpoint
				.ticket
				.as_ref()
				.ok_or_else(|| OversyncError::Connector("trino: missing ticket".into()))?;

			let mut stream = client
				.do_get(ticket.clone())
				.await
				.map_err(|e| OversyncError::Connector(format!("flight_sql do_get: {e}")))?;

			while let Some(batch) = stream
				.try_next()
				.await
				.map_err(|e| OversyncError::Connector(format!("flight_sql stream: {e}")))?
			{
				let rows = record_batch_to_rows(&batch, &schema, key_column)?;
				all_rows.extend(rows);
			}
		}

		debug!(count = all_rows.len(), "fetched rows via flight sql");
		Ok(all_rows)
	}
}

#[async_trait]
impl OriginConnector for FlightSqlConnector {
	fn name(&self) -> &str {
		&self.source_name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		self.execute_query(sql, key_column).await
	}

	async fn fetch_into(
		&self,
		sql: &str,
		key_column: &str,
		_batch_size: usize,
		tx: mpsc::Sender<Vec<RawRow>>,
	) -> Result<usize, OversyncError> {
		let mut client = self.connect().await?;

		let flight_info = client
			.execute(sql.to_string(), None)
			.await
			.map_err(|e| OversyncError::Connector(format!("flight_sql execute: {e}")))?;

		let schema = Arc::new(
			Schema::try_from(flight_info.clone())
				.map_err(|e| OversyncError::Connector(format!("flight_sql schema: {e}")))?,
		);

		let mut total = 0;

		for endpoint in &flight_info.endpoint {
			let ticket = endpoint
				.ticket
				.as_ref()
				.ok_or_else(|| OversyncError::Connector("trino: missing ticket".into()))?;

			let mut stream = client
				.do_get(ticket.clone())
				.await
				.map_err(|e| OversyncError::Connector(format!("flight_sql do_get: {e}")))?;

			while let Some(batch) = stream
				.try_next()
				.await
				.map_err(|e| OversyncError::Connector(format!("flight_sql stream: {e}")))?
			{
				let rows = record_batch_to_rows(&batch, &schema, key_column)?;
				total += rows.len();
				tx.send(rows)
					.await
					.map_err(|_| OversyncError::Internal("channel closed".into()))?;
			}
		}

		debug!(total, "streamed rows via flight sql");
		Ok(total)
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		let mut client = self.connect().await?;
		let _ = client
			.execute("SELECT 1".to_string(), None)
			.await
			.map_err(|e| OversyncError::Connector(format!("flight_sql test: {e}")))?;
		Ok(())
	}
}

fn record_batch_to_rows(
	batch: &arrow_array::RecordBatch,
	schema: &Schema,
	key_column: &str,
) -> Result<Vec<RawRow>, OversyncError> {
	let key_idx = schema
		.index_of(key_column)
		.map_err(|_| OversyncError::Connector(format!("key column '{key_column}' not found")))?;

	let num_rows = batch.num_rows();
	let mut rows = Vec::with_capacity(num_rows);

	for row_idx in 0..num_rows {
		let key = arrow_value_to_string(batch.column(key_idx), row_idx);

		let mut data = serde_json::Map::with_capacity(schema.fields().len());
		for (col_idx, field) in schema.fields().iter().enumerate() {
			let col = batch.column(col_idx);
			let val = arrow_value_to_json(col, row_idx, field.data_type());
			data.insert(field.name().clone(), val);
		}

		rows.push(RawRow {
			row_key: key,
			row_data: serde_json::Value::Object(data),
		});
	}

	Ok(rows)
}

fn arrow_value_to_string(col: &dyn Array, row: usize) -> String {
	if col.is_null(row) {
		return String::new();
	}

	match col.data_type() {
		DataType::Utf8 => col.as_string::<i32>().value(row).to_string(),
		DataType::LargeUtf8 => col.as_string::<i64>().value(row).to_string(),
		DataType::Int32 => col
			.as_primitive::<arrow::datatypes::Int32Type>()
			.value(row)
			.to_string(),
		DataType::Int64 => col
			.as_primitive::<arrow::datatypes::Int64Type>()
			.value(row)
			.to_string(),
		_ => col
			.as_string_opt::<i32>()
			.map(|a| a.value(row).to_string())
			.unwrap_or_default(),
	}
}

fn arrow_value_to_json(col: &dyn Array, row: usize, data_type: &DataType) -> serde_json::Value {
	if col.is_null(row) {
		return serde_json::Value::Null;
	}

	match data_type {
		DataType::Boolean => serde_json::Value::Bool(col.as_boolean().value(row)),
		DataType::Int8 => {
			serde_json::json!(col.as_primitive::<arrow::datatypes::Int8Type>().value(row))
		}
		DataType::Int16 => {
			serde_json::json!(col.as_primitive::<arrow::datatypes::Int16Type>().value(row))
		}
		DataType::Int32 => {
			serde_json::json!(col.as_primitive::<arrow::datatypes::Int32Type>().value(row))
		}
		DataType::Int64 => {
			serde_json::json!(col.as_primitive::<arrow::datatypes::Int64Type>().value(row))
		}
		DataType::Float32 => {
			serde_json::json!(
				col.as_primitive::<arrow::datatypes::Float32Type>()
					.value(row)
			)
		}
		DataType::Float64 => {
			serde_json::json!(
				col.as_primitive::<arrow::datatypes::Float64Type>()
					.value(row)
			)
		}
		DataType::Utf8 => serde_json::Value::String(col.as_string::<i32>().value(row).to_string()),
		DataType::LargeUtf8 => {
			serde_json::Value::String(col.as_string::<i64>().value(row).to_string())
		}
		DataType::Date32 => {
			let days = col
				.as_primitive::<arrow::datatypes::Date32Type>()
				.value(row);
			serde_json::Value::String(format!("{days}"))
		}
		DataType::Date64 => {
			let ms = col
				.as_primitive::<arrow::datatypes::Date64Type>()
				.value(row);
			serde_json::Value::String(format!("{ms}"))
		}
		DataType::Timestamp(_, _) => {
			// Timestamps come as i64 — format as string
			let val = col
				.as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
				.value(row);
			serde_json::Value::String(format!("{val}"))
		}
		DataType::Decimal128(_, scale) => {
			let val = col
				.as_primitive::<arrow::datatypes::Decimal128Type>()
				.value(row);
			let f = val as f64 / 10f64.powi(*scale as i32);
			serde_json::json!(f)
		}
		_ => {
			// Fallback: try as string
			col.as_string_opt::<i32>()
				.map(|a| serde_json::Value::String(a.value(row).to_string()))
				.unwrap_or(serde_json::Value::Null)
		}
	}
}
