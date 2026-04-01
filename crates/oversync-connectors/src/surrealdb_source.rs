use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::{debug, warn};

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

pub struct SurrealDbConnector {
	client: Surreal<Any>,
	source_name: String,
}

pub struct SurrealDbLiveConnector {
	client: Surreal<Any>,
	source_name: String,
	table: String,
	key_column: String,
	state: Arc<Mutex<HashMap<String, serde_json::Value>>>,
	started: Arc<Mutex<bool>>,
}

impl SurrealDbConnector {
	pub async fn new(
		name: &str,
		url: &str,
		namespace: &str,
		database: &str,
		username: &str,
		password: &str,
	) -> Result<Self, OversyncError> {
		let client = surrealdb::engine::any::connect(url)
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb connect: {e}")))?;

		client
			.signin(surrealdb::opt::auth::Root {
				username: username.to_string(),
				password: password.to_string(),
			})
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb signin: {e}")))?;

		client
			.use_ns(namespace)
			.use_db(database)
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb use ns/db: {e}")))?;

		Ok(Self {
			client,
			source_name: name.to_string(),
		})
	}

	pub fn from_client(name: &str, client: Surreal<Any>) -> Self {
		Self {
			client,
			source_name: name.to_string(),
		}
	}
}

impl SurrealDbLiveConnector {
	#[allow(clippy::too_many_arguments)] // connection params are inherently numerous
	pub async fn new(
		name: &str,
		url: &str,
		namespace: &str,
		database: &str,
		username: &str,
		password: &str,
		table: &str,
		key_column: &str,
	) -> Result<Self, OversyncError> {
		let client = surrealdb::engine::any::connect(url)
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb live connect: {e}")))?;

		client
			.signin(surrealdb::opt::auth::Root {
				username: username.to_string(),
				password: password.to_string(),
			})
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb live signin: {e}")))?;

		client
			.use_ns(namespace)
			.use_db(database)
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb live use ns/db: {e}")))?;

		Ok(Self {
			client,
			source_name: name.to_string(),
			table: table.to_string(),
			key_column: key_column.to_string(),
			state: Arc::new(Mutex::new(HashMap::new())),
			started: Arc::new(Mutex::new(false)),
		})
	}

	pub fn from_client(name: &str, client: Surreal<Any>, table: &str, key_column: &str) -> Self {
		Self {
			client,
			source_name: name.to_string(),
			table: table.to_string(),
			key_column: key_column.to_string(),
			state: Arc::new(Mutex::new(HashMap::new())),
			started: Arc::new(Mutex::new(false)),
		}
	}

	async fn start_live_stream(&self) -> Result<(), OversyncError> {
		let mut stream = self
			.client
			.select(&*self.table)
			.live()
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb live select: {e}")))?;

		let state = Arc::clone(&self.state);
		let key_column = self.key_column.clone();
		let table = self.table.clone();

		tokio::spawn(async move {
			while let Some(result) = stream.next().await {
				match result {
					Ok(notification) => {
						let data: serde_json::Value = notification.data;
						let key = extract_key(&data, &key_column);
						match notification.action {
							surrealdb::types::Action::Create | surrealdb::types::Action::Update => {
								state.lock().unwrap().insert(key, data);
							}
							surrealdb::types::Action::Delete => {
								state.lock().unwrap().remove(&key);
							}
							_ => {}
						}
					}
					Err(e) => {
						warn!(table = %table, "live query error: {e}");
					}
				}
			}
			debug!(table = %table, "live query stream ended");
		});

		*self.started.lock().unwrap() = true;
		Ok(())
	}
}

#[async_trait]
impl OriginConnector for SurrealDbLiveConnector {
	fn name(&self) -> &str {
		&self.source_name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		let already_started = *self.started.lock().unwrap();

		if !already_started {
			let mut response =
				self.client.query(sql).await.map_err(|e| {
					OversyncError::Connector(format!("surrealdb initial query: {e}"))
				})?;

			let rows: Vec<serde_json::Value> = response
				.take(0)
				.map_err(|e| OversyncError::Connector(format!("surrealdb take: {e}")))?;

			{
				let mut state = self.state.lock().unwrap();
				for row in &rows {
					let key = extract_key(row, key_column);
					state.insert(key, row.clone());
				}
			}

			self.start_live_stream().await?;
			debug!(count = rows.len(), table = %self.table, "initial fetch + live stream started");
		}

		let state = self.state.lock().unwrap();
		let result: Vec<RawRow> = state
			.iter()
			.map(|(key, data)| RawRow {
				row_key: key.clone(),
				row_data: data.clone(),
			})
			.collect();

		debug!(count = result.len(), table = %self.table, "returning live state snapshot");
		Ok(result)
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.client
			.query("RETURN 1")
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb live test: {e}")))?;
		Ok(())
	}
}

fn extract_key(val: &serde_json::Value, key_column: &str) -> String {
	match val.get(key_column) {
		Some(serde_json::Value::String(s)) => {
			s.split_once(':').map(|(_, k)| k).unwrap_or(s).to_string()
		}
		Some(v) => v.to_string().trim_matches('"').to_string(),
		None => String::new(),
	}
}

#[async_trait]
impl OriginConnector for SurrealDbConnector {
	fn name(&self) -> &str {
		&self.source_name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		let mut response = self
			.client
			.query(sql)
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb query: {e}")))?;

		let rows: Vec<serde_json::Value> = match response.take(0) {
			Ok(r) => r,
			Err(e) if e.to_string().contains("does not exist") => {
				debug!("table does not exist, returning empty");
				return Ok(Vec::new());
			}
			Err(e) => return Err(OversyncError::Connector(format!("surrealdb take: {e}"))),
		};

		let result: Vec<RawRow> = rows
			.iter()
			.map(|row| RawRow {
				row_key: extract_key(row, key_column),
				row_data: row.clone(),
			})
			.collect();

		debug!(count = result.len(), "fetched rows from surrealdb");
		Ok(result)
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.client
			.query("RETURN 1")
			.await
			.map_err(|e| OversyncError::Connector(format!("surrealdb test: {e}")))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extract_key_record_id() {
		let row = serde_json::json!({"id": "user:abc123", "name": "Alice"});
		assert_eq!(extract_key(&row, "id"), "abc123");
	}

	#[test]
	fn extract_key_plain_string() {
		let row = serde_json::json!({"id": "plain-id", "name": "Bob"});
		assert_eq!(extract_key(&row, "id"), "plain-id");
	}

	#[test]
	fn extract_key_numeric() {
		let row = serde_json::json!({"id": 42, "name": "Charlie"});
		assert_eq!(extract_key(&row, "id"), "42");
	}

	#[test]
	fn extract_key_missing_column() {
		let row = serde_json::json!({"name": "Dave"});
		assert_eq!(extract_key(&row, "id"), "");
	}

	#[test]
	fn extract_key_non_id_column() {
		let row = serde_json::json!({"id": "user:1", "email": "test@example.com"});
		assert_eq!(extract_key(&row, "email"), "test@example.com");
	}

	#[test]
	fn extract_key_nested_colon_in_id() {
		let row = serde_json::json!({"id": "table:complex:key"});
		assert_eq!(extract_key(&row, "id"), "complex:key");
	}

	#[test]
	fn extract_key_boolean_value() {
		let row = serde_json::json!({"active": true});
		assert_eq!(extract_key(&row, "active"), "true");
	}

	#[tokio::test]
	async fn factory_missing_url() {
		use crate::factory::SurrealDbOriginFactory;
		use oversync_core::traits::OriginFactory;

		let config = serde_json::json!({"namespace": "test", "database": "test"});
		match SurrealDbOriginFactory.create("test", &config).await {
			Err(e) => assert!(e.to_string().contains("missing 'url'"), "got: {e}"),
			Ok(_) => panic!("expected error"),
		}
	}

	#[tokio::test]
	async fn factory_missing_namespace() {
		use crate::factory::SurrealDbOriginFactory;
		use oversync_core::traits::OriginFactory;

		let config = serde_json::json!({"url": "mem://", "database": "test"});
		match SurrealDbOriginFactory.create("test", &config).await {
			Err(e) => assert!(e.to_string().contains("missing 'namespace'"), "got: {e}"),
			Ok(_) => panic!("expected error"),
		}
	}

	#[tokio::test]
	async fn factory_missing_database() {
		use crate::factory::SurrealDbOriginFactory;
		use oversync_core::traits::OriginFactory;

		let config = serde_json::json!({"url": "mem://", "namespace": "test"});
		match SurrealDbOriginFactory.create("test", &config).await {
			Err(e) => assert!(e.to_string().contains("missing 'database'"), "got: {e}"),
			Ok(_) => panic!("expected error"),
		}
	}
}
