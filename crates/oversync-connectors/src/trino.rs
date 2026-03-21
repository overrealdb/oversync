use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, info, warn};

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

// ── Config ──────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TrinoExtraCredentials {
	pub username: String,
	pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrinoConfig {
	#[serde(rename = "dsn")]
	pub url: String,
	#[serde(default = "default_user")]
	pub user: String,
	pub catalog: Option<String>,
	pub schema: Option<String>,
	#[serde(default = "default_timeout")]
	pub timeout_secs: u64,
	#[serde(default)]
	pub auth: Option<TrinoAuth>,
	#[serde(default)]
	pub extra_credentials: Option<TrinoExtraCredentials>,
}

fn default_user() -> String {
	"oversync".into()
}
fn default_timeout() -> u64 {
	300
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TrinoAuth {
	Bearer { token: String },
	Basic { username: String, password: String },
}

// ── Protocol types ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryResults {
	id: String,
	#[serde(default)]
	next_uri: Option<String>,
	#[serde(default)]
	columns: Option<Vec<TrinoColumn>>,
	#[serde(default)]
	data: Option<Vec<Vec<serde_json::Value>>>,
	#[serde(default)]
	error: Option<QueryError>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoColumn {
	name: String,
	#[allow(dead_code)] // Deserialized for future type-aware conversion
	#[serde(rename = "type")]
	type_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryError {
	message: String,
	#[serde(default)]
	error_name: String,
}

fn encode_credential_value(s: &str) -> String {
	s.replace('%', "%25")
		.replace(',', "%2C")
		.replace('=', "%3D")
}

pub(crate) fn build_extra_credential_header(creds: &TrinoExtraCredentials) -> String {
	format!(
		"db.user={}, db.password={}",
		encode_credential_value(&creds.username),
		encode_credential_value(&creds.password),
	)
}

// ── TrinoClient ─────────────────────────────────────────────

pub(crate) struct TrinoClient {
	http: reqwest::Client,
	base_url: String,
	user: String,
	catalog: Option<String>,
	schema: Option<String>,
	timeout: Duration,
	bearer_token: Option<String>,
	extra_credentials: Option<TrinoExtraCredentials>,
}

impl TrinoClient {
	pub fn new(config: &TrinoConfig) -> Result<Self, OversyncError> {
		let mut builder = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(20))
			.timeout(Duration::from_secs(config.timeout_secs));

		if let Some(TrinoAuth::Basic { ref username, ref password }) = config.auth {
			builder = builder.default_headers({
				let mut h = reqwest::header::HeaderMap::new();
				let creds = base64_encode(&format!("{username}:{password}"));
				h.insert(
					reqwest::header::AUTHORIZATION,
					format!("Basic {creds}")
						.parse()
						.map_err(|e| OversyncError::Config(format!("trino basic auth header: {e}")))?,
				);
				h
			});
		}

		let http = builder
			.build()
			.map_err(|e| OversyncError::Connector(format!("trino http client: {e}")))?;

		let bearer_token = match &config.auth {
			Some(TrinoAuth::Bearer { token }) => Some(token.clone()),
			_ => None,
		};

		Ok(Self {
			http,
			base_url: config.url.trim_end_matches('/').to_string(),
			user: config.user.clone(),
			catalog: config.catalog.clone(),
			schema: config.schema.clone(),
			timeout: Duration::from_secs(config.timeout_secs),
			bearer_token,
			extra_credentials: config.extra_credentials.clone(),
		})
	}

	#[tracing::instrument(skip(self, sql), fields(url = %self.base_url, user = %self.user))]
	pub async fn execute(&self, sql: &str) -> Result<QueryExecution, OversyncError> {
		let mut req = self
			.http
			.post(format!("{}/v1/statement", self.base_url))
			.header("X-Trino-User", &self.user)
			.header("X-Trino-Source", "oversync")
			.header("X-Trino-Transaction-Id", "NONE")
			.header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
			.header("Content-Type", "text/plain; charset=utf-8");

		if let Some(ref catalog) = self.catalog {
			req = req.header("X-Trino-Catalog", catalog);
		}
		if let Some(ref schema) = self.schema {
			req = req.header("X-Trino-Schema", schema);
		}
		if let Some(ref token) = self.bearer_token {
			req = req.bearer_auth(token);
		}
		if let Some(ref creds) = self.extra_credentials {
			req = req.header("X-Trino-Extra-Credential", build_extra_credential_header(creds));
		}

		let resp = req
			.body(sql.to_string())
			.send()
			.await
			.map_err(|e| OversyncError::Connector(format!("trino submit: {e}")))?;

		if !resp.status().is_success() {
			return Err(OversyncError::Connector(format!(
				"trino submit: HTTP {}",
				resp.status()
			)));
		}

		let results: QueryResults = resp
			.json()
			.await
			.map_err(|e| OversyncError::Connector(format!("trino response parse: {e}")))?;

		if let Some(ref err) = results.error {
			return Err(OversyncError::Connector(format!(
				"trino query {}: {} ({})",
				results.id, err.message, err.error_name
			)));
		}

		Ok(QueryExecution {
			http: self.http.clone(),
			timeout: self.timeout,
			next_uri: results.next_uri,
			columns: results.columns,
			query_id: results.id,
			initial_data: results.data,
			heartbeat_handle: None,
			heartbeat_uri_tx: None,
		})
	}

}

fn base64_encode(s: &str) -> String {
	use base64::Engine;
	base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

// ── QueryExecution ──────────────────────────────────────────

pub(crate) struct QueryExecution {
	http: reqwest::Client,
	timeout: Duration,
	next_uri: Option<String>,
	columns: Option<Vec<TrinoColumn>>,
	query_id: String,
	initial_data: Option<Vec<Vec<serde_json::Value>>>,
	heartbeat_handle: Option<JoinHandle<()>>,
	heartbeat_uri_tx: Option<watch::Sender<String>>,
}

impl QueryExecution {
	/// Take any data from the initial POST response.
	pub fn take_initial_data(&mut self) -> Option<Vec<Vec<serde_json::Value>>> {
		self.initial_data.take()
	}

	#[tracing::instrument(skip(self), fields(query = %self.query_id))]
	pub async fn advance(&mut self) -> Result<Option<Vec<Vec<serde_json::Value>>>, OversyncError> {
		loop {
			let next = match self.next_uri.take() {
				Some(uri) => uri,
				None => return Ok(None), // query finished
			};

			self.update_heartbeat(&next);

			let resp = self.request_with_retry(&next).await?;
			let results: QueryResults = resp
				.json()
				.await
				.map_err(|e| OversyncError::Connector(format!("trino page parse: {e}")))?;

			self.next_uri = results.next_uri;
			if self.columns.is_none() {
				self.columns = results.columns;
			}

			if let Some(ref err) = results.error {
				self.cancel_heartbeat();
				return Err(OversyncError::Connector(format!(
					"trino query {}: {} ({})",
					self.query_id, err.message, err.error_name
				)));
			}

			if self.next_uri.is_none() {
				self.cancel_heartbeat();
			}

			// If this page has data, return it.
			// If no data but nextUri exists, keep polling (progress-only page).
			// If no data and no nextUri, query is done — return None.
			match results.data {
				Some(data) if !data.is_empty() => return Ok(Some(data)),
				_ if self.next_uri.is_some() => continue, // progress page, keep polling
				_ => return Ok(None),                      // done
			}
		}
	}

	pub fn columns(&self) -> &[TrinoColumn] {
		self.columns.as_deref().unwrap_or(&[])
	}

	pub fn query_id(&self) -> &str {
		&self.query_id
	}

	async fn request_with_retry(&self, url: &str) -> Result<reqwest::Response, OversyncError> {
		let start = Instant::now();
		let mut attempts = 0u32;

		loop {
			match self.http.get(url).send().await {
				Ok(resp) if resp.status().is_success() => return Ok(resp),
				Ok(resp) if should_retry(resp.status()) => {
					debug!(
						status = %resp.status(),
						attempt = attempts + 1,
						query = %self.query_id,
						"trino: retryable status"
					);
				}
				Ok(resp) if resp.status() == StatusCode::UNAUTHORIZED => {
					return Err(OversyncError::Connector(
						"trino: authentication failed (401)".into(),
					));
				}
				Ok(resp) => {
					return Err(OversyncError::Connector(format!(
						"trino: HTTP {}",
						resp.status()
					)));
				}
				Err(e) if is_transient(&e) => {
					debug!(
						error = %e,
						attempt = attempts + 1,
						query = %self.query_id,
						"trino: transient error, retrying"
					);
				}
				Err(e) => {
					return Err(OversyncError::Connector(format!("trino: {e}")));
				}
			}

			attempts += 1;
			if start.elapsed() > self.timeout {
				return Err(OversyncError::Connector(format!(
					"trino: retry timeout after {attempts} attempts ({:.1}s)",
					start.elapsed().as_secs_f64()
				)));
			}
			tokio::time::sleep(Duration::from_millis(attempts as u64 * 100)).await;
		}
	}

	fn update_heartbeat(&mut self, next_uri: &str) {
		if let Some(ref tx) = self.heartbeat_uri_tx {
			if tx.send(next_uri.to_string()).is_err() {
				warn!(query = %self.query_id, "heartbeat task died, restarting");
				self.heartbeat_uri_tx = None;
				self.heartbeat_handle = None;
				// Fall through to spawn a new one
			} else {
				return;
			}
		}

		// First call — spawn heartbeat task
		let (tx, mut rx) = watch::channel(next_uri.to_string());
		let http = self.http.clone();
		self.heartbeat_uri_tx = Some(tx);
		self.heartbeat_handle = Some(tokio::spawn(async move {
			let mut failures = 0u32;
			loop {
				tokio::time::sleep(Duration::from_secs(30)).await;
				let uri = rx.borrow().clone();
				match http.head(&uri).send().await {
					Ok(r) if r.status().is_success() => {
						failures = 0;
					}
					Ok(r)
						if r.status() == StatusCode::NOT_FOUND
							|| r.status() == StatusCode::METHOD_NOT_ALLOWED =>
					{
						break;
					}
					_ => {
						failures += 1;
						if failures >= 3 {
							break;
						}
					}
				}
			}
		}));
	}

	fn cancel_heartbeat(&mut self) {
		self.heartbeat_uri_tx.take();
		if let Some(h) = self.heartbeat_handle.take() {
			h.abort();
		}
	}
}

impl Drop for QueryExecution {
	fn drop(&mut self) {
		if let Some(uri) = self.next_uri.take() {
			let http = self.http.clone();
			let query_id = self.query_id.clone();
			tokio::spawn(async move {
				let result = tokio::time::timeout(
					Duration::from_secs(5),
					http.delete(&uri).send(),
				)
				.await;
				match result {
					Ok(Ok(_)) => {}
					Ok(Err(e)) => {
						tracing::warn!(query = %query_id, error = %e, "failed to cancel trino query");
					}
					Err(_) => {
						tracing::warn!(query = %query_id, "trino query cancel timed out");
					}
				}
			});
		}
		self.cancel_heartbeat();
	}
}

fn should_retry(status: StatusCode) -> bool {
	matches!(status.as_u16(), 502 | 503 | 504)
}

fn is_transient(e: &reqwest::Error) -> bool {
	e.is_timeout() || e.is_connect() || e.is_request()
}

// ── Type conversion ─────────────────────────────────────────

fn rows_to_raw_rows(
	columns: &[TrinoColumn],
	data: &[Vec<serde_json::Value>],
	key_column: &str,
) -> Result<Vec<RawRow>, OversyncError> {
	let key_idx = columns
		.iter()
		.position(|c| c.name == key_column)
		.ok_or_else(|| {
			OversyncError::Connector(format!("trino: key column '{key_column}' not found"))
		})?;

	let mut rows = Vec::with_capacity(data.len());
	for row_values in data {
		let key_val = row_values.get(key_idx).ok_or_else(|| {
			OversyncError::Connector(format!(
				"trino: row has {} columns, key column '{key_column}' at index {key_idx}",
				row_values.len()
			))
		})?;
		if key_val.is_null() {
			return Err(OversyncError::Connector(format!(
				"trino: NULL key in column '{key_column}'"
			)));
		}
		let key = value_to_string(key_val);

		let mut map = serde_json::Map::with_capacity(columns.len());
		for (i, col) in columns.iter().enumerate() {
			let val = row_values.get(i).cloned().unwrap_or(serde_json::Value::Null);
			map.insert(col.name.clone(), val);
		}

		rows.push(RawRow {
			row_key: key,
			row_data: serde_json::Value::Object(map),
		});
	}
	Ok(rows)
}

fn value_to_string(v: &serde_json::Value) -> String {
	match v {
		serde_json::Value::String(s) => s.clone(),
		serde_json::Value::Number(n) => n.to_string(),
		serde_json::Value::Bool(b) => b.to_string(),
		serde_json::Value::Null => String::new(),
		other => other.to_string(),
	}
}

// ── TrinoConnector (OriginConnector) ────────────────────────

pub struct TrinoConnector {
	client: TrinoClient,
	source_name: String,
}

impl TrinoConnector {
	pub fn new(name: &str, config: TrinoConfig) -> Result<Self, OversyncError> {
		let client = TrinoClient::new(&config)?;
		Ok(Self {
			client,
			source_name: name.to_string(),
		})
	}
}

#[async_trait]
impl OriginConnector for TrinoConnector {
	fn name(&self) -> &str {
		&self.source_name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		async {
			let mut exec = self.client.execute(sql).await?;
			let query_id = exec.query_id().to_string();
			let mut all_rows = Vec::new();
			let mut pages = 0u32;

			if let Some(data) = exec.take_initial_data() {
				if !data.is_empty() && !exec.columns().is_empty() {
					all_rows.extend(rows_to_raw_rows(exec.columns(), &data, key_column)?);
					pages += 1;
				}
			}

			while let Some(data) = exec.advance().await? {
				if !data.is_empty() && !exec.columns().is_empty() {
					all_rows.extend(rows_to_raw_rows(exec.columns(), &data, key_column)?);
					pages += 1;
				}
			}

			info!(
				rows = all_rows.len(),
				pages,
				query_id = %query_id,
				"trino fetch_all complete"
			);
			Ok(all_rows)
		}
		.instrument(tracing::info_span!("trino_fetch_all", source = %self.source_name))
		.await
	}

	async fn fetch_into(
		&self,
		sql: &str,
		key_column: &str,
		_batch_size: usize,
		tx: mpsc::Sender<Vec<RawRow>>,
	) -> Result<usize, OversyncError> {
		async {
			let mut exec = self.client.execute(sql).await?;
			let query_id = exec.query_id().to_string();
			let mut total = 0;
			let mut pages = 0u32;

			if let Some(data) = exec.take_initial_data() {
				if !data.is_empty() && !exec.columns().is_empty() {
					let rows = rows_to_raw_rows(exec.columns(), &data, key_column)?;
					total += rows.len();
					pages += 1;
					tx.send(rows)
						.await
						.map_err(|_| OversyncError::Internal("channel closed".into()))?;
				}
			}

			while let Some(data) = exec.advance().await? {
				if !data.is_empty() && !exec.columns().is_empty() {
					let rows = rows_to_raw_rows(exec.columns(), &data, key_column)?;
					total += rows.len();
					pages += 1;
					tx.send(rows)
						.await
						.map_err(|_| OversyncError::Internal("channel closed".into()))?;
				}
			}

			info!(
				total,
				pages,
				query_id = %query_id,
				"trino fetch_into complete"
			);
			Ok(total)
		}
		.instrument(tracing::info_span!("trino_fetch_into", source = %self.source_name))
		.await
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		async {
			let mut exec = self.client.execute("SELECT 1 AS _health").await?;
			while exec.advance().await?.is_some() {}
			info!("trino health check passed");
			Ok(())
		}
		.instrument(tracing::info_span!("trino_health", source = %self.source_name))
		.await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_config_minimal() {
		let json = serde_json::json!({"dsn": "http://trino:8080"});
		let config: TrinoConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.url, "http://trino:8080");
		assert_eq!(config.user, "oversync");
		assert!(config.catalog.is_none());
		assert_eq!(config.timeout_secs, 300);
		assert!(config.auth.is_none());
	}

	#[test]
	fn parse_config_full() {
		let json = serde_json::json!({
			"dsn": "http://trino:8080",
			"user": "analyst",
			"catalog": "hive",
			"schema": "prod",
			"timeout_secs": 600,
			"auth": {"type": "bearer", "token": "tok123"}
		});
		let config: TrinoConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.user, "analyst");
		assert_eq!(config.catalog.as_deref(), Some("hive"));
		assert_eq!(config.schema.as_deref(), Some("prod"));
		assert_eq!(config.timeout_secs, 600);
		assert!(matches!(config.auth, Some(TrinoAuth::Bearer { ref token }) if token == "tok123"));
	}

	#[test]
	fn parse_query_results_with_data() {
		let json = serde_json::json!({
			"id": "q1",
			"nextUri": "http://trino/v1/next",
			"columns": [
				{"name": "id", "type": "varchar"},
				{"name": "val", "type": "integer"}
			],
			"data": [["a", 1], ["b", 2]]
		});
		let results: QueryResults = serde_json::from_value(json).unwrap();
		assert_eq!(results.id, "q1");
		assert!(results.next_uri.is_some());
		assert_eq!(results.columns.as_ref().unwrap().len(), 2);
		assert_eq!(results.data.as_ref().unwrap().len(), 2);
		assert!(results.error.is_none());
	}

	#[test]
	fn parse_query_results_progress_only() {
		let json = serde_json::json!({
			"id": "q1",
			"nextUri": "http://trino/v1/next",
			"stats": {"state": "QUEUED"}
		});
		let results: QueryResults = serde_json::from_value(json).unwrap();
		assert!(results.columns.is_none());
		assert!(results.data.is_none());
		assert!(results.next_uri.is_some());
	}

	#[test]
	fn parse_query_results_error() {
		let json = serde_json::json!({
			"id": "q1",
			"error": {
				"message": "Table not found",
				"errorName": "TABLE_NOT_FOUND"
			}
		});
		let results: QueryResults = serde_json::from_value(json).unwrap();
		assert!(results.error.is_some());
		assert_eq!(results.error.as_ref().unwrap().message, "Table not found");
		assert!(results.next_uri.is_none());
	}

	#[test]
	fn rows_to_raw_rows_basic() {
		let cols = vec![
			TrinoColumn { name: "id".into(), type_name: "varchar".into() },
			TrinoColumn { name: "val".into(), type_name: "integer".into() },
		];
		let data = vec![
			vec![serde_json::json!("a"), serde_json::json!(42)],
			vec![serde_json::json!("b"), serde_json::json!(99)],
		];
		let rows = rows_to_raw_rows(&cols, &data, "id").unwrap();
		assert_eq!(rows.len(), 2);
		assert_eq!(rows[0].row_key, "a");
		assert_eq!(rows[0].row_data["val"], 42);
		assert_eq!(rows[1].row_key, "b");
	}

	#[test]
	fn rows_to_raw_rows_missing_key_errors() {
		let cols = vec![TrinoColumn { name: "id".into(), type_name: "varchar".into() }];
		let data = vec![vec![serde_json::json!("a")]];
		let result = rows_to_raw_rows(&cols, &data, "nonexistent");
		assert!(result.is_err());
	}

	#[test]
	fn rows_to_raw_rows_numeric_key() {
		let cols = vec![TrinoColumn { name: "id".into(), type_name: "bigint".into() }];
		let data = vec![vec![serde_json::json!(12345)]];
		let rows = rows_to_raw_rows(&cols, &data, "id").unwrap();
		assert_eq!(rows[0].row_key, "12345");
	}

	#[test]
	fn should_retry_correct_codes() {
		assert!(should_retry(StatusCode::BAD_GATEWAY));
		assert!(should_retry(StatusCode::SERVICE_UNAVAILABLE));
		assert!(should_retry(StatusCode::GATEWAY_TIMEOUT));
		assert!(!should_retry(StatusCode::OK));
		assert!(!should_retry(StatusCode::NOT_FOUND));
		assert!(!should_retry(StatusCode::INTERNAL_SERVER_ERROR));
	}

	#[test]
	fn value_to_string_types() {
		assert_eq!(value_to_string(&serde_json::json!("hello")), "hello");
		assert_eq!(value_to_string(&serde_json::json!(42)), "42");
		assert_eq!(value_to_string(&serde_json::json!(true)), "true");
		assert_eq!(value_to_string(&serde_json::Value::Null), "");
	}

	#[test]
	fn extra_credential_header_format() {
		let creds = TrinoExtraCredentials {
			username: "ivan".into(),
			password: "s3cret".into(),
		};
		let header = build_extra_credential_header(&creds);
		assert_eq!(header, "db.user=ivan, db.password=s3cret");
	}

	#[test]
	fn extra_credential_url_encoding() {
		let creds = TrinoExtraCredentials {
			username: "user=admin".into(),
			password: "p,a=ss%word".into(),
		};
		let header = build_extra_credential_header(&creds);
		assert_eq!(header, "db.user=user%3Dadmin, db.password=p%2Ca%3Dss%25word");
	}

	#[test]
	fn extra_credential_config_parsing() {
		let json = serde_json::json!({
			"dsn": "http://trino:8080",
			"extra_credentials": {
				"username": "ivan",
				"password": "secret"
			}
		});
		let config: TrinoConfig = serde_json::from_value(json).unwrap();
		let creds = config.extra_credentials.unwrap();
		assert_eq!(creds.username, "ivan");
		assert_eq!(creds.password, "secret");
	}

	#[test]
	fn no_extra_credentials_by_default() {
		let json = serde_json::json!({"dsn": "http://trino:8080"});
		let config: TrinoConfig = serde_json::from_value(json).unwrap();
		assert!(config.extra_credentials.is_none());
	}
}
