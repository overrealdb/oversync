use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{Instrument, info};

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseConfig {
	#[serde(rename = "dsn")]
	pub url: String,
	#[serde(default = "default_user")]
	pub user: String,
	pub password: Option<String>,
	pub database: Option<String>,
	#[serde(default = "default_timeout")]
	pub timeout_secs: u64,
}

fn default_user() -> String {
	"default".into()
}
fn default_timeout() -> u64 {
	60
}

pub struct ClickHouseConnector {
	client: reqwest::Client,
	base_url: String,
	user: String,
	password: Option<String>,
	database: Option<String>,
	source_name: String,
}

impl ClickHouseConnector {
	pub fn new(name: &str, config: ClickHouseConfig) -> Result<Self, OversyncError> {
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.timeout(Duration::from_secs(config.timeout_secs))
			.build()
			.map_err(|e| OversyncError::Connector(format!("clickhouse http client: {e}")))?;

		Ok(Self {
			client,
			base_url: config.url.trim_end_matches('/').to_string(),
			user: config.user,
			password: config.password,
			database: config.database,
			source_name: name.to_string(),
		})
	}

	fn query_url(&self) -> String {
		let mut url = format!("{}/?default_format=JSONEachRow", self.base_url);
		if let Some(ref db) = self.database {
			url.push_str(&format!("&database={db}"));
		}
		url
	}

	fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
		match &self.password {
			Some(pass) => req.basic_auth(&self.user, Some(pass)),
			None => req.header("X-ClickHouse-User", &self.user),
		}
	}

	async fn execute_query(&self, sql: &str) -> Result<String, OversyncError> {
		let req = self.client.post(self.query_url()).body(sql.to_string());
		let req = self.apply_auth(req);

		let resp = req
			.send()
			.await
			.map_err(|e| OversyncError::Connector(format!("clickhouse request: {e}")))?;

		let status = resp.status();
		let body = resp
			.text()
			.await
			.map_err(|e| OversyncError::Connector(format!("clickhouse response read: {e}")))?;

		if !status.is_success() {
			return Err(OversyncError::Connector(format!(
				"clickhouse HTTP {status}: {body}"
			)));
		}

		// ClickHouse can return 200 with error in body
		if body.starts_with("Code:") || body.contains("DB::Exception") {
			return Err(OversyncError::Connector(format!(
				"clickhouse error: {body}"
			)));
		}

		Ok(body)
	}
}

fn parse_jsonl(body: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
	let mut rows = Vec::new();
	for line in body.lines() {
		let line = line.trim();
		if line.is_empty() {
			continue;
		}
		let json: serde_json::Value = serde_json::from_str(line)
			.map_err(|e| OversyncError::Connector(format!("clickhouse JSON parse: {e}")))?;

		let key = json.get(key_column).ok_or_else(|| {
			OversyncError::Connector(format!(
				"clickhouse: key column '{key_column}' not found in row"
			))
		})?;

		let key_str = match key {
			serde_json::Value::String(s) => s.clone(),
			serde_json::Value::Number(n) => n.to_string(),
			serde_json::Value::Null => {
				return Err(OversyncError::Connector(format!(
					"clickhouse: NULL key in column '{key_column}'"
				)));
			}
			other => other.to_string(),
		};

		rows.push(RawRow {
			row_key: key_str,
			row_data: json,
		});
	}
	Ok(rows)
}

#[async_trait]
impl OriginConnector for ClickHouseConnector {
	fn name(&self) -> &str {
		&self.source_name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		async {
			let body = self.execute_query(sql).await?;
			let rows = parse_jsonl(&body, key_column)?;

			info!(rows = rows.len(), "clickhouse fetch_all complete");
			Ok(rows)
		}
		.instrument(tracing::info_span!("clickhouse_fetch_all", source = %self.source_name))
		.await
	}

	async fn fetch_into(
		&self,
		sql: &str,
		key_column: &str,
		batch_size: usize,
		tx: mpsc::Sender<Vec<RawRow>>,
	) -> Result<usize, OversyncError> {
		async {
			let body = self.execute_query(sql).await?;
			let all_rows = parse_jsonl(&body, key_column)?;
			let total = all_rows.len();

			for chunk in all_rows.chunks(batch_size) {
				tx.send(chunk.to_vec())
					.await
					.map_err(|_| OversyncError::Internal("channel closed".into()))?;
			}

			info!(total, "clickhouse fetch_into complete");
			Ok(total)
		}
		.instrument(tracing::info_span!("clickhouse_fetch_into", source = %self.source_name))
		.await
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		async {
			self.execute_query("SELECT 1").await?;
			info!("clickhouse health check passed");
			Ok(())
		}
		.instrument(tracing::info_span!("clickhouse_health", source = %self.source_name))
		.await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_config_minimal() {
		let json = serde_json::json!({"dsn": "http://localhost:8123"});
		let config: ClickHouseConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.url, "http://localhost:8123");
		assert_eq!(config.user, "default");
		assert_eq!(config.timeout_secs, 60);
		assert!(config.password.is_none());
		assert!(config.database.is_none());
	}

	#[test]
	fn parse_config_full() {
		let json = serde_json::json!({
			"dsn": "http://ch:8123",
			"user": "admin",
			"password": "secret",
			"database": "analytics",
			"timeout_secs": 120,
		});
		let config: ClickHouseConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.user, "admin");
		assert_eq!(config.password.as_deref(), Some("secret"));
		assert_eq!(config.database.as_deref(), Some("analytics"));
	}

	#[test]
	fn parse_jsonl_basic() {
		let body = r#"{"id":"1","name":"users","engine":"MergeTree"}
{"id":"2","name":"orders","engine":"ReplicatedMergeTree"}"#;
		let rows = parse_jsonl(body, "id").unwrap();
		assert_eq!(rows.len(), 2);
		assert_eq!(rows[0].row_key, "1");
		assert_eq!(rows[0].row_data["name"], "users");
		assert_eq!(rows[1].row_key, "2");
	}

	#[test]
	fn parse_jsonl_numeric_key() {
		let body = r#"{"id":42,"val":"hello"}"#;
		let rows = parse_jsonl(body, "id").unwrap();
		assert_eq!(rows[0].row_key, "42");
	}

	#[test]
	fn parse_jsonl_missing_key_errors() {
		let body = r#"{"name":"test"}"#;
		let result = parse_jsonl(body, "id");
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("id"));
	}

	#[test]
	fn parse_jsonl_null_key_errors() {
		let body = r#"{"id":null,"name":"test"}"#;
		let result = parse_jsonl(body, "id");
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("NULL"));
	}

	#[test]
	fn parse_jsonl_empty_lines_skipped() {
		let body = "  \n{\"id\":\"1\",\"v\":1}\n\n{\"id\":\"2\",\"v\":2}\n  ";
		let rows = parse_jsonl(body, "id").unwrap();
		assert_eq!(rows.len(), 2);
	}

	#[test]
	fn query_url_without_database() {
		let conn = ClickHouseConnector::new(
			"test",
			ClickHouseConfig {
				url: "http://localhost:8123".into(),
				user: "default".into(),
				password: None,
				database: None,
				timeout_secs: 60,
			},
		)
		.unwrap();
		assert_eq!(
			conn.query_url(),
			"http://localhost:8123/?default_format=JSONEachRow"
		);
	}

	#[test]
	fn query_url_with_database() {
		let conn = ClickHouseConnector::new(
			"test",
			ClickHouseConfig {
				url: "http://localhost:8123".into(),
				user: "default".into(),
				password: None,
				database: Some("analytics".into()),
				timeout_secs: 60,
			},
		)
		.unwrap();
		assert!(conn.query_url().contains("database=analytics"));
	}
}
