use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

use crate::http_common::{apply_auth, extract_items, items_to_rows, navigate_path, AuthConfig};

#[derive(Debug, Clone, Deserialize)]
pub struct GraphqlConfig {
	#[serde(rename = "dsn")]
	pub endpoint: String,
	#[serde(default)]
	pub headers: HashMap<String, String>,
	#[serde(default)]
	pub auth: Option<AuthConfig>,
	#[serde(default)]
	pub response_path: Option<String>,
	#[serde(default = "default_timeout")]
	pub timeout_secs: u64,
	#[serde(default)]
	pub pagination: Option<GraphqlPagination>,
}

fn default_timeout() -> u64 {
	30
}

#[derive(Debug, Clone, Deserialize)]
pub struct GraphqlPagination {
	#[serde(default = "default_cursor_variable")]
	pub cursor_variable: String,
	#[serde(default = "default_has_next_path")]
	pub has_next_path: String,
	#[serde(default = "default_end_cursor_path")]
	pub end_cursor_path: String,
}

fn default_cursor_variable() -> String {
	"cursor".into()
}
fn default_has_next_path() -> String {
	"pageInfo.hasNextPage".into()
}
fn default_end_cursor_path() -> String {
	"pageInfo.endCursor".into()
}

pub struct GraphqlConnector {
	client: reqwest::Client,
	config: GraphqlConfig,
	name: String,
}

impl GraphqlConnector {
	pub fn new(name: &str, config: GraphqlConfig) -> Result<Self, OversyncError> {
		let client = reqwest::Client::builder()
			.timeout(Duration::from_secs(config.timeout_secs))
			.build()
			.map_err(|e| OversyncError::Connector(format!("graphql client: {e}")))?;

		Ok(Self {
			client,
			config,
			name: name.to_string(),
		})
	}

	async fn execute_query(
		&self,
		query: &str,
		variables: serde_json::Value,
	) -> Result<serde_json::Value, OversyncError> {
		let body = serde_json::json!({
			"query": query,
			"variables": variables,
		});

		let mut req = self.client.post(&self.config.endpoint);
		req = apply_auth(req, &self.config.auth);
		for (k, v) in &self.config.headers {
			req = req.header(k, v);
		}
		req = req.json(&body);

		let resp = req
			.send()
			.await
			.map_err(|e| OversyncError::Connector(format!("graphql: {e}")))?;

		let status = resp.status();
		if !status.is_success() {
			return Err(OversyncError::Connector(format!(
				"graphql: HTTP {status}"
			)));
		}

		let json: serde_json::Value = resp
			.json()
			.await
			.map_err(|e| OversyncError::Connector(format!("graphql json: {e}")))?;

		if let Some(errors) = json.get("errors").and_then(|e| e.as_array()) {
			if !errors.is_empty() {
				let msg = errors
					.iter()
					.filter_map(|e| e.get("message").and_then(|m| m.as_str()))
					.collect::<Vec<_>>()
					.join("; ");
				return Err(OversyncError::Connector(format!(
					"graphql errors: {msg}"
				)));
			}
		}

		Ok(json)
	}

	fn fetch_page_info(
		&self,
		body: &serde_json::Value,
		pagination: &GraphqlPagination,
	) -> (bool, Option<String>) {
		// pageInfo is a sibling of the items array — navigate to parent of response_path
		let parent = match &self.config.response_path {
			Some(path) => match path.rsplit_once('.') {
				Some((parent_path, _)) => navigate_path(body, parent_path),
				None => body,
			},
			None => body,
		};

		let has_next = navigate_path(parent, &pagination.has_next_path)
			.as_bool()
			.unwrap_or(false);

		let end_cursor = match navigate_path(parent, &pagination.end_cursor_path) {
			serde_json::Value::String(s) => Some(s.clone()),
			_ => None,
		};

		(has_next, end_cursor)
	}

	fn make_variables(
		&self,
		pagination: &GraphqlPagination,
		cursor: &Option<String>,
	) -> serde_json::Value {
		let mut vars = serde_json::Map::new();
		match cursor {
			Some(c) => {
				vars.insert(
					pagination.cursor_variable.clone(),
					serde_json::Value::String(c.clone()),
				);
			}
			None => {
				vars.insert(
					pagination.cursor_variable.clone(),
					serde_json::Value::Null,
				);
			}
		}
		serde_json::Value::Object(vars)
	}
}

#[async_trait]
impl OriginConnector for GraphqlConnector {
	fn name(&self) -> &str {
		&self.name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		match &self.config.pagination {
			None => {
				let body = self
					.execute_query(sql, serde_json::json!({}))
					.await?;
				let items = extract_items(&body, &self.config.response_path);
				debug!(count = items.len(), "fetched items from graphql");
				items_to_rows(&items, key_column)
			}
			Some(pagination) => {
				let pagination = pagination.clone();
				let mut all_rows = Vec::new();
				let mut cursor: Option<String> = None;

				loop {
					let variables = self.make_variables(&pagination, &cursor);
					let body = self.execute_query(sql, variables).await?;
					let items = extract_items(&body, &self.config.response_path);
					if items.is_empty() {
						break;
					}
					all_rows.extend(items_to_rows(&items, key_column)?);

					let (has_next, end_cursor) =
						self.fetch_page_info(&body, &pagination);
					if !has_next {
						break;
					}
					cursor = end_cursor;
					if cursor.is_none() {
						break;
					}
				}

				debug!(count = all_rows.len(), "fetched all pages from graphql (relay)");
				Ok(all_rows)
			}
		}
	}

	async fn fetch_into(
		&self,
		sql: &str,
		key_column: &str,
		batch_size: usize,
		tx: mpsc::Sender<Vec<RawRow>>,
	) -> Result<usize, OversyncError> {
		match &self.config.pagination {
			None => {
				let all = self.fetch_all(sql, key_column).await?;
				let total = all.len();
				for chunk in all.chunks(batch_size) {
					tx.send(chunk.to_vec())
						.await
						.map_err(|_| OversyncError::Internal("channel closed".into()))?;
				}
				Ok(total)
			}
			Some(pagination) => {
				let pagination = pagination.clone();
				let mut total = 0usize;
				let mut cursor: Option<String> = None;

				loop {
					let variables = self.make_variables(&pagination, &cursor);
					let body = self.execute_query(sql, variables).await?;
					let items = extract_items(&body, &self.config.response_path);
					if items.is_empty() {
						break;
					}
					let rows = items_to_rows(&items, key_column)?;
					total += rows.len();
					tx.send(rows)
						.await
						.map_err(|_| OversyncError::Internal("channel closed".into()))?;

					let (has_next, end_cursor) =
						self.fetch_page_info(&body, &pagination);
					if !has_next {
						break;
					}
					cursor = end_cursor;
					if cursor.is_none() {
						break;
					}
				}
				Ok(total)
			}
		}
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.execute_query("{ __typename }", serde_json::json!({}))
			.await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_config_minimal() {
		let json = serde_json::json!({
			"dsn": "https://api.example.com/graphql"
		});
		let config: GraphqlConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.endpoint, "https://api.example.com/graphql");
		assert!(config.headers.is_empty());
		assert!(config.auth.is_none());
		assert!(config.pagination.is_none());
		assert!(config.response_path.is_none());
		assert_eq!(config.timeout_secs, 30);
	}

	#[test]
	fn parse_config_full() {
		let json = serde_json::json!({
			"dsn": "https://api.example.com/graphql",
			"headers": {"Accept": "application/json"},
			"auth": {"type": "bearer", "token": "gql-token"},
			"response_path": "data.items.nodes",
			"timeout_secs": 60,
			"pagination": {
				"cursor_variable": "after",
				"has_next_path": "pageInfo.hasNextPage",
				"end_cursor_path": "pageInfo.endCursor"
			}
		});
		let config: GraphqlConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.endpoint, "https://api.example.com/graphql");
		assert!(config.auth.is_some());
		let pg = config.pagination.unwrap();
		assert_eq!(pg.cursor_variable, "after");
	}

	#[test]
	fn pagination_defaults() {
		let json = serde_json::json!({});
		let pg: GraphqlPagination = serde_json::from_value(json).unwrap();
		assert_eq!(pg.cursor_variable, "cursor");
		assert_eq!(pg.has_next_path, "pageInfo.hasNextPage");
		assert_eq!(pg.end_cursor_path, "pageInfo.endCursor");
	}
}
