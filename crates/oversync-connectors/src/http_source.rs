use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

pub use crate::http_common::AuthConfig;
use crate::http_common::{extract_cursor, extract_items, items_to_rows};

#[derive(Debug, Clone, Deserialize)]
pub struct HttpSourceConfig {
	#[serde(rename = "dsn")]
	pub base_url: String,
	#[serde(default)]
	pub headers: HashMap<String, String>,
	#[serde(default)]
	pub auth: Option<AuthConfig>,
	#[serde(default)]
	pub pagination: Option<PaginationConfig>,
	#[serde(default)]
	pub response_path: Option<String>,
	#[serde(default = "default_timeout")]
	pub timeout_secs: u64,
}

fn default_timeout() -> u64 {
	30
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationConfig {
	Offset {
		page_size: usize,
		#[serde(default = "default_limit_param")]
		limit_param: String,
		#[serde(default = "default_offset_param")]
		offset_param: String,
	},
	Cursor {
		page_size: usize,
		#[serde(default = "default_cursor_param")]
		cursor_param: String,
		cursor_path: String,
	},
}

fn default_limit_param() -> String {
	"limit".into()
}
fn default_offset_param() -> String {
	"offset".into()
}
fn default_cursor_param() -> String {
	"cursor".into()
}

pub struct HttpSource {
	client: reqwest::Client,
	config: HttpSourceConfig,
	name: String,
}

impl HttpSource {
	pub fn new(name: &str, config: HttpSourceConfig) -> Result<Self, OversyncError> {
		let client = reqwest::Client::builder()
			.timeout(Duration::from_secs(config.timeout_secs))
			.build()
			.map_err(|e| OversyncError::Connector(format!("http client: {e}")))?;

		Ok(Self {
			client,
			config,
			name: name.to_string(),
		})
	}

	fn build_url(&self, path: &str) -> String {
		format!(
			"{}{}",
			self.config.base_url.trim_end_matches('/'),
			path
		)
	}

	fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
		crate::http_common::apply_auth(req, &self.config.auth)
	}

	fn apply_headers(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
		for (k, v) in &self.config.headers {
			req = req.header(k, v);
		}
		req
	}

	async fn request_page(
		&self,
		path: &str,
		extra_params: &[(String, String)],
	) -> Result<serde_json::Value, OversyncError> {
		let url = self.build_url(path);
		let mut req = self.client.get(&url);
		req = self.apply_headers(req);
		req = self.apply_auth(req);
		if !extra_params.is_empty() {
			req = req.query(extra_params);
		}

		let resp = req
			.send()
			.await
			.map_err(|e| OversyncError::Connector(format!("http: {e}")))?;

		let status = resp.status();
		if !status.is_success() {
			return Err(OversyncError::Connector(format!("http {status}: {url}")));
		}

		resp.json()
			.await
			.map_err(|e| OversyncError::Connector(format!("json decode: {e}")))
	}
}

#[async_trait]
impl OriginConnector for HttpSource {
	fn name(&self) -> &str {
		&self.name
	}

	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		let pagination = self.config.pagination.clone();
		match pagination {
			None => {
				let body = self.request_page(sql, &[]).await?;
				let items = extract_items(&body, &self.config.response_path);
				debug!(count = items.len(), "fetched items from http");
				items_to_rows(&items, key_column)
			}
			Some(PaginationConfig::Offset {
				page_size,
				ref limit_param,
				ref offset_param,
			}) => {
				let mut all_rows = Vec::new();
				let mut offset = 0usize;
				loop {
					let params = vec![
						(limit_param.clone(), page_size.to_string()),
						(offset_param.clone(), offset.to_string()),
					];
					let body = self.request_page(sql, &params).await?;
					let items = extract_items(&body, &self.config.response_path);
					if items.is_empty() {
						break;
					}
					let count = items.len();
					all_rows.extend(items_to_rows(&items, key_column)?);
					if count < page_size {
						break;
					}
					offset += count;
				}
				debug!(count = all_rows.len(), "fetched all pages from http (offset)");
				Ok(all_rows)
			}
			Some(PaginationConfig::Cursor {
				page_size: _,
				ref cursor_param,
				ref cursor_path,
			}) => {
				let mut all_rows = Vec::new();
				let mut cursor: Option<String> = None;
				loop {
					let mut params = Vec::new();
					if let Some(ref c) = cursor {
						params.push((cursor_param.clone(), c.clone()));
					}
					let body = self.request_page(sql, &params).await?;
					let items = extract_items(&body, &self.config.response_path);
					if items.is_empty() {
						break;
					}
					all_rows.extend(items_to_rows(&items, key_column)?);
					cursor = extract_cursor(&body, cursor_path);
					if cursor.is_none() {
						break;
					}
				}
				debug!(count = all_rows.len(), "fetched all pages from http (cursor)");
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
		let pagination = self.config.pagination.clone();
		match pagination {
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
			Some(PaginationConfig::Offset {
				page_size,
				ref limit_param,
				ref offset_param,
			}) => {
				let mut total = 0usize;
				let mut offset = 0usize;
				loop {
					let params = vec![
						(limit_param.clone(), page_size.to_string()),
						(offset_param.clone(), offset.to_string()),
					];
					let body = self.request_page(sql, &params).await?;
					let items = extract_items(&body, &self.config.response_path);
					if items.is_empty() {
						break;
					}
					let count = items.len();
					let rows = items_to_rows(&items, key_column)?;
					total += rows.len();
					tx.send(rows)
						.await
						.map_err(|_| OversyncError::Internal("channel closed".into()))?;
					if count < page_size {
						break;
					}
					offset += count;
				}
				Ok(total)
			}
			Some(PaginationConfig::Cursor {
				page_size: _,
				ref cursor_param,
				ref cursor_path,
			}) => {
				let mut total = 0usize;
				let mut cursor: Option<String> = None;
				loop {
					let mut params = Vec::new();
					if let Some(ref c) = cursor {
						params.push((cursor_param.clone(), c.clone()));
					}
					let body = self.request_page(sql, &params).await?;
					let items = extract_items(&body, &self.config.response_path);
					if items.is_empty() {
						break;
					}
					let rows = items_to_rows(&items, key_column)?;
					total += rows.len();
					tx.send(rows)
						.await
						.map_err(|_| OversyncError::Internal("channel closed".into()))?;
					cursor = extract_cursor(&body, cursor_path);
					if cursor.is_none() {
						break;
					}
				}
				Ok(total)
			}
		}
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		let mut req = self.client.get(&self.config.base_url);
		req = self.apply_headers(req);
		req = self.apply_auth(req);
		let resp = req
			.send()
			.await
			.map_err(|e| OversyncError::Connector(format!("http test: {e}")))?;
		if !resp.status().is_success() {
			return Err(OversyncError::Connector(format!(
				"http test: status {}",
				resp.status()
			)));
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_config_minimal() {
		let json = serde_json::json!({
			"dsn": "https://api.example.com"
		});
		let config: HttpSourceConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.base_url, "https://api.example.com");
		assert!(config.headers.is_empty());
		assert!(config.auth.is_none());
		assert!(config.pagination.is_none());
		assert!(config.response_path.is_none());
		assert_eq!(config.timeout_secs, 30);
	}

	#[test]
	fn parse_config_full() {
		let json = serde_json::json!({
			"dsn": "https://api.example.com",
			"headers": {"Accept": "application/json", "X-Custom": "val"},
			"auth": {"type": "bearer", "token": "sk-123"},
			"pagination": {"type": "offset", "page_size": 50, "limit_param": "per_page", "offset_param": "page"},
			"response_path": "data.items",
			"timeout_secs": 60
		});
		let config: HttpSourceConfig = serde_json::from_value(json).unwrap();
		assert_eq!(config.base_url, "https://api.example.com");
		assert_eq!(config.headers.len(), 2);
		assert!(matches!(config.auth, Some(AuthConfig::Bearer { ref token }) if token == "sk-123"));
		assert_eq!(config.timeout_secs, 60);
		assert_eq!(config.response_path.as_deref(), Some("data.items"));
	}

	#[test]
	fn parse_auth_basic() {
		let json = serde_json::json!({"type": "basic", "username": "user", "password": "pass"});
		let auth: AuthConfig = serde_json::from_value(json).unwrap();
		assert!(matches!(auth, AuthConfig::Basic { ref username, ref password } if username == "user" && password == "pass"));
	}

	#[test]
	fn parse_auth_header() {
		let json = serde_json::json!({"type": "header", "name": "X-API-Key", "value": "key123"});
		let auth: AuthConfig = serde_json::from_value(json).unwrap();
		assert!(matches!(auth, AuthConfig::Header { ref name, ref value } if name == "X-API-Key" && value == "key123"));
	}

	#[test]
	fn parse_pagination_offset_defaults() {
		let json = serde_json::json!({"type": "offset", "page_size": 100});
		let pg: PaginationConfig = serde_json::from_value(json).unwrap();
		match pg {
			PaginationConfig::Offset {
				page_size,
				limit_param,
				offset_param,
			} => {
				assert_eq!(page_size, 100);
				assert_eq!(limit_param, "limit");
				assert_eq!(offset_param, "offset");
			}
			_ => panic!("expected Offset"),
		}
	}

	#[test]
	fn parse_pagination_cursor() {
		let json = serde_json::json!({
			"type": "cursor",
			"page_size": 50,
			"cursor_param": "after",
			"cursor_path": "meta.next_cursor"
		});
		let pg: PaginationConfig = serde_json::from_value(json).unwrap();
		match pg {
			PaginationConfig::Cursor {
				page_size,
				cursor_param,
				cursor_path,
			} => {
				assert_eq!(page_size, 50);
				assert_eq!(cursor_param, "after");
				assert_eq!(cursor_path, "meta.next_cursor");
			}
			_ => panic!("expected Cursor"),
		}
	}

	#[test]
	fn build_url_joins_path() {
		let config = make_config("https://api.example.com");
		let source = HttpSource::new("test", config).unwrap();
		assert_eq!(source.build_url("/v1/items"), "https://api.example.com/v1/items");
	}

	#[test]
	fn build_url_strips_trailing_slash() {
		let config = make_config("https://api.example.com/");
		let source = HttpSource::new("test", config).unwrap();
		assert_eq!(source.build_url("/v1/items"), "https://api.example.com/v1/items");
	}

	#[test]
	fn extract_items_top_level_array() {
		let body = serde_json::json!([{"id": 1}, {"id": 2}]);
		let items = extract_items(&body, &None);
		assert_eq!(items.len(), 2);
	}

	#[test]
	fn extract_items_nested_path() {
		let body = serde_json::json!({"data": {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}});
		let items = extract_items(&body, &Some("data.items".into()));
		assert_eq!(items.len(), 3);
	}

	#[test]
	fn extract_items_single_level_path() {
		let body = serde_json::json!({"results": [{"id": "a"}]});
		let items = extract_items(&body, &Some("results".into()));
		assert_eq!(items.len(), 1);
	}

	#[test]
	fn extract_items_empty_path_is_top_level() {
		let body = serde_json::json!([{"id": 1}]);
		let items = extract_items(&body, &Some("".into()));
		assert_eq!(items.len(), 1);
	}

	#[test]
	fn extract_items_invalid_path_returns_empty() {
		let body = serde_json::json!({"data": [{"id": 1}]});
		let items = extract_items(&body, &Some("nonexistent.path".into()));
		assert!(items.is_empty());
	}

	#[test]
	fn extract_items_non_array_returns_empty() {
		let body = serde_json::json!({"data": "not an array"});
		let items = extract_items(&body, &Some("data".into()));
		assert!(items.is_empty());
	}

	#[test]
	fn items_to_rows_string_key() {
		let items = vec![
			serde_json::json!({"id": "abc", "name": "Alpha"}),
			serde_json::json!({"id": "def", "name": "Beta"}),
		];
		let rows = items_to_rows(&items, "id").unwrap();
		assert_eq!(rows.len(), 2);
		assert_eq!(rows[0].row_key, "abc");
		assert_eq!(rows[1].row_key, "def");
	}

	#[test]
	fn items_to_rows_numeric_key() {
		let items = vec![serde_json::json!({"id": 42, "val": "x"})];
		let rows = items_to_rows(&items, "id").unwrap();
		assert_eq!(rows[0].row_key, "42");
	}

	#[test]
	fn items_to_rows_missing_key_errors() {
		let items = vec![serde_json::json!({"name": "no id field"})];
		let result = items_to_rows(&items, "id");
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("missing key field"));
	}

	#[test]
	fn items_to_rows_preserves_full_data() {
		let item = serde_json::json!({"id": "k1", "a": 1, "b": "two"});
		let rows = items_to_rows(&[item.clone()], "id").unwrap();
		assert_eq!(rows[0].row_data, item);
	}

	#[test]
	fn extract_cursor_string() {
		let body = serde_json::json!({"meta": {"next": "cursor_abc"}});
		assert_eq!(extract_cursor(&body, "meta.next"), Some("cursor_abc".into()));
	}

	#[test]
	fn extract_cursor_number() {
		let body = serde_json::json!({"next_page": 5});
		assert_eq!(extract_cursor(&body, "next_page"), Some("5".into()));
	}

	#[test]
	fn extract_cursor_null_returns_none() {
		let body = serde_json::json!({"meta": {"next": null}});
		assert_eq!(extract_cursor(&body, "meta.next"), None);
	}

	#[test]
	fn extract_cursor_missing_returns_none() {
		let body = serde_json::json!({"data": []});
		assert_eq!(extract_cursor(&body, "meta.next"), None);
	}

	fn make_config(base_url: &str) -> HttpSourceConfig {
		serde_json::from_value(serde_json::json!({"dsn": base_url})).unwrap()
	}
}
