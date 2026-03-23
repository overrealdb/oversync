use std::process::Stdio;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{debug, info};

use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;
use oversync_core::traits::OriginConnector;

#[derive(Debug, Clone, Deserialize)]
pub struct McpConfig {
	pub dsn: String,
	#[serde(default)]
	pub args: Vec<String>,
	#[serde(default = "default_key_field")]
	pub key_field: String,
	#[serde(default)]
	pub response_path: Option<String>,
}

fn default_key_field() -> String {
	"id".into()
}

pub struct McpOriginConnector {
	name: String,
	config: McpConfig,
	process: Mutex<Option<McpProcess>>,
}

struct McpProcess {
	child: Child,
	stdin: tokio::process::ChildStdin,
	reader: BufReader<tokio::process::ChildStdout>,
	next_id: u64,
}

impl McpOriginConnector {
	pub fn new(name: &str, config: McpConfig) -> Self {
		Self {
			name: name.to_string(),
			config,
			process: Mutex::new(None),
		}
	}

	async fn ensure_connected(&self) -> Result<(), OversyncError> {
		let mut proc = self.process.lock().await;
		if proc.is_some() {
			return Ok(());
		}

		let command = &self.config.dsn;
		let mut child = Command::new(command)
			.args(&self.config.args)
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::null())
			.spawn()
			.map_err(|e| OversyncError::Connector(format!("mcp spawn '{command}': {e}")))?;

		let stdin = child
			.stdin
			.take()
			.ok_or_else(|| OversyncError::Connector("mcp: failed to capture stdin".into()))?;
		let stdout = child
			.stdout
			.take()
			.ok_or_else(|| OversyncError::Connector("mcp: failed to capture stdout".into()))?;
		let reader = BufReader::new(stdout);

		let mut mcp_proc = McpProcess {
			child,
			stdin,
			reader,
			next_id: 1,
		};

		// MCP handshake: initialize → read response → send initialized notification
		let init_req = serde_json::json!({
			"jsonrpc": "2.0",
			"id": mcp_proc.next_id,
			"method": "initialize",
			"params": {
				"protocolVersion": "2024-11-05",
				"capabilities": {},
				"clientInfo": {
					"name": "oversync",
					"version": env!("CARGO_PKG_VERSION")
				}
			}
		});
		mcp_proc.next_id += 1;

		send_message(&mut mcp_proc.stdin, &init_req).await?;
		let _init_resp = read_message(&mut mcp_proc.reader).await?;

		let initialized = serde_json::json!({
			"jsonrpc": "2.0",
			"method": "notifications/initialized"
		});
		send_message(&mut mcp_proc.stdin, &initialized).await?;

		info!(connector = %self.name, command = %command, "MCP server connected");
		*proc = Some(mcp_proc);
		Ok(())
	}
}

#[async_trait]
impl OriginConnector for McpOriginConnector {
	fn name(&self) -> &str {
		&self.name
	}

	async fn fetch_all(
		&self,
		tool_name: &str,
		key_column: &str,
	) -> Result<Vec<RawRow>, OversyncError> {
		self.ensure_connected().await?;
		let mut proc_guard = self.process.lock().await;
		let proc = proc_guard
			.as_mut()
			.ok_or_else(|| OversyncError::Connector("mcp: not connected".into()))?;

		// Parse tool_name as "tool_name" or "tool_name:arg1=val1,arg2=val2"
		let (tool, arguments) = parse_tool_call(tool_name)?;

		let call_req = serde_json::json!({
			"jsonrpc": "2.0",
			"id": proc.next_id,
			"method": "tools/call",
			"params": {
				"name": tool,
				"arguments": arguments
			}
		});
		proc.next_id += 1;

		send_message(&mut proc.stdin, &call_req).await?;
		let response = read_message(&mut proc.reader).await?;

		debug!(connector = %self.name, tool = %tool, "MCP tool call response received");

		let result = response.get("result").ok_or_else(|| {
			let error = response
				.get("error")
				.cloned()
				.unwrap_or(serde_json::Value::Null);
			OversyncError::Connector(format!("mcp tool call failed: {error}"))
		})?;

		// Extract content from MCP tool result
		let data = extract_content_data(result, self.config.response_path.as_deref())?;

		let key_field = if key_column.is_empty() {
			&self.config.key_field
		} else {
			key_column
		};

		let rows = data_to_rows(&data, key_field)?;
		Ok(rows)
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.ensure_connected().await
	}
}

impl Drop for McpOriginConnector {
	fn drop(&mut self) {
		if let Ok(mut guard) = self.process.try_lock()
			&& let Some(mut proc) = guard.take()
		{
			let _ = proc.child.start_kill();
		}
	}
}

/// Parse "tool_name" or "tool_name:key1=val1,key2=val2" into (name, arguments).
fn parse_tool_call(spec: &str) -> Result<(&str, serde_json::Value), OversyncError> {
	if let Some((name, args_str)) = spec.split_once(':') {
		let mut args = serde_json::Map::new();
		for pair in args_str.split(',') {
			if let Some((k, v)) = pair.split_once('=') {
				args.insert(
					k.trim().to_string(),
					serde_json::Value::String(v.trim().to_string()),
				);
			}
		}
		Ok((name, serde_json::Value::Object(args)))
	} else {
		Ok((spec, serde_json::json!({})))
	}
}

/// Extract data array from MCP tool result content.
fn extract_content_data(
	result: &serde_json::Value,
	response_path: Option<&str>,
) -> Result<Vec<serde_json::Value>, OversyncError> {
	// MCP tool results have content: [{type: "text", text: "..."}]
	let content = result
		.get("content")
		.and_then(|c| c.as_array())
		.ok_or_else(|| OversyncError::Connector("mcp: result missing 'content' array".into()))?;

	// Find the first text content block
	let text = content
		.iter()
		.find_map(|c| {
			if c.get("type").and_then(|t| t.as_str()) == Some("text") {
				c.get("text").and_then(|t| t.as_str())
			} else {
				None
			}
		})
		.ok_or_else(|| OversyncError::Connector("mcp: no text content in result".into()))?;

	let parsed: serde_json::Value = serde_json::from_str(text).map_err(|e| {
		OversyncError::Connector(format!("mcp: failed to parse text content as JSON: {e}"))
	})?;

	// Navigate to response_path if specified
	let target = match response_path {
		Some(path) => {
			let mut current = &parsed;
			for segment in path.split('.') {
				current = current.get(segment).ok_or_else(|| {
					OversyncError::Connector(format!(
						"mcp: response_path '{path}' not found at '{segment}'"
					))
				})?;
			}
			current.clone()
		}
		None => parsed,
	};

	match target {
		serde_json::Value::Array(arr) => Ok(arr),
		obj @ serde_json::Value::Object(_) => Ok(vec![obj]),
		other => Err(OversyncError::Connector(format!(
			"mcp: expected array or object, got {}",
			other_type_name(&other)
		))),
	}
}

fn other_type_name(v: &serde_json::Value) -> &'static str {
	match v {
		serde_json::Value::Null => "null",
		serde_json::Value::Bool(_) => "bool",
		serde_json::Value::Number(_) => "number",
		serde_json::Value::String(_) => "string",
		serde_json::Value::Array(_) => "array",
		serde_json::Value::Object(_) => "object",
	}
}

fn data_to_rows(data: &[serde_json::Value], key_field: &str) -> Result<Vec<RawRow>, OversyncError> {
	let mut rows = Vec::with_capacity(data.len());
	for (i, item) in data.iter().enumerate() {
		let key = match item.get(key_field) {
			Some(serde_json::Value::String(s)) => s.clone(),
			Some(other) => other.to_string(),
			None => {
				return Err(OversyncError::Connector(format!(
					"mcp: row {i} missing key field '{key_field}'"
				)));
			}
		};

		rows.push(RawRow {
			row_key: key,
			row_data: item.clone(),
		});
	}
	Ok(rows)
}

async fn send_message(
	stdin: &mut tokio::process::ChildStdin,
	msg: &serde_json::Value,
) -> Result<(), OversyncError> {
	let json = serde_json::to_string(msg)
		.map_err(|e| OversyncError::Connector(format!("mcp serialize: {e}")))?;
	stdin
		.write_all(json.as_bytes())
		.await
		.map_err(|e| OversyncError::Connector(format!("mcp write: {e}")))?;
	stdin
		.write_all(b"\n")
		.await
		.map_err(|e| OversyncError::Connector(format!("mcp write newline: {e}")))?;
	stdin
		.flush()
		.await
		.map_err(|e| OversyncError::Connector(format!("mcp flush: {e}")))?;
	Ok(())
}

async fn read_message(
	reader: &mut BufReader<tokio::process::ChildStdout>,
) -> Result<serde_json::Value, OversyncError> {
	let mut line = String::new();
	let n = tokio::time::timeout(std::time::Duration::from_secs(60), reader.read_line(&mut line))
		.await
		.map_err(|_| OversyncError::Connector("mcp read: timed out after 60s".into()))?
		.map_err(|e| OversyncError::Connector(format!("mcp read: {e}")))?;

	if n == 0 {
		return Err(OversyncError::Connector("mcp: server closed stdout".into()));
	}

	serde_json::from_str(line.trim())
		.map_err(|e| OversyncError::Connector(format!("mcp parse response: {e}")))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_tool_call_simple() {
		let (name, args) = parse_tool_call("list_repos").unwrap();
		assert_eq!(name, "list_repos");
		assert_eq!(args, serde_json::json!({}));
	}

	#[test]
	fn parse_tool_call_with_args() {
		let (name, args) = parse_tool_call("search:query=rust,limit=10").unwrap();
		assert_eq!(name, "search");
		assert_eq!(args["query"], "rust");
		assert_eq!(args["limit"], "10");
	}

	#[test]
	fn extract_content_text_array() {
		let result = serde_json::json!({
			"content": [
				{"type": "text", "text": "[{\"id\": 1, \"name\": \"alice\"}, {\"id\": 2, \"name\": \"bob\"}]"}
			]
		});
		let data = extract_content_data(&result, None).unwrap();
		assert_eq!(data.len(), 2);
		assert_eq!(data[0]["name"], "alice");
	}

	#[test]
	fn extract_content_with_response_path() {
		let result = serde_json::json!({
			"content": [
				{"type": "text", "text": "{\"data\": {\"items\": [{\"id\": 1}]}}"}
			]
		});
		let data = extract_content_data(&result, Some("data.items")).unwrap();
		assert_eq!(data.len(), 1);
		assert_eq!(data[0]["id"], 1);
	}

	#[test]
	fn extract_content_single_object() {
		let result = serde_json::json!({
			"content": [
				{"type": "text", "text": "{\"id\": 42, \"status\": \"ok\"}"}
			]
		});
		let data = extract_content_data(&result, None).unwrap();
		assert_eq!(data.len(), 1);
		assert_eq!(data[0]["id"], 42);
	}

	#[test]
	fn extract_content_no_text_block_errors() {
		let result = serde_json::json!({
			"content": [
				{"type": "image", "data": "..."}
			]
		});
		let err = extract_content_data(&result, None).unwrap_err();
		assert!(err.to_string().contains("no text content"));
	}

	#[test]
	fn extract_content_invalid_json_errors() {
		let result = serde_json::json!({
			"content": [
				{"type": "text", "text": "not json"}
			]
		});
		let err = extract_content_data(&result, None).unwrap_err();
		assert!(err.to_string().contains("parse text content"));
	}

	#[test]
	fn data_to_rows_uses_key_field() {
		let data = vec![
			serde_json::json!({"id": "abc", "name": "alice"}),
			serde_json::json!({"id": "def", "name": "bob"}),
		];
		let rows = data_to_rows(&data, "id").unwrap();
		assert_eq!(rows.len(), 2);
		assert_eq!(rows[0].row_key, "abc");
		assert_eq!(rows[1].row_key, "def");
	}

	#[test]
	fn data_to_rows_missing_key_errors() {
		let data = vec![
			serde_json::json!({"name": "alice"}),
			serde_json::json!({"name": "bob"}),
		];
		let err = data_to_rows(&data, "id").unwrap_err();
		assert!(err.to_string().contains("missing key field"));
	}

	#[test]
	fn data_to_rows_numeric_key() {
		let data = vec![serde_json::json!({"id": 42, "v": 1})];
		let rows = data_to_rows(&data, "id").unwrap();
		assert_eq!(rows[0].row_key, "42");
	}

	#[test]
	fn extract_content_bad_response_path_errors() {
		let result = serde_json::json!({
			"content": [
				{"type": "text", "text": "{\"x\": 1}"}
			]
		});
		let err = extract_content_data(&result, Some("missing.path")).unwrap_err();
		assert!(err.to_string().contains("not found"));
	}
}
