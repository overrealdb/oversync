use std::process::Stdio;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{debug, info};

use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;

#[derive(Debug, Clone, Deserialize)]
pub struct McpSinkConfig {
	pub dsn: String,
	#[serde(default)]
	pub args: Vec<String>,
	pub tool_name: String,
}

pub struct McpSink {
	name: String,
	config: McpSinkConfig,
	process: Mutex<Option<McpSinkProcess>>,
}

struct McpSinkProcess {
	child: Child,
	stdin: tokio::process::ChildStdin,
	reader: BufReader<tokio::process::ChildStdout>,
	next_id: u64,
}

impl McpSink {
	pub fn new(name: &str, config: McpSinkConfig) -> Self {
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
			.map_err(|e| OversyncError::Sink(format!("mcp spawn '{command}': {e}")))?;

		let stdin = child.stdin.take().ok_or_else(|| {
			OversyncError::Sink("mcp sink: failed to capture stdin".into())
		})?;
		let stdout = child.stdout.take().ok_or_else(|| {
			OversyncError::Sink("mcp sink: failed to capture stdout".into())
		})?;
		let reader = BufReader::new(stdout);

		let mut mcp_proc = McpSinkProcess {
			child,
			stdin,
			reader,
			next_id: 1,
		};

		let init_req = serde_json::json!({
			"jsonrpc": "2.0",
			"id": mcp_proc.next_id,
			"method": "initialize",
			"params": {
				"protocolVersion": "2024-11-05",
				"capabilities": {},
				"clientInfo": {
					"name": "oversync-sink",
					"version": env!("CARGO_PKG_VERSION")
				}
			}
		});
		mcp_proc.next_id += 1;

		send(&mut mcp_proc.stdin, &init_req).await?;
		let _init_resp = recv(&mut mcp_proc.reader).await?;

		let initialized = serde_json::json!({
			"jsonrpc": "2.0",
			"method": "notifications/initialized"
		});
		send(&mut mcp_proc.stdin, &initialized).await?;

		info!(sink = %self.name, command = %command, "MCP sink connected");
		*proc = Some(mcp_proc);
		Ok(())
	}
}

#[async_trait]
impl Sink for McpSink {
	fn name(&self) -> &str {
		&self.name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.ensure_connected().await?;
		let mut proc_guard = self.process.lock().await;
		let proc = proc_guard.as_mut().ok_or_else(|| {
			OversyncError::Sink("mcp sink: not connected".into())
		})?;

		let call_req = serde_json::json!({
			"jsonrpc": "2.0",
			"id": proc.next_id,
			"method": "tools/call",
			"params": {
				"name": self.config.tool_name,
				"arguments": {
					"event": serde_json::to_value(envelope)
						.map_err(|e| OversyncError::Sink(format!("serialize event: {e}")))?
				}
			}
		});
		proc.next_id += 1;

		send(&mut proc.stdin, &call_req).await?;
		let response = recv(&mut proc.reader).await?;

		if response.get("error").is_some() {
			let error = response["error"].clone();
			return Err(OversyncError::Sink(format!("mcp tool call failed: {error}")));
		}

		debug!(sink = %self.name, tool = %self.config.tool_name, "MCP event delivered");
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.ensure_connected().await
	}
}

impl Drop for McpSink {
	fn drop(&mut self) {
		if let Ok(mut guard) = self.process.try_lock() {
			if let Some(mut proc) = guard.take() {
				let _ = proc.child.start_kill();
			}
		}
	}
}

async fn send(
	stdin: &mut tokio::process::ChildStdin,
	msg: &serde_json::Value,
) -> Result<(), OversyncError> {
	let json = serde_json::to_string(msg)
		.map_err(|e| OversyncError::Sink(format!("mcp serialize: {e}")))?;
	stdin.write_all(json.as_bytes()).await
		.map_err(|e| OversyncError::Sink(format!("mcp write: {e}")))?;
	stdin.write_all(b"\n").await
		.map_err(|e| OversyncError::Sink(format!("mcp write: {e}")))?;
	stdin.flush().await
		.map_err(|e| OversyncError::Sink(format!("mcp flush: {e}")))?;
	Ok(())
}

async fn recv(
	reader: &mut BufReader<tokio::process::ChildStdout>,
) -> Result<serde_json::Value, OversyncError> {
	let mut line = String::new();
	let n = reader.read_line(&mut line).await
		.map_err(|e| OversyncError::Sink(format!("mcp read: {e}")))?;
	if n == 0 {
		return Err(OversyncError::Sink("mcp sink: server closed stdout".into()));
	}
	serde_json::from_str(line.trim())
		.map_err(|e| OversyncError::Sink(format!("mcp parse: {e}")))
}
