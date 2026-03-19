use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use tracing::warn;

use oversync_core::error::OversyncError;
use oversync_core::model::{AuthConfig, EventEnvelope};
use oversync_core::traits::Sink;

pub struct HttpSink {
	client: reqwest::Client,
	url: String,
	method: reqwest::Method,
	sink_name: String,
	retry_count: u32,
	headers: HashMap<String, String>,
	auth: Option<AuthConfig>,
}

impl HttpSink {
	pub fn new(
		name: &str,
		url: &str,
		method: reqwest::Method,
		headers: HashMap<String, String>,
		auth: Option<AuthConfig>,
		timeout_secs: u64,
		retry_count: u32,
	) -> Result<Self, OversyncError> {
		let client = reqwest::Client::builder()
			.timeout(Duration::from_secs(timeout_secs))
			.build()
			.map_err(|e| OversyncError::Sink(format!("http client: {e}")))?;

		Ok(Self {
			client,
			url: url.to_string(),
			method,
			sink_name: name.to_string(),
			retry_count,
			headers,
			auth,
		})
	}

	fn apply_request_config(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
		for (k, v) in &self.headers {
			req = req.header(k, v);
		}
		match &self.auth {
			Some(AuthConfig::Bearer { token }) => req.bearer_auth(token),
			Some(AuthConfig::Header { name, value }) => req.header(name, value),
			Some(AuthConfig::Basic { username, password }) => {
				req.basic_auth(username, Some(password))
			}
			None => req,
		}
	}

	async fn send_with_retry(&self, body: serde_json::Value) -> Result<(), OversyncError> {
		for attempt in 0..=self.retry_count {
			let req = self.client.request(self.method.clone(), &self.url);
			let req = self.apply_request_config(req).json(&body);
			match req.send().await {
				Ok(resp) => {
					let status = resp.status();
					if status.is_success() {
						return Ok(());
					}
					if matches!(status.as_u16(), 429 | 502 | 503 | 504)
						&& attempt < self.retry_count
					{
						let delay = Duration::from_secs((1u64 << attempt.min(6)).min(60));
						warn!(
							sink = %self.sink_name,
							status = %status,
							attempt = attempt + 1,
							delay_secs = delay.as_secs(),
							"retryable HTTP status, backing off"
						);
						tokio::time::sleep(delay).await;
						continue;
					}
					return Err(OversyncError::Sink(format!(
						"http sink: {status} from {}",
						self.url
					)));
				}
				Err(e) => {
					if attempt < self.retry_count {
						let delay = Duration::from_secs((1u64 << attempt.min(6)).min(60));
						warn!(
							sink = %self.sink_name,
							error = %e,
							attempt = attempt + 1,
							"HTTP request failed, retrying"
						);
						tokio::time::sleep(delay).await;
						continue;
					}
					return Err(OversyncError::Sink(format!("http sink: {e}")));
				}
			}
		}
		unreachable!()
	}
}

#[async_trait]
impl Sink for HttpSink {
	fn name(&self) -> &str {
		&self.sink_name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let body = serde_json::to_value(envelope)
			.map_err(|e| OversyncError::Sink(format!("serialize: {e}")))?;
		self.send_with_retry(body).await
	}

	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		if envelopes.is_empty() {
			return Ok(());
		}
		let body = serde_json::to_value(envelopes)
			.map_err(|e| OversyncError::Sink(format!("serialize: {e}")))?;
		self.send_with_retry(body).await
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		let req = self.client.head(&self.url);
		let req = self.apply_request_config(req);
		let resp = req
			.send()
			.await
			.map_err(|e| OversyncError::Sink(format!("http test: {e}")))?;
		if !resp.status().is_success() {
			return Err(OversyncError::Sink(format!(
				"http test: status {}",
				resp.status()
			)));
		}
		Ok(())
	}
}
