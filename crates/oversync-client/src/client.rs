use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::types::{
	CreateCredentialRequest, CreatePipePresetRequest, CreatePipeRequest, CreateSinkRequest,
	CredentialListResponse, ErrorResponse, ExportConfigFormat, ExportConfigResponse,
	HealthResponse, HistoryResponse, ImportConfigRequest, ImportConfigResponse, MutationResponse,
	PipeInfo, PipeListResponse, PipePresetInfo, PipePresetListResponse, PipeRunResponse, SinkInfo,
	SinkListResponse, StatusResponse, UpdateCredentialRequest, UpdatePipePresetRequest,
	UpdatePipeRequest, UpdateSinkRequest,
};

#[derive(Debug, Error)]
pub enum OversyncClientError {
	#[error("request to {url} failed: {source}")]
	Transport {
		url: String,
		#[source]
		source: reqwest::Error,
	},
	#[error("request to {url} returned HTTP {status}: {message}")]
	Api {
		url: String,
		status: StatusCode,
		message: String,
		body: Option<String>,
	},
	#[error("failed to decode response from {url}: {source}")]
	Decode {
		url: String,
		#[source]
		source: reqwest::Error,
	},
}

#[derive(Debug, Clone)]
pub struct OversyncClientBuilder {
	base_url: String,
	api_key: Option<String>,
	timeout: Option<Duration>,
}

impl OversyncClientBuilder {
	pub fn new(base_url: impl Into<String>) -> Self {
		Self {
			base_url: normalize_base_url(base_url.into()),
			api_key: None,
			timeout: None,
		}
	}

	pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
		self.api_key = Some(api_key.into());
		self
	}

	pub fn timeout(mut self, timeout: Duration) -> Self {
		self.timeout = Some(timeout);
		self
	}

	pub fn build(self) -> Result<OversyncClient, reqwest::Error> {
		let mut client = reqwest::Client::builder();
		if let Some(timeout) = self.timeout {
			client = client.timeout(timeout);
		}
		Ok(OversyncClient {
			client: client.build()?,
			base_url: self.base_url,
			api_key: self.api_key,
		})
	}
}

#[derive(Clone)]
pub struct OversyncClient {
	client: reqwest::Client,
	base_url: String,
	api_key: Option<String>,
}

impl OversyncClient {
	pub fn builder(base_url: impl Into<String>) -> OversyncClientBuilder {
		OversyncClientBuilder::new(base_url)
	}

	pub fn new(base_url: impl Into<String>) -> Result<Self, reqwest::Error> {
		Self::builder(base_url).build()
	}

	pub fn with_api_key(
		base_url: impl Into<String>,
		api_key: impl Into<String>,
	) -> Result<Self, reqwest::Error> {
		Self::builder(base_url).api_key(api_key).build()
	}

	pub fn base_url(&self) -> &str {
		&self.base_url
	}

	pub async fn health(&self) -> Result<HealthResponse, OversyncClientError> {
		self.get("/health").await
	}

	pub async fn list_sinks(&self) -> Result<SinkListResponse, OversyncClientError> {
		self.get("/sinks").await
	}

	pub async fn get_sink(&self, name: &str) -> Result<SinkInfo, OversyncClientError> {
		self.get(&format!("/sinks/{name}")).await
	}

	pub async fn create_sink(
		&self,
		request: &CreateSinkRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::POST, "/sinks", Some(request)).await
	}

	pub async fn update_sink(
		&self,
		name: &str,
		request: &UpdateSinkRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::PUT, &format!("/sinks/{name}"), Some(request))
			.await
	}

	pub async fn delete_sink(&self, name: &str) -> Result<MutationResponse, OversyncClientError> {
		self.send_json::<(), MutationResponse>(Method::DELETE, &format!("/sinks/{name}"), None)
			.await
	}

	pub async fn list_pipes(&self) -> Result<PipeListResponse, OversyncClientError> {
		self.get("/pipes").await
	}

	pub async fn get_pipe(&self, name: &str) -> Result<PipeInfo, OversyncClientError> {
		self.get(&format!("/pipes/{name}")).await
	}

	pub async fn create_pipe(
		&self,
		request: &CreatePipeRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::POST, "/pipes", Some(request)).await
	}

	pub async fn update_pipe(
		&self,
		name: &str,
		request: &UpdatePipeRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::PUT, &format!("/pipes/{name}"), Some(request))
			.await
	}

	pub async fn delete_pipe(&self, name: &str) -> Result<MutationResponse, OversyncClientError> {
		self.send_json::<(), MutationResponse>(Method::DELETE, &format!("/pipes/{name}"), None)
			.await
	}

	pub async fn run_pipe(&self, name: &str) -> Result<PipeRunResponse, OversyncClientError> {
		self.send_json::<(), PipeRunResponse>(Method::POST, &format!("/pipes/{name}/run"), None)
			.await
	}

	pub async fn list_pipe_presets(&self) -> Result<PipePresetListResponse, OversyncClientError> {
		self.get("/pipe-presets").await
	}

	pub async fn get_pipe_preset(&self, name: &str) -> Result<PipePresetInfo, OversyncClientError> {
		self.get(&format!("/pipe-presets/{name}")).await
	}

	pub async fn create_pipe_preset(
		&self,
		request: &CreatePipePresetRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::POST, "/pipe-presets", Some(request))
			.await
	}

	pub async fn update_pipe_preset(
		&self,
		name: &str,
		request: &UpdatePipePresetRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::PUT, &format!("/pipe-presets/{name}"), Some(request))
			.await
	}

	pub async fn delete_pipe_preset(
		&self,
		name: &str,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json::<(), MutationResponse>(
			Method::DELETE,
			&format!("/pipe-presets/{name}"),
			None,
		)
		.await
	}

	pub async fn pause_sync(&self) -> Result<MutationResponse, OversyncClientError> {
		self.send_json::<(), MutationResponse>(Method::POST, "/sync/pause", None)
			.await
	}

	pub async fn resume_sync(&self) -> Result<MutationResponse, OversyncClientError> {
		self.send_json::<(), MutationResponse>(Method::POST, "/sync/resume", None)
			.await
	}

	pub async fn sync_status(&self) -> Result<StatusResponse, OversyncClientError> {
		self.get("/sync/status").await
	}

	pub async fn history(&self) -> Result<HistoryResponse, OversyncClientError> {
		self.get("/history").await
	}

	pub async fn export_config(
		&self,
		format: ExportConfigFormat,
	) -> Result<ExportConfigResponse, OversyncClientError> {
		let suffix = match format {
			ExportConfigFormat::Toml => "toml",
			ExportConfigFormat::Json => "json",
		};
		self.get(&format!("/config/export?format={suffix}")).await
	}

	pub async fn import_config(
		&self,
		request: &ImportConfigRequest,
	) -> Result<ImportConfigResponse, OversyncClientError> {
		self.send_json(Method::POST, "/config/import", Some(request))
			.await
	}

	pub async fn list_credentials(&self) -> Result<CredentialListResponse, OversyncClientError> {
		self.get("/credentials").await
	}

	pub async fn create_credential(
		&self,
		request: &CreateCredentialRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::POST, "/credentials", Some(request))
			.await
	}

	pub async fn update_credential(
		&self,
		name: &str,
		request: &UpdateCredentialRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json(Method::PUT, &format!("/credentials/{name}"), Some(request))
			.await
	}

	pub async fn delete_credential(
		&self,
		name: &str,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json::<(), MutationResponse>(
			Method::DELETE,
			&format!("/credentials/{name}"),
			None,
		)
		.await
	}

	async fn get<T>(&self, path: &str) -> Result<T, OversyncClientError>
	where
		T: DeserializeOwned,
	{
		self.send_json::<(), T>(Method::GET, path, None).await
	}

	async fn send_json<B, T>(
		&self,
		method: Method,
		path: &str,
		body: Option<&B>,
	) -> Result<T, OversyncClientError>
	where
		B: serde::Serialize + ?Sized,
		T: DeserializeOwned,
	{
		let url = self.url(path);
		let mut req = self.client.request(method, &url);
		if let Some(api_key) = &self.api_key {
			req = req.bearer_auth(api_key);
		}
		if let Some(body) = body {
			req = req.json(body);
		}

		let response = req
			.send()
			.await
			.map_err(|source| OversyncClientError::Transport {
				url: url.clone(),
				source,
			})?;

		let status = response.status();
		if !status.is_success() {
			let raw = response
				.text()
				.await
				.map_err(|source| OversyncClientError::Transport {
					url: url.clone(),
					source,
				})?;

			let message = serde_json::from_str::<ErrorResponse>(&raw)
				.map(|payload| payload.error)
				.unwrap_or_else(|_| raw.clone());

			return Err(OversyncClientError::Api {
				url,
				status,
				message,
				body: Some(raw),
			});
		}

		response
			.json::<T>()
			.await
			.map_err(|source| OversyncClientError::Decode { url, source })
	}

	fn url(&self, path: &str) -> String {
		format!("{}{}", self.base_url, normalize_path(path))
	}
}

fn normalize_base_url(base_url: String) -> String {
	base_url.trim_end_matches('/').to_string()
}

fn normalize_path(path: &str) -> String {
	if path.starts_with('/') {
		path.to_string()
	} else {
		format!("/{path}")
	}
}

#[cfg(test)]
mod tests {
	use axum::{
		Json, Router,
		routing::{get, post, put},
	};
	use serde_json::json;

	use super::*;

	#[tokio::test]
	async fn health_round_trips_json() {
		let app = Router::new().route(
			"/health",
			get(|| async {
				Json(HealthResponse {
					status: "ok".to_string(),
					version: "0.6.1".to_string(),
				})
			}),
		);

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let server = tokio::spawn(async move {
			axum::serve(listener, app).await.unwrap();
		});

		let client = OversyncClient::new(format!("http://{addr}")).unwrap();
		let health = client.health().await.unwrap();
		assert_eq!(health.status, "ok");
		assert_eq!(health.version, "0.6.1");

		server.abort();
	}

	#[tokio::test]
	async fn update_pipe_omits_unset_fields_and_keeps_null_recipe() {
		let app = Router::new().route(
			"/pipes/demo",
			put(|Json(value): Json<serde_json::Value>| async move { Json(value) }),
		);

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let server = tokio::spawn(async move {
			axum::serve(listener, app).await.unwrap();
		});

		let client = OversyncClient::new(format!("http://{addr}")).unwrap();
		let body = UpdatePipeRequest {
			origin_connector: None,
			origin_dsn: None,
			origin_credential: None,
			trino_url: None,
			origin_config: None,
			targets: None,
			schedule: None,
			delta: None,
			retry: None,
			recipe: Some(None),
			filters: None,
			transforms: None,
			links: None,
			queries: None,
			enabled: Some(true),
		};

		let echoed: serde_json::Value = client
			.send_json(Method::PUT, "/pipes/demo", Some(&body))
			.await
			.unwrap();
		assert_eq!(echoed, json!({"recipe": null, "enabled": true}));

		server.abort();
	}

	#[tokio::test]
	async fn client_uses_bearer_auth() {
		let app = Router::new().route(
			"/sync/pause",
			post(|headers: axum::http::HeaderMap| async move {
				let auth = headers
					.get(axum::http::header::AUTHORIZATION)
					.and_then(|value| value.to_str().ok())
					.unwrap_or_default()
					.to_string();
				Json(MutationResponse {
					ok: true,
					message: auth,
				})
			}),
		);

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let server = tokio::spawn(async move {
			axum::serve(listener, app).await.unwrap();
		});

		let client =
			OversyncClient::with_api_key(format!("http://{addr}"), "secret-token").unwrap();
		let response = client.pause_sync().await.unwrap();
		assert_eq!(response.message, "Bearer secret-token");

		server.abort();
	}
}
