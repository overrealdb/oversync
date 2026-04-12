use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

use crate::generated::{
	Client as GeneratedClient, Error as GeneratedError, ResponseValue as GeneratedResponseValue,
	types as generated_types,
};
use crate::types::{
	CreateCredentialRequest, CreatePipePresetRequest, CreatePipeRequest, CreateSinkRequest,
	CredentialListResponse, ErrorResponse, ExportConfigFormat, ExportConfigResponse,
	HealthResponse, HistoryResponse, ImportConfigRequest, ImportConfigResponse, MutationResponse,
	PipeInfo, PipeListResponse, PipePresetInfo, PipePresetListResponse, PipeRunResponse, SinkInfo,
	SinkListResponse, StatusResponse, UpdatePipePresetRequest, UpdatePipeRequest,
	UpdateSinkRequest,
};

#[derive(Debug, Error)]
pub enum OversyncClientBuildError {
	#[error("invalid API key for Authorization header: {source}")]
	InvalidApiKeyHeader {
		#[source]
		source: reqwest::header::InvalidHeaderValue,
	},
	#[error("failed to build reqwest client: {0}")]
	Http(#[from] reqwest::Error),
}

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
	#[error("failed to convert {context} between stable and generated API types: {source}")]
	Conversion {
		context: String,
		#[source]
		source: serde_json::Error,
	},
	#[error("generated client contract error for {context}: {message}")]
	Contract { context: String, message: String },
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

	pub fn build(self) -> Result<OversyncClient, OversyncClientBuildError> {
		let mut builder = reqwest::Client::builder();
		if let Some(timeout) = self.timeout {
			builder = builder.timeout(timeout);
		}
		if let Some(api_key) = &self.api_key {
			let mut default_headers = HeaderMap::new();
			let mut auth = HeaderValue::from_str(&format!("Bearer {api_key}"))
				.map_err(|source| OversyncClientBuildError::InvalidApiKeyHeader { source })?;
			auth.set_sensitive(true);
			default_headers.insert(AUTHORIZATION, auth);
			builder = builder.default_headers(default_headers);
		}
		let client = builder.build()?;
		let generated = GeneratedClient::new_with_client(&self.base_url, client.clone());
		Ok(OversyncClient {
			client,
			generated,
			base_url: self.base_url,
		})
	}
}

#[derive(Clone)]
pub struct OversyncClient {
	client: reqwest::Client,
	generated: GeneratedClient,
	base_url: String,
}

impl OversyncClient {
	pub fn builder(base_url: impl Into<String>) -> OversyncClientBuilder {
		OversyncClientBuilder::new(base_url)
	}

	pub fn new(base_url: impl Into<String>) -> Result<Self, OversyncClientBuildError> {
		Self::builder(base_url).build()
	}

	pub fn with_api_key(
		base_url: impl Into<String>,
		api_key: impl Into<String>,
	) -> Result<Self, OversyncClientBuildError> {
		Self::builder(base_url).api_key(api_key).build()
	}

	pub fn base_url(&self) -> &str {
		&self.base_url
	}

	pub fn generated(&self) -> &GeneratedClient {
		&self.generated
	}

	pub async fn health(&self) -> Result<HealthResponse, OversyncClientError> {
		self.call_generated("/health", self.generated.health())
			.await
	}

	pub async fn list_sinks(&self) -> Result<SinkListResponse, OversyncClientError> {
		self.call_generated("/sinks", self.generated.list_sinks())
			.await
	}

	pub async fn get_sink(&self, name: &str) -> Result<SinkInfo, OversyncClientError> {
		self.list_sinks()
			.await?
			.sinks
			.into_iter()
			.find(|sink| sink.name == name)
			.ok_or_else(|| OversyncClientError::Api {
				url: self.url(&format!("/sinks/{name}")),
				status: StatusCode::NOT_FOUND,
				message: format!("sink not found: {name}"),
				body: None,
			})
	}

	pub async fn create_sink(
		&self,
		request: &CreateSinkRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		let body = convert_to_generated::<_, generated_types::CreateSinkRequest>(
			request,
			"create sink request",
		)?;
		self.call_generated("/sinks", self.generated.create_sink(&body))
			.await
	}

	pub async fn update_sink(
		&self,
		name: &str,
		request: &UpdateSinkRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		let body = convert_to_generated::<_, generated_types::UpdateSinkRequest>(
			request,
			"update sink request",
		)?;
		self.call_generated(
			&format!("/sinks/{name}"),
			self.generated.update_sink(name, &body),
		)
		.await
	}

	pub async fn delete_sink(&self, name: &str) -> Result<MutationResponse, OversyncClientError> {
		self.call_generated(&format!("/sinks/{name}"), self.generated.delete_sink(name))
			.await
	}

	pub async fn list_pipes(&self) -> Result<PipeListResponse, OversyncClientError> {
		self.call_generated("/pipes", self.generated.list_pipes())
			.await
	}

	pub async fn get_pipe(&self, name: &str) -> Result<PipeInfo, OversyncClientError> {
		self.call_generated(&format!("/pipes/{name}"), self.generated.get_pipe(name))
			.await
	}

	pub async fn create_pipe(
		&self,
		request: &CreatePipeRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		let body = convert_to_generated::<_, generated_types::CreatePipeRequest>(
			request,
			"create pipe request",
		)?;
		self.call_generated("/pipes", self.generated.create_pipe(&body))
			.await
	}

	pub async fn update_pipe(
		&self,
		name: &str,
		request: &UpdatePipeRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		self.send_json_manual(
			reqwest::Method::PUT,
			&format!("/pipes/{name}"),
			Some(request),
		)
		.await
	}

	pub async fn delete_pipe(&self, name: &str) -> Result<MutationResponse, OversyncClientError> {
		self.call_generated(&format!("/pipes/{name}"), self.generated.delete_pipe(name))
			.await
	}

	pub async fn run_pipe(&self, name: &str) -> Result<PipeRunResponse, OversyncClientError> {
		self.call_generated(&format!("/pipes/{name}/run"), self.generated.run_pipe(name))
			.await
	}

	pub async fn list_pipe_presets(&self) -> Result<PipePresetListResponse, OversyncClientError> {
		self.call_generated("/pipe-presets", self.generated.list_pipe_presets())
			.await
	}

	pub async fn get_pipe_preset(&self, name: &str) -> Result<PipePresetInfo, OversyncClientError> {
		self.call_generated(
			&format!("/pipe-presets/{name}"),
			self.generated.get_pipe_preset(name),
		)
		.await
	}

	pub async fn create_pipe_preset(
		&self,
		request: &CreatePipePresetRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		let body = convert_to_generated::<_, generated_types::CreatePipePresetRequest>(
			request,
			"create pipe preset request",
		)?;
		self.call_generated("/pipe-presets", self.generated.create_pipe_preset(&body))
			.await
	}

	pub async fn update_pipe_preset(
		&self,
		name: &str,
		request: &UpdatePipePresetRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		let body = convert_to_generated::<_, generated_types::UpdatePipePresetRequest>(
			request,
			"update pipe preset request",
		)?;
		self.call_generated(
			&format!("/pipe-presets/{name}"),
			self.generated.update_pipe_preset(name, &body),
		)
		.await
	}

	pub async fn delete_pipe_preset(
		&self,
		name: &str,
	) -> Result<MutationResponse, OversyncClientError> {
		self.call_generated(
			&format!("/pipe-presets/{name}"),
			self.generated.delete_pipe_preset(name),
		)
		.await
	}

	pub async fn pause_sync(&self) -> Result<MutationResponse, OversyncClientError> {
		self.call_generated("/sync/pause", self.generated.pause_sync())
			.await
	}

	pub async fn resume_sync(&self) -> Result<MutationResponse, OversyncClientError> {
		self.call_generated("/sync/resume", self.generated.resume_sync())
			.await
	}

	pub async fn sync_status(&self) -> Result<StatusResponse, OversyncClientError> {
		self.call_generated("/sync/status", self.generated.sync_status())
			.await
	}

	pub async fn history(&self) -> Result<HistoryResponse, OversyncClientError> {
		self.call_generated("/history", self.generated.get_history())
			.await
	}

	pub async fn export_config(
		&self,
		format: ExportConfigFormat,
	) -> Result<ExportConfigResponse, OversyncClientError> {
		let format = convert_to_generated::<_, generated_types::ExportConfigFormat>(
			&format,
			"export format",
		)?;
		self.call_generated("/config/export", self.generated.export_config(Some(format)))
			.await
	}

	pub async fn import_config(
		&self,
		request: &ImportConfigRequest,
	) -> Result<ImportConfigResponse, OversyncClientError> {
		let body = convert_to_generated::<_, generated_types::ImportConfigRequest>(
			request,
			"import config request",
		)?;
		self.call_generated("/config/import", self.generated.import_config(&body))
			.await
	}

	pub async fn list_credentials(&self) -> Result<CredentialListResponse, OversyncClientError> {
		self.call_generated("/credentials", self.generated.list_credentials())
			.await
	}

	pub async fn create_credential(
		&self,
		request: &CreateCredentialRequest,
	) -> Result<MutationResponse, OversyncClientError> {
		let body = convert_to_generated::<_, generated_types::CreateCredentialRequest>(
			request,
			"create credential request",
		)?;
		self.call_generated("/credentials", self.generated.create_credential(&body))
			.await
	}

	pub async fn delete_credential(
		&self,
		name: &str,
	) -> Result<MutationResponse, OversyncClientError> {
		self.call_generated(
			&format!("/credentials/{name}"),
			self.generated.delete_credential(name),
		)
		.await
	}

	async fn call_generated<T, U, E>(
		&self,
		path: &str,
		call: impl std::future::Future<Output = Result<GeneratedResponseValue<U>, GeneratedError<E>>>,
	) -> Result<T, OversyncClientError>
	where
		T: DeserializeOwned,
		U: Serialize,
		E: Serialize,
	{
		let response = call
			.await
			.map_err(|error| self.map_generated_error(path, error))?;
		convert_from_generated(
			response.into_inner(),
			&format!("generated response for {path}"),
		)
	}

	fn map_generated_error<E: Serialize>(
		&self,
		path: &str,
		error: GeneratedError<E>,
	) -> OversyncClientError {
		let url = self.url(path);
		match error {
			GeneratedError::CommunicationError(source)
			| GeneratedError::InvalidUpgrade(source)
			| GeneratedError::ResponseBodyError(source) => OversyncClientError::Transport { url, source },
			GeneratedError::ErrorResponse(response) => {
				let status = response.status();
				let (message, body) = error_payload_message(response.into_inner());
				OversyncClientError::Api {
					url,
					status,
					message,
					body,
				}
			}
			GeneratedError::UnexpectedResponse(response) => OversyncClientError::Api {
				url,
				status: response.status(),
				message: format!("unexpected response status {}", response.status()),
				body: None,
			},
			GeneratedError::InvalidResponsePayload(_, source) => OversyncClientError::Contract {
				context: url,
				message: format!("invalid response payload: {source}"),
			},
			GeneratedError::InvalidRequest(message) | GeneratedError::Custom(message) => {
				OversyncClientError::Contract {
					context: url,
					message,
				}
			}
		}
	}

	async fn send_json_manual<B, T>(
		&self,
		method: reqwest::Method,
		path: &str,
		body: Option<&B>,
	) -> Result<T, OversyncClientError>
	where
		B: Serialize + ?Sized,
		T: DeserializeOwned,
	{
		let url = self.url(path);
		let mut req = self.client.request(method, &url);
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

fn convert_to_generated<T, U>(value: &T, context: &str) -> Result<U, OversyncClientError>
where
	T: Serialize + ?Sized,
	U: DeserializeOwned,
{
	let json = serde_json::to_value(value).map_err(|source| OversyncClientError::Conversion {
		context: context.to_string(),
		source,
	})?;
	serde_json::from_value(json).map_err(|source| OversyncClientError::Conversion {
		context: context.to_string(),
		source,
	})
}

fn convert_from_generated<T, U>(value: U, context: &str) -> Result<T, OversyncClientError>
where
	U: Serialize,
	T: DeserializeOwned,
{
	let json = serde_json::to_value(value).map_err(|source| OversyncClientError::Conversion {
		context: context.to_string(),
		source,
	})?;
	serde_json::from_value(json).map_err(|source| OversyncClientError::Conversion {
		context: context.to_string(),
		source,
	})
}

fn error_payload_message<E: Serialize>(payload: E) -> (String, Option<String>) {
	match serde_json::to_value(&payload) {
		Ok(value) => {
			let body = serde_json::to_string(&value).ok();
			let message = value
				.get("error")
				.and_then(Value::as_str)
				.map(ToOwned::to_owned)
				.or_else(|| body.clone())
				.unwrap_or_else(|| "request failed".to_string());
			(message, body)
		}
		Err(_) => ("request failed".to_string(), None),
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
			.send_json_manual(reqwest::Method::PUT, "/pipes/demo", Some(&body))
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

	#[tokio::test]
	async fn get_sink_uses_list_surface() {
		let app = Router::new().route(
			"/sinks",
			get(|| async {
				Json(SinkListResponse {
					sinks: vec![SinkInfo {
						name: "stdout".into(),
						sink_type: "stdout".into(),
						config: None,
					}],
				})
			}),
		);

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let server = tokio::spawn(async move {
			axum::serve(listener, app).await.unwrap();
		});

		let client = OversyncClient::new(format!("http://{addr}")).unwrap();
		let sink = client.get_sink("stdout").await.unwrap();
		assert_eq!(sink.name, "stdout");

		server.abort();
	}
}
