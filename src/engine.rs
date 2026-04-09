use std::path::Path;
use std::sync::Arc;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::info;

use oversync_connectors::{
	ClickHouseOriginFactory, FlightSqlOriginFactory, GraphqlOriginFactory, HttpOriginFactory,
	KafkaOriginFactory, McpOriginFactory, MysqlOriginFactory, PostgresOriginFactory,
	SurrealDbOriginFactory, TrinoOriginFactory,
};
use oversync_core::error::OversyncError;
use oversync_core::traits::{OriginFactory, TargetFactory};
use oversync_delta::DeltaEngine;
use oversync_sinks::{
	ClickHouseTargetFactory, HttpTargetFactory, KafkaTargetFactory, McpTargetFactory,
	MysqlTargetFactory, PostgresTargetFactory, StdoutTargetFactory, SurrealDbTargetFactory,
};

use crate::config::{SurrealDbDef, SyncConfig};
use crate::lifecycle::LifecycleManager;
use crate::registry::PluginRegistry;

/// Builder for [`OversyncEngine`]. Created via [`OversyncEngine::builder()`].
///
/// Configures SurrealDB connections, registers connector/sink factories,
/// and controls schema application.
pub struct OversyncEngineBuilder {
	url: String,
	namespace: String,
	database: String,
	username: String,
	password: String,
	snapshot_url: Option<String>,
	snapshot_ns: Option<String>,
	snapshot_db: Option<String>,
	snapshot_username: Option<String>,
	snapshot_password: Option<String>,
	extra_sources: Vec<Box<dyn OriginFactory>>,
	extra_sinks: Vec<Box<dyn TargetFactory>>,
	skip_defaults: bool,
	skip_schema: bool,
	api_key: Option<String>,
}

/// High-level facade for the oversync data sync engine.
///
/// Encapsulates `DeltaEngine`, `LifecycleManager`, and `PluginRegistry`.
/// Use as an embedded library or as the core of a standalone binary.
///
/// # Example
///
/// ```rust,no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// use oversync::OversyncEngine;
///
/// let engine = OversyncEngine::builder("http://localhost:8000")
///     .namespace("myapp")
///     .credentials("root", "root")
///     .build()
///     .await?;
///
/// engine.start_from_toml(std::path::Path::new("oversync.toml")).await?;
/// // engine.pause().await;
/// // engine.resume().await?;
/// engine.shutdown().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct OversyncEngine {
	lifecycle: Arc<LifecycleManager>,
	state_client: Arc<Surreal<Any>>,
	health_cancel: tokio_util::sync::CancellationToken,
	surreal_def: SurrealDbDef,
	#[allow(dead_code)] // used by api_router() behind #[cfg(feature = "api")]
	api_key: Option<String>,
}

impl OversyncEngine {
	/// Create a new engine builder. The `surrealdb_url` is the connection URL
	/// for the state store (e.g. `"http://localhost:8000"` or `"mem://"`).
	pub fn builder(surrealdb_url: &str) -> OversyncEngineBuilder {
		OversyncEngineBuilder {
			url: surrealdb_url.to_string(),
			namespace: "oversync".into(),
			database: "sync".into(),
			username: String::new(),
			password: String::new(),
			snapshot_url: None,
			snapshot_ns: None,
			snapshot_db: None,
			snapshot_username: None,
			snapshot_password: None,
			extra_sources: vec![],
			extra_sinks: vec![],
			skip_defaults: false,
			skip_schema: false,
			api_key: None,
		}
	}

	/// Start syncing with the given config. Validates, resolves credentials, versions, then starts.
	pub async fn start(&self, mut config: SyncConfig) -> Result<(), OversyncError> {
		self.prepare_config(&mut config).await?;
		if let Err(e) =
			crate::config_version::save_version(&self.state_client, &config, "auto-save on start")
				.await
		{
			tracing::warn!(error = %e, "failed to save config version (non-fatal)");
		}
		self.lifecycle.start(config).await
	}

	/// Load config from SurrealDB tables and start.
	pub async fn start_from_db(&self) -> Result<(), OversyncError> {
		let mut config =
			crate::config_db::load_config_from_db(&self.state_client, &self.surreal_def).await?;
		self.prepare_config(&mut config).await?;
		self.lifecycle.start(config).await
	}

	/// Parse a TOML config file and start syncing.
	pub async fn start_from_toml(&self, path: &Path) -> Result<(), OversyncError> {
		let mut config = SyncConfig::from_file(path)?;
		self.prepare_config(&mut config).await?;
		self.lifecycle.start(config).await
	}

	async fn prepare_config(&self, config: &mut SyncConfig) -> Result<(), OversyncError> {
		let issues = crate::config::validate_config(config);
		for issue in &issues {
			match issue.severity {
				crate::config::Severity::Error => {
					tracing::error!(issue = %issue.message, "config validation error");
				}
				crate::config::Severity::Warning => {
					tracing::warn!(issue = %issue.message, "config validation warning");
				}
			}
		}
		if issues
			.iter()
			.any(|i| i.severity == crate::config::Severity::Error)
		{
			return Err(OversyncError::Config(format!(
				"config validation failed: {}",
				issues
					.iter()
					.filter(|i| i.severity == crate::config::Severity::Error)
					.map(|i| i.message.as_str())
					.collect::<Vec<_>>()
					.join("; ")
			)));
		}
		self.resolve_credentials(config).await
	}

	async fn resolve_credentials(&self, config: &mut SyncConfig) -> Result<(), OversyncError> {
		let has_credentials = config.pipes.iter().any(|p| p.origin.credential.is_some());
		if !has_credentials {
			return Ok(());
		}
		let cred_key = std::env::var("OVERSYNC_CREDENTIAL_KEY").map_err(|_| {
			OversyncError::Config(
				"OVERSYNC_CREDENTIAL_KEY env var is required when pipes reference credentials. \
				 Set it to a strong passphrase (32+ chars)."
					.into(),
			)
		})?;
		let store = crate::credential::AesGcmStore::from_passphrase(&cred_key);
		crate::credential::resolve_pipe_credentials(&mut config.pipes, &self.state_client, &store)
			.await
	}

	/// Stop all sync tasks and clear config.
	pub async fn shutdown(&self) {
		self.health_cancel.cancel();
		self.lifecycle.shutdown().await;
	}

	/// Pause sync — stops the scheduler but remembers config for resume.
	pub async fn pause(&self) {
		self.lifecycle.pause().await;
	}

	/// Resume sync with the previously stored config.
	pub async fn resume(&self) -> Result<(), OversyncError> {
		self.lifecycle.resume().await
	}

	pub async fn is_running(&self) -> bool {
		self.lifecycle.is_running().await
	}

	pub async fn is_paused(&self) -> bool {
		self.lifecycle.is_paused().await
	}

	pub async fn current_config(&self) -> Option<SyncConfig> {
		self.lifecycle.current_config().await
	}

	pub fn state_client(&self) -> &Arc<Surreal<Any>> {
		&self.state_client
	}

	pub fn surreal_def(&self) -> &SurrealDbDef {
		&self.surreal_def
	}

	/// Build an axum `Router` with the full oversync REST API.
	/// Requires the `api` feature. Mount into your own axum app or serve standalone.
	#[cfg(feature = "api")]
	pub async fn api_router(&self) -> Result<axum::Router, OversyncError> {
		use std::collections::HashMap;
		use tokio::sync::RwLock;

		let lifecycle_adapter = LifecycleAdapter {
			lifecycle: Arc::clone(&self.lifecycle),
			surreal_def: self.surreal_def.clone(),
		};

		let api_state = Arc::new(oversync_api::state::ApiState {
			sources: Arc::new(RwLock::new(vec![])),
			sinks: Arc::new(RwLock::new(vec![])),
			pipes: Arc::new(RwLock::new(vec![])),
			cycle_status: Arc::new(RwLock::new(HashMap::new())),
			db_client: Some(self.state_client.clone()),
			lifecycle: Some(Arc::new(lifecycle_adapter)),
			api_key: self.api_key.clone(),
		});

		// Load initial cache from DB so history/sources/sinks are populated
		oversync_api::mutations::refresh_read_cache(&api_state).await?;

		// Install prometheus metrics exporter
		let prom_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
			.install_recorder()
			.ok();

		let base = oversync_api::router(api_state.clone());

		let registry = crate::engine::default_registry();
		let dry_run_state = Arc::new(DryRunState { registry });

		let credential_store = credential_store_from_passphrase(
			std::env::var("OVERSYNC_CREDENTIAL_KEY").ok().as_deref(),
			"credential API routes",
		)?;
		let cred_state = Arc::new(CredentialState {
			store: credential_store,
			db: self.state_client.clone(),
		});

		// Protected routes — require API key when configured
		let engine_protected = axum::Router::new()
			.route(
				"/pipes/dry-run",
				axum::routing::post(dry_run_handler).with_state(dry_run_state),
			)
			.route(
				"/credentials",
				axum::routing::get(list_credentials)
					.post(create_credential)
					.with_state(cred_state.clone()),
			)
			.route(
				"/credentials/{name}",
				axum::routing::delete(delete_credential).with_state(cred_state),
			)
			.route(
				"/config/versions",
				axum::routing::get(list_config_versions).with_state(self.state_client.clone()),
			)
			.route_layer(axum::middleware::from_fn_with_state(
				api_state.clone(),
				oversync_api::auth::require_api_key,
			));

		// Public route — no auth
		Ok(base.merge(engine_protected).route(
			"/metrics",
			axum::routing::get(move || async move {
				match prom_handle {
					Some(ref h) => axum::response::Response::builder()
						.header("content-type", "text/plain; version=0.0.4")
						.body(axum::body::Body::from(h.render()))
						.unwrap_or_default(),
					None => axum::response::Response::builder()
						.status(503)
						.body(axum::body::Body::from("metrics not available"))
						.unwrap_or_default(),
				}
			}),
		))
	}
}

#[cfg(any(feature = "api", test))]
fn credential_store_from_passphrase(
	passphrase: Option<&str>,
	context: &str,
) -> Result<crate::credential::AesGcmStore, OversyncError> {
	let passphrase = passphrase.ok_or_else(|| {
		OversyncError::Config(format!(
			"OVERSYNC_CREDENTIAL_KEY env var is required for {context}. \
			 Set it to a strong passphrase (32+ chars)."
		))
	})?;
	Ok(crate::credential::AesGcmStore::from_passphrase(passphrase))
}

#[cfg(feature = "api")]
struct DryRunState {
	registry: PluginRegistry,
}

#[cfg(feature = "api")]
async fn dry_run_handler(
	axum::extract::State(state): axum::extract::State<Arc<DryRunState>>,
	axum::Json(req): axum::Json<crate::dry_run::DryRunRequest>,
) -> Result<axum::Json<crate::dry_run::DryRunResult>, axum::Json<oversync_api::types::ErrorResponse>>
{
	let transform_hook: Option<std::sync::Arc<dyn oversync_core::traits::TransformHook>> =
		if req.transforms.is_empty() {
			None
		} else {
			let chain = oversync_transforms::parse_steps(&req.transforms).map_err(|e| {
				axum::Json(oversync_api::types::ErrorResponse {
					error: e.to_string(),
				})
			})?;
			Some(std::sync::Arc::new(chain))
		};

	let result = crate::dry_run::execute_dry_run(&req, &state.registry, transform_hook, None)
		.await
		.map_err(|e| {
			axum::Json(oversync_api::types::ErrorResponse {
				error: e.to_string(),
			})
		})?;
	Ok(axum::Json(result))
}

#[cfg(feature = "api")]
struct CredentialState {
	store: crate::credential::AesGcmStore,
	db: Arc<surrealdb::Surreal<surrealdb::engine::any::Any>>,
}

#[cfg(feature = "api")]
async fn create_credential(
	axum::extract::State(state): axum::extract::State<Arc<CredentialState>>,
	axum::Json(req): axum::Json<oversync_api::types::CreateCredentialRequest>,
) -> Result<
	axum::Json<oversync_api::types::MutationResponse>,
	axum::Json<oversync_api::types::ErrorResponse>,
> {
	let encrypted = state.store.encrypt(&req.secret).map_err(|e| {
		axum::Json(oversync_api::types::ErrorResponse {
			error: e.to_string(),
		})
	})?;

	const SQL_DEL_CRED: &str = oversync_queries::credential::DELETE_CREDENTIAL;
	const SQL_CREATE_CRED: &str = oversync_queries::credential::CREATE_CREDENTIAL;

	state
		.db
		.query(SQL_DEL_CRED)
		.bind(("name", req.name.clone()))
		.await
		.map_err(|e| {
			axum::Json(oversync_api::types::ErrorResponse {
				error: format!("db: {e}"),
			})
		})?;

	state
		.db
		.query(SQL_CREATE_CRED)
		.bind(("name", req.name.clone()))
		.bind(("ctype", req.credential_type))
		.bind(("enc", encrypted))
		.await
		.map_err(|e| {
			axum::Json(oversync_api::types::ErrorResponse {
				error: format!("db: {e}"),
			})
		})?;

	Ok(axum::Json(oversync_api::types::MutationResponse {
		ok: true,
		message: format!("credential '{}' created", req.name),
	}))
}

#[cfg(feature = "api")]
async fn list_credentials(
	axum::extract::State(state): axum::extract::State<Arc<CredentialState>>,
) -> Result<
	axum::Json<oversync_api::types::CredentialListResponse>,
	axum::Json<oversync_api::types::ErrorResponse>,
> {
	const SQL_LIST_CREDS: &str = oversync_queries::credential::LIST_CREDENTIALS;

	let mut resp = state.db.query(SQL_LIST_CREDS).await.map_err(|e| {
		axum::Json(oversync_api::types::ErrorResponse {
			error: format!("db: {e}"),
		})
	})?;

	let rows: Vec<serde_json::Value> = resp.take(0).map_err(|e| {
		axum::Json(oversync_api::types::ErrorResponse {
			error: format!("db: {e}"),
		})
	})?;

	let credentials = rows
		.iter()
		.filter_map(|r| {
			Some(oversync_api::types::CredentialInfo {
				name: r.get("name")?.as_str()?.to_string(),
				credential_type: r.get("credential_type")?.as_str()?.to_string(),
				created_at: r.get("created_at")?.as_str()?.to_string(),
			})
		})
		.collect();

	Ok(axum::Json(oversync_api::types::CredentialListResponse {
		credentials,
	}))
}

#[cfg(feature = "api")]
async fn delete_credential(
	axum::extract::State(state): axum::extract::State<Arc<CredentialState>>,
	axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<
	axum::Json<oversync_api::types::MutationResponse>,
	axum::Json<oversync_api::types::ErrorResponse>,
> {
	const SQL_DEL: &str = oversync_queries::credential::DELETE_CREDENTIAL;

	state
		.db
		.query(SQL_DEL)
		.bind(("name", name.clone()))
		.await
		.map_err(|e| {
			axum::Json(oversync_api::types::ErrorResponse {
				error: format!("db: {e}"),
			})
		})?;

	Ok(axum::Json(oversync_api::types::MutationResponse {
		ok: true,
		message: format!("credential '{name}' deleted"),
	}))
}

#[cfg(feature = "api")]
async fn list_config_versions(
	axum::extract::State(db): axum::extract::State<
		Arc<surrealdb::Surreal<surrealdb::engine::any::Any>>,
	>,
) -> Result<axum::Json<serde_json::Value>, axum::Json<oversync_api::types::ErrorResponse>> {
	let versions = crate::config_version::list_versions(&db)
		.await
		.map_err(|e| {
			axum::Json(oversync_api::types::ErrorResponse {
				error: e.to_string(),
			})
		})?;
	Ok(axum::Json(
		serde_json::to_value(&versions).unwrap_or_default(),
	))
}

impl OversyncEngineBuilder {
	/// SurrealDB namespace (default: `"oversync"`).
	pub fn namespace(mut self, ns: &str) -> Self {
		self.namespace = ns.to_string();
		self
	}

	/// SurrealDB database (default: `"sync"`).
	pub fn database(mut self, db: &str) -> Self {
		self.database = db.to_string();
		self
	}

	/// SurrealDB credentials. Required for non-`mem://` URLs.
	pub fn credentials(mut self, username: &str, password: &str) -> Self {
		self.username = username.to_string();
		self.password = password.to_string();
		self
	}

	/// Separate SurrealDB for snapshot storage. If not set, uses embedded `mem://`.
	pub fn snapshot_url(mut self, url: &str) -> Self {
		self.snapshot_url = Some(url.to_string());
		self
	}

	pub fn snapshot_credentials(mut self, username: &str, password: &str) -> Self {
		self.snapshot_username = Some(username.to_string());
		self.snapshot_password = Some(password.to_string());
		self
	}

	pub fn snapshot_namespace(mut self, ns: &str) -> Self {
		self.snapshot_ns = Some(ns.to_string());
		self
	}

	pub fn snapshot_database(mut self, db: &str) -> Self {
		self.snapshot_db = Some(db.to_string());
		self
	}

	/// Register an additional source connector factory (on top of built-in ones).
	pub fn register_source(mut self, factory: Box<dyn OriginFactory>) -> Self {
		self.extra_sources.push(factory);
		self
	}

	/// Register an additional sink factory (on top of built-in ones).
	pub fn register_sink(mut self, factory: Box<dyn TargetFactory>) -> Self {
		self.extra_sinks.push(factory);
		self
	}

	/// If true, don't register built-in connectors/sinks — only custom ones.
	pub fn skip_defaults(mut self, skip: bool) -> Self {
		self.skip_defaults = skip;
		self
	}

	/// If true, don't apply overshift schema on build (assumes schema already exists).
	pub fn skip_schema(mut self, skip: bool) -> Self {
		self.skip_schema = skip;
		self
	}

	/// Set an API key for the REST API. When set, all mutation/operation
	/// endpoints require `Authorization: Bearer <key>` or `X-API-Key: <key>`.
	/// Health and OpenAPI endpoints remain public.
	pub fn api_key(mut self, key: &str) -> Self {
		self.api_key = Some(key.to_string());
		self
	}

	/// Build the engine: connect to SurrealDB, apply schema, register factories.
	pub async fn build(self) -> Result<OversyncEngine, OversyncError> {
		let health_cancel = tokio_util::sync::CancellationToken::new();
		let is_mem = self.url.starts_with("mem://");

		if !is_mem {
			require_surreal_credentials(&self.url, &self.username, &self.password, "state")?;
		}

		let state_client: Arc<Surreal<Any>> = if is_mem {
			let db = surrealdb::engine::any::connect(&self.url)
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("connect: {e}")))?;
			db.use_ns(&self.namespace)
				.use_db(&self.database)
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("use ns/db: {e}")))?;

			#[cfg(feature = "schema")]
			if !self.skip_schema {
				apply_schema(&db, &self.namespace, &self.database).await?;
			}

			Arc::new(db)
		} else {
			let rdb =
				crate::resilient_db::ResilientDb::connect(crate::resilient_db::ResilientDbConfig {
					url: self.url.clone(),
					username: self.username.clone(),
					password: self.password.clone(),
					namespace: self.namespace.clone(),
					database: self.database.clone(),
					health_interval: std::time::Duration::from_secs(1),
					token_refresh_interval: Some(std::time::Duration::from_secs(3000)),
				})
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("resilient connect: {e}")))?;

			#[cfg(feature = "schema")]
			if !self.skip_schema {
				apply_schema(&rdb.client(), &self.namespace, &self.database).await?;
			}

			rdb.start_health_loop(health_cancel.clone());
			rdb.client()
		};

		let snapshot_client = match &self.snapshot_url {
			Some(url) => {
				let snapshot_credentials = if !url.starts_with("mem://") {
					let snap_user = self
						.snapshot_username
						.clone()
						.unwrap_or_else(|| self.username.clone());
					let snap_pass = self
						.snapshot_password
						.clone()
						.unwrap_or_else(|| self.password.clone());
					require_surreal_credentials(url, &snap_user, &snap_pass, "snapshot")?;
					Some((snap_user, snap_pass))
				} else {
					None
				};

				let snap = surrealdb::engine::any::connect(url)
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot connect: {e}")))?;

				if let Some((snap_user, snap_pass)) = snapshot_credentials {
					snap.signin(surrealdb::opt::auth::Root {
						username: snap_user,
						password: snap_pass,
					})
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot signin: {e}")))?;
				}

				#[cfg(feature = "schema")]
				if !self.skip_schema {
					let ns = self.snapshot_ns.as_deref().unwrap_or(&self.namespace);
					let db = self.snapshot_db.as_deref().unwrap_or(&self.database);
					apply_schema(&snap, ns, db).await?;
				}

				let ns = self.snapshot_ns.as_deref().unwrap_or(&self.namespace);
				let db = self.snapshot_db.as_deref().unwrap_or(&self.database);
				snap.use_ns(ns)
					.use_db(db)
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot use ns/db: {e}")))?;

				info!(url = %url, "snapshot DB connected (separate)");
				snap
			}
			None => {
				let snap = surrealdb::engine::any::connect("mem://")
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot mem: {e}")))?;

				#[cfg(feature = "schema")]
				if !self.skip_schema {
					apply_schema(&snap, "oversync", "snapshot").await?;
				}

				snap.use_ns("oversync")
					.use_db("snapshot")
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot use ns/db: {e}")))?;

				info!("snapshot DB: embedded kv-mem");
				snap
			}
		};

		let delta_engine = DeltaEngine::new(Arc::clone(&state_client), snapshot_client);

		let mut registry = if self.skip_defaults {
			PluginRegistry::new()
		} else {
			default_registry()
		};
		for factory in self.extra_sources {
			registry.register_source(factory);
		}
		for factory in self.extra_sinks {
			registry.register_sink(factory);
		}

		let lifecycle = Arc::new(LifecycleManager::new(delta_engine, registry));

		let snapshot = self.snapshot_url.map(|url| crate::config::SnapshotDbDef {
			url,
			username: self
				.snapshot_username
				.unwrap_or_else(|| self.username.clone()),
			password: self
				.snapshot_password
				.unwrap_or_else(|| self.password.clone()),
			namespace: self.snapshot_ns.unwrap_or_else(|| self.namespace.clone()),
			database: self.snapshot_db.unwrap_or_else(|| self.database.clone()),
		});

		let surreal_def = SurrealDbDef {
			url: self.url,
			username: self.username,
			password: self.password,
			namespace: self.namespace,
			database: self.database,
			snapshot,
		};

		Ok(OversyncEngine {
			lifecycle,
			state_client,
			health_cancel,
			surreal_def,
			api_key: self.api_key,
		})
	}
}

fn require_surreal_credentials(
	url: &str,
	username: &str,
	password: &str,
	label: &str,
) -> Result<(), OversyncError> {
	if !url.starts_with("mem://") && (username.is_empty() || password.is_empty()) {
		return Err(OversyncError::Config(format!(
			"{label} SurrealDB credentials are required for non-mem URLs; set both username and password explicitly"
		)));
	}

	Ok(())
}

pub(crate) fn default_registry() -> PluginRegistry {
	let mut registry = PluginRegistry::new();
	registry.register_source(Box::new(PostgresOriginFactory));
	registry.register_source(Box::new(HttpOriginFactory));
	registry.register_source(Box::new(MysqlOriginFactory));
	registry.register_source(Box::new(FlightSqlOriginFactory));
	registry.register_source(Box::new(TrinoOriginFactory));
	registry.register_source(Box::new(GraphqlOriginFactory));
	registry.register_source(Box::new(ClickHouseOriginFactory));
	registry.register_source(Box::new(McpOriginFactory));
	registry.register_source(Box::new(KafkaOriginFactory));
	registry.register_source(Box::new(SurrealDbOriginFactory));
	registry.register_sink(Box::new(StdoutTargetFactory));
	registry.register_sink(Box::new(KafkaTargetFactory));
	registry.register_sink(Box::new(SurrealDbTargetFactory));
	registry.register_sink(Box::new(HttpTargetFactory));
	registry.register_sink(Box::new(PostgresTargetFactory));
	registry.register_sink(Box::new(MysqlTargetFactory));
	registry.register_sink(Box::new(McpTargetFactory));
	registry.register_sink(Box::new(ClickHouseTargetFactory));
	registry
}

#[cfg(feature = "schema")]
pub(crate) async fn apply_schema(
	db: &Surreal<Any>,
	ns: &str,
	db_name: &str,
) -> Result<(), OversyncError> {
	let surql_dir = resolve_surql_dir()?;
	let mut manifest = overshift::Manifest::load(surql_dir.path())
		.map_err(|e| OversyncError::Migration(format!("load manifest: {e}")))?;
	manifest.meta.ns = ns.to_string();
	manifest.meta.db = db_name.to_string();
	let plan = overshift::plan(db, &manifest)
		.await
		.map_err(|e| OversyncError::Migration(format!("plan: {e}")))?;
	let result = plan
		.apply(db)
		.await
		.map_err(|e| OversyncError::Migration(format!("apply: {e}")))?;
	info!(
		migrations = result.applied_migrations,
		modules = result.applied_modules,
		"schema applied"
	);
	Ok(())
}

#[cfg(feature = "schema")]
fn resolve_surql_dir() -> Result<ResolvedSurqlDir, OversyncError> {
	if let Ok(dir) = std::env::var("OVERSYNC_SURQL_DIR") {
		return Ok(ResolvedSurqlDir::persistent(dir.into()));
	}

	materialize_embedded_surql()
}

#[cfg(feature = "schema")]
fn materialize_embedded_surql() -> Result<ResolvedSurqlDir, OversyncError> {
	let root = create_embedded_surql_root()?;
	std::fs::create_dir_all(&root).map_err(|e| {
		OversyncError::Migration(format!(
			"prepare embedded surql dir {}: {e}",
			root.display()
		))
	})?;

	for (rel_path, contents) in crate::embedded_surql::FILES {
		let path = root.join(rel_path);
		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent).map_err(|e| {
				OversyncError::Migration(format!(
					"prepare embedded surql dir {}: {e}",
					parent.display()
				))
			})?;
		}
		std::fs::write(&path, contents).map_err(|e| {
			OversyncError::Migration(format!("write embedded surql file {}: {e}", path.display()))
		})?;
	}

	Ok(ResolvedSurqlDir::temporary(root))
}

#[cfg(feature = "schema")]
fn create_embedded_surql_root() -> Result<std::path::PathBuf, OversyncError> {
	let base = std::env::temp_dir();
	for attempt in 0..8 {
		let nonce = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map_err(|e| OversyncError::Migration(format!("clock error: {e}")))?
			.as_nanos();
		let candidate = base.join(format!(
			"oversync-surql-{}-{}-{nonce}-{attempt}",
			env!("CARGO_PKG_VERSION"),
			std::process::id()
		));
		if !candidate.exists() {
			return Ok(candidate);
		}
	}

	Err(OversyncError::Migration(
		"failed to allocate embedded surql temp dir".into(),
	))
}

#[cfg(feature = "schema")]
struct ResolvedSurqlDir {
	path: std::path::PathBuf,
	cleanup_root: Option<std::path::PathBuf>,
}

#[cfg(feature = "schema")]
impl ResolvedSurqlDir {
	fn persistent(path: std::path::PathBuf) -> Self {
		Self {
			path,
			cleanup_root: None,
		}
	}

	fn temporary(path: std::path::PathBuf) -> Self {
		Self {
			cleanup_root: Some(path.clone()),
			path,
		}
	}

	fn path(&self) -> &Path {
		&self.path
	}
}

#[cfg(feature = "schema")]
impl Drop for ResolvedSurqlDir {
	fn drop(&mut self) {
		if let Some(path) = self.cleanup_root.take() {
			let _ = std::fs::remove_dir_all(path);
		}
	}
}

#[cfg(feature = "api")]
struct LifecycleAdapter {
	lifecycle: Arc<LifecycleManager>,
	surreal_def: SurrealDbDef,
}

#[cfg(feature = "api")]
#[async_trait::async_trait]
impl oversync_api::state::LifecycleControl for LifecycleAdapter {
	async fn restart_with_config_json(&self, db: &Surreal<Any>) -> Result<(), OversyncError> {
		let config = crate::config_db::load_config_from_db(db, &self.surreal_def).await?;
		self.lifecycle.start(config).await
	}

	async fn pause(&self) {
		self.lifecycle.pause().await;
	}

	async fn resume(&self) -> Result<(), OversyncError> {
		self.lifecycle.resume().await
	}

	async fn is_running(&self) -> bool {
		self.lifecycle.is_running().await
	}

	async fn is_paused(&self) -> bool {
		self.lifecycle.is_paused().await
	}
}

#[cfg(test)]
mod tests {
	use super::credential_store_from_passphrase;

	#[test]
	fn credential_store_requires_passphrase() {
		match credential_store_from_passphrase(None, "credential API routes") {
			Err(err) => assert!(err.to_string().contains("OVERSYNC_CREDENTIAL_KEY")),
			Ok(_) => panic!("missing passphrase should fail"),
		}
	}

	#[test]
	fn credential_store_accepts_passphrase() {
		assert!(
			credential_store_from_passphrase(Some("test-passphrase"), "credential API routes")
				.is_ok()
		);
	}

	#[cfg(feature = "schema")]
	#[test]
	fn materialized_embedded_surql_dir_is_cleaned_on_drop() {
		let dir = super::materialize_embedded_surql().expect("materialize embedded surql");
		let root = dir.path().to_path_buf();
		assert!(root.exists(), "embedded surql dir should exist");
		assert!(
			root.join("manifest.toml").exists(),
			"embedded surql manifest should be present"
		);
		drop(dir);
		assert!(
			!root.exists(),
			"embedded surql temp dir should be removed after use"
		);
	}

	#[cfg(feature = "schema")]
	#[test]
	fn persistent_surql_dir_is_not_cleaned_on_drop() {
		let root = std::env::temp_dir().join(format!(
			"oversync-persistent-surql-test-{}",
			std::process::id()
		));
		std::fs::create_dir_all(&root).expect("create persistent surql test dir");
		let dir = super::ResolvedSurqlDir::persistent(root.clone());
		drop(dir);
		assert!(root.exists(), "explicit surql dir should not be removed");
		std::fs::remove_dir_all(&root).expect("cleanup persistent surql test dir");
	}
}
