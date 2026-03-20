use std::path::Path;
use std::sync::Arc;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::info;

use oversync_connectors::{
	ClickHouseOriginFactory, FlightSqlOriginFactory, GraphqlOriginFactory, HttpOriginFactory,
	MysqlOriginFactory, PostgresOriginFactory, TrinoOriginFactory,
};
use oversync_core::error::OversyncError;
use oversync_core::traits::{TargetFactory, OriginFactory};
use oversync_delta::DeltaEngine;
use oversync_sinks::{HttpTargetFactory, KafkaTargetFactory, StdoutTargetFactory, SurrealDbTargetFactory};

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
	state_client: Surreal<Any>,
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
			username: "root".into(),
			password: "root".into(),
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

	/// Start syncing with the given config. Stops any running scheduler first.
	pub async fn start(&self, config: SyncConfig) -> Result<(), OversyncError> {
		self.lifecycle.start(config).await
	}

	/// Load config from `source_config`/`query_config`/`sink_config` tables in SurrealDB and start.
	pub async fn start_from_db(&self) -> Result<(), OversyncError> {
		let config = crate::config_db::load_config_from_db(&self.state_client, &self.surreal_def)
			.await?;
		self.lifecycle.start(config).await
	}

	/// Parse a TOML config file and start syncing.
	pub async fn start_from_toml(&self, path: &Path) -> Result<(), OversyncError> {
		let config = SyncConfig::from_file(path)?;
		self.lifecycle.start(config).await
	}

	/// Stop all sync tasks and clear config.
	pub async fn shutdown(&self) {
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

	pub fn state_client(&self) -> &Surreal<Any> {
		&self.state_client
	}

	pub fn surreal_def(&self) -> &SurrealDbDef {
		&self.surreal_def
	}

	/// Build an axum `Router` with the full oversync REST API.
	/// Requires the `api` feature. Mount into your own axum app or serve standalone.
	#[cfg(feature = "api")]
	pub fn api_router(&self) -> axum::Router {
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

		let base = oversync_api::router(api_state);

		let registry = crate::engine::default_registry();
		let dry_run_state = Arc::new(DryRunState { registry });
		base.route(
			"/pipes/dry-run",
			axum::routing::post(dry_run_handler).with_state(dry_run_state),
		)
	}
}

#[cfg(feature = "api")]
struct DryRunState {
	registry: PluginRegistry,
}

#[cfg(feature = "api")]
async fn dry_run_handler(
	axum::extract::State(state): axum::extract::State<Arc<DryRunState>>,
	axum::Json(req): axum::Json<crate::dry_run::DryRunRequest>,
) -> Result<axum::Json<crate::dry_run::DryRunResult>, axum::Json<oversync_api::types::ErrorResponse>> {
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

	let result = crate::dry_run::execute_dry_run(&req, &state.registry, transform_hook)
		.await
		.map_err(|e| {
			axum::Json(oversync_api::types::ErrorResponse {
				error: e.to_string(),
			})
		})?;
	Ok(axum::Json(result))
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

	/// SurrealDB credentials (default: `"root"` / `"root"`).
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
		let state_client = surrealdb::engine::any::connect(&self.url)
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("connect: {e}")))?;
		if !self.url.starts_with("mem://") {
			state_client
				.signin(surrealdb::opt::auth::Root {
					username: self.username.clone(),
					password: self.password.clone(),
				})
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("signin: {e}")))?;
		}

		#[cfg(feature = "schema")]
		if !self.skip_schema {
			apply_schema(&state_client, &self.namespace, &self.database).await?;
		}

		state_client
			.use_ns(&self.namespace)
			.use_db(&self.database)
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("use ns/db: {e}")))?;

		let snapshot_client = match &self.snapshot_url {
			Some(url) => {
				let snap = surrealdb::engine::any::connect(url)
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot connect: {e}")))?;
				if !url.starts_with("mem://") {
					let snap_user = self.snapshot_username.as_deref().unwrap_or(&self.username);
					let snap_pass = self.snapshot_password.as_deref().unwrap_or(&self.password);
					snap.signin(surrealdb::opt::auth::Root {
						username: snap_user.to_string(),
						password: snap_pass.to_string(),
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

		let delta_engine = DeltaEngine::new(state_client.clone(), snapshot_client);

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

		let surreal_def = SurrealDbDef {
			url: self.url,
			username: self.username,
			password: self.password,
			namespace: self.namespace,
			database: self.database,
			snapshot: self.snapshot_url.map(|url| {
				crate::config::SnapshotDbDef {
					url,
					username: self.snapshot_username.unwrap_or_else(|| "root".into()),
					password: self.snapshot_password.unwrap_or_else(|| "root".into()),
					namespace: self.snapshot_ns.unwrap_or_else(|| "oversync".into()),
					database: self.snapshot_db.unwrap_or_else(|| "sync".into()),
				}
			}),
		};

		Ok(OversyncEngine {
			lifecycle,
			state_client,
			surreal_def,
			api_key: self.api_key,
		})
	}
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
	registry.register_sink(Box::new(StdoutTargetFactory));
	registry.register_sink(Box::new(KafkaTargetFactory));
	registry.register_sink(Box::new(SurrealDbTargetFactory));
	registry.register_sink(Box::new(HttpTargetFactory));
	registry
}

#[cfg(feature = "schema")]
pub(crate) async fn apply_schema(
	db: &Surreal<Any>,
	ns: &str,
	db_name: &str,
) -> Result<(), OversyncError> {
	let mut manifest = overshift::Manifest::load("surql/")
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

#[cfg(feature = "api")]
struct LifecycleAdapter {
	lifecycle: Arc<LifecycleManager>,
	surreal_def: SurrealDbDef,
}

#[cfg(feature = "api")]
#[async_trait::async_trait]
impl oversync_api::state::LifecycleControl for LifecycleAdapter {
	async fn restart_with_config_json(
		&self,
		db: &Surreal<Any>,
	) -> Result<(), OversyncError> {
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
