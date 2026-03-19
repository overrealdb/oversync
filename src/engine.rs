use std::path::Path;
use std::sync::Arc;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::info;

use oversync_connectors::{
	FlightSqlSourceFactory, GraphqlSourceFactory, HttpSourceFactory, MysqlSourceFactory,
	PostgresSourceFactory, TrinoSourceFactory,
};
use oversync_core::error::OversyncError;
use oversync_core::traits::{SinkFactory, SourceFactory};
use oversync_delta::DeltaEngine;
use oversync_sinks::{HttpSinkFactory, KafkaSinkFactory, StdoutSinkFactory, SurrealDbSinkFactory};

use crate::config::{SurrealDbDef, SyncConfig};
use crate::lifecycle::LifecycleManager;
use crate::registry::PluginRegistry;

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
	extra_sources: Vec<Box<dyn SourceFactory>>,
	extra_sinks: Vec<Box<dyn SinkFactory>>,
	skip_defaults: bool,
	skip_schema: bool,
}

#[derive(Clone)]
pub struct OversyncEngine {
	lifecycle: Arc<LifecycleManager>,
	state_client: Surreal<Any>,
	surreal_def: SurrealDbDef,
}

impl OversyncEngine {
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
		}
	}

	pub async fn start(&self, config: SyncConfig) -> Result<(), OversyncError> {
		self.lifecycle.start(config).await
	}

	pub async fn start_from_db(&self) -> Result<(), OversyncError> {
		let config = crate::config_db::load_config_from_db(&self.state_client, &self.surreal_def)
			.await?;
		self.lifecycle.start(config).await
	}

	pub async fn start_from_toml(&self, path: &Path) -> Result<(), OversyncError> {
		let config = SyncConfig::from_file(path)?;
		self.lifecycle.start(config).await
	}

	pub async fn shutdown(&self) {
		self.lifecycle.shutdown().await;
	}

	pub async fn pause(&self) {
		self.lifecycle.pause().await;
	}

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
			cycle_status: Arc::new(RwLock::new(HashMap::new())),
			db_client: Some(self.state_client.clone()),
			lifecycle: Some(Arc::new(lifecycle_adapter)),
		});

		oversync_api::router(api_state)
	}
}

impl OversyncEngineBuilder {
	pub fn namespace(mut self, ns: &str) -> Self {
		self.namespace = ns.to_string();
		self
	}

	pub fn database(mut self, db: &str) -> Self {
		self.database = db.to_string();
		self
	}

	pub fn credentials(mut self, username: &str, password: &str) -> Self {
		self.username = username.to_string();
		self.password = password.to_string();
		self
	}

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

	pub fn register_source(mut self, factory: Box<dyn SourceFactory>) -> Self {
		self.extra_sources.push(factory);
		self
	}

	pub fn register_sink(mut self, factory: Box<dyn SinkFactory>) -> Self {
		self.extra_sinks.push(factory);
		self
	}

	pub fn skip_defaults(mut self, skip: bool) -> Self {
		self.skip_defaults = skip;
		self
	}

	pub fn skip_schema(mut self, skip: bool) -> Self {
		self.skip_schema = skip;
		self
	}

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
		})
	}
}

fn default_registry() -> PluginRegistry {
	let mut registry = PluginRegistry::new();
	registry.register_source(Box::new(PostgresSourceFactory));
	registry.register_source(Box::new(HttpSourceFactory));
	registry.register_source(Box::new(MysqlSourceFactory));
	registry.register_source(Box::new(FlightSqlSourceFactory));
	registry.register_source(Box::new(TrinoSourceFactory));
	registry.register_source(Box::new(GraphqlSourceFactory));
	registry.register_sink(Box::new(StdoutSinkFactory));
	registry.register_sink(Box::new(KafkaSinkFactory));
	registry.register_sink(Box::new(SurrealDbSinkFactory));
	registry.register_sink(Box::new(HttpSinkFactory));
	registry
}

#[cfg(feature = "schema")]
async fn apply_schema(
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
