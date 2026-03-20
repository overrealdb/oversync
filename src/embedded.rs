use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use oversync_core::error::OversyncError;
use oversync_core::model::DeltaResult;
use oversync_core::traits::{Sink, TargetFactory, OriginFactory, TransformHook};
use oversync_delta::DeltaEngine;

use crate::config::{MissedTickPolicy, PipeConfig, SourceDef};
use crate::cycle::{CycleConfig, CycleRunner};
use crate::registry::PluginRegistry;

fn build_connector_config(pipe: &PipeConfig) -> serde_json::Value {
	let mut map = match &pipe.origin.config {
		serde_json::Value::Object(m) => m.clone(),
		_ => serde_json::Map::new(),
	};
	map.insert("dsn".into(), serde_json::Value::String(pipe.origin.dsn.clone()));
	serde_json::Value::Object(map)
}

pub struct EmbeddedSyncBuilder {
	state_db: Option<Surreal<Any>>,
	snapshot_db: Option<Surreal<Any>>,
	skip_schema: bool,
	pipes: Vec<PipeConfig>,
	sinks: HashMap<String, Arc<dyn Sink>>,
	transform_hooks: HashMap<String, Arc<dyn TransformHook>>,
	extra_sources: Vec<Box<dyn OriginFactory>>,
	extra_sinks: Vec<Box<dyn TargetFactory>>,
}

pub struct EmbeddedSync {
	delta_engine: Arc<DeltaEngine>,
	pipes: Vec<PipeConfig>,
	sinks: HashMap<String, Arc<dyn Sink>>,
	transform_hooks: HashMap<String, Arc<dyn TransformHook>>,
	registry: PluginRegistry,
	shutdown_tx: std::sync::Mutex<Option<watch::Sender<bool>>>,
	handle: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for EmbeddedSync {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("EmbeddedSync")
			.field("pipes", &self.pipes.len())
			.field("sinks", &self.sinks.len())
			.field("transform_hooks", &self.transform_hooks.len())
			.finish()
	}
}

impl EmbeddedSync {
	pub fn builder() -> EmbeddedSyncBuilder {
		EmbeddedSyncBuilder {
			state_db: None,
			snapshot_db: None,
			skip_schema: false,
			pipes: vec![],
			sinks: HashMap::new(),
			transform_hooks: HashMap::new(),
			extra_sources: vec![],
			extra_sinks: vec![],
		}
	}

	/// Run one sync cycle for the given pipe and query. Returns the delta.
	pub async fn run_once(
		&self,
		pipe_name: &str,
		query_id: &str,
	) -> Result<DeltaResult, OversyncError> {
		let pipe = self
			.pipes
			.iter()
			.find(|p| p.name == pipe_name)
			.ok_or_else(|| {
				OversyncError::Config(format!("unknown pipe '{pipe_name}'"))
			})?;

		let query = pipe
			.queries
			.iter()
			.find(|q| q.id == query_id)
			.ok_or_else(|| {
				OversyncError::Config(format!(
					"pipe '{pipe_name}': unknown query '{query_id}'"
				))
			})?;

		let connector = self
			.registry
			.create_source(&pipe.origin.connector, &pipe.name, &build_connector_config(pipe))
			.await?;

		let query_sinks = self.resolve_sinks(pipe, &query.sinks)?;

		let cycle_config = CycleConfig {
			origin_id: pipe.name.clone(),
			query_id: query.id.clone(),
			sql: query.sql.clone(),
			key_column: query.key_column.clone(),
			fail_safe_threshold: pipe.delta.fail_safe_threshold,
			diff_mode: pipe.delta.diff_mode.clone(),
			transform: query.transform.clone(),
		};

		let pipe_engine = self.delta_engine.for_source(pipe_name);
		pipe_engine.ensure_tables().await?;
		let mut runner =
			CycleRunner::new(&pipe_engine, connector.as_ref(), &query_sinks);

		if !pipe.filters.is_empty() {
			let chain = oversync_transforms::parse_steps(&pipe.filters)?;
			runner = runner.with_pre_filter(Arc::new(chain));
		}

		if !pipe.transforms.is_empty() {
			let chain = oversync_transforms::parse_steps(&pipe.transforms)?;
			runner = runner.with_transform(Arc::new(chain));
		} else if let Some(ref transform_name) = query.transform {
			if let Some(hook) = self.transform_hooks.get(transform_name) {
				runner = runner.with_transform(Arc::clone(hook));
			}
		}

		runner.run(&cycle_config).await
	}

	/// Start scheduled polling for all pipes. Non-blocking — spawns background tasks.
	pub async fn start(&self) -> Result<(), OversyncError> {
		{
			let guard = self.shutdown_tx.lock().unwrap();
			if guard.is_some() {
				return Err(OversyncError::Internal("already running".into()));
			}
		}

		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		let engine = Arc::clone(&self.delta_engine);
		let pipes = self.pipes.clone();
		let sinks = self.sinks.clone();
		let transform_hooks = self.transform_hooks.clone();
		let registry = self.registry.clone();

		let handle = tokio::spawn(async move {
			run_all_pipes(engine, registry, pipes, sinks, transform_hooks, shutdown_rx)
				.await;
		});

		*self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);
		*self.handle.lock().await = Some(handle);
		Ok(())
	}

	pub fn shutdown(&self) {
		if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
			let _ = tx.send(true);
		}
	}

	fn resolve_sinks(
		&self,
		pipe: &PipeConfig,
		query_sinks: &Option<Vec<String>>,
	) -> Result<Vec<Arc<dyn Sink>>, OversyncError> {
		let target_names: Option<&[String]> = match query_sinks {
			Some(qs) => Some(qs.as_slice()),
			None if !pipe.targets.is_empty() => Some(pipe.targets.as_slice()),
			None => None,
		};

		match target_names {
			None => Ok(self.sinks.values().cloned().collect()),
			Some(names) => {
				let mut resolved = Vec::with_capacity(names.len());
				for name in names {
					let sink = self.sinks.get(name).ok_or_else(|| {
						OversyncError::Config(format!("unknown sink '{name}'"))
					})?;
					resolved.push(sink.clone());
				}
				Ok(resolved)
			}
		}
	}
}

impl EmbeddedSyncBuilder {
	pub fn state_db(mut self, db: Surreal<Any>) -> Self {
		self.state_db = Some(db);
		self
	}

	pub fn snapshot_db(mut self, db: Surreal<Any>) -> Self {
		self.snapshot_db = Some(db);
		self
	}

	pub fn skip_schema(mut self) -> Self {
		self.skip_schema = true;
		self
	}

	/// Add a pipe to the embedded sync engine.
	pub fn add_pipe(mut self, pipe: PipeConfig) -> Self {
		self.pipes.push(pipe);
		self
	}

	/// Add a legacy source definition. Internally converted to PipeConfig.
	pub fn add_source(mut self, def: SourceDef) -> Self {
		self.pipes.push(PipeConfig::from(&def));
		self
	}

	pub fn add_sink(mut self, name: &str, sink: Arc<dyn Sink>) -> Self {
		self.sinks.insert(name.to_string(), sink);
		self
	}

	pub fn add_transform(mut self, name: &str, hook: Arc<dyn TransformHook>) -> Self {
		self.transform_hooks.insert(name.to_string(), hook);
		self
	}

	pub fn register_source(mut self, factory: Box<dyn OriginFactory>) -> Self {
		self.extra_sources.push(factory);
		self
	}

	pub fn register_sink(mut self, factory: Box<dyn TargetFactory>) -> Self {
		self.extra_sinks.push(factory);
		self
	}

	pub async fn build(self) -> Result<EmbeddedSync, OversyncError> {
		let state_db = self
			.state_db
			.ok_or_else(|| OversyncError::Config("state_db is required".into()))?;

		let snapshot_db = match self.snapshot_db {
			Some(db) => db,
			None => {
				let snap = surrealdb::engine::any::connect("mem://")
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot mem: {e}")))?;
				snap.use_ns("oversync")
					.use_db("snapshot")
					.await
					.map_err(|e| OversyncError::SurrealDb(format!("snapshot use: {e}")))?;

				#[cfg(feature = "schema")]
				if !self.skip_schema {
					crate::engine::apply_schema(&snap, "oversync", "snapshot").await?;
				}

				snap
			}
		};

		let delta_engine = Arc::new(DeltaEngine::new(state_db, snapshot_db));

		let mut registry = crate::engine::default_registry();
		for factory in self.extra_sources {
			registry.register_source(factory);
		}
		for factory in self.extra_sinks {
			registry.register_sink(factory);
		}

		Ok(EmbeddedSync {
			delta_engine,
			pipes: self.pipes,
			sinks: self.sinks,
			transform_hooks: self.transform_hooks,
			registry,
			shutdown_tx: std::sync::Mutex::new(None),
			handle: Mutex::new(None),
		})
	}
}

async fn run_all_pipes(
	engine: Arc<DeltaEngine>,
	registry: PluginRegistry,
	pipes: Vec<PipeConfig>,
	sinks: HashMap<String, Arc<dyn Sink>>,
	transform_hooks: HashMap<String, Arc<dyn TransformHook>>,
	shutdown_rx: watch::Receiver<bool>,
) {
	let registry = Arc::new(registry);
	let sinks = Arc::new(sinks);
	let transform_hooks = Arc::new(transform_hooks);
	let mut handles = Vec::new();

	for pipe in &pipes {
		if !pipe.enabled {
			info!(pipe = %pipe.name, "pipe disabled, skipping");
			continue;
		}

		for query in &pipe.queries {
			let engine = Arc::clone(&engine);
			let registry = Arc::clone(&registry);
			let sinks = Arc::clone(&sinks);
			let transform_hooks = Arc::clone(&transform_hooks);
			let pipe = pipe.clone();
			let query = query.clone();
			let mut shutdown = shutdown_rx.clone();

			let handle = tokio::spawn(async move {
				run_embedded_pipe_query(
					engine,
					registry,
					pipe,
					query,
					sinks,
					transform_hooks,
					&mut shutdown,
				)
				.await;
			});

			handles.push(handle);
		}
	}

	info!(tasks = handles.len(), "embedded sync started");
	for handle in handles {
		let _ = handle.await;
	}
	info!("embedded sync stopped");
}

async fn run_embedded_pipe_query(
	engine: Arc<DeltaEngine>,
	registry: Arc<PluginRegistry>,
	pipe: PipeConfig,
	query: crate::config::QueryDef,
	sinks: Arc<HashMap<String, Arc<dyn Sink>>>,
	transform_hooks: Arc<HashMap<String, Arc<dyn TransformHook>>>,
	shutdown: &mut watch::Receiver<bool>,
) {
	let connector = match registry
		.create_source(&pipe.origin.connector, &pipe.name, &build_connector_config(&pipe))
		.await
	{
		Ok(c) => c,
		Err(e) => {
			error!(pipe = %pipe.name, error = %e, "failed to create connector");
			return;
		}
	};

	let query_sinks: Vec<Arc<dyn Sink>> = {
		let target_names = if let Some(ref qs) = query.sinks {
			Some(qs.as_slice())
		} else if !pipe.targets.is_empty() {
			Some(pipe.targets.as_slice())
		} else {
			None
		};

		match target_names {
			None => sinks.values().cloned().collect(),
			Some(names) => {
				let mut resolved = Vec::new();
				for name in names {
					match sinks.get(name) {
						Some(s) => resolved.push(s.clone()),
						None => {
							error!(sink = %name, "unknown sink in pipe config");
							return;
						}
					}
				}
				resolved
			}
		}
	};

	let pipe_engine = engine.for_source(&pipe.name);
	if let Err(e) = pipe_engine.ensure_tables().await {
		error!(pipe = %pipe.name, error = %e, "failed to create pipeline tables");
		return;
	}

	let interval_secs = pipe.schedule.interval_secs.max(1);
	let interval = Duration::from_secs(interval_secs);

	info!(
		pipe = %pipe.name,
		query = %query.id,
		interval_secs = pipe.schedule.interval_secs,
		tables = ?pipe_engine.tables(),
		"embedded polling task started"
	);

	run_timed_embedded_cycle(
		&pipe_engine,
		connector.as_ref(),
		&query_sinks,
		&pipe,
		&query,
		&transform_hooks,
		interval,
	)
	.await;

	let mut ticker = tokio::time::interval(interval);
	ticker.tick().await;

	match pipe.schedule.missed_tick_policy {
		MissedTickPolicy::Skip => {
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		}
		MissedTickPolicy::Burst => {
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
		}
	}

	loop {
		tokio::select! {
			_ = ticker.tick() => {
				run_timed_embedded_cycle(
					&pipe_engine,
					connector.as_ref(),
					&query_sinks,
					&pipe,
					&query,
					&transform_hooks,
					interval,
				).await;
			}
			_ = shutdown.changed() => {
				info!(pipe = %pipe.name, query = %query.id, "shutting down");
				break;
			}
		}
	}
}

async fn run_timed_embedded_cycle(
	engine: &DeltaEngine,
	connector: &dyn oversync_core::traits::OriginConnector,
	sinks: &[Arc<dyn Sink>],
	pipe: &PipeConfig,
	query: &crate::config::QueryDef,
	transform_hooks: &HashMap<String, Arc<dyn TransformHook>>,
	interval: Duration,
) {
	let start = Instant::now();
	run_embedded_cycle(engine, connector, sinks, pipe, query, transform_hooks).await;
	let elapsed = start.elapsed();

	if elapsed > interval {
		warn!(
			pipe = %pipe.name,
			query = %query.id,
			elapsed_secs = elapsed.as_secs(),
			interval_secs = interval.as_secs(),
			policy = ?pipe.schedule.missed_tick_policy,
			"cycle took longer than polling interval"
		);
	}
}

async fn run_embedded_cycle(
	engine: &DeltaEngine,
	connector: &dyn oversync_core::traits::OriginConnector,
	sinks: &[Arc<dyn Sink>],
	pipe: &PipeConfig,
	query: &crate::config::QueryDef,
	transform_hooks: &HashMap<String, Arc<dyn TransformHook>>,
) {
	let cycle_config = CycleConfig {
		origin_id: pipe.name.clone(),
		query_id: query.id.clone(),
		sql: query.sql.clone(),
		key_column: query.key_column.clone(),
		fail_safe_threshold: pipe.delta.fail_safe_threshold,
		diff_mode: pipe.delta.diff_mode.clone(),
		transform: query.transform.clone(),
	};

	let mut runner = CycleRunner::new(engine, connector, sinks);

	if !pipe.filters.is_empty() {
		match oversync_transforms::parse_steps(&pipe.filters) {
			Ok(chain) => {
				runner = runner.with_pre_filter(Arc::new(chain));
			}
			Err(e) => {
				error!(pipe = %pipe.name, error = %e, "failed to parse filters");
				return;
			}
		}
	}

	// Pipe-level transforms (from config)
	if !pipe.transforms.is_empty() {
		match oversync_transforms::parse_steps(&pipe.transforms) {
			Ok(chain) => {
				runner = runner.with_transform(Arc::new(chain));
			}
			Err(e) => {
				error!(pipe = %pipe.name, error = %e, "failed to parse transforms");
				return;
			}
		}
	} else if let Some(ref transform_name) = query.transform {
		// Legacy: query-level named transform hook
		if let Some(hook) = transform_hooks.get(transform_name) {
			runner = runner.with_transform(Arc::clone(hook));
		}
	}

	for attempt in 0..=pipe.retry.max_retries {
		match runner.run(&cycle_config).await {
			Ok(diff) => {
				if !diff.is_empty() {
					info!(
						pipe = %pipe.name,
						query = %query.id,
						created = diff.created.len(),
						updated = diff.updated.len(),
						deleted = diff.deleted.len(),
						"cycle produced events"
					);
				}
				return;
			}
			Err(e) => {
				if attempt < pipe.retry.max_retries {
					let delay = Duration::from_secs(
						pipe.retry.retry_base_delay_secs.saturating_mul(
							2u64.saturating_pow(attempt),
						),
					);
					warn!(
						pipe = %pipe.name,
						attempt = attempt + 1,
						error = %e,
						"cycle failed, retrying"
					);
					tokio::time::sleep(delay).await;
				} else {
					error!(
						pipe = %pipe.name,
						attempts = pipe.retry.max_retries + 1,
						error = %e,
						"cycle failed after all retries"
					);
				}
			}
		}
	}
}
