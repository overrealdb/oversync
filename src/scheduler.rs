use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use oversync_core::error::OversyncError;
use oversync_core::model::DeltaResult;
use oversync_core::traits::{OriginConnector, Sink};
use oversync_delta::DeltaEngine;

use crate::config::{MissedTickPolicy, PipeConfig, QueryDef, SyncConfig};
use crate::cycle::{CycleConfig, CycleRunner};
use crate::registry::PluginRegistry;

pub struct Scheduler {
	engine: Arc<DeltaEngine>,
	config: SyncConfig,
	registry: Arc<PluginRegistry>,
	instance_id: String,
	shutdown_tx: watch::Sender<bool>,
	shutdown_rx: watch::Receiver<bool>,
}

#[derive(Debug, Clone)]
pub struct ManualRunResult {
	pub query_id: String,
	pub created: usize,
	pub updated: usize,
	pub deleted: usize,
}

impl Scheduler {
	pub fn new(engine: DeltaEngine, config: SyncConfig, registry: PluginRegistry) -> Self {
		Self::from_arc_engine(Arc::new(engine), config, registry)
	}

	pub fn from_arc_engine(
		engine: Arc<DeltaEngine>,
		config: SyncConfig,
		registry: PluginRegistry,
	) -> Self {
		Self::from_arc_engine_with_instance_id(engine, config, registry, resolve_instance_id(None))
	}

	pub fn with_instance_id(
		engine: DeltaEngine,
		config: SyncConfig,
		registry: PluginRegistry,
		instance_id: impl Into<String>,
	) -> Self {
		Self::from_arc_engine_with_instance_id(
			Arc::new(engine),
			config,
			registry,
			instance_id.into(),
		)
	}

	pub fn from_arc_engine_with_instance_id(
		engine: Arc<DeltaEngine>,
		config: SyncConfig,
		registry: PluginRegistry,
		instance_id: String,
	) -> Self {
		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		Self {
			engine,
			config,
			registry: Arc::new(registry),
			instance_id,
			shutdown_tx,
			shutdown_rx,
		}
	}

	pub fn shutdown(&self) {
		let _ = self.shutdown_tx.send(true);
	}

	pub fn shutdown_tx_clone(&self) -> watch::Sender<bool> {
		self.shutdown_tx.clone()
	}

	pub async fn run(&self) -> Result<(), OversyncError> {
		let pipes = crate::recipes::expand_runtime_pipes(self.config.effective_pipes()).await?;
		let task_count: usize = pipes
			.iter()
			.filter(|pipe| pipe.enabled)
			.map(|pipe| pipe.queries.len())
			.sum();
		if task_count == 0 {
			info!("no enabled pipe queries configured, scheduler exiting");
			return Ok(());
		}

		let named_sinks = create_named_sinks(&self.config, self.registry.as_ref()).await?;
		let named_sinks = Arc::new(named_sinks);
		let mut handles = Vec::new();

		// Pre-create tables sequentially to avoid TiKV DDL lock contention.
		// On TiKV backend, concurrent DEFINE TABLE operations can deadlock.
		for pipe in &pipes {
			if !pipe.enabled {
				continue;
			}
			let pipe_engine = self.engine.for_source(&pipe.name);
			if let Err(e) = pipe_engine.ensure_tables().await {
				error!(pipe = %pipe.name, error = %e, "failed to create pipeline tables");
				return Err(e);
			}
		}

		for pipe in &pipes {
			if !pipe.enabled {
				info!(pipe = %pipe.name, "pipe disabled, skipping");
				continue;
			}

			for query in &pipe.queries {
				let query_sinks = resolve_pipe_query_sinks(&named_sinks, pipe, query)?;

				let engine = self.engine.clone();
				let registry = self.registry.clone();
				let pipe = pipe.clone();
				let query = query.clone();
				let instance_id = self.instance_id.clone();
				let mut shutdown = self.shutdown_rx.clone();

				let handle = tokio::spawn(async move {
					run_pipe_query(
						engine,
						registry,
						pipe,
						query,
						query_sinks,
						instance_id,
						&mut shutdown,
					)
					.await;
				});

				handles.push(handle);
			}
		}

		info!(tasks = handles.len(), "scheduler started");

		for handle in handles {
			let _ = handle.await;
		}

		info!("scheduler stopped");
		Ok(())
	}
}

pub async fn run_pipe_once(
	engine: Arc<DeltaEngine>,
	config: SyncConfig,
	registry: PluginRegistry,
	pipe_name: &str,
) -> Result<Vec<ManualRunResult>, OversyncError> {
	let pipes = crate::recipes::expand_runtime_pipes(config.effective_pipes()).await?;
	let pipe = pipes
		.into_iter()
		.find(|pipe| pipe.name == pipe_name)
		.ok_or_else(|| OversyncError::Config(format!("unknown pipe '{pipe_name}'")))?;

	if !pipe.enabled {
		return Err(OversyncError::Config(format!(
			"pipe '{pipe_name}' is disabled"
		)));
	}

	let named_sinks = create_named_sinks(&config, &registry).await?;
	let (effective_connector, connector_config) = build_effective_connector(&pipe)?;
	let connector = registry
		.create_source(&effective_connector, &pipe.name, &connector_config)
		.await?;

	let pipe_engine = Arc::new(engine.for_source(&pipe.name));
	pipe_engine.ensure_tables().await?;

	let mut results = Vec::with_capacity(pipe.queries.len());
	for query in &pipe.queries {
		let query_sinks = resolve_pipe_query_sinks(&named_sinks, &pipe, query)?;
		let diff =
			execute_query_with_retry(&pipe_engine, connector.as_ref(), &query_sinks, &pipe, query)
				.await?;
		results.push(ManualRunResult {
			query_id: query.id.clone(),
			created: diff.created.len(),
			updated: diff.updated.len(),
			deleted: diff.deleted.len(),
		});
	}

	Ok(results)
}

async fn create_named_sinks(
	config: &SyncConfig,
	registry: &PluginRegistry,
) -> Result<HashMap<String, Arc<dyn Sink>>, OversyncError> {
	let mut sinks = HashMap::new();
	for sink_def in &config.sinks {
		let sink = registry
			.create_sink(&sink_def.sink_type, &sink_def.name, &sink_def.config)
			.await?;
		sinks.insert(sink_def.name.clone(), Arc::from(sink));
	}
	if sinks.is_empty() {
		return Err(OversyncError::Config(
			"no sinks configured; define at least one [[sinks]] entry instead of relying on an implicit stdout fallback".into(),
		));
	}
	Ok(sinks)
}

/// Resolve sinks for a query within a pipe.
///
/// Priority: query-level sinks > pipe-level targets > all named sinks.
pub fn resolve_pipe_query_sinks(
	named_sinks: &HashMap<String, Arc<dyn Sink>>,
	pipe: &PipeConfig,
	query: &QueryDef,
) -> Result<Vec<Arc<dyn Sink>>, OversyncError> {
	let target_names = if let Some(ref query_sinks) = query.sinks {
		Some(query_sinks.as_slice())
	} else if !pipe.targets.is_empty() {
		Some(pipe.targets.as_slice())
	} else {
		None
	};

	match target_names {
		None => Ok(named_sinks.values().cloned().collect()),
		Some(names) => {
			let mut resolved = Vec::with_capacity(names.len());
			for name in names {
				let sink = named_sinks.get(name).ok_or_else(|| {
					OversyncError::Config(format!(
						"pipe '{}' query '{}': unknown sink '{name}'",
						pipe.name, query.id
					))
				})?;
				resolved.push(sink.clone());
			}
			Ok(resolved)
		}
	}
}

struct LockCycleContext<'a> {
	engine: &'a DeltaEngine,
	connector: &'a dyn OriginConnector,
	sinks: &'a [Arc<dyn Sink>],
	pipe: &'a PipeConfig,
	query: &'a QueryDef,
	interval: Duration,
	pipe_lock: crate::distributed_lock::PipeLock,
	lock_key: String,
	lock_ttl: u64,
}

async fn run_pipe_query(
	engine: Arc<DeltaEngine>,
	registry: Arc<PluginRegistry>,
	pipe: PipeConfig,
	query: QueryDef,
	sinks: Vec<Arc<dyn Sink>>,
	instance_id: String,
	shutdown: &mut watch::Receiver<bool>,
) {
	let (effective_connector, connector_config) = match build_effective_connector(&pipe) {
		Ok(result) => result,
		Err(e) => {
			error!(pipe = %pipe.name, error = %e, "failed to build connector config");
			return;
		}
	};
	let connector = match registry
		.create_source(&effective_connector, &pipe.name, &connector_config)
		.await
	{
		Ok(c) => c,
		Err(e) => {
			error!(
				pipe = %pipe.name,
				error = %e,
				"failed to create connector, task exiting"
			);
			return;
		}
	};

	// Tables already ensured by Scheduler::run() before spawning tasks.
	let pipe_engine = Arc::new(engine.for_source(&pipe.name));

	let interval_secs = pipe.schedule.interval_secs.max(1);
	let interval = Duration::from_secs(interval_secs);

	// Distributed lock for horizontal scaling
	let pipe_lock = crate::distributed_lock::PipeLock::new(
		std::sync::Arc::clone(engine.state_client()),
		instance_id,
	);
	let lock_key = format!("{}:{}", pipe.name, query.id);
	let lock_ttl = interval_secs.saturating_mul(3).max(60); // 3x interval or 60s min

	info!(
		pipe = %pipe.name,
		query = %query.id,
		interval_secs = interval_secs,
		max_retries = pipe.retry.max_retries,
		tables = ?pipe_engine.tables(),
		"polling task started"
	);

	let mut rate_limiter = pipe
		.schedule
		.max_requests_per_minute
		.map(crate::rate_limit::RateLimiter::per_minute);

	if let Some(ref mut rl) = rate_limiter {
		rl.acquire().await;
	}
	match pipe_lock.try_acquire(&lock_key, lock_ttl).await {
		Ok(true) => {
			run_timed_cycle_with_lock(LockCycleContext {
				engine: &pipe_engine,
				connector: connector.as_ref(),
				sinks: &sinks,
				pipe: &pipe,
				query: &query,
				interval,
				pipe_lock: pipe_lock.clone(),
				lock_key: lock_key.clone(),
				lock_ttl,
			})
			.await;
		}
		Ok(false) => {
			debug!(pipe = %pipe.name, "initial cycle skipped — lock held by another instance");
		}
		Err(e) => {
			warn!(pipe = %pipe.name, error = %e, "lock acquire failed — skipping cycle");
		}
	}

	let mut ticker = tokio::time::interval(interval);
	ticker.tick().await;

	match pipe.schedule.missed_tick_policy {
		MissedTickPolicy::Skip => {
			ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
		}
		MissedTickPolicy::Burst => {
			ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
		}
	}

	loop {
		tokio::select! {
			_ = ticker.tick() => {
				match pipe_lock.try_acquire(&lock_key, lock_ttl).await {
					Ok(true) => {}
					Ok(false) => {
						debug!(pipe = %pipe.name, query = %query.id, "skipping cycle — lock held by another instance");
						continue;
					}
					Err(e) => {
						warn!(pipe = %pipe.name, query = %query.id, error = %e, "lock acquire failed — skipping cycle");
						continue;
					}
				}
				if let Some(ref mut rl) = rate_limiter {
					rl.acquire().await;
				}
				run_timed_cycle_with_lock(LockCycleContext {
					engine: &pipe_engine,
					connector: connector.as_ref(),
					sinks: &sinks,
					pipe: &pipe,
					query: &query,
					interval,
					pipe_lock: pipe_lock.clone(),
					lock_key: lock_key.clone(),
					lock_ttl,
				})
				.await;
			}
			_ = shutdown.changed() => {
				info!(pipe = %pipe.name, query = %query.id, "shutting down");
				let _ = pipe_lock.release(&lock_key).await;
				break;
			}
		}
	}
}

fn resolve_instance_id(explicit: Option<String>) -> String {
	if let Some(instance_id) = explicit.filter(|value| !value.trim().is_empty()) {
		return instance_id;
	}

	if let Ok(instance_id) = std::env::var("OVERSYNC_INSTANCE_ID")
		&& !instance_id.trim().is_empty()
	{
		return instance_id;
	}

	let hostname = hostname::get()
		.map(|value| value.to_string_lossy().into_owned())
		.unwrap_or_else(|_| "unknown-host".into());
	let pid = std::process::id();
	let boot_id = Uuid::new_v4().simple().to_string();
	format!("{hostname}:{pid}:{boot_id}")
}

async fn run_timed_cycle_with_lock(ctx: LockCycleContext<'_>) {
	let LockCycleContext {
		engine,
		connector,
		sinks,
		pipe,
		query,
		interval,
		pipe_lock,
		lock_key,
		lock_ttl,
	} = ctx;

	let renew_every = Duration::from_secs((lock_ttl / 3).max(5));
	let (stop_tx, mut stop_rx) = watch::channel(false);
	let renew_lock = pipe_lock.clone();
	let renew_pipe_name = pipe.name.clone();
	let renew_query_id = query.id.clone();
	let renew_lock_key = lock_key.clone();

	let renew_task = tokio::spawn(async move {
		let mut ticker = tokio::time::interval(renew_every);
		ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
		ticker.tick().await;

		loop {
			tokio::select! {
				_ = ticker.tick() => {
					match renew_lock.renew(&renew_lock_key, lock_ttl).await {
						Ok(true) => {}
						Ok(false) => {
							warn!(pipe = %renew_pipe_name, query = %renew_query_id, "lock renewal failed — lock no longer held");
							break;
						}
						Err(e) => {
							warn!(pipe = %renew_pipe_name, query = %renew_query_id, error = %e, "lock renewal failed");
						}
					}
				}
				changed = stop_rx.changed() => {
					if changed.is_err() || *stop_rx.borrow() {
						break;
					}
				}
			}
		}
	});

	run_timed_cycle(engine, connector, sinks, pipe, query, interval).await;

	let _ = stop_tx.send(true);
	let _ = renew_task.await;

	if let Err(e) = pipe_lock.release(&lock_key).await {
		warn!(pipe = %pipe.name, error = %e, "failed to release lock (will expire via TTL)");
	}
}

async fn run_timed_cycle(
	engine: &DeltaEngine,
	connector: &dyn OriginConnector,
	sinks: &[Arc<dyn Sink>],
	pipe: &PipeConfig,
	query: &QueryDef,
	interval: Duration,
) {
	let start = Instant::now();
	run_with_retry(engine, connector, sinks, pipe, query).await;
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

async fn run_with_retry(
	engine: &DeltaEngine,
	connector: &dyn OriginConnector,
	sinks: &[Arc<dyn Sink>],
	pipe: &PipeConfig,
	query: &QueryDef,
) {
	match execute_query_with_retry(engine, connector, sinks, pipe, query).await {
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
		}
		Err(e) => {
			error!(
				pipe = %pipe.name,
				query = %query.id,
				attempts = pipe.retry.max_retries + 1,
				error = %e,
				"cycle failed after all retries"
			);
		}
	}
}

fn build_effective_connector(
	pipe: &PipeConfig,
) -> Result<(String, serde_json::Value), OversyncError> {
	if !pipe.origin.needs_trino_bridge() {
		let mut map = match &pipe.origin.config {
			serde_json::Value::Object(m) => m.clone(),
			_ => serde_json::Map::new(),
		};
		map.insert(
			"dsn".into(),
			serde_json::Value::String(pipe.origin.dsn.clone()),
		);
		return Ok((
			pipe.origin.connector.clone(),
			serde_json::Value::Object(map),
		));
	}

	let trino_url = pipe.origin.trino_url.as_deref().ok_or_else(|| {
		OversyncError::Config(format!(
			"pipe '{}': connector '{}' requires trino_url",
			pipe.name, pipe.origin.connector
		))
	})?;

	info!(
		pipe = %pipe.name,
		connector = %pipe.origin.connector,
		trino_url = %trino_url,
		"non-native connector, routing through Trino"
	);

	Ok((
		"trino".to_string(),
		serde_json::json!({
			"dsn": trino_url,
			"catalog": pipe.origin.connector,
		}),
	))
}

async fn execute_query_with_retry(
	engine: &DeltaEngine,
	connector: &dyn OriginConnector,
	sinks: &[Arc<dyn Sink>],
	pipe: &PipeConfig,
	query: &QueryDef,
) -> Result<DeltaResult, OversyncError> {
	let cycle_config = CycleConfig {
		origin_id: pipe.name.clone(),
		query_id: query.id.clone(),
		sql: query.sql.clone(),
		key_column: query.key_column.clone(),
		fail_safe_threshold: pipe.delta.fail_safe_threshold,
		diff_mode: pipe.delta.diff_mode.clone(),
		transform: query.transform.clone(),
		links: vec![],
	};

	let mut runner = CycleRunner::new(engine, connector, sinks);

	if !pipe.filters.is_empty() {
		match oversync_transforms::parse_steps(&pipe.filters) {
			Ok(chain) => {
				runner = runner.with_pre_filter(Arc::new(chain));
			}
			Err(e) => {
				return Err(e);
			}
		}
	}

	if !pipe.transforms.is_empty() {
		match oversync_transforms::parse_steps(&pipe.transforms) {
			Ok(chain) => {
				runner = runner.with_transform(Arc::new(chain));
			}
			Err(e) => {
				return Err(e);
			}
		}
	}

	for attempt in 0..=pipe.retry.max_retries {
		match runner.run(&cycle_config).await {
			Ok(diff) => return Ok(diff),
			Err(e) => {
				if attempt < pipe.retry.max_retries {
					let delay = Duration::from_secs(
						pipe.retry
							.retry_base_delay_secs
							.saturating_mul(2u64.saturating_pow(attempt)),
					);
					warn!(
						pipe = %pipe.name,
						query = %query.id,
						attempt = attempt + 1,
						max_retries = pipe.retry.max_retries,
						delay_secs = delay.as_secs(),
						error = %e,
						"cycle failed, retrying"
					);
					tokio::time::sleep(delay).await;
				} else {
					return Err(e);
				}
			}
		}
	}

	Err(OversyncError::Internal(format!(
		"pipe '{}' query '{}' failed without a concrete error",
		pipe.name, query.id
	)))
}

#[cfg(test)]
mod tests {
	use super::resolve_instance_id;

	#[test]
	fn generated_instance_ids_are_unique_without_override() {
		let first = resolve_instance_id(None);
		let second = resolve_instance_id(None);
		assert_ne!(first, second);
		assert!(!first.is_empty());
		assert!(!second.is_empty());
	}

	#[test]
	fn explicit_instance_id_wins() {
		let instance_id = resolve_instance_id(Some("manual-instance".into()));
		assert_eq!(instance_id, "manual-instance");
	}
}
