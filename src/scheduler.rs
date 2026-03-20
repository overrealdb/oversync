use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

use oversync_core::error::OversyncError;
use oversync_core::traits::{Sink, OriginConnector};
use oversync_delta::DeltaEngine;

use crate::config::{MissedTickPolicy, PipeConfig, QueryDef, SyncConfig};
use crate::cycle::{CycleConfig, CycleRunner};
use crate::registry::PluginRegistry;

pub struct Scheduler {
	engine: Arc<DeltaEngine>,
	config: SyncConfig,
	registry: Arc<PluginRegistry>,
	shutdown_tx: watch::Sender<bool>,
	shutdown_rx: watch::Receiver<bool>,
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
		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		Self {
			engine,
			config,
			registry: Arc::new(registry),
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
		let named_sinks = self.create_sinks().await?;
		let named_sinks = Arc::new(named_sinks);
		let mut handles = Vec::new();

		let pipes = self.config.effective_pipes();
		for pipe in &pipes {
			if !pipe.enabled {
				info!(pipe = %pipe.name, "pipe disabled, skipping");
				continue;
			}

			for query in &pipe.queries {
				let query_sinks =
					resolve_pipe_query_sinks(&named_sinks, pipe, query)?;

				let engine = self.engine.clone();
				let registry = self.registry.clone();
				let pipe = pipe.clone();
				let query = query.clone();
				let mut shutdown = self.shutdown_rx.clone();

				let handle = tokio::spawn(async move {
					run_pipe_query(engine, registry, pipe, query, query_sinks, &mut shutdown)
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

	async fn create_sinks(&self) -> Result<HashMap<String, Arc<dyn Sink>>, OversyncError> {
		let mut sinks = HashMap::new();
		for sink_def in &self.config.sinks {
			let sink = self
				.registry
				.create_sink(&sink_def.sink_type, &sink_def.name, &sink_def.config)
				.await?;
			sinks.insert(sink_def.name.clone(), Arc::from(sink));
		}
		if sinks.is_empty() {
			let sink = self
				.registry
				.create_sink("stdout", "default", &serde_json::json!({}))
				.await?;
			sinks.insert("default".into(), Arc::from(sink));
		}
		Ok(sinks)
	}
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

async fn run_pipe_query(
	engine: Arc<DeltaEngine>,
	registry: Arc<PluginRegistry>,
	pipe: PipeConfig,
	query: QueryDef,
	sinks: Vec<Arc<dyn Sink>>,
	shutdown: &mut watch::Receiver<bool>,
) {
	let connector_config = {
		let mut map = match &pipe.origin.config {
			serde_json::Value::Object(m) => m.clone(),
			_ => serde_json::Map::new(),
		};
		map.insert("dsn".into(), serde_json::Value::String(pipe.origin.dsn.clone()));
		serde_json::Value::Object(map)
	};
	let connector = match registry
		.create_source(&pipe.origin.connector, &pipe.name, &connector_config)
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

	let pipe_engine = engine.for_source(&pipe.name);
	if let Err(e) = pipe_engine.ensure_tables().await {
		error!(pipe = %pipe.name, error = %e, "failed to create pipeline tables");
		return;
	}

	let pipe_engine = Arc::new(pipe_engine);

	let interval_secs = pipe.schedule.interval_secs.max(1);
	let interval = Duration::from_secs(interval_secs);

	info!(
		pipe = %pipe.name,
		query = %query.id,
		interval_secs = interval_secs,
		max_retries = pipe.retry.max_retries,
		tables = ?pipe_engine.tables(),
		"polling task started"
	);

	run_timed_cycle(&pipe_engine, connector.as_ref(), &sinks, &pipe, &query, interval).await;

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
				run_timed_cycle(
					&pipe_engine,
					connector.as_ref(),
					&sinks,
					&pipe,
					&query,
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
						query = %query.id,
						attempt = attempt + 1,
						max_retries = pipe.retry.max_retries,
						delay_secs = delay.as_secs(),
						error = %e,
						"cycle failed, retrying"
					);
					tokio::time::sleep(delay).await;
				} else {
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
	}
}
