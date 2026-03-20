use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

use oversync_core::error::OversyncError;
use oversync_core::traits::{Sink, OriginConnector};
use oversync_delta::DeltaEngine;

use crate::config::{QueryDef, SourceDef, SyncConfig};
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

		for source in &self.config.sources {
			for query in &source.queries {
				let query_sinks =
					resolve_query_sinks(&named_sinks, &query.sinks, &source.name, &query.id)?;

				let engine = self.engine.clone();
				let registry = self.registry.clone();
				let source = source.clone();
				let query = query.clone();
				let mut shutdown = self.shutdown_rx.clone();

				let handle = tokio::spawn(async move {
					run_source_query(engine, registry, source, query, query_sinks, &mut shutdown)
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
		// Default to stdout if no sinks configured
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

pub fn resolve_query_sinks(
	named_sinks: &HashMap<String, Arc<dyn Sink>>,
	query_sinks: &Option<Vec<String>>,
	source_name: &str,
	query_id: &str,
) -> Result<Vec<Arc<dyn Sink>>, OversyncError> {
	match query_sinks {
		None => Ok(named_sinks.values().cloned().collect()),
		Some(names) => {
			let mut resolved = Vec::with_capacity(names.len());
			for name in names {
				let sink = named_sinks.get(name).ok_or_else(|| {
					OversyncError::Config(format!(
						"source '{source_name}' query '{query_id}': unknown sink '{name}'"
					))
				})?;
				resolved.push(sink.clone());
			}
			Ok(resolved)
		}
	}
}

async fn run_source_query(
	engine: Arc<DeltaEngine>,
	registry: Arc<PluginRegistry>,
	source: SourceDef,
	query: QueryDef,
	sinks: Vec<Arc<dyn Sink>>,
	shutdown: &mut watch::Receiver<bool>,
) {
	let connector_config = {
		let mut map = match &source.config {
			serde_json::Value::Object(m) => m.clone(),
			_ => serde_json::Map::new(),
		};
		map.insert("dsn".into(), serde_json::Value::String(source.dsn.clone()));
		serde_json::Value::Object(map)
	};
	let connector = match registry
		.create_source(&source.connector, &source.name, &connector_config)
		.await
	{
		Ok(c) => c,
		Err(e) => {
			error!(
				source = %source.name,
				error = %e,
				"failed to create connector, task exiting"
			);
			return;
		}
	};

	// Per-source delta engine with isolated tables.
	let source_engine = engine.for_source(&source.name);
	if let Err(e) = source_engine.ensure_tables().await {
		error!(source = %source.name, error = %e, "failed to create pipeline tables");
		return;
	}

	let source_engine = Arc::new(source_engine);

	let interval = Duration::from_secs(source.interval_secs);

	info!(
		source = %source.name,
		query = %query.id,
		interval_secs = source.interval_secs,
		max_retries = source.max_retries,
		tables = ?source_engine.tables(),
		"polling task started"
	);

	run_timed_cycle(&source_engine, connector.as_ref(), &sinks, &source, &query, interval).await;

	let mut ticker = tokio::time::interval(interval);
	ticker.tick().await;

	match source.missed_tick_policy {
		crate::config::MissedTickPolicy::Skip => {
			ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
		}
		crate::config::MissedTickPolicy::Burst => {
			ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
		}
	}

	loop {
		tokio::select! {
			_ = ticker.tick() => {
				run_timed_cycle(
					&source_engine,
					connector.as_ref(),
					&sinks,
					&source,
					&query,
					interval,
				).await;
			}
			_ = shutdown.changed() => {
				info!(source = %source.name, query = %query.id, "shutting down");
				break;
			}
		}
	}
}

async fn run_timed_cycle(
	engine: &DeltaEngine,
	connector: &dyn OriginConnector,
	sinks: &[Arc<dyn Sink>],
	source: &SourceDef,
	query: &QueryDef,
	interval: Duration,
) {
	let start = Instant::now();
	run_with_retry(engine, connector, sinks, source, query).await;
	let elapsed = start.elapsed();

	if elapsed > interval {
		warn!(
			source = %source.name,
			query = %query.id,
			elapsed_secs = elapsed.as_secs(),
			interval_secs = interval.as_secs(),
			policy = ?source.missed_tick_policy,
			"cycle took longer than polling interval"
		);
	}
}

async fn run_with_retry(
	engine: &DeltaEngine,
	connector: &dyn OriginConnector,
	sinks: &[Arc<dyn Sink>],
	source: &SourceDef,
	query: &QueryDef,
) {
	let cycle_config = CycleConfig {
		origin_id: source.name.clone(),
		query_id: query.id.clone(),
		sql: query.sql.clone(),
		key_column: query.key_column.clone(),
		fail_safe_threshold: source.fail_safe_threshold,
		diff_mode: source.diff_mode.clone(),
		transform: query.transform.clone(),
	};

	let runner = CycleRunner::new(engine, connector, sinks);

	for attempt in 0..=source.max_retries {
		match runner.run(&cycle_config).await {
			Ok(diff) => {
				if !diff.is_empty() {
					info!(
						source = %source.name,
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
				if attempt < source.max_retries {
					let delay =
						Duration::from_secs(source.retry_base_delay_secs * 2u64.pow(attempt));
					warn!(
						source = %source.name,
						query = %query.id,
						attempt = attempt + 1,
						max_retries = source.max_retries,
						delay_secs = delay.as_secs(),
						error = %e,
						"cycle failed, retrying"
					);
					tokio::time::sleep(delay).await;
				} else {
					error!(
						source = %source.name,
						query = %query.id,
						attempts = source.max_retries + 1,
						error = %e,
						"cycle failed after all retries"
					);
				}
			}
		}
	}
}
