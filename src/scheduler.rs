use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{error, info, warn};

use oversync_connectors::PostgresConnector;
use oversync_core::error::OversyncError;
use oversync_core::traits::Sink;
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutSink;

use crate::config::{QueryDef, SourceDef, SyncConfig};
use crate::cycle::{CycleConfig, CycleRunner};

pub struct Scheduler {
	engine: Arc<DeltaEngine>,
	config: SyncConfig,
	shutdown_tx: watch::Sender<bool>,
	shutdown_rx: watch::Receiver<bool>,
}

impl Scheduler {
	pub fn new(engine: DeltaEngine, config: SyncConfig) -> Self {
		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		Self {
			engine: Arc::new(engine),
			config,
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
		let mut handles = Vec::new();

		for source in &self.config.sources {
			for query in &source.queries {
				let engine = self.engine.clone();
				let source = source.clone();
				let query = query.clone();
				let mut shutdown = self.shutdown_rx.clone();

				let handle = tokio::spawn(async move {
					run_source_query(engine, source, query, &mut shutdown).await;
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

async fn run_source_query(
	engine: Arc<DeltaEngine>,
	source: SourceDef,
	query: QueryDef,
	shutdown: &mut watch::Receiver<bool>,
) {
	let connector = match create_connector(&source).await {
		Ok(c) => c,
		Err(e) => {
			error!(source = %source.name, error = %e, "failed to create connector, task exiting");
			return;
		}
	};

	let sinks: Vec<Box<dyn Sink>> = vec![Box::new(StdoutSink::new(false))];
	let interval = Duration::from_secs(source.interval_secs);

	info!(
		source = %source.name,
		query = %query.id,
		interval_secs = source.interval_secs,
		max_retries = source.max_retries,
		"polling task started"
	);

	// Run first cycle immediately (with retry)
	run_with_retry(&engine, &connector, &sinks, &source, &query).await;

	let mut ticker = tokio::time::interval(interval);
	ticker.tick().await; // skip first immediate tick

	loop {
		tokio::select! {
			_ = ticker.tick() => {
				run_with_retry(&engine, &connector, &sinks, &source, &query).await;
			}
			_ = shutdown.changed() => {
				info!(source = %source.name, query = %query.id, "shutting down");
				break;
			}
		}
	}
}

async fn run_with_retry(
	engine: &DeltaEngine,
	connector: &PostgresConnector,
	sinks: &[Box<dyn Sink>],
	source: &SourceDef,
	query: &QueryDef,
) {
	let cycle_config = CycleConfig {
		source_id: source.name.clone(),
		query_id: query.id.clone(),
		sql: query.sql.clone(),
		key_column: query.key_column.clone(),
		fail_safe_threshold: source.fail_safe_threshold,
		diff_mode: source.diff_mode.clone(),
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
					let delay = Duration::from_secs(
						source.retry_base_delay_secs * 2u64.pow(attempt),
					);
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

async fn create_connector(source: &SourceDef) -> Result<PostgresConnector, OversyncError> {
	match source.connector.as_str() {
		"postgres" => PostgresConnector::new(&source.name, &source.dsn).await,
		other => Err(OversyncError::Config(format!(
			"unknown connector: {other}"
		))),
	}
}
