use std::sync::Arc;

use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use oversync_core::error::OversyncError;
use oversync_delta::DeltaEngine;

use crate::config::SyncConfig;
use crate::registry::PluginRegistry;
use crate::scheduler::{ManualRunResult, Scheduler};

pub struct LifecycleManager {
	engine: Arc<DeltaEngine>,
	registry: PluginRegistry,
	inner: Arc<Mutex<Inner>>,
}

struct Inner {
	shutdown_tx: Option<watch::Sender<bool>>,
	run_handle: Option<JoinHandle<()>>,
	config: Option<SyncConfig>,
	paused: bool,
}

impl LifecycleManager {
	pub fn new(engine: DeltaEngine, registry: PluginRegistry) -> Self {
		Self {
			engine: Arc::new(engine),
			registry,
			inner: Arc::new(Mutex::new(Inner {
				shutdown_tx: None,
				run_handle: None,
				config: None,
				paused: false,
			})),
		}
	}

	pub fn from_arc_engine(engine: Arc<DeltaEngine>, registry: PluginRegistry) -> Self {
		Self {
			engine,
			registry,
			inner: Arc::new(Mutex::new(Inner {
				shutdown_tx: None,
				run_handle: None,
				config: None,
				paused: false,
			})),
		}
	}

	pub async fn start(&self, config: SyncConfig) -> Result<(), OversyncError> {
		let mut inner = self.inner.lock().await;
		self.stop_inner(&mut inner).await;

		inner.config = Some(config.clone());
		if inner.paused {
			info!("lifecycle: config updated while paused, will apply on resume");
			return Ok(());
		}

		self.spawn_inner(&mut inner, config);
		Ok(())
	}

	pub async fn shutdown(&self) {
		let mut inner = self.inner.lock().await;
		self.stop_inner(&mut inner).await;
		inner.config = None;
		info!("lifecycle: shutdown");
	}

	pub async fn pause(&self) {
		let mut inner = self.inner.lock().await;
		if inner.paused {
			return;
		}
		inner.paused = true;
		self.stop_inner(&mut inner).await;
		info!("lifecycle: paused");
	}

	pub async fn resume(&self) -> Result<(), OversyncError> {
		let mut inner = self.inner.lock().await;
		if !inner.paused {
			return Ok(());
		}
		inner.paused = false;
		if let Some(config) = inner.config.clone() {
			self.spawn_inner(&mut inner, config);
			info!("lifecycle: resumed");
		} else {
			warn!("lifecycle: resume called but no config available");
		}
		Ok(())
	}

	pub async fn is_running(&self) -> bool {
		let inner = self.inner.lock().await;
		inner.shutdown_tx.is_some() && !inner.paused
	}

	pub async fn is_paused(&self) -> bool {
		let inner = self.inner.lock().await;
		inner.paused
	}

	pub async fn current_config(&self) -> Option<SyncConfig> {
		let inner = self.inner.lock().await;
		inner.config.clone()
	}

	pub async fn run_pipe_once(
		&self,
		pipe_name: &str,
	) -> Result<Vec<ManualRunResult>, OversyncError> {
		let config = self
			.current_config()
			.await
			.ok_or_else(|| OversyncError::Config("no runtime config loaded".into()))?;

		crate::scheduler::run_pipe_once(
			Arc::clone(&self.engine),
			config,
			self.registry.clone(),
			pipe_name,
		)
		.await
	}

	async fn stop_inner(&self, inner: &mut Inner) {
		if let Some(tx) = inner.shutdown_tx.take() {
			let _ = tx.send(true);
		}
		if let Some(handle) = inner.run_handle.take() {
			let _ = handle.await;
		}
	}

	fn spawn_inner(&self, inner: &mut Inner, config: SyncConfig) {
		let engine = Arc::clone(&self.engine);
		let registry = self.registry.clone();
		let scheduler = Scheduler::from_arc_engine(engine, config, registry);
		let shutdown_tx = scheduler.shutdown_tx_clone();

		let handle = tokio::spawn(async move {
			if let Err(e) = scheduler.run().await {
				warn!(error = %e, "scheduler exited with error");
			}
		});

		inner.shutdown_tx = Some(shutdown_tx);
		inner.run_handle = Some(handle);
	}
}
