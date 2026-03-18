use tokio::sync::watch;

use oversync_core::error::OversyncError;
use oversync_delta::DeltaEngine;

use crate::config::SyncConfig;
use crate::registry::PluginRegistry;
use crate::scheduler::Scheduler;

pub struct OversyncEngine {
	scheduler: Scheduler,
}

impl OversyncEngine {
	pub fn new(engine: DeltaEngine, config: SyncConfig, registry: PluginRegistry) -> Self {
		Self {
			scheduler: Scheduler::new(engine, config, registry),
		}
	}

	pub async fn run(&self) -> Result<(), OversyncError> {
		self.scheduler.run().await
	}

	pub fn shutdown(&self) {
		self.scheduler.shutdown();
	}

	pub fn shutdown_handle(&self) -> watch::Sender<bool> {
		self.scheduler.shutdown_tx_clone()
	}
}
