use std::sync::Arc;

use oversync_core::error::OversyncError;
use oversync_core::model::{CycleStatus, DeltaEvent, DeltaResult, EventEnvelope};
use oversync_core::traits::{Sink, SourceConnector, TransformHook};
use oversync_delta::{DeltaEngine, check_fail_safe};
use tracing::{Instrument, error, info, warn};

use crate::config::DiffMode;

pub struct CycleConfig {
	pub source_id: String,
	pub query_id: String,
	pub sql: String,
	pub key_column: String,
	pub fail_safe_threshold: f64,
	pub diff_mode: DiffMode,
	pub transform: Option<String>,
}

pub struct CycleRunner<'a> {
	engine: &'a DeltaEngine,
	connector: &'a dyn SourceConnector,
	sinks: &'a [Arc<dyn Sink>],
	transform_hook: Option<Arc<dyn TransformHook>>,
}

impl<'a> CycleRunner<'a> {
	pub fn new(
		engine: &'a DeltaEngine,
		connector: &'a dyn SourceConnector,
		sinks: &'a [Arc<dyn Sink>],
	) -> Self {
		Self {
			engine,
			connector,
			sinks,
			transform_hook: None,
		}
	}

	pub fn with_transform(mut self, hook: Arc<dyn TransformHook>) -> Self {
		self.transform_hook = Some(hook);
		self
	}

	#[tracing::instrument(skip(self, config), fields(source = %config.source_id, query = %config.query_id))]
	pub async fn run(&self, config: &CycleConfig) -> Result<DeltaResult, OversyncError> {
		self.deliver_pending(config).await;

		let cycle_id = self
			.engine
			.next_cycle_id(&config.source_id, &config.query_id)
			.await?;

		info!(
			source = %config.source_id,
			query = %config.query_id,
			cycle = cycle_id,
			"starting cycle"
		);

		self.engine
			.log_cycle_start(&config.source_id, &config.query_id, cycle_id)
			.await?;

		let result = self.run_inner(config, cycle_id).await;

		match &result {
			Ok(diff) => {
				self.engine
					.log_cycle_finish(
						&config.source_id,
						&config.query_id,
						cycle_id,
						CycleStatus::Success,
						diff.total() as i64,
						diff.created.len() as i64,
						diff.updated.len() as i64,
						diff.deleted.len() as i64,
					)
					.await?;
				info!(
					source = %config.source_id,
					cycle = cycle_id,
					created = diff.created.len(),
					updated = diff.updated.len(),
					deleted = diff.deleted.len(),
					"cycle complete"
				);
			}
			Err(e) => {
				let status = if e.to_string().contains("fail-safe") {
					CycleStatus::Aborted
				} else {
					CycleStatus::Failed
				};
				let _ = self
					.engine
					.log_cycle_finish(
						&config.source_id,
						&config.query_id,
						cycle_id,
						status,
						0,
						0,
						0,
						0,
					)
					.await;
				error!(source = %config.source_id, cycle = cycle_id, error = %e, "cycle failed");
			}
		}

		result
	}

	async fn run_inner(
		&self,
		config: &CycleConfig,
		cycle_id: i64,
	) -> Result<DeltaResult, OversyncError> {
		let (diff, total_upserted) = match config.diff_mode {
			DiffMode::Memory => self.run_memory_diff(config, cycle_id).await?,
			DiffMode::Db => self.run_db_diff(config, cycle_id).await?,
		};

		let deleted_count = diff.deleted.len();
		let previous_count = total_upserted + deleted_count;

		if !check_fail_safe(previous_count, deleted_count, config.fail_safe_threshold) {
			warn!(
				source = %config.source_id,
				previous = previous_count,
				deleted = deleted_count,
				threshold = config.fail_safe_threshold,
				"fail-safe triggered"
			);
			return Err(OversyncError::Internal(format!(
				"fail-safe: {deleted_count}/{previous_count} rows deleted (>{:.0}%)",
				config.fail_safe_threshold,
			)));
		}

		if !diff.is_empty() {
			let mut envelopes: Vec<EventEnvelope> = diff
				.created
				.iter()
				.chain(diff.updated.iter())
				.chain(diff.deleted.iter())
				.map(EventEnvelope::from)
				.collect();

			if let Some(ref hook) = self.transform_hook {
				envelopes = hook.transform(envelopes).await?;
			} else if let Some(ref fn_name) = config.transform {
				envelopes = self.engine.apply_transform(fn_name, envelopes).await?;
			}

			let delivered = async { self.deliver_paged(config, cycle_id, &envelopes).await }
				.instrument(tracing::info_span!("deliver", source = %config.source_id))
				.await?;

			if !delivered {
				return Err(OversyncError::Sink(
					"delivery failed, events pending".into(),
				));
			}
		}

		async {
			self.engine
				.delete_stale(&config.source_id, &config.query_id, cycle_id)
				.await
		}
		.instrument(tracing::info_span!("cleanup", source = %config.source_id))
		.await?;

		Ok(diff)
	}

	/// DB-side diff: prep_prev_hash → stream upsert → compute_delta_from_db.
	/// Low memory but slow on large datasets (SurrealQL interpreter bottleneck).
	async fn run_db_diff(
		&self,
		config: &CycleConfig,
		cycle_id: i64,
	) -> Result<(DeltaResult, usize), OversyncError> {
		self.engine
			.prep_prev_hash(&config.source_id, &config.query_id)
			.await?;

		let total = self.stream_and_upsert(config, cycle_id).await?;

		let diff = async {
			self.engine
				.compute_delta_from_db(&config.source_id, &config.query_id, cycle_id)
				.await
		}
		.instrument(tracing::info_span!("compute_delta", source = %config.source_id))
		.await?;

		Ok((diff, total))
	}

	/// Rust-native diff: read snapshot keys → fetch all → compute_diff (HashMap).
	/// Fast but needs O(keys) memory. Best for datasets up to ~5M rows.
	async fn run_memory_diff(
		&self,
		config: &CycleConfig,
		cycle_id: i64,
	) -> Result<(DeltaResult, usize), OversyncError> {
		let previous = async {
			self.engine
				.read_snapshot_keys_paged(&config.source_id, &config.query_id)
				.await
		}
		.instrument(tracing::info_span!("read_keys", source = %config.source_id))
		.await?;

		info!(
			source = %config.source_id,
			keys = previous.len(),
			"loaded snapshot keys for memory diff"
		);

		let total = self.stream_and_upsert(config, cycle_id).await?;

		// Read current keys+hashes after upsert
		let current = async {
			self.engine
				.read_snapshot_keys_paged(&config.source_id, &config.query_id)
				.await
		}
		.instrument(tracing::info_span!("read_current", source = %config.source_id))
		.await?;

		// Direct hash comparison (no re-hashing needed)
		let now = chrono::Utc::now();
		let mut diff = DeltaResult::default();

		for (key, hash) in &current {
			match previous.get(key) {
				None => diff.created.push(DeltaEvent {
					op: oversync_core::model::OpType::Created,
					source_id: config.source_id.clone(),
					query_id: config.query_id.clone(),
					row_key: key.clone(),
					row_data: serde_json::Value::Null,
					row_hash: hash.clone(),
					cycle_id: cycle_id as u64,
					timestamp: now,
				}),
				Some(prev_hash) if prev_hash != hash => diff.updated.push(DeltaEvent {
					op: oversync_core::model::OpType::Updated,
					source_id: config.source_id.clone(),
					query_id: config.query_id.clone(),
					row_key: key.clone(),
					row_data: serde_json::Value::Null,
					row_hash: hash.clone(),
					cycle_id: cycle_id as u64,
					timestamp: now,
				}),
				_ => {}
			}
		}

		for (key, hash) in &previous {
			if !current.contains_key(key) {
				diff.deleted.push(DeltaEvent {
					op: oversync_core::model::OpType::Deleted,
					source_id: config.source_id.clone(),
					query_id: config.query_id.clone(),
					row_key: key.clone(),
					row_data: serde_json::Value::Null,
					row_hash: hash.clone(),
					cycle_id: cycle_id as u64,
					timestamp: now,
				});
			}
		}

		info!(
			source = %config.source_id,
			created = diff.created.len(),
			updated = diff.updated.len(),
			deleted = diff.deleted.len(),
			"memory diff computed"
		);

		Ok((diff, total))
	}

	async fn stream_and_upsert(
		&self,
		config: &CycleConfig,
		cycle_id: i64,
	) -> Result<usize, OversyncError> {
		let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<oversync_core::model::RawRow>>(4);

		let engine = &self.engine;
		let source_id = config.source_id.clone();
		let query_id = config.query_id.clone();
		let sql = config.sql.clone();
		let key_col = config.key_column.clone();
		let connector = self.connector;

		let fetch_span = tracing::info_span!("fetch", source = %config.source_id);
		let upsert_span = tracing::info_span!("upsert", source = %config.source_id);

		let producer =
			async { connector.fetch_into(&sql, &key_col, 500, tx).await }.instrument(fetch_span);

		let consumer = async {
			let mut total: usize = 0;
			while let Some(batch) = rx.recv().await {
				let n = batch.len();
				engine
					.upsert_batch_raw(&source_id, &query_id, cycle_id, &batch)
					.await?;
				total += n;
			}
			Ok::<usize, OversyncError>(total)
		}
		.instrument(upsert_span);

		let (fetch_result, upsert_result) = tokio::join!(producer, consumer);
		fetch_result?;
		let total = upsert_result?;

		info!(source = %config.source_id, fetched = total, "ingested rows");
		Ok(total)
	}

	/// Deliver envelopes in pages of 1000. Each page saved to outbox before sending.
	async fn deliver_paged(
		&self,
		config: &CycleConfig,
		cycle_id: i64,
		envelopes: &[EventEnvelope],
	) -> Result<bool, OversyncError> {
		const PAGE: usize = 1000;

		for (i, chunk) in envelopes.chunks(PAGE).enumerate() {
			let envelopes = chunk;

			let page_id = cycle_id * 10000 + i as i64;

			// Save to outbox before delivery (crash-safe)
			self.engine
				.save_pending_events(&config.source_id, &config.query_id, page_id, &envelopes)
				.await?;

			// Deliver to all sinks
			if !self.deliver_to_sinks(&envelopes).await {
				warn!(
					source = %config.source_id,
					page = i,
					events = envelopes.len(),
					"sink delivery failed, events in outbox"
				);
				return Ok(false);
			}

			// Clear outbox page
			self.engine
				.delete_pending_events(&config.source_id, &config.query_id, page_id)
				.await?;
		}

		Ok(true)
	}

	async fn deliver_to_sinks(&self, envelopes: &[EventEnvelope]) -> bool {
		for sink in self.sinks {
			if let Err(e) = sink.send_batch(envelopes).await {
				error!(sink = sink.name(), error = %e, "sink delivery failed");
				return false;
			}
		}
		true
	}

	async fn deliver_pending(&self, config: &CycleConfig) {
		let pending = match self
			.engine
			.read_pending_events(&config.source_id, &config.query_id)
			.await
		{
			Ok(p) => p,
			Err(e) => {
				warn!(error = %e, "failed to read pending events");
				return;
			}
		};

		if pending.is_empty() {
			return;
		}

		info!(
			source = %config.source_id,
			batches = pending.len(),
			"retrying pending event delivery"
		);

		let mut max_delivered = 0i64;
		for (page_id, envelopes) in &pending {
			if self.deliver_to_sinks(envelopes).await {
				max_delivered = *page_id;
			} else {
				break;
			}
		}

		if max_delivered > 0 {
			let _ = self
				.engine
				.delete_pending_events(&config.source_id, &config.query_id, max_delivered)
				.await;
		}
	}
}
