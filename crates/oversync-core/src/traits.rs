use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::OversyncError;
use crate::model::{EventEnvelope, RawRow};

/// Rust-native event transform hook. Consumers implement this to modify
/// [`EventEnvelope`]s in-flight before they reach sinks. Takes precedence
/// over SurrealQL `fn::*` transforms when set on a [`CycleRunner`].
#[async_trait]
pub trait TransformHook: Send + Sync {
	async fn transform(
		&self,
		envelopes: Vec<EventEnvelope>,
	) -> Result<Vec<EventEnvelope>, OversyncError>;
}

/// An origin connector fetches rows from an external data source.
///
/// Implementations exist for PostgreSQL, MySQL, Trino, ClickHouse, HTTP APIs,
/// GraphQL, and Apache Arrow Flight SQL. Each connector is created via an
/// [`OriginFactory`] from a JSON config object.
///
/// # Lifecycle
///
/// 1. Factory creates the connector (connection pool established)
/// 2. [`fetch_all`] or [`fetch_into`] called once per cycle
/// 3. Connector is reused across cycles (connection pooling)
#[async_trait]
pub trait OriginConnector: Send + Sync {
	/// Human-readable name of this connector instance.
	fn name(&self) -> &str;

	/// Fetch all rows matching the query. Returns the full result set in memory.
	async fn fetch_all(&self, sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError>;

	/// Stream rows in batches into a channel. Memory bounded by batch_size * channel buffer.
	/// Default: calls fetch_all, chunks, sends.
	async fn fetch_into(
		&self,
		sql: &str,
		key_column: &str,
		batch_size: usize,
		tx: mpsc::Sender<Vec<RawRow>>,
	) -> Result<usize, OversyncError> {
		let all = self.fetch_all(sql, key_column).await?;
		let total = all.len();
		for chunk in all.chunks(batch_size) {
			tx.send(chunk.to_vec())
				.await
				.map_err(|_| OversyncError::Internal("channel closed".into()))?;
		}
		Ok(total)
	}

	async fn test_connection(&self) -> Result<(), OversyncError>;
}

/// A target connector delivers delta events to a destination.
///
/// Built-in implementations: stdout, HTTP webhook, Kafka, SurrealDB.
/// Custom sinks implement this trait for application-specific delivery
/// (e.g., DatacatSink transforms events into catalog entities).
///
/// # Batching
///
/// The default [`send_batch`] iterates and calls [`send_event`] per item.
/// Override for targets that support native batching (e.g., Kafka produce).
#[async_trait]
pub trait Sink: Send + Sync {
	/// Human-readable name of this target instance.
	fn name(&self) -> &str;

	/// Deliver a single event to the target.
	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError>;

	/// Send a batch of envelopes. Default: iterates and calls send_event.
	/// Override for sinks that support native batching (e.g., Kafka produce batch).
	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		for envelope in envelopes {
			self.send_event(envelope).await?;
		}
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError>;
}

/// Factory for creating [`OriginConnector`]s from a JSON config object.
///
/// Registered in the [`PluginRegistry`] by connector type name (e.g., `"postgres"`).
/// The factory is called once per source to create a connector instance.
#[async_trait]
pub trait OriginFactory: Send + Sync {
	fn connector_type(&self) -> &str;

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError>;
}

/// Factory for creating [`Sink`] target connectors from a JSON config object.
#[async_trait]
pub trait TargetFactory: Send + Sync {
	fn sink_type(&self) -> &str;

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError>;
}

/// Chain multiple [`TransformHook`]s into a sequential pipeline.
///
/// Each hook receives the output of the previous one. If any hook
/// returns an error, the pipeline short-circuits.
pub struct TransformPipeline {
	hooks: Vec<std::sync::Arc<dyn TransformHook>>,
}

impl TransformPipeline {
	pub fn new(hooks: Vec<std::sync::Arc<dyn TransformHook>>) -> Self {
		Self { hooks }
	}
}

#[async_trait]
impl TransformHook for TransformPipeline {
	async fn transform(
		&self,
		mut envelopes: Vec<EventEnvelope>,
	) -> Result<Vec<EventEnvelope>, OversyncError> {
		for hook in &self.hooks {
			envelopes = hook.transform(envelopes).await?;
		}
		Ok(envelopes)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::model::{EventEnvelope, EventMeta, OpType};

	struct DoubleTransform;

	#[async_trait]
	impl TransformHook for DoubleTransform {
		async fn transform(
			&self,
			envelopes: Vec<EventEnvelope>,
		) -> Result<Vec<EventEnvelope>, OversyncError> {
			let mut out = envelopes.clone();
			out.extend(envelopes);
			Ok(out)
		}
	}

	fn test_envelope() -> EventEnvelope {
		EventEnvelope {
			meta: EventMeta {
				op: OpType::Created,
				origin_id: "s".into(),
				query_id: "q".into(),
				key: "k".into(),
				hash: "h".into(),
				cycle_id: 1,
				timestamp: chrono::Utc::now(),
			},
			data: serde_json::json!({}),
		}
	}

	#[tokio::test]
	async fn transform_hook_receives_and_returns_envelopes() {
		let hook = DoubleTransform;
		let input = vec![test_envelope()];
		let output = hook.transform(input).await.unwrap();
		assert_eq!(output.len(), 2);
	}

	#[tokio::test]
	async fn transform_hook_can_return_empty() {
		struct DropAll;
		#[async_trait]
		impl TransformHook for DropAll {
			async fn transform(
				&self,
				_envelopes: Vec<EventEnvelope>,
			) -> Result<Vec<EventEnvelope>, OversyncError> {
				Ok(vec![])
			}
		}

		let output = DropAll.transform(vec![test_envelope()]).await.unwrap();
		assert!(output.is_empty());
	}

	#[tokio::test]
	async fn transform_hook_can_return_error() {
		struct FailTransform;
		#[async_trait]
		impl TransformHook for FailTransform {
			async fn transform(
				&self,
				_envelopes: Vec<EventEnvelope>,
			) -> Result<Vec<EventEnvelope>, OversyncError> {
				Err(OversyncError::Internal("transform failed".into()))
			}
		}

		let result = FailTransform.transform(vec![test_envelope()]).await;
		let err = result.unwrap_err();
		assert!(
			matches!(err, OversyncError::Internal(_)),
			"expected Internal variant, got: {err}"
		);
		assert!(err.to_string().contains("transform failed"));
	}

	// ── TransformPipeline tests ──────────────────────────────

	#[tokio::test]
	async fn pipeline_chains_transforms_in_order() {
		struct AppendSuffix(&'static str);
		#[async_trait]
		impl TransformHook for AppendSuffix {
			async fn transform(
				&self,
				envelopes: Vec<EventEnvelope>,
			) -> Result<Vec<EventEnvelope>, OversyncError> {
				Ok(envelopes
					.into_iter()
					.map(|mut e| {
						e.meta.origin_id.push_str(self.0);
						e
					})
					.collect())
			}
		}

		let pipeline = TransformPipeline::new(vec![
			std::sync::Arc::new(AppendSuffix("_a")),
			std::sync::Arc::new(AppendSuffix("_b")),
		]);
		let output = pipeline.transform(vec![test_envelope()]).await.unwrap();
		assert_eq!(output[0].meta.origin_id, "s_a_b");
	}

	#[tokio::test]
	async fn pipeline_empty_hooks_is_passthrough() {
		let pipeline = TransformPipeline::new(vec![]);
		let input = vec![test_envelope()];
		let output = pipeline.transform(input.clone()).await.unwrap();
		assert_eq!(output.len(), 1);
		assert_eq!(output[0].meta.key, input[0].meta.key);
	}

	#[tokio::test]
	async fn pipeline_short_circuits_on_error() {
		struct Fail;
		#[async_trait]
		impl TransformHook for Fail {
			async fn transform(
				&self,
				_: Vec<EventEnvelope>,
			) -> Result<Vec<EventEnvelope>, OversyncError> {
				Err(OversyncError::Internal("stage 2 failed".into()))
			}
		}

		let pipeline = TransformPipeline::new(vec![
			std::sync::Arc::new(DoubleTransform),
			std::sync::Arc::new(Fail),
		]);
		let result = pipeline.transform(vec![test_envelope()]).await;
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("stage 2"));
	}
}
