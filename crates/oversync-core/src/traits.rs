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

#[async_trait]
pub trait SourceConnector: Send + Sync {
	fn name(&self) -> &str;

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

#[async_trait]
pub trait Sink: Send + Sync {
	fn name(&self) -> &str;

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

/// Factory for creating SourceConnectors from config.
#[async_trait]
pub trait SourceFactory: Send + Sync {
	fn connector_type(&self) -> &str;

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn SourceConnector>, OversyncError>;
}

/// Factory for creating Sinks from config.
#[async_trait]
pub trait SinkFactory: Send + Sync {
	fn sink_type(&self) -> &str;

	async fn create(
		&self,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError>;
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
				source_id: "s".into(),
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
}
