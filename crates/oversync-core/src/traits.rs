use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::OversyncError;
use crate::model::{EventEnvelope, RawRow};

#[async_trait]
pub trait SourceConnector: Send + Sync {
	fn name(&self) -> &str;

	async fn fetch_all(
		&self,
		sql: &str,
		key_column: &str,
	) -> Result<Vec<RawRow>, OversyncError>;

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

	/// Send a single event envelope. This is the fundamental delivery unit.
	async fn send_event(
		&self,
		envelope: &EventEnvelope,
	) -> Result<(), OversyncError>;

	/// Send a batch of envelopes. Default: iterates and calls send_event.
	/// Override for sinks that support native batching (e.g., Kafka produce batch).
	async fn send_batch(
		&self,
		envelopes: &[EventEnvelope],
	) -> Result<(), OversyncError> {
		for envelope in envelopes {
			self.send_event(envelope).await?;
		}
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError>;
}
