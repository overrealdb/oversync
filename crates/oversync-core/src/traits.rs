use async_trait::async_trait;

use crate::error::OversyncError;
use crate::model::{DeltaEvent, RawRow};

#[async_trait]
pub trait SourceConnector: Send + Sync {
	fn name(&self) -> &str;

	async fn fetch_batch(
		&self,
		offset: u64,
		limit: u64,
	) -> Result<Vec<RawRow>, OversyncError>;

	async fn test_connection(&self) -> Result<(), OversyncError>;
}

#[async_trait]
pub trait Sink: Send + Sync {
	fn name(&self) -> &str;

	async fn send_batch(
		&self,
		events: &[DeltaEvent],
	) -> Result<(), OversyncError>;

	async fn test_connection(&self) -> Result<(), OversyncError>;
}
