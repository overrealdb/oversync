mod store;

pub use store::{AesGcmStore, Credential, CredentialType};

use async_trait::async_trait;
use oversync_core::error::OversyncError;

/// Trait for credential storage backends.
///
/// Implementations encrypt secrets at rest and decrypt on retrieval.
/// Secrets are never logged or returned in API responses.
#[async_trait]
pub trait CredentialStore: Send + Sync {
	async fn store(&self, credential: &Credential) -> Result<(), OversyncError>;
	async fn get(&self, name: &str) -> Result<Credential, OversyncError>;
	async fn delete(&self, name: &str) -> Result<(), OversyncError>;
	async fn list(&self) -> Result<Vec<CredentialSummary>, OversyncError>;
}

/// Credential metadata returned by list (no secrets).
#[derive(Debug, Clone, serde::Serialize)]
pub struct CredentialSummary {
	pub name: String,
	pub credential_type: String,
	pub created_at: chrono::DateTime<chrono::Utc>,
}
