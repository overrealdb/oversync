mod store;

pub use store::{AesGcmStore, Credential, CredentialType};

use async_trait::async_trait;
use oversync_core::error::OversyncError;
use tracing::info;

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

/// Resolve credential references in pipe configs.
///
/// For each pipe with `origin.credential` set, looks up the encrypted credential
/// from SurrealDB, decrypts it, and replaces the DSN with the decrypted secret.
pub async fn resolve_pipe_credentials(
	pipes: &mut [crate::config::PipeConfig],
	db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	store: &AesGcmStore,
) -> Result<(), OversyncError> {
	for pipe in pipes.iter_mut() {
		let cred_name = match &pipe.origin.credential {
			Some(name) => name.clone(),
			None => continue,
		};

		const SQL_LOOKUP: &str = include_str!("../../surql/queries/credential/lookup_credential.surql");

		let mut resp = db
			.query(SQL_LOOKUP)
			.bind(("name", cred_name.clone()))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("credential lookup: {e}")))?;

		let rows: Vec<serde_json::Value> = resp
			.take(0)
			.map_err(|e| OversyncError::SurrealDb(format!("credential take: {e}")))?;

		let encrypted = rows
			.first()
			.and_then(|r| r.get("encrypted"))
			.and_then(|v| v.as_str())
			.ok_or_else(|| {
				OversyncError::Config(format!(
					"pipe '{}': credential '{}' not found",
					pipe.name, cred_name
				))
			})?;

		let decrypted = store.decrypt(encrypted)?;
		pipe.origin.dsn = decrypted;

		info!(
			pipe = %pipe.name,
			credential = %cred_name,
			"credential resolved into DSN"
		);
	}
	Ok(())
}

/// Credential metadata returned by list (no secrets).
#[derive(Debug, Clone, serde::Serialize)]
pub struct CredentialSummary {
	pub name: String,
	pub credential_type: String,
	pub created_at: chrono::DateTime<chrono::Utc>,
}
