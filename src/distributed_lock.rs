use std::sync::Arc;

use oversync_core::error::OversyncError;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::debug;

const SQL_TRY_ACQUIRE: &str = oversync_queries::mutations::LOCK_TRY_ACQUIRE;
const SQL_RELEASE: &str = oversync_queries::mutations::LOCK_RELEASE;

/// Distributed lock per pipe, backed by SurrealDB.
///
/// Ensures only one oversync instance processes a given pipe at a time.
/// Uses a `pipe_lock` table with TTL-based expiry for crash recovery.
pub struct PipeLock {
	db: Arc<Surreal<Any>>,
	instance_id: String,
}

impl PipeLock {
	pub fn new(db: Arc<Surreal<Any>>, instance_id: String) -> Self {
		Self { db, instance_id }
	}

	/// Try to acquire the lock for a pipe. Returns true if acquired.
	/// Lock expires after `ttl_secs` if not released (crash recovery).
	pub async fn try_acquire(&self, pipe: &str, ttl_secs: u64) -> Result<bool, OversyncError> {
		let mut resp = self
			.db
			.query(SQL_TRY_ACQUIRE)
			.bind(("pipe", pipe.to_string()))
			.bind(("instance", self.instance_id.clone()))
			.bind(("ttl_secs", ttl_secs as i64))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("lock acquire: {e}")))?;

		let last_idx = resp.num_statements().checked_sub(1).ok_or_else(|| {
			OversyncError::Internal("lock acquire: query returned no statements".into())
		})?;
		let rows: Vec<serde_json::Value> = resp
			.take(last_idx)
			.map_err(|e| OversyncError::SurrealDb(format!("lock acquire take[{last_idx}]: {e}")))?;
		let acquired = rows
			.first()
			.and_then(|row| row.get("acquired"))
			.and_then(|v| v.as_bool())
			.ok_or_else(|| {
				OversyncError::Internal(format!(
					"lock acquire: missing boolean 'acquired' result at statement {last_idx}"
				))
			})?;

		if acquired {
			debug!(pipe = %pipe, instance = %self.instance_id, "lock acquired");
		} else {
			debug!(pipe = %pipe, instance = %self.instance_id, "lock not acquired (held by another instance)");
		}

		Ok(acquired)
	}

	/// Release the lock for a pipe.
	pub async fn release(&self, pipe: &str) -> Result<(), OversyncError> {
		self.db
			.query(SQL_RELEASE)
			.bind(("pipe", pipe.to_string()))
			.bind(("instance", self.instance_id.clone()))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("lock release: {e}")))?;

		debug!(pipe = %pipe, instance = %self.instance_id, "lock released");
		Ok(())
	}
}
