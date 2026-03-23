use oversync_core::error::OversyncError;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::{debug, warn};

const SQL_TRY_ACQUIRE: &str = include_str!("../surql/queries/mutations/lock_try_acquire.surql");
const SQL_RELEASE: &str = include_str!("../surql/queries/mutations/lock_release.surql");

/// Distributed lock per pipe, backed by SurrealDB.
///
/// Ensures only one oversync instance processes a given pipe at a time.
/// Uses a `pipe_lock` table with TTL-based expiry for crash recovery.
pub struct PipeLock {
	db: Surreal<Any>,
	instance_id: String,
}

impl PipeLock {
	pub fn new(db: Surreal<Any>, instance_id: String) -> Self {
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

		// Multi-statement query: LET, LET, LET, IF{..RETURN..}
		// The RETURN result is at the last statement index.
		// Try indices until we find the result.
		let mut acquired_val = None;
		for idx in 0..10 {
			if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(idx) {
				if let Some(first) = rows.first() {
					if first.get("acquired").is_some() {
						acquired_val = first.get("acquired").and_then(|v| v.as_bool());
						break;
					}
				}
			}
		}
		let acquired = acquired_val.unwrap_or(false);

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
