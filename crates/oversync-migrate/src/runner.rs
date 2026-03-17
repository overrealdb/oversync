use sha2::{Digest, Sha256};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use tracing::{debug, info};

use oversync_core::error::OversyncError;

const BOOTSTRAP_SQL: &str = include_str!("../../../sql/migration_lock.surql");
const V001_SQL: &str = include_str!("../../../sql/migrations/v001_schema.surql");

#[derive(Debug, Clone)]
pub struct Migration {
	pub version: u32,
	pub name: String,
	pub content: String,
	pub checksum: String,
}

#[derive(Debug, Clone)]
pub struct AppliedMigration {
	pub version: u32,
	pub applied_at: String,
	pub checksum: String,
	pub instance_id: String,
}

#[derive(Debug, Clone)]
pub struct MigrationRunner {
	client: Surreal<Any>,
	instance_id: String,
}

pub fn compute_checksum(content: &str) -> String {
	let mut hasher = Sha256::new();
	hasher.update(content.as_bytes());
	hex::encode(hasher.finalize())
}

fn embedded_migrations() -> Vec<Migration> {
	vec![Migration {
		version: 1,
		name: "schema".into(),
		content: V001_SQL.into(),
		checksum: compute_checksum(V001_SQL),
	}]
}

pub fn list_migrations() -> Vec<Migration> {
	embedded_migrations()
}

pub fn latest_version() -> u32 {
	embedded_migrations().last().map(|m| m.version).unwrap_or(0)
}

impl MigrationRunner {
	pub fn new(client: Surreal<Any>) -> Self {
		Self {
			client,
			instance_id: uuid::Uuid::new_v4().to_string(),
		}
	}

	pub fn with_instance_id(client: Surreal<Any>, instance_id: String) -> Self {
		Self {
			client,
			instance_id,
		}
	}

	pub fn instance_id(&self) -> &str {
		&self.instance_id
	}

	pub async fn run(&self) -> Result<Vec<AppliedMigration>, OversyncError> {
		let migrations = embedded_migrations();
		info!(
			count = migrations.len(),
			instance_id = %self.instance_id,
			"starting migration run"
		);

		self.bootstrap().await?;
		let applied = self.apply_pending(&migrations).await?;
		self.verify(&migrations).await?;

		info!(
			applied = applied.len(),
			total = migrations.len(),
			"migration run complete"
		);
		Ok(applied)
	}

	async fn bootstrap(&self) -> Result<(), OversyncError> {
		debug!("bootstrapping migration tables");
		self.client
			.query(BOOTSTRAP_SQL)
			.await
			.map_err(|e| OversyncError::Migration(format!("bootstrap failed: {e}")))?;
		Ok(())
	}

	async fn read_applied(&self) -> Result<Vec<AppliedMigration>, OversyncError> {
		let query = r#"
			SELECT version, <string> applied_at AS applied_at, checksum, instance_id
			FROM migration_lock ORDER BY version ASC
		"#;

		let mut response = self
			.client
			.query(query)
			.await
			.map_err(|e| OversyncError::Migration(format!("read applied failed: {e}")))?;

		let rows: Vec<serde_json::Value> = response
			.take(0)
			.map_err(|e| OversyncError::Migration(format!("take applied failed: {e}")))?;

		Ok(rows
			.iter()
			.map(|row| AppliedMigration {
				version: row.get("version").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
				applied_at: row
					.get("applied_at")
					.and_then(|v| v.as_str())
					.unwrap_or("")
					.to_string(),
				checksum: row
					.get("checksum")
					.and_then(|v| v.as_str())
					.unwrap_or("")
					.to_string(),
				instance_id: row
					.get("instance_id")
					.and_then(|v| v.as_str())
					.unwrap_or("")
					.to_string(),
			})
			.collect())
	}

	async fn apply_pending(
		&self,
		migrations: &[Migration],
	) -> Result<Vec<AppliedMigration>, OversyncError> {
		let applied = self.read_applied().await?;
		let applied_versions: std::collections::HashSet<u32> =
			applied.iter().map(|a| a.version).collect();

		for existing in &applied {
			if let Some(expected) = migrations.iter().find(|m| m.version == existing.version) {
				if existing.checksum != expected.checksum {
					return Err(OversyncError::Migration(format!(
						"checksum mismatch for v{:03}_{}: expected {}, found {}",
						existing.version, expected.name, expected.checksum, existing.checksum,
					)));
				}
			}
		}

		let mut newly_applied = Vec::new();

		for migration in migrations {
			if applied_versions.contains(&migration.version) {
				debug!(version = migration.version, name = %migration.name, "already applied");
				continue;
			}

			info!(version = migration.version, name = %migration.name, "applying migration");

			let response = self.client.query(&migration.content).await.map_err(|e| {
				OversyncError::Migration(format!(
					"v{:03}_{} failed: {e}",
					migration.version, migration.name,
				))
			})?;

			if let Err(e) = response.check() {
				return Err(OversyncError::Migration(format!(
					"v{:03}_{} had errors: {e}",
					migration.version, migration.name,
				)));
			}

			self.record_applied(migration).await?;

			newly_applied.push(AppliedMigration {
				version: migration.version,
				applied_at: chrono::Utc::now().to_rfc3339(),
				checksum: migration.checksum.clone(),
				instance_id: self.instance_id.clone(),
			});

			info!(version = migration.version, name = %migration.name, "applied");
		}

		Ok(newly_applied)
	}

	async fn record_applied(&self, migration: &Migration) -> Result<(), OversyncError> {
		self.client
			.query(
				r#"CREATE migration_lock SET
				version = $version,
				applied_at = time::now(),
				checksum = $checksum,
				instance_id = $instance_id"#,
			)
			.bind(("version", migration.version as i64))
			.bind(("checksum", migration.checksum.clone()))
			.bind(("instance_id", self.instance_id.clone()))
			.await
			.map_err(|e| {
				OversyncError::Migration(format!(
					"failed to record v{:03}_{}: {e}",
					migration.version, migration.name,
				))
			})?;
		Ok(())
	}

	async fn verify(&self, migrations: &[Migration]) -> Result<(), OversyncError> {
		let applied = self.read_applied().await?;
		let applied_versions: std::collections::HashSet<u32> =
			applied.iter().map(|a| a.version).collect();

		let missing: Vec<String> = migrations
			.iter()
			.filter(|m| !applied_versions.contains(&m.version))
			.map(|m| format!("v{:03}_{}", m.version, m.name))
			.collect();

		if !missing.is_empty() {
			return Err(OversyncError::Migration(format!(
				"verification failed — missing: {}",
				missing.join(", "),
			)));
		}

		debug!("all {} migrations verified", migrations.len());
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn checksum_is_deterministic() {
		assert_eq!(compute_checksum("SELECT 1;"), compute_checksum("SELECT 1;"));
	}

	#[test]
	fn checksum_differs_for_different_content() {
		assert_ne!(compute_checksum("SELECT 1;"), compute_checksum("SELECT 2;"));
	}

	#[test]
	fn checksum_is_hex_sha256() {
		let hash = compute_checksum("hello");
		assert_eq!(hash.len(), 64);
		assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
		assert_eq!(
			hash,
			"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
		);
	}

	#[test]
	fn embedded_migrations_are_sorted() {
		let migrations = embedded_migrations();
		for pair in migrations.windows(2) {
			assert!(pair[0].version < pair[1].version);
		}
	}

	#[test]
	fn embedded_migrations_have_unique_versions() {
		let migrations = embedded_migrations();
		let mut seen = std::collections::HashSet::new();
		for m in &migrations {
			assert!(seen.insert(m.version), "duplicate version: {}", m.version);
		}
	}

	#[test]
	fn embedded_migrations_start_at_one() {
		let migrations = embedded_migrations();
		assert!(!migrations.is_empty());
		assert_eq!(migrations[0].version, 1);
	}

	#[test]
	fn embedded_migrations_are_contiguous() {
		let migrations = embedded_migrations();
		for (i, m) in migrations.iter().enumerate() {
			assert_eq!(m.version, (i + 1) as u32);
		}
	}

	#[test]
	fn embedded_migrations_have_nonempty_content() {
		for m in &embedded_migrations() {
			assert!(!m.content.trim().is_empty(), "v{:03} empty", m.version);
		}
	}

	#[test]
	fn embedded_migrations_checksums_match_content() {
		for m in &embedded_migrations() {
			assert_eq!(m.checksum, compute_checksum(&m.content));
		}
	}

	#[test]
	fn latest_version_is_1() {
		assert_eq!(latest_version(), 1);
	}

	#[test]
	fn list_migrations_returns_all() {
		let migrations = list_migrations();
		assert_eq!(migrations.len(), 1);
		assert_eq!(migrations[0].name, "schema");
	}

	#[test]
	fn bootstrap_sql_is_nonempty() {
		assert!(!BOOTSTRAP_SQL.trim().is_empty());
		assert!(BOOTSTRAP_SQL.contains("migration_lock"));
	}

	#[test]
	fn migration_names_are_nonempty() {
		for m in &embedded_migrations() {
			assert!(!m.name.is_empty());
		}
	}
}
