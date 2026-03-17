pub mod runner;

pub use runner::{AppliedMigration, Migration, MigrationRunner, compute_checksum, latest_version, list_migrations};

use oversync_core::error::OversyncError;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

pub async fn run_migrations(client: &Surreal<Any>) -> Result<Vec<AppliedMigration>, OversyncError> {
	let runner = MigrationRunner::new(client.clone());
	runner.run().await
}
