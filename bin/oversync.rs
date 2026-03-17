use clap::Parser;
use oversync_core::config::OversyncConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let config = OversyncConfig::parse();

	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| format!("oversync={}", config.log_level).into()),
		)
		.init();

	tracing::info!(bind = %config.bind, "starting oversync");

	let db = surrealdb::engine::any::connect(&config.surrealdb.url).await?;
	db.signin(surrealdb::opt::auth::Root {
		username: config.surrealdb.user.clone(),
		password: config.surrealdb.pass.clone(),
	})
	.await?;
	db.use_ns(&config.surrealdb.ns)
		.use_db(&config.surrealdb.db)
		.await?;
	tracing::info!(
		url = %config.surrealdb.url,
		ns = %config.surrealdb.ns,
		db = %config.surrealdb.db,
		"connected to SurrealDB"
	);

	let applied = oversync_migrate::run_migrations(&db).await?;
	tracing::info!(applied = applied.len(), "migrations complete");

	tracing::info!("oversync ready (scheduler not yet implemented)");
	Ok(())
}
