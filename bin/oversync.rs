use std::path::PathBuf;

use clap::Parser;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use oversync::config::SyncConfig;
use oversync::scheduler::Scheduler;
use oversync_delta::DeltaEngine;

#[derive(Parser)]
#[command(name = "oversync", about = "Lightweight data sync engine: poll, delta, sink")]
struct Cli {
	#[arg(short, long, env = "OVERSYNC_CONFIG", default_value = "oversync.toml")]
	config: PathBuf,

	#[arg(long, env = "OVERSYNC_LOG_LEVEL", default_value = "info")]
	log_level: String,

	#[arg(long, env = "OVERSYNC_OTEL_ENDPOINT")]
	otel_endpoint: Option<String>,
}

async fn apply_schema(db: &Surreal<Any>, ns: &str, db_name: &str) -> anyhow::Result<()> {
	let mut manifest = overshift::Manifest::load("surql/")?;
	manifest.meta.ns = ns.to_string();
	manifest.meta.db = db_name.to_string();
	let plan = overshift::plan(db, &manifest).await?;
	let result = plan.apply(db).await?;
	tracing::info!(
		migrations = result.applied_migrations,
		modules = result.applied_modules,
		"schema applied"
	);
	Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let cli = Cli::parse();

	let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| format!("oversync={}", cli.log_level).into());

	let fmt_layer = tracing_subscriber::fmt::layer();

	let registry = tracing_subscriber::registry()
		.with(env_filter)
		.with(fmt_layer);

	if let Some(ref endpoint) = cli.otel_endpoint {
		let exporter = opentelemetry_otlp::SpanExporter::builder()
			.with_tonic()
			.with_endpoint(endpoint)
			.build()?;

		let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
			.with_batch_exporter(exporter)
			.with_resource(
				opentelemetry_sdk::Resource::builder()
					.with_service_name("oversync")
					.build(),
			)
			.build();

		let otel_layer = tracing_opentelemetry::layer()
			.with_tracer(tracer_provider.tracer("oversync"));

		registry.with(otel_layer).init();
		tracing::info!(endpoint = %endpoint, "OpenTelemetry export enabled");
	} else {
		registry.init();
	};

	let config = SyncConfig::from_file(&cli.config)?;
	tracing::info!(
		sources = config.sources.len(),
		"loaded config from {}",
		cli.config.display()
	);

	let db = surrealdb::engine::any::connect(&config.surrealdb.url).await?;
	db.signin(surrealdb::opt::auth::Root {
		username: config.surrealdb.username.clone(),
		password: config.surrealdb.password.clone(),
	})
	.await?;

	apply_schema(&db, &config.surrealdb.namespace, &config.surrealdb.database).await?;

	// Snapshot client: embedded kv-mem (fast) or same DB (simple)
	let engine = if let Some(ref snap_cfg) = config.surrealdb.snapshot {
		let snap_db = surrealdb::engine::any::connect(&snap_cfg.url).await?;
		snap_db
			.signin(surrealdb::opt::auth::Root {
				username: snap_cfg.username.clone(),
				password: snap_cfg.password.clone(),
			})
			.await?;
		apply_schema(&snap_db, &snap_cfg.namespace, &snap_cfg.database).await?;
		tracing::info!(url = %snap_cfg.url, "snapshot DB connected (separate)");
		DeltaEngine::new(db, snap_db)
	} else {
		let snap_db = surrealdb::engine::any::connect("mem://").await?;
		apply_schema(&snap_db, "oversync", "snapshot").await?;
		tracing::info!("snapshot DB: embedded kv-mem");
		DeltaEngine::new(db, snap_db)
	};
	let scheduler = Scheduler::new(engine, config);

	let shutdown_handle = scheduler.shutdown_tx_clone();
	tokio::spawn(async move {
		tokio::signal::ctrl_c().await.ok();
		tracing::info!("received ctrl-c, shutting down");
		let _ = shutdown_handle.send(true);
	});

	scheduler.run().await?;
	Ok(())
}
