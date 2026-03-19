use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use oversync::config::SyncConfig;
use oversync::lifecycle::LifecycleManager;
use oversync::registry::PluginRegistry;
use oversync_connectors::{
	FlightSqlSourceFactory, GraphqlSourceFactory, HttpSourceFactory, MysqlSourceFactory,
	PostgresSourceFactory, TrinoSourceFactory,
};
use oversync_delta::DeltaEngine;
use oversync_sinks::{HttpSinkFactory, KafkaSinkFactory, StdoutSinkFactory, SurrealDbSinkFactory};

#[derive(Parser)]
#[command(
	name = "oversync",
	about = "Lightweight data sync engine: poll, delta, sink"
)]
struct Cli {
	#[arg(short, long, env = "OVERSYNC_CONFIG", default_value = "oversync.toml")]
	config: PathBuf,

	#[arg(long, env = "OVERSYNC_BIND", default_value = "0.0.0.0:4200")]
	bind: String,

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

fn default_registry() -> PluginRegistry {
	let mut registry = PluginRegistry::new();
	registry.register_source(Box::new(PostgresSourceFactory));
	registry.register_source(Box::new(HttpSourceFactory));
	registry.register_source(Box::new(MysqlSourceFactory));
	registry.register_source(Box::new(FlightSqlSourceFactory));
	registry.register_source(Box::new(TrinoSourceFactory));
	registry.register_source(Box::new(GraphqlSourceFactory));
	registry.register_sink(Box::new(StdoutSinkFactory));
	registry.register_sink(Box::new(KafkaSinkFactory));
	registry.register_sink(Box::new(SurrealDbSinkFactory));
	registry.register_sink(Box::new(HttpSinkFactory));
	registry
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

		let otel_layer =
			tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("oversync"));

		registry.with(otel_layer).init();
		tracing::info!(endpoint = %endpoint, "OpenTelemetry export enabled");
	} else {
		registry.init();
	};

	let config = SyncConfig::from_file(&cli.config)?;
	tracing::info!(
		sources = config.sources.len(),
		sinks = config.sinks.len(),
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

	let delta_engine = if let Some(ref snap_cfg) = config.surrealdb.snapshot {
		let snap_db = surrealdb::engine::any::connect(&snap_cfg.url).await?;
		snap_db
			.signin(surrealdb::opt::auth::Root {
				username: snap_cfg.username.clone(),
				password: snap_cfg.password.clone(),
			})
			.await?;
		apply_schema(&snap_db, &snap_cfg.namespace, &snap_cfg.database).await?;
		tracing::info!(
			url = %snap_cfg.url,
			"snapshot DB connected (separate)"
		);
		DeltaEngine::new(db.clone(), snap_db)
	} else {
		let snap_db = surrealdb::engine::any::connect("mem://").await?;
		apply_schema(&snap_db, "oversync", "snapshot").await?;
		tracing::info!("snapshot DB: embedded kv-mem");
		DeltaEngine::new(db.clone(), snap_db)
	};

	let lifecycle = Arc::new(LifecycleManager::new(delta_engine, default_registry));
	let surreal_def = config.surrealdb.clone();
	let db_for_api = db.clone();

	// Build ApiState with DB and lifecycle
	let api_state = build_api_state(&config, Some(db_for_api), Some(lifecycle.clone()), &surreal_def);

	// Start with TOML config, lifecycle will manage scheduler
	lifecycle.start(config).await?;

	let shutdown_lifecycle = lifecycle.clone();
	tokio::spawn(async move {
		tokio::signal::ctrl_c().await.ok();
		tracing::info!("received ctrl-c, shutting down");
		shutdown_lifecycle.shutdown().await;
	});

	// Start API server
	let bind = cli.bind.clone();
	let app = oversync_api::router(api_state);
	let listener = tokio::net::TcpListener::bind(&bind).await?;
	tracing::info!(bind = %bind, "API server started");
	axum::serve(listener, app).await?;

	Ok(())
}

struct LifecycleAdapter {
	lifecycle: Arc<LifecycleManager>,
	surreal_def: oversync::config::SurrealDbDef,
}

#[async_trait::async_trait]
impl oversync_api::state::LifecycleControl for LifecycleAdapter {
	async fn restart_with_config_json(
		&self,
		db: &Surreal<Any>,
	) -> Result<(), oversync_core::error::OversyncError> {
		let config = oversync::load_config_from_db(db, &self.surreal_def).await?;
		self.lifecycle.start(config).await
	}

	async fn pause(&self) {
		self.lifecycle.pause().await;
	}

	async fn resume(&self) -> Result<(), oversync_core::error::OversyncError> {
		self.lifecycle.resume().await
	}

	async fn is_running(&self) -> bool {
		self.lifecycle.is_running().await
	}

	async fn is_paused(&self) -> bool {
		self.lifecycle.is_paused().await
	}
}

fn build_api_state(
	config: &SyncConfig,
	db_client: Option<Surreal<Any>>,
	lifecycle: Option<Arc<LifecycleManager>>,
	surreal_def: &oversync::config::SurrealDbDef,
) -> Arc<oversync_api::state::ApiState> {
	use oversync_api::state::*;

	let sources = config
		.sources
		.iter()
		.map(|s| SourceConfig {
			name: s.name.clone(),
			connector: s.connector.clone(),
			interval_secs: s.interval_secs,
			queries: s
				.queries
				.iter()
				.map(|q| QueryConfig {
					id: q.id.clone(),
					key_column: q.key_column.clone(),
				})
				.collect(),
		})
		.collect();

	let sinks = config
		.sinks
		.iter()
		.map(|s| SinkConfig {
			name: s.name.clone(),
			sink_type: s.sink_type.clone(),
		})
		.collect();

	let lifecycle_control: Option<Arc<dyn LifecycleControl>> =
		lifecycle.map(|lc| -> Arc<dyn LifecycleControl> {
			Arc::new(LifecycleAdapter {
				lifecycle: lc,
				surreal_def: surreal_def.clone(),
			})
		});

	Arc::new(ApiState {
		sources: Arc::new(tokio::sync::RwLock::new(sources)),
		sinks: Arc::new(tokio::sync::RwLock::new(sinks)),
		cycle_status: Arc::new(tokio::sync::RwLock::new(
			std::collections::HashMap::new(),
		)),
		db_client,
		lifecycle: lifecycle_control,
	})
}
