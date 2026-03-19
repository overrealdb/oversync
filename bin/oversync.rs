use std::path::PathBuf;

use clap::Parser;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use oversync::OversyncEngine;
use oversync::config::SyncConfig;

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

	#[arg(long, env = "OVERSYNC_API_KEY")]
	api_key: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let cli = Cli::parse();
	init_tracing(&cli);

	let config = SyncConfig::from_file(&cli.config)?;
	tracing::info!(
		sources = config.sources.len(),
		sinks = config.sinks.len(),
		"loaded config from {}",
		cli.config.display()
	);

	let mut builder = OversyncEngine::builder(&config.surrealdb.url)
		.namespace(&config.surrealdb.namespace)
		.database(&config.surrealdb.database)
		.credentials(&config.surrealdb.username, &config.surrealdb.password);

	if let Some(ref snap) = config.surrealdb.snapshot {
		builder = builder
			.snapshot_url(&snap.url)
			.snapshot_credentials(&snap.username, &snap.password)
			.snapshot_namespace(&snap.namespace)
			.snapshot_database(&snap.database);
	}

	if let Some(ref key) = cli.api_key {
		builder = builder.api_key(key);
	}

	let engine = builder.build().await?;
	engine.start(config).await?;

	let engine_shutdown = engine.clone();
	tokio::spawn(async move {
		tokio::signal::ctrl_c().await.ok();
		tracing::info!("received ctrl-c, shutting down");
		engine_shutdown.shutdown().await;
	});

	let app = engine.api_router();
	let listener = tokio::net::TcpListener::bind(&cli.bind).await?;
	tracing::info!(bind = %cli.bind, "API server started");
	axum::serve(listener, app).await?;

	Ok(())
}

fn init_tracing(cli: &Cli) {
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
			.build()
			.expect("otel exporter");

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
}
