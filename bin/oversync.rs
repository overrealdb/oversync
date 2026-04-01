use std::path::PathBuf;

use clap::{Parser, Subcommand};
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

	#[command(subcommand)]
	command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
	/// Start the sync engine with API server (default)
	Serve,

	/// Validate a config file without starting
	Validate {
		#[arg(short, long)]
		file: Option<PathBuf>,
	},

	/// Show what would change if config is applied
	Diff {
		#[arg(short, long)]
		file: Option<PathBuf>,
	},

	/// Export current config (secrets masked)
	Export,

	/// Run a dry-run for a specific pipe and query
	DryRun {
		#[arg(long)]
		pipe: String,
		#[arg(long)]
		query: String,
		#[arg(long)]
		mock: Option<PathBuf>,
		#[arg(long, default_value = "100")]
		limit: usize,
	},
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let mut cli = Cli::parse();
	init_tracing(&cli);

	let command = cli.command.take().unwrap_or(Command::Serve);
	match command {
		Command::Serve => cmd_serve(&cli).await,
		Command::Validate { file } => cmd_validate(&cli, file),
		Command::Diff { file } => cmd_diff(&cli, file),
		Command::Export => cmd_export(&cli),
		Command::DryRun {
			pipe,
			query,
			mock,
			limit,
		} => cmd_dry_run(&cli, &pipe, &query, mock, limit).await,
	}
}

async fn cmd_serve(cli: &Cli) -> anyhow::Result<()> {
	let config = SyncConfig::from_file(&cli.config)?;
	tracing::info!(
		pipes = config.effective_pipes().len(),
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

	let app = engine.api_router().await;
	let listener = tokio::net::TcpListener::bind(&cli.bind).await?;
	tracing::info!(bind = %cli.bind, "API server started");
	axum::serve(listener, app).await?;

	Ok(())
}

fn cmd_validate(cli: &Cli, file: Option<PathBuf>) -> anyhow::Result<()> {
	let path = file.as_ref().unwrap_or(&cli.config);
	let config = SyncConfig::from_file(path)?;
	let issues = oversync::config::validate_config(&config);

	if issues.is_empty() {
		println!("Config is valid: {}", path.display());
		return Ok(());
	}

	for issue in &issues {
		match issue.severity {
			oversync::config::Severity::Error => eprintln!("ERROR: {}", issue.message),
			oversync::config::Severity::Warning => eprintln!("WARN:  {}", issue.message),
		}
	}

	let errors = issues
		.iter()
		.filter(|i| i.severity == oversync::config::Severity::Error)
		.count();
	if errors > 0 {
		anyhow::bail!("{errors} validation error(s)");
	}
	Ok(())
}

fn cmd_diff(cli: &Cli, file: Option<PathBuf>) -> anyhow::Result<()> {
	let path = file.as_ref().unwrap_or(&cli.config);
	let config = SyncConfig::from_file(path)?;
	let pipes = config.effective_pipes();

	println!("Config: {}", path.display());
	println!("Pipes:  {}", pipes.len());
	for pipe in &pipes {
		println!(
			"  {} ({} → {} targets, {} queries, interval {}s{})",
			pipe.name,
			pipe.origin.connector,
			if pipe.targets.is_empty() {
				"all".to_string()
			} else {
				pipe.targets.join(", ")
			},
			pipe.queries.len(),
			pipe.schedule.interval_secs,
			if pipe.enabled { "" } else { " [DISABLED]" }
		);
	}
	println!("Sinks:  {}", config.sinks.len());
	for sink in &config.sinks {
		println!("  {} ({})", sink.name, sink.sink_type);
	}
	Ok(())
}

fn cmd_export(cli: &Cli) -> anyhow::Result<()> {
	let config = SyncConfig::from_file(&cli.config)?;
	let mut json = serde_json::to_value(&config)?;

	// Mask secrets
	if let Some(obj) = json.as_object_mut()
		&& let Some(surreal) = obj.get_mut("surrealdb").and_then(|v| v.as_object_mut())
		&& surreal.contains_key("password")
	{
		surreal.insert("password".into(), serde_json::json!("***"));
	}

	println!("{}", serde_json::to_string_pretty(&json)?);
	Ok(())
}

async fn cmd_dry_run(
	cli: &Cli,
	pipe_name: &str,
	query_id: &str,
	mock_file: Option<PathBuf>,
	limit: usize,
) -> anyhow::Result<()> {
	let config = SyncConfig::from_file(&cli.config)?;
	let pipe = config
		.effective_pipes()
		.into_iter()
		.find(|p| p.name == pipe_name)
		.ok_or_else(|| anyhow::anyhow!("pipe '{}' not found", pipe_name))?;

	let mock_data = match mock_file {
		Some(path) => {
			let content = std::fs::read_to_string(&path)?;
			serde_json::from_str(&content)?
		}
		None => vec![],
	};

	let mode = if mock_data.is_empty() {
		oversync::dry_run::DryRunMode::Live
	} else {
		oversync::dry_run::DryRunMode::Mock
	};

	let req = oversync::dry_run::DryRunRequest {
		pipe,
		query_id: query_id.to_string(),
		mode,
		mock_data,
		row_limit: limit,
		transforms: vec![],
		use_existing_state: false,
		credentials: None,
	};

	let registry = oversync::PluginRegistry::default();
	let result = oversync::dry_run::execute_dry_run(&req, &registry, None, None).await?;

	println!("Rows fetched:    {}", result.stats.rows_fetched);
	println!("Changes:");
	println!("  Created:       {}", result.changes.created);
	println!("  Updated:       {}", result.changes.updated);
	println!("  Deleted:       {}", result.changes.deleted);
	println!("Events before:   {}", result.stats.events_before_transform);
	println!("Events after:    {}", result.stats.events_after_transform);
	println!("Filtered out:    {}", result.stats.events_filtered_out);

	if !result.input_sample.is_empty() {
		println!("\nSample (first {}):", result.input_sample.len());
		for row in &result.input_sample {
			println!("  {} → {}", row.row_key, row.row_data);
		}
	}

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
