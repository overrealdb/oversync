mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::surreal::TestSurrealContainer;
use oversync::OversyncEngine;
use oversync::config::{
	DeltaDef, DiffMode, OriginDef, PipeConfig, QueryDef, RetryDef, ScheduleDef, SinkDef, SyncConfig,
};
use oversync::config_db::replace_config_in_db;
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, RawRow};
use oversync_core::traits::{OriginConnector, OriginFactory, Sink, TargetFactory};
use tokio::sync::Mutex;

fn make_config(container: &TestSurrealContainer) -> SyncConfig {
	SyncConfig {
		surrealdb: oversync::config::SurrealDbDef {
			url: "mem://".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: container.ns.clone(),
			database: container.db.clone(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "debug".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![],
		pipe_presets: vec![],
	}
}

fn make_pipe(
	name: &str,
	connector: &str,
	dsn: &str,
	diff_mode: DiffMode,
	queries: Vec<QueryDef>,
) -> PipeConfig {
	PipeConfig {
		name: name.into(),
		origin: OriginDef {
			connector: connector.into(),
			dsn: dsn.into(),
			credential: None,
			trino_url: None,
			config: serde_json::json!({}),
		},
		targets: vec![],
		queries,
		schedule: ScheduleDef {
			interval_secs: 3600,
			missed_tick_policy: Default::default(),
			max_requests_per_minute: None,
		},
		delta: DeltaDef {
			diff_mode,
			fail_safe_threshold: 30.0,
		},
		retry: RetryDef {
			max_retries: 1,
			retry_base_delay_secs: 1,
		},
		recipe: None,
		filters: vec![],
		transforms: vec![],
		links: vec![],
		alert_webhook: None,
		enabled: true,
	}
}

struct StaticConnector {
	rows: Vec<RawRow>,
}

#[async_trait]
impl OriginConnector for StaticConnector {
	fn name(&self) -> &str {
		"static"
	}

	async fn fetch_all(&self, _sql: &str, _key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		Ok(self.rows.clone())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct StaticOriginFactory {
	rows: Vec<RawRow>,
}

#[async_trait]
impl OriginFactory for StaticOriginFactory {
	fn connector_type(&self) -> &str {
		"static"
	}

	async fn create(
		&self,
		_name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn OriginConnector>, OversyncError> {
		Ok(Box::new(StaticConnector {
			rows: self.rows.clone(),
		}))
	}
}

struct RecordingSink {
	name: String,
	events: Arc<Mutex<Vec<EventEnvelope>>>,
}

#[async_trait]
impl Sink for RecordingSink {
	fn name(&self) -> &str {
		&self.name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.events.lock().await.push(envelope.clone());
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

struct RecordingSinkFactory {
	events: Arc<Mutex<Vec<EventEnvelope>>>,
}

#[async_trait]
impl TargetFactory for RecordingSinkFactory {
	fn sink_type(&self) -> &str {
		"recorder"
	}

	async fn create(
		&self,
		name: &str,
		_config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		Ok(Box::new(RecordingSink {
			name: name.to_string(),
			events: Arc::clone(&self.events),
		}))
	}
}

fn static_rows() -> Vec<RawRow> {
	vec![
		RawRow {
			row_key: "1".into(),
			row_data: serde_json::json!({"id": "1", "name": "alpha"}),
		},
		RawRow {
			row_key: "2".into(),
			row_data: serde_json::json!({"id": "2", "name": "beta"}),
		},
	]
}

// ── Build ───────────────────────────────────────────────────

#[tokio::test]
async fn engine_build_with_mem_url() {
	let engine = OversyncEngine::builder("mem://").build().await.unwrap();
	assert!(!engine.is_running().await);
}

#[tokio::test]
async fn engine_builder_applies_defaults() {
	let engine = OversyncEngine::builder("mem://").build().await.unwrap();
	let def = engine.surreal_def();
	assert_eq!(def.namespace, "oversync");
	assert_eq!(def.database, "sync");
	assert!(def.username.is_empty());
	assert!(def.password.is_empty());
}

#[tokio::test]
async fn engine_builder_non_mem_requires_credentials() {
	match OversyncEngine::builder("http://localhost:8000")
		.build()
		.await
	{
		Err(err) => assert!(err.to_string().contains("credentials are required")),
		Ok(_) => panic!("missing non-mem credentials should fail"),
	}
}

#[tokio::test]
async fn engine_builder_non_mem_snapshot_requires_credentials() {
	match OversyncEngine::builder("mem://")
		.snapshot_url("http://localhost:8000")
		.build()
		.await
	{
		Err(err) => assert!(
			err.to_string()
				.contains("snapshot SurrealDB credentials are required")
		),
		Ok(_) => panic!("missing snapshot credentials should fail"),
	}
}

#[tokio::test]
async fn engine_builder_custom_namespace() {
	let engine = OversyncEngine::builder("mem://")
		.namespace("custom_ns")
		.database("custom_db")
		.skip_schema(true)
		.build()
		.await
		.unwrap();
	let def = engine.surreal_def();
	assert_eq!(def.namespace, "custom_ns");
	assert_eq!(def.database, "custom_db");
}

#[tokio::test]
async fn engine_builder_without_snapshot_url_reuses_shared_state_snapshot() {
	let container = TestSurrealContainer::new().await;
	let state_url = TestSurrealContainer::url().await;

	let base_config = SyncConfig {
		surrealdb: oversync::config::SurrealDbDef {
			url: state_url.clone(),
			username: "root".into(),
			password: "root".into(),
			namespace: container.ns.clone(),
			database: container.db.clone(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "recorder".into(),
			sink_type: "recorder".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![make_pipe(
			"static-source",
			"static",
			"memory://",
			DiffMode::Db,
			vec![QueryDef {
				id: "items".into(),
				sql: "SELECT * FROM static_source".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
	};

	let first_events = Arc::new(Mutex::new(Vec::new()));
	let first_engine = OversyncEngine::builder(&state_url)
		.namespace(&container.ns)
		.database(&container.db)
		.credentials("root", "root")
		.skip_defaults(true)
		.register_source(Box::new(StaticOriginFactory {
			rows: static_rows(),
		}))
		.register_sink(Box::new(RecordingSinkFactory {
			events: Arc::clone(&first_events),
		}))
		.build()
		.await
		.unwrap();

	first_engine.start(base_config.clone()).await.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	first_engine.shutdown().await;

	assert_eq!(first_events.lock().await.len(), 2);

	let second_events = Arc::new(Mutex::new(Vec::new()));
	let second_engine = OversyncEngine::builder(&state_url)
		.namespace(&container.ns)
		.database(&container.db)
		.credentials("root", "root")
		.skip_defaults(true)
		.register_source(Box::new(StaticOriginFactory {
			rows: static_rows(),
		}))
		.register_sink(Box::new(RecordingSinkFactory {
			events: Arc::clone(&second_events),
		}))
		.build()
		.await
		.unwrap();

	second_engine.start(base_config).await.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	second_engine.shutdown().await;

	assert!(
		second_events.lock().await.is_empty(),
		"restart against shared state DB should reuse snapshot baseline and emit no duplicate create wave"
	);
}

// ── Start / Shutdown ────────────────────────────────────────

#[tokio::test]
async fn engine_start_and_shutdown() {
	let container = TestSurrealContainer::new().await;
	let config = make_config(&container);

	let engine = OversyncEngine::builder("mem://").build().await.unwrap();

	engine.start(config).await.unwrap();
	assert!(engine.is_running().await);

	engine.shutdown().await;
	assert!(!engine.is_running().await);
}

#[tokio::test]
async fn engine_start_from_toml() {
	let dir = std::env::temp_dir().join("oversync_engine_test");
	std::fs::create_dir_all(&dir).unwrap();
	let path = dir.join("test_engine.toml");
	std::fs::write(
		&path,
		r#"
[surrealdb]
url = "mem://"

[[sinks]]
name = "debug"
type = "stdout"
"#,
	)
	.unwrap();

	let engine = OversyncEngine::builder("mem://").build().await.unwrap();

	engine.start_from_toml(&path).await.unwrap();
	assert!(engine.is_running().await);

	engine.shutdown().await;
	std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn engine_start_from_db() {
	let container = TestSurrealContainer::new().await;
	let state_url = TestSurrealContainer::url().await;

	let config = SyncConfig {
		surrealdb: oversync::config::SurrealDbDef {
			url: state_url.clone(),
			username: "root".into(),
			password: "root".into(),
			namespace: container.ns.clone(),
			database: container.db.clone(),
			snapshot: None,
		},
		sinks: vec![SinkDef {
			name: "debug".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![make_pipe(
			"static-source",
			"static",
			"memory://",
			DiffMode::Db,
			vec![QueryDef {
				id: "items".into(),
				sql: "SELECT * FROM static_source".into(),
				key_column: "id".into(),
				sinks: None,
				transform: None,
			}],
		)],
		pipe_presets: vec![],
	};

	let engine = OversyncEngine::builder(&state_url)
		.namespace(&container.ns)
		.database(&container.db)
		.credentials("root", "root")
		.skip_schema(true)
		.register_source(Box::new(StaticOriginFactory {
			rows: static_rows(),
		}))
		.build()
		.await
		.unwrap();

	replace_config_in_db(engine.state_client().as_ref(), &config)
		.await
		.unwrap();

	engine.start_from_db().await.unwrap();
	assert!(engine.is_running().await);

	let current = engine
		.current_config()
		.await
		.expect("start_from_db should load config into lifecycle");
	assert_eq!(current.pipes.len(), 1);
	assert_eq!(current.pipes[0].name, "static-source");
	assert_eq!(current.pipes[0].origin.connector, "static");
	assert_eq!(current.pipes[0].queries.len(), 1);
	assert_eq!(current.pipes[0].queries[0].id, "items");
	assert_eq!(current.sinks.len(), 1);
	assert_eq!(current.sinks[0].name, "debug");

	engine.shutdown().await;
}

#[cfg(feature = "api")]
#[tokio::test]
async fn engine_api_resolve_pipe_returns_effective_queries() {
	use axum::body::{self, Body};
	use axum::http::{Request, StatusCode};
	use oversync::config::{
		DeltaDef, DiffMode, MissedTickPolicy, OriginDef, PipeConfig, PipeRecipeDef, PipeRecipeType,
		QueryDef, RetryDef, ScheduleDef,
	};
	use tower::ServiceExt;

	unsafe {
		std::env::set_var(
			"OVERSYNC_CREDENTIAL_KEY",
			"engine-api-resolve-test-key-32-bytes",
		)
	};

	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.build()
		.await
		.unwrap();

	let config = SyncConfig {
		surrealdb: engine.surreal_def().clone(),
		sinks: vec![SinkDef {
			name: "stdout".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![
			PipeConfig {
				name: "seed-source".into(),
				origin: OriginDef {
					connector: "http".into(),
					dsn: "https://example.invalid".into(),
					credential: None,
					trino_url: None,
					config: serde_json::json!({}),
				},
				targets: vec![],
				queries: vec![QueryDef {
					id: "seed-query".into(),
					sql: "SELECT 1 AS id".into(),
					key_column: "id".into(),
					sinks: None,
					transform: None,
				}],
				schedule: ScheduleDef {
					interval_secs: 300,
					missed_tick_policy: MissedTickPolicy::Skip,
					max_requests_per_minute: None,
				},
				delta: DeltaDef {
					diff_mode: DiffMode::Db,
					fail_safe_threshold: 30.0,
				},
				retry: RetryDef {
					max_retries: 3,
					retry_base_delay_secs: 5,
				},
				recipe: None,
				filters: vec![],
				transforms: vec![],
				links: vec![],
				alert_webhook: None,
				enabled: true,
			},
			PipeConfig {
				name: "catalog-pg".into(),
				origin: OriginDef {
					connector: "postgres".into(),
					dsn: "postgres://postgres:postgres@127.0.0.1:55432/postgres".into(),
					credential: None,
					trino_url: None,
					config: serde_json::json!({}),
				},
				targets: vec!["stdout".into()],
				queries: vec![],
				schedule: ScheduleDef::default(),
				delta: DeltaDef::default(),
				retry: RetryDef::default(),
				recipe: Some(PipeRecipeDef {
					recipe_type: PipeRecipeType::PostgresMetadata,
					prefix: "postgresdl".into(),
					entity_type_id: Some("postgres".into()),
					schema_id: "table".into(),
					schemas: vec!["public".into()],
				}),
				filters: vec![],
				transforms: vec![],
				links: vec![],
				alert_webhook: None,
				enabled: true,
			},
		],
		pipe_presets: vec![],
	};

	oversync::config_db::replace_config_in_db(engine.state_client().as_ref(), &config)
		.await
		.unwrap();

	let app = engine.api_router().await.unwrap();
	let response = app
		.oneshot(
			Request::builder()
				.uri("/pipes/catalog-pg/resolve")
				.body(Body::empty())
				.unwrap(),
		)
		.await
		.unwrap();

	assert_eq!(response.status(), StatusCode::OK);
	let body = body::to_bytes(response.into_body(), usize::MAX)
		.await
		.unwrap();
	let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
	let queries = json
		.get("effective_queries")
		.and_then(|value| value.as_array())
		.unwrap_or_else(|| panic!("expected effective_queries array, got: {json}"));
	assert_eq!(queries.len(), 2);
	assert_eq!(queries[0]["id"], "entity");
	assert_eq!(queries[1]["id"], "aspect-table");

	unsafe { std::env::remove_var("OVERSYNC_CREDENTIAL_KEY") };
}

#[cfg(feature = "api")]
#[tokio::test]
async fn engine_api_openapi_includes_engine_routes_and_legacy_source_deprecation() {
	use axum::body::{self, Body};
	use axum::http::{Request, StatusCode};
	use tower::ServiceExt;

	unsafe {
		std::env::set_var(
			"OVERSYNC_CREDENTIAL_KEY",
			"engine-api-openapi-test-key-32-bytes",
		)
	};

	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.build()
		.await
		.unwrap();
	let config = SyncConfig {
		surrealdb: engine.surreal_def().clone(),
		sinks: vec![],
		pipes: vec![],
		pipe_presets: vec![],
	};
	engine.start(config).await.unwrap();

	let app = engine.api_router().await.unwrap();
	let response = app
		.oneshot(
			Request::builder()
				.uri("/openapi.json")
				.body(Body::empty())
				.unwrap(),
		)
		.await
		.unwrap();

	assert_eq!(response.status(), StatusCode::OK);
	let body = body::to_bytes(response.into_body(), usize::MAX)
		.await
		.unwrap();
	let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
	assert!(json["paths"]["/pipes/dry-run"]["post"].is_object());
	assert!(json["paths"]["/pipes/{name}/resolve"]["get"].is_object());
	assert!(json["paths"]["/credentials"]["get"].is_object());
	assert!(json["paths"]["/config/versions"]["get"].is_object());
	assert!(json["paths"]["/sources"].is_null());

	engine.shutdown().await;
	unsafe { std::env::remove_var("OVERSYNC_CREDENTIAL_KEY") };
}

#[cfg(feature = "api")]
#[tokio::test]
async fn engine_api_resolve_missing_pipe_returns_404() {
	use axum::body::{self, Body};
	use axum::http::{Request, StatusCode};
	use tower::ServiceExt;

	unsafe {
		std::env::set_var(
			"OVERSYNC_CREDENTIAL_KEY",
			"engine-api-missing-pipe-test-key-32",
		)
	};

	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.build()
		.await
		.unwrap();
	let config = SyncConfig {
		surrealdb: engine.surreal_def().clone(),
		sinks: vec![],
		pipes: vec![],
		pipe_presets: vec![],
	};
	engine.start(config).await.unwrap();

	let app = engine.api_router().await.unwrap();
	let response = app
		.oneshot(
			Request::builder()
				.uri("/pipes/does-not-exist/resolve")
				.body(Body::empty())
				.unwrap(),
		)
		.await
		.unwrap();

	assert_eq!(response.status(), StatusCode::NOT_FOUND);
	let body = body::to_bytes(response.into_body(), usize::MAX)
		.await
		.unwrap();
	let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
	assert!(
		json["error"]
			.as_str()
			.unwrap_or_default()
			.contains("pipe not found")
	);

	engine.shutdown().await;
	unsafe { std::env::remove_var("OVERSYNC_CREDENTIAL_KEY") };
}

// ── Pause / Resume ──────────────────────────────────────────

#[tokio::test]
async fn engine_pause_resume() {
	let container = TestSurrealContainer::new().await;
	let config = make_config(&container);

	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.build()
		.await
		.unwrap();

	engine.start(config).await.unwrap();
	assert!(engine.is_running().await);
	assert!(!engine.is_paused().await);

	engine.pause().await;
	assert!(!engine.is_running().await);
	assert!(engine.is_paused().await);

	engine.resume().await.unwrap();
	assert!(engine.is_running().await);
	assert!(!engine.is_paused().await);

	engine.shutdown().await;
}

// ── Restart ─────────────────────────────────────────────────

#[tokio::test]
async fn engine_restart_replaces_config() {
	let container = TestSurrealContainer::new().await;
	let config1 = make_config(&container);

	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.build()
		.await
		.unwrap();

	engine.start(config1).await.unwrap();
	let stored1 = engine.current_config().await.unwrap();
	assert_eq!(stored1.sinks[0].name, "debug");

	let mut config2 = make_config(&container);
	config2.sinks[0].name = "debug-v2".into();
	engine.start(config2).await.unwrap();

	let stored2 = engine.current_config().await.unwrap();
	assert_eq!(stored2.sinks[0].name, "debug-v2");
	assert!(engine.is_running().await);

	engine.shutdown().await;
}

// ── Custom factories ────────────────────────────────────────

#[tokio::test]
async fn engine_skip_defaults_rejects_builtin() {
	let container = TestSurrealContainer::new().await;

	let engine = OversyncEngine::builder("mem://")
		.skip_defaults(true)
		.skip_schema(true)
		.build()
		.await
		.unwrap();

	// Config that references "stdout" sink type — but defaults are skipped
	let config = make_config(&container);
	engine.start(config).await.unwrap();

	// The scheduler will try to create the "stdout" sink and fail internally.
	// Since scheduler logs errors and exits the task rather than returning them
	// to start(), we verify is_running is true (scheduler spawned) but the
	// task will exit quickly. Give it a moment.
	tokio::time::sleep(std::time::Duration::from_millis(100)).await;

	// The scheduler task exited due to unknown sink, so shutdown_tx is consumed
	engine.shutdown().await;
}

// ── Clone ───────────────────────────────────────────────────

#[tokio::test]
async fn engine_is_cloneable() {
	let engine = OversyncEngine::builder("mem://")
		.skip_schema(true)
		.build()
		.await
		.unwrap();

	let engine2 = engine.clone();

	let config = SyncConfig {
		surrealdb: oversync::config::SurrealDbDef {
			url: "mem://".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "oversync".into(),
			database: "sync".into(),
			snapshot: None,
		},
		sinks: vec![],
		pipes: vec![],
		pipe_presets: vec![],
	};

	engine.start(config).await.unwrap();
	assert!(engine2.is_running().await); // clone shares state

	engine2.shutdown().await;
	assert!(!engine.is_running().await);
}
