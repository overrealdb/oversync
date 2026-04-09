mod common;

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;
use oversync::config::{
	DeltaDef, DiffMode, LinkDef, LinkStrategy, MissedTickPolicy, OriginDef, PipePresetDef,
	PipePresetSpec, QueryDef, RetryDef, ScheduleDef, SurrealDbDef, SyncConfig,
};
use oversync::config_db::{load_config_from_db, replace_config_in_db};
use oversync::recipes::expand_runtime_pipe;

fn make_surreal_def(container: &TestSurrealContainer) -> SurrealDbDef {
	SurrealDbDef {
		url: "mem://".into(),
		username: "root".into(),
		password: "root".into(),
		namespace: container.ns.clone(),
		database: container.db.clone(),
		snapshot: None,
	}
}

#[tokio::test]
async fn load_empty_config_returns_empty() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);
	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert!(config.sources.is_empty());
	assert!(config.sinks.is_empty());
	assert!(config.pipe_presets.is_empty());
}

#[tokio::test]
async fn replace_and_load_pipe_presets_round_trip() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	let config = SyncConfig {
		surrealdb: def.clone(),
		sources: vec![],
		sinks: vec![],
		pipes: vec![],
		pipe_presets: vec![PipePresetDef {
			name: "columns-aspect".into(),
			description: Some("Reusable manual columns aspect".into()),
			spec: PipePresetSpec {
				origin: OriginDef {
					connector: "postgres".into(),
					dsn: "".into(),
					credential: Some("shared-secret".into()),
					trino_url: None,
					config: serde_json::json!({ "sslmode": "require" }),
				},
				targets: vec!["kafka-main".into()],
				queries: vec![QueryDef {
					id: "aspect-columns".into(),
					sql: "SELECT entity_id, payload FROM columns".into(),
					key_column: "entity_id".into(),
					sinks: None,
					transform: Some("smt::columns".into()),
				}],
				schedule: ScheduleDef {
					interval_secs: 900,
					missed_tick_policy: MissedTickPolicy::Skip,
					max_requests_per_minute: Some(4),
				},
				delta: DeltaDef {
					diff_mode: DiffMode::Db,
					fail_safe_threshold: 15.0,
				},
				retry: RetryDef {
					max_retries: 5,
					retry_base_delay_secs: 11,
				},
				recipe: None,
				filters: vec![serde_json::json!({ "type": "keep" })],
				transforms: vec![serde_json::json!({ "type": "rename" })],
				links: vec![LinkDef {
					name: "link-columns".into(),
					left_field: "entity_id".into(),
					right_field: "id".into(),
					strategy: LinkStrategy::Normalized,
					target_origin: "entities".into(),
					target_query: "entity".into(),
				}],
			},
		}],
	};

	replace_config_in_db(&container.client, &config)
		.await
		.unwrap();

	let loaded = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(loaded.pipe_presets.len(), 1);
	assert_eq!(loaded.pipe_presets[0].name, "columns-aspect");
	assert_eq!(
		loaded.pipe_presets[0].description.as_deref(),
		Some("Reusable manual columns aspect")
	);
	assert_eq!(loaded.pipe_presets[0].spec.origin.connector, "postgres");
	assert_eq!(
		loaded.pipe_presets[0].spec.origin.credential.as_deref(),
		Some("shared-secret")
	);
	assert_eq!(loaded.pipe_presets[0].spec.targets, vec!["kafka-main"]);
	assert_eq!(loaded.pipe_presets[0].spec.queries.len(), 1);
	assert_eq!(loaded.pipe_presets[0].spec.queries[0].id, "aspect-columns");
	assert_eq!(
		loaded.pipe_presets[0].spec.queries[0].transform.as_deref(),
		Some("smt::columns")
	);
	assert_eq!(loaded.pipe_presets[0].spec.schedule.interval_secs, 900);
	assert_eq!(
		loaded.pipe_presets[0].spec.schedule.max_requests_per_minute,
		Some(4)
	);
	assert_eq!(loaded.pipe_presets[0].spec.delta.fail_safe_threshold, 15.0);
	assert_eq!(loaded.pipe_presets[0].spec.retry.max_retries, 5);
	assert_eq!(loaded.pipe_presets[0].spec.filters[0]["type"], "keep");
	assert_eq!(loaded.pipe_presets[0].spec.transforms[0]["type"], "rename");
	assert_eq!(loaded.pipe_presets[0].spec.links[0].name, "link-columns");
	assert!(matches!(
		loaded.pipe_presets[0].spec.links[0].strategy,
		LinkStrategy::Normalized
	));
}

#[tokio::test]
async fn load_sources_from_db() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	// Insert a source config
	container
		.client
		.query(
			"CREATE source_config SET name = 'pg-main', connector = 'postgres', config = { dsn: 'postgres://localhost/db', interval_secs: 120 }, enabled = true",
		)
		.await
		.unwrap();

	// Insert associated query
	container
		.client
		.query(
			"CREATE query_config SET origin_id = 'pg-main', name = 'users', query = 'SELECT id, name FROM users', key_column = 'id', transform = 'smt::normalize_users', enabled = true",
		)
		.await
		.unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.sources.len(), 1);
	assert_eq!(config.sources[0].name, "pg-main");
	assert_eq!(config.sources[0].connector, "postgres");
	assert_eq!(config.sources[0].dsn, "postgres://localhost/db");
	assert_eq!(config.sources[0].interval_secs, 120);
	assert_eq!(config.sources[0].queries.len(), 1);
	assert_eq!(config.sources[0].queries[0].id, "users");
	assert_eq!(config.sources[0].queries[0].key_column, "id");
	assert_eq!(
		config.sources[0].queries[0].transform.as_deref(),
		Some("smt::normalize_users")
	);
}

#[tokio::test]
async fn load_sinks_from_db() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	container
		.client
		.query(
			"CREATE sink_config SET name = 'kafka-prod', sink_type = 'kafka', config = { brokers: 'localhost:9092', topic: 'events' }, enabled = true",
		)
		.await
		.unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.sinks.len(), 1);
	assert_eq!(config.sinks[0].name, "kafka-prod");
	assert_eq!(config.sinks[0].sink_type, "kafka");
	let cfg = config.sinks[0].config.as_object().unwrap();
	assert_eq!(cfg["brokers"], "localhost:9092");
}

#[tokio::test]
async fn disabled_sources_excluded() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	container
		.client
		.query("CREATE source_config SET name = 'active', connector = 'postgres', config = { dsn: 'postgres://localhost/db' }, enabled = true")
		.await
		.unwrap();
	container
		.client
		.query("CREATE source_config SET name = 'disabled', connector = 'postgres', config = { dsn: 'postgres://localhost/db2' }, enabled = false")
		.await
		.unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.sources.len(), 1);
	assert_eq!(config.sources[0].name, "active");
}

#[tokio::test]
async fn disabled_sinks_excluded() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	container
		.client
		.query(
			"CREATE sink_config SET name = 'active', sink_type = 'stdout', config = {}, enabled = true",
		)
		.await
		.unwrap();
	container
		.client
		.query(
			"CREATE sink_config SET name = 'off', sink_type = 'kafka', config = {}, enabled = false",
		)
		.await
		.unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.sinks.len(), 1);
	assert_eq!(config.sinks[0].name, "active");
}

#[tokio::test]
async fn multiple_queries_per_source() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	container
		.client
		.query("CREATE source_config SET name = 'pg', connector = 'postgres', config = { dsn: 'postgres://localhost/db' }, enabled = true")
		.await
		.unwrap();
	container
		.client
		.query("CREATE query_config SET origin_id = 'pg', name = 'q1', query = 'SELECT 1 AS id', key_column = 'id', enabled = true")
		.await
		.unwrap();
	container
		.client
		.query("CREATE query_config SET origin_id = 'pg', name = 'q2', query = 'SELECT 2 AS id', key_column = 'id', enabled = true")
		.await
		.unwrap();
	// Disabled query should be excluded
	container
		.client
		.query("CREATE query_config SET origin_id = 'pg', name = 'q3', query = 'SELECT 3 AS id', key_column = 'id', enabled = false")
		.await
		.unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.sources[0].queries.len(), 2);
}

#[tokio::test]
async fn load_pipes_preserves_filters_transforms_and_links() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	let mut resp = container
		.client
		.query(oversync_queries::mutations::CREATE_PIPE)
		.bind(("name", "catalog-pipe"))
		.bind(("origin_connector", "postgres"))
		.bind(("origin_dsn", "postgres://localhost/db"))
		.bind(("origin_credential", Some("shared-secret")))
		.bind(("trino_url", Some("http://trino:8080")))
		.bind(("origin_config", serde_json::json!({})))
		.bind(("targets", serde_json::json!(["stdout"])))
		.bind((
			"schedule",
			serde_json::json!({ "interval_secs": 120, "missed_tick_policy": "burst" }),
		))
		.bind((
			"delta",
			serde_json::json!({ "diff_mode": "memory", "fail_safe_threshold": 12.5 }),
		))
		.bind((
			"retry",
			serde_json::json!({ "max_retries": 7, "retry_base_delay_secs": 9 }),
		))
		.bind((
			"recipe",
			serde_json::json!({
				"type": "postgres_metadata",
				"prefix": "some-postgresql-source",
				"entity_type_id": "postgres",
				"schema_id": "table",
				"schemas": ["public"]
			}),
		))
		.bind((
			"filters",
			serde_json::json!([{ "type": "keep", "field": "status", "equals": "active" }]),
		))
		.bind((
			"transforms",
			serde_json::json!([{ "type": "rename", "field": "old_name", "to": "new_name" }]),
		))
		.bind((
			"links",
			serde_json::json!([{
				"name": "user-contact-email",
				"left_field": "email",
				"right_field": "email",
				"strategy": "exact",
				"target_origin": "contacts",
				"target_query": "emails"
			}]),
		))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = resp.take(0).unwrap();

	container
		.client
		.query(
			"CREATE query_config SET origin_id = 'catalog-pipe', name = 'users', query = 'SELECT id, email FROM users', key_column = 'id', transform = 'smt::project', enabled = true",
		)
		.await
		.unwrap();

	let mut res = container
		.client
		.query("SELECT * FROM pipe_config")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert!(
		!rows.is_empty(),
		"pipe_config should contain the inserted row, got: {rows:?}"
	);

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.pipes.len(), 1);

	let pipe = &config.pipes[0];
	assert_eq!(pipe.name, "catalog-pipe");
	assert_eq!(pipe.origin.credential.as_deref(), Some("shared-secret"));
	assert_eq!(pipe.origin.trino_url.as_deref(), Some("http://trino:8080"));
	assert_eq!(pipe.filters.len(), 1);
	assert_eq!(pipe.transforms.len(), 1);
	assert_eq!(pipe.links.len(), 1);
	assert_eq!(pipe.schedule.interval_secs, 120);
	assert!(matches!(
		pipe.schedule.missed_tick_policy,
		oversync::config::MissedTickPolicy::Burst
	));
	assert!(matches!(
		pipe.delta.diff_mode,
		oversync::config::DiffMode::Memory
	));
	assert_eq!(pipe.delta.fail_safe_threshold, 12.5);
	assert_eq!(pipe.retry.max_retries, 7);
	assert_eq!(pipe.retry.retry_base_delay_secs, 9);
	assert!(pipe.recipe.is_some());
	let recipe = pipe.recipe.as_ref().unwrap();
	assert!(matches!(
		recipe.recipe_type,
		oversync::config::PipeRecipeType::PostgresMetadata
	));
	assert_eq!(recipe.prefix, "some-postgresql-source");
	assert_eq!(recipe.schemas, vec!["public"]);
	assert_eq!(pipe.filters[0]["type"], "keep");
	assert_eq!(pipe.transforms[0]["type"], "rename");
	assert_eq!(pipe.links[0].name, "user-contact-email");
	assert_eq!(pipe.links[0].target_origin, "contacts");
	assert_eq!(pipe.queries.len(), 1);
	assert_eq!(pipe.queries[0].id, "users");
	assert_eq!(pipe.queries[0].transform.as_deref(), Some("smt::project"));
}

#[tokio::test]
async fn load_pipe_presets_from_db() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	let mut resp = container
		.client
		.query(oversync_queries::mutations::CREATE_PIPE_PRESET)
		.bind(("name", "datacat-columns"))
		.bind(("description", Some("Reusable columns aspect preset")))
		.bind((
			"spec",
			serde_json::json!({
				"origin": {
					"connector": "postgres",
					"dsn": "postgres://localhost/db",
					"credential": null,
					"trino_url": null,
					"config": {}
				},
				"targets": ["stdout"],
				"queries": [{
					"id": "aspect-columns",
					"sql": "SELECT id, payload FROM columns_view",
					"key_column": "id",
					"sinks": null,
					"transform": null
				}],
				"schedule": { "interval_secs": 900, "missed_tick_policy": "skip" },
				"delta": { "diff_mode": "db", "fail_safe_threshold": 25.0 },
				"retry": { "max_retries": 2, "retry_base_delay_secs": 3 },
				"recipe": null,
				"filters": [],
				"transforms": [],
				"links": []
			}),
		))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = resp.take(0).unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.pipe_presets.len(), 1);
	assert_eq!(config.pipe_presets[0].name, "datacat-columns");
	assert_eq!(
		config.pipe_presets[0].description.as_deref(),
		Some("Reusable columns aspect preset")
	);
	assert_eq!(config.pipe_presets[0].spec.origin.connector, "postgres");
	assert_eq!(config.pipe_presets[0].spec.targets, vec!["stdout"]);
	assert_eq!(config.pipe_presets[0].spec.queries.len(), 1);
	assert_eq!(config.pipe_presets[0].spec.queries[0].id, "aspect-columns");
}

#[tokio::test]
async fn loaded_pipe_presets_export_to_toml() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	let mut resp = container
		.client
		.query(oversync_queries::mutations::CREATE_PIPE_PRESET)
		.bind(("name", "datacat-columns"))
		.bind(("description", Some("Reusable columns aspect preset")))
		.bind((
			"spec",
			serde_json::json!({
				"origin": {
					"connector": "postgres",
					"dsn": "postgres://localhost/db",
					"credential": null,
					"trino_url": null,
					"config": {}
				},
				"targets": ["stdout"],
				"queries": [{
					"id": "aspect-columns",
					"sql": "SELECT id, payload FROM columns_view",
					"key_column": "id",
					"sinks": null,
					"transform": null
				}],
				"schedule": { "interval_secs": 900, "missed_tick_policy": "skip" },
				"delta": { "diff_mode": "db", "fail_safe_threshold": 25.0 },
				"retry": { "max_retries": 2, "retry_base_delay_secs": 3 },
				"recipe": null,
				"filters": [],
				"transforms": [],
				"links": []
			}),
		))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = resp.take(0).unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	let toml = toml::to_string_pretty(&config).unwrap();
	assert!(toml.contains("[[pipe_presets]]"));
	assert!(toml.contains("name = \"datacat-columns\""));
	assert!(toml.contains("[pipe_presets.spec.origin]"));
}

#[tokio::test]
async fn legacy_flat_pipe_presets_export_to_toml() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	let mut resp = container
		.client
		.query(oversync_queries::mutations::CREATE_PIPE_PRESET)
		.bind(("name", "legacy-flat-preset"))
		.bind(("description", Some("Legacy flat preset shape")))
		.bind((
			"spec",
			serde_json::json!({
				"origin_connector": "postgres",
				"origin_dsn": "postgres://localhost/db",
				"origin_credential": null,
				"trino_url": null,
				"origin_config": {},
				"targets": ["stdout"],
				"queries": [{
					"id": "aspect-columns",
					"sql": "SELECT id, payload FROM columns_view",
					"key_column": "id",
					"sinks": null,
					"transform": null
				}],
				"schedule": { "interval_secs": 900, "missed_tick_policy": "skip" },
				"delta": { "diff_mode": "db", "fail_safe_threshold": 25.0 },
				"retry": { "max_retries": 2, "retry_base_delay_secs": 3 },
				"recipe": null,
				"filters": [],
				"transforms": [],
				"links": []
			}),
		))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = resp.take(0).unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.pipe_presets.len(), 1);
	assert_eq!(config.pipe_presets[0].name, "legacy-flat-preset");
	assert_eq!(config.pipe_presets[0].spec.origin.connector, "postgres");
	assert_eq!(config.pipe_presets[0].spec.queries[0].id, "aspect-columns");

	let toml = toml::to_string_pretty(&config).unwrap();
	assert!(toml.contains("[[pipe_presets]]"));
	assert!(toml.contains("name = \"legacy-flat-preset\""));
	assert!(toml.contains("[pipe_presets.spec.origin]"));
}

#[tokio::test]
async fn db_loaded_postgres_metadata_recipe_expands_into_queries() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);

	let mut resp = container
		.client
		.query(oversync_queries::mutations::CREATE_PIPE)
		.bind(("name", "metadata-pipe"))
		.bind(("origin_connector", "postgres"))
		.bind(("origin_dsn", "postgres://localhost/db"))
		.bind(("origin_credential", Option::<String>::None))
		.bind(("trino_url", Option::<String>::None))
		.bind(("origin_config", serde_json::json!({})))
		.bind(("targets", serde_json::json!(["stdout"])))
		.bind(("schedule", serde_json::json!({ "interval_secs": 300 })))
		.bind(("delta", serde_json::json!({ "diff_mode": "db" })))
		.bind(("retry", serde_json::json!({})))
		.bind((
			"recipe",
			serde_json::json!({
				"type": "postgres_metadata",
				"prefix": "some-postgresql-source",
				"entity_type_id": "postgres",
				"schema_id": "table",
				"schemas": ["public"]
			}),
		))
		.bind(("filters", serde_json::json!([])))
		.bind(("transforms", serde_json::json!([])))
		.bind(("links", serde_json::json!([])))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = resp.take(0).unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.pipes.len(), 1);

	let effective = config.effective_pipes();
	assert_eq!(effective.len(), 1);
	assert!(effective[0].recipe.is_none());
	assert_eq!(effective[0].queries.len(), 2);
	assert_eq!(effective[0].queries[0].id, "entity");
	assert_eq!(effective[0].queries[1].id, "aspect-table");
	assert!(
		effective[0].queries[0]
			.sql
			.contains("'some-postgresql-source.' || n.nspname || '.' || c.relname AS id")
	);
}

#[tokio::test]
async fn db_loaded_postgres_snapshot_recipe_expands_at_runtime() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);
	let pg = TestPostgres::new().await;

	pg.run_sql(&format!(
		r#"CREATE TABLE "{schema}"."customers" (
			id INT PRIMARY KEY,
			email TEXT
		)"#,
		schema = pg.schema
	))
	.await;

	let mut resp = container
		.client
		.query(oversync_queries::mutations::CREATE_PIPE)
		.bind(("name", "snapshot-pipe"))
		.bind(("origin_connector", "postgres"))
		.bind(("origin_dsn", pg.dsn.clone()))
		.bind(("origin_credential", Option::<String>::None))
		.bind(("trino_url", Option::<String>::None))
		.bind(("origin_config", serde_json::json!({})))
		.bind(("targets", serde_json::json!(["stdout"])))
		.bind(("schedule", serde_json::json!({ "interval_secs": 300 })))
		.bind(("delta", serde_json::json!({ "diff_mode": "db" })))
		.bind(("retry", serde_json::json!({})))
		.bind((
			"recipe",
			serde_json::json!({
				"type": "postgres_snapshot",
				"prefix": "some-postgresql-source",
				"entity_type_id": "postgres",
				"schema_id": "table",
				"schemas": [pg.schema.clone()]
			}),
		))
		.bind(("filters", serde_json::json!([])))
		.bind(("transforms", serde_json::json!([])))
		.bind(("links", serde_json::json!([])))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = resp.take(0).unwrap();

	let config = load_config_from_db(&container.client, &def).await.unwrap();
	assert_eq!(config.pipes.len(), 1);

	let expanded = expand_runtime_pipe(config.pipes[0].clone()).await.unwrap();
	assert!(expanded.recipe.is_none());
	assert_eq!(expanded.queries.len(), 1);
	assert_eq!(
		expanded.queries[0].id,
		format!("some-postgresql-source.{}.customers", pg.schema)
	);
	assert_eq!(expanded.queries[0].key_column, "__oversync_key");
	assert!(
		expanded.queries[0]
			.sql
			.contains(r#"t."id"::text AS "__oversync_key""#)
	);
}
