mod common;

use common::surreal::TestSurrealContainer;
use oversync::config::SurrealDbDef;
use oversync::config_db::load_config_from_db;

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
	assert_eq!(pipe.filters[0]["type"], "keep");
	assert_eq!(pipe.transforms[0]["type"], "rename");
	assert_eq!(pipe.links[0].name, "user-contact-email");
	assert_eq!(pipe.links[0].target_origin, "contacts");
	assert_eq!(pipe.queries.len(), 1);
	assert_eq!(pipe.queries[0].id, "users");
	assert_eq!(pipe.queries[0].transform.as_deref(), Some("smt::project"));
}
