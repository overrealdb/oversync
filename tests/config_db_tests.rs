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
			"CREATE query_config SET origin_id = 'pg-main', name = 'users', query = 'SELECT id, name FROM users', key_column = 'id', enabled = true",
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
