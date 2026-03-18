mod common;

use common::surreal::TestSurrealContainer;

#[tokio::test]
async fn declarative_schema_overwrites_field_definition() {
	let t = TestSurrealContainer::new().await;

	// Change row_hash field type from string to int
	t.client
		.query("DEFINE FIELD OVERWRITE row_hash ON snapshot TYPE int")
		.await
		.unwrap();

	// Re-apply schema via overshift — OVERWRITE should restore original type
	let mut manifest = overshift::Manifest::load("surql/").expect("manifest");
	manifest.meta.ns = t.ns.clone();
	manifest.meta.db = t.db.clone();
	let plan = overshift::plan(&t.client, &manifest).await.unwrap();
	plan.apply(&t.client).await.unwrap();

	// After reapply, row_hash should be back to string — verify by inserting string value
	t.client
		.query(
			"CREATE snapshot SET source_id = 'test', query_id = 'q', \
			 row_key = 'overwrite_test', row_data = {}, row_hash = 'hash_string', cycle_id = 1",
		)
		.await
		.expect("inserting string into row_hash should succeed after OVERWRITE restores type");
}

#[tokio::test]
async fn schema_applies_successfully() {
	let t = TestSurrealContainer::new_raw().await;
	let mut manifest = overshift::Manifest::load("surql/").expect("manifest load");
	manifest.meta.ns = t.ns.clone();
	manifest.meta.db = t.db.clone();
	let plan = overshift::plan(&t.client, &manifest).await.expect("plan");
	assert!(!plan.is_empty(), "first plan should have work to do");
	let result = plan.apply(&t.client).await.expect("apply");
	assert!(result.applied_modules > 0);
}

#[tokio::test]
async fn schema_is_idempotent() {
	let t = TestSurrealContainer::new().await;
	let mut manifest = overshift::Manifest::load("surql/").expect("manifest load");
	manifest.meta.ns = t.ns.clone();
	manifest.meta.db = t.db.clone();
	let plan = overshift::plan(&t.client, &manifest).await.expect("plan");
	let result = plan.apply(&t.client).await.expect("apply");
	assert_eq!(
		result.applied_migrations, 0,
		"second run should apply zero migrations"
	);
}

#[tokio::test]
async fn apply_instance_id_is_unique() {
	let t1 = TestSurrealContainer::new_raw().await;
	let t2 = TestSurrealContainer::new_raw().await;

	let mut m1 = overshift::Manifest::load("surql/").expect("manifest");
	m1.meta.ns = t1.ns.clone();
	m1.meta.db = t1.db.clone();
	let r1 = overshift::plan(&t1.client, &m1)
		.await
		.unwrap()
		.apply(&t1.client)
		.await
		.unwrap();

	let mut m2 = overshift::Manifest::load("surql/").expect("manifest");
	m2.meta.ns = t2.ns.clone();
	m2.meta.db = t2.db.clone();
	let r2 = overshift::plan(&t2.client, &m2)
		.await
		.unwrap()
		.apply(&t2.client)
		.await
		.unwrap();

	assert_ne!(r1.instance_id, r2.instance_id);
}

#[tokio::test]
async fn snapshot_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query(
			"CREATE snapshot SET source_id = 'src', query_id = 'q', \
			 row_key = 'k', row_data = {}, row_hash = 'h', cycle_id = 1",
		)
		.await
		.expect("snapshot table should exist");
}

#[tokio::test]
async fn cycle_log_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query(
			"CREATE cycle_log SET source_id = 'src', query_id = 'q', \
			 cycle_id = 1, started_at = time::now()",
		)
		.await
		.expect("cycle_log table should exist");
}

#[tokio::test]
async fn source_config_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query("CREATE source_config SET name = 'pg-prod', connector = 'postgresql', config = {}")
		.await
		.expect("source_config table should exist");
}

#[tokio::test]
async fn query_config_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query(
			"CREATE query_config SET source_id = 'pg-prod', name = 'users', \
			 query = 'SELECT * FROM users', key_column = 'id'",
		)
		.await
		.expect("query_config table should exist");
}

#[tokio::test]
async fn sink_config_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query("CREATE sink_config SET name = 'kafka-main', sink_type = 'kafka', config = {}")
		.await
		.expect("sink_config table should exist");
}

#[tokio::test]
async fn link_rule_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query("CREATE link_rule SET name = 'dag-match', rule_type = 'exact', config = {}")
		.await
		.expect("link_rule table should exist");
}

#[tokio::test]
async fn resolved_link_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query("CREATE resolved_link SET source_key = 'a', target_key = 'b', rule_name = 'test'")
		.await
		.expect("resolved_link table should exist");
}

#[tokio::test]
async fn plugin_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query("CREATE plugin SET name = 'dag-resolver', version = '0.1.0'")
		.await
		.expect("plugin table should exist");
}

#[tokio::test]
async fn pending_event_table_exists() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query(
			"CREATE pending_event SET source_id = 'src', query_id = 'q', \
			 cycle_id = 1, events_json = '[]'",
		)
		.await
		.expect("pending_event table should exist");
}

#[tokio::test]
async fn snapshot_unique_index_enforced() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query(
			"CREATE snapshot SET source_id = 'src', query_id = 'q', \
			 row_key = 'k1', row_data = {}, row_hash = 'h1', cycle_id = 1",
		)
		.await
		.unwrap();

	let mut res = t
		.client
		.query(
			"CREATE snapshot SET source_id = 'src', query_id = 'q', \
			 row_key = 'k1', row_data = {}, row_hash = 'h2', cycle_id = 2",
		)
		.await
		.unwrap();

	let rows: Result<Vec<serde_json::Value>, _> = res.take(0);
	assert!(
		rows.is_err() || rows.unwrap_or_default().is_empty(),
		"duplicate snapshot key should be rejected by UNIQUE index"
	);
}

#[tokio::test]
async fn source_config_unique_index_enforced() {
	let t = TestSurrealContainer::new().await;
	t.client
		.query("CREATE source_config SET name = 'dup', connector = 'pg', config = {}")
		.await
		.unwrap();

	let mut res = t
		.client
		.query("CREATE source_config SET name = 'dup', connector = 'rest', config = {}")
		.await
		.unwrap();

	let rows: Result<Vec<serde_json::Value>, _> = res.take(0);
	assert!(
		rows.is_err() || rows.unwrap_or_default().is_empty(),
		"duplicate source name should be rejected by UNIQUE index"
	);
}
