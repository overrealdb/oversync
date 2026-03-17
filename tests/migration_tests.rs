mod common;

use common::surreal::TestSurreal;

#[tokio::test]
async fn migrations_apply_successfully() {
	let t = TestSurreal::new().await;
	let mut res = t.client.query("SELECT * FROM migration_lock").await.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["version"], 1);
}

#[tokio::test]
async fn migrations_are_idempotent() {
	let t = TestSurreal::new().await;
	let applied = oversync_migrate::run_migrations(&t.client).await.unwrap();
	assert_eq!(applied.len(), 0);
}

#[tokio::test]
async fn runner_instance_id_is_unique() {
	let t = TestSurreal::new_raw().await;
	let r1 = oversync_migrate::MigrationRunner::new(t.client.clone());
	let r2 = oversync_migrate::MigrationRunner::new(t.client.clone());
	assert_ne!(r1.instance_id(), r2.instance_id());
}

#[tokio::test]
async fn snapshot_table_exists() {
	let t = TestSurreal::new().await;
	let res = t
		.client
		.query(
			"CREATE snapshot SET source_id = 'src', query_id = 'q', row_key = 'k', row_data = {}, row_hash = 'h', cycle_id = 1",
		)
		.await;
	assert!(res.is_ok());
}

#[tokio::test]
async fn cycle_log_table_exists() {
	let t = TestSurreal::new().await;
	let res = t
		.client
		.query(
			"CREATE cycle_log SET source_id = 'src', query_id = 'q', cycle_id = 1, started_at = time::now()",
		)
		.await;
	assert!(res.is_ok());
}

#[tokio::test]
async fn source_config_table_exists() {
	let t = TestSurreal::new().await;
	let res = t
		.client
		.query("CREATE source_config SET name = 'pg-prod', connector = 'postgresql', config = {}")
		.await;
	assert!(res.is_ok());
}

#[tokio::test]
async fn sink_config_table_exists() {
	let t = TestSurreal::new().await;
	let res = t
		.client
		.query("CREATE sink_config SET name = 'kafka-main', sink_type = 'kafka', config = {}")
		.await;
	assert!(res.is_ok());
}

#[tokio::test]
async fn snapshot_unique_index_enforced() {
	let t = TestSurreal::new().await;
	t.client
		.query(
			"CREATE snapshot SET source_id = 'src', query_id = 'q', row_key = 'k1', row_data = {}, row_hash = 'h', cycle_id = 1",
		)
		.await
		.unwrap();

	// Same key should fail due to UNIQUE index
	let mut res = t
		.client
		.query(
			"CREATE snapshot SET source_id = 'src', query_id = 'q', row_key = 'k1', row_data = {}, row_hash = 'h2', cycle_id = 2",
		)
		.await
		.unwrap();

	let check: Result<Vec<serde_json::Value>, _> = res.take(0);
	assert!(check.is_err() || {
		// SurrealDB may return empty result instead of error for duplicate unique
		let rows: Vec<serde_json::Value> = check.unwrap_or_default();
		rows.is_empty()
	});
}
