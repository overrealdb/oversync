mod common;

use common::surreal::TestSurrealContainer;
use oversync_connectors::{SurrealDbConnector, SurrealDbOriginFactory};
use oversync_core::traits::{OriginConnector, OriginFactory};

#[tokio::test]
async fn surrealdb_source_fetches_all_records() {
	let surreal = TestSurrealContainer::new_raw().await;
	let connector = SurrealDbConnector::from_client("test", surreal.client.clone());

	surreal
		.client
		.query(
			"INSERT INTO users [
				{ id: 'user1', name: 'Alice' },
				{ id: 'user2', name: 'Bob' },
				{ id: 'user3', name: 'Charlie' }
			]",
		)
		.await
		.unwrap();

	let rows = connector
		.fetch_all("SELECT * FROM users", "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn surrealdb_source_extracts_record_id_key() {
	let surreal = TestSurrealContainer::new_raw().await;
	let connector = SurrealDbConnector::from_client("test", surreal.client.clone());

	surreal
		.client
		.query("INSERT INTO users { id: 'abc', name: 'Alice' }")
		.await
		.unwrap();

	let rows = connector
		.fetch_all("SELECT * FROM users", "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "abc");
}

#[tokio::test]
async fn surrealdb_source_extracts_custom_field_key() {
	let surreal = TestSurrealContainer::new_raw().await;
	let connector = SurrealDbConnector::from_client("test", surreal.client.clone());

	surreal
		.client
		.query("INSERT INTO users { id: 'u1', name: 'Alice', email: 'alice@example.com' }")
		.await
		.unwrap();

	let rows = connector
		.fetch_all("SELECT * FROM users", "email")
		.await
		.unwrap();

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "alice@example.com");
}

#[tokio::test]
async fn surrealdb_source_empty_table_returns_empty() {
	let surreal = TestSurrealContainer::new_raw().await;
	let connector = SurrealDbConnector::from_client("test", surreal.client.clone());

	let rows = connector
		.fetch_all("SELECT * FROM nonexistent", "id")
		.await
		.unwrap();

	assert!(rows.is_empty());
}

#[tokio::test]
async fn surrealdb_source_test_connection() {
	let surreal = TestSurrealContainer::new_raw().await;
	let connector = SurrealDbConnector::from_client("test", surreal.client.clone());

	connector.test_connection().await.unwrap();
}

#[tokio::test]
async fn surrealdb_source_fetch_with_where_clause() {
	let surreal = TestSurrealContainer::new_raw().await;
	let connector = SurrealDbConnector::from_client("test", surreal.client.clone());

	surreal
		.client
		.query(
			"INSERT INTO users [
				{ id: 'u1', name: 'Alice', active: true },
				{ id: 'u2', name: 'Bob', active: false },
				{ id: 'u3', name: 'Charlie', active: true },
				{ id: 'u4', name: 'Dave', active: false },
				{ id: 'u5', name: 'Eve', active: true }
			]",
		)
		.await
		.unwrap();

	let rows = connector
		.fetch_all("SELECT * FROM users WHERE active = true", "id")
		.await
		.unwrap();

	assert_eq!(rows.len(), 3);
	for row in &rows {
		assert_eq!(row.row_data["active"], true);
	}
}

#[tokio::test]
async fn surrealdb_source_factory_creates_connector() {
	let surreal = TestSurrealContainer::new_raw().await;
	let url = TestSurrealContainer::url().await;

	let config = serde_json::json!({
		"url": url,
		"namespace": surreal.ns,
		"database": surreal.db,
		"username": "root",
		"password": "root"
	});

	let factory = SurrealDbOriginFactory;
	assert_eq!(factory.connector_type(), "surrealdb");

	let connector = factory.create("test-factory", &config).await.unwrap();
	connector.test_connection().await.unwrap();
}
