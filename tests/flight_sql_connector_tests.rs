//! Arrow Flight SQL connector integration tests.
//!
//! All tests are #[ignore] — no Flight SQL-compatible testcontainers server available.
//! Run manually with: cargo test --test flight_sql_connector_tests -- --ignored
//! Requires a running Flight SQL server (DataFusion, Dremio, Doris).

mod common;

use oversync_connectors::FlightSqlConnector;
use oversync_core::traits::OriginConnector;

#[tokio::test]
#[ignore = "requires Flight SQL server — no testcontainers image available"]
async fn flight_sql_test_connection() {
	let endpoint = std::env::var("FLIGHT_SQL_ENDPOINT")
		.unwrap_or_else(|_| "http://localhost:50051".into());
	let conn = FlightSqlConnector::new("test", &endpoint).unwrap();
	conn.test_connection().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Flight SQL server — no testcontainers image available"]
async fn flight_sql_fetch_all() {
	let endpoint = std::env::var("FLIGHT_SQL_ENDPOINT")
		.unwrap_or_else(|_| "http://localhost:50051".into());
	let conn = FlightSqlConnector::new("test", &endpoint).unwrap();

	let rows = conn
		.fetch_all("SELECT 1 AS id, 'hello' AS name", "id")
		.await
		.unwrap();

	assert!(!rows.is_empty());
}
