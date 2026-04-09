//! Arrow Flight SQL connector integration tests.
//!
//! All tests are #[ignore] — no Flight SQL-compatible test server is wired into the shared stack yet.
//! Run manually with: cargo test --test flight_sql_connector_tests -- --ignored
//! Requires a running Flight SQL server (DataFusion, Dremio, Doris).

mod common;

use oversync_connectors::FlightSqlConnector;
use oversync_core::traits::OriginConnector;

#[tokio::test]
#[ignore = "requires Flight SQL server — set FLIGHT_SQL_ENDPOINT"]
async fn flight_sql_test_connection() {
	let Some(endpoint) = std::env::var("FLIGHT_SQL_ENDPOINT").ok() else {
		eprintln!("FLIGHT_SQL_ENDPOINT not set, skipping");
		return;
	};
	let conn = FlightSqlConnector::new("test", &endpoint).unwrap();
	conn.test_connection().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Flight SQL server — set FLIGHT_SQL_ENDPOINT"]
async fn flight_sql_fetch_all() {
	let Some(endpoint) = std::env::var("FLIGHT_SQL_ENDPOINT").ok() else {
		eprintln!("FLIGHT_SQL_ENDPOINT not set, skipping");
		return;
	};
	let conn = FlightSqlConnector::new("test", &endpoint).unwrap();

	let rows = conn
		.fetch_all("SELECT 1 AS id, 'hello' AS name", "id")
		.await
		.unwrap();

	assert!(!rows.is_empty());
}
