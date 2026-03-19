use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::OnceCell;

struct SharedTrinoContainer {
	flight_endpoint: String,
	_container: testcontainers::ContainerAsync<GenericImage>,
}

static SHARED_TRINO: OnceCell<SharedTrinoContainer> = OnceCell::const_new();

async fn shared_trino() -> &'static SharedTrinoContainer {
	SHARED_TRINO
		.get_or_init(|| async {
			let container = GenericImage::new("trinodb/trino", "latest")
				.with_exposed_port(8080.into())
				.with_startup_timeout(Duration::from_secs(120))
				.start()
				.await
				.expect("failed to start trino container");

			let host = container.get_host().await.expect("trino host");
			let port = container.get_host_port_ipv4(8080).await.expect("trino port");

			// Wait for Trino to be ready
			let client = reqwest::Client::new();
			let url = format!("http://{host}:{port}/v1/info");
			for _ in 0..60 {
				if let Ok(resp) = client.get(&url).send().await {
					if let Ok(body) = resp.text().await {
						if body.contains("starting") || body.contains("true") {
							break;
						}
					}
				}
				tokio::time::sleep(Duration::from_secs(2)).await;
			}

			// Give Trino a bit more time to fully initialize
			tokio::time::sleep(Duration::from_secs(5)).await;

			SharedTrinoContainer {
				flight_endpoint: format!("http://{host}:{port}"),
				_container: container,
			}
		})
		.await
}

pub struct TestFlightSql {
	pub flight_endpoint: String,
	pub http_endpoint: String,
}

impl TestFlightSql {
	pub async fn new() -> Self {
		let shared = shared_trino().await;
		Self {
			flight_endpoint: shared.flight_endpoint.clone(),
			http_endpoint: shared.flight_endpoint.clone(),
		}
	}

	/// Execute SQL via Trino REST API (for test setup — creating tables, inserting data).
	pub async fn run_sql(&self, sql: &str) {
		let client = reqwest::Client::new();
		let resp = client
			.post(&format!("{}/v1/statement", self.http_endpoint))
			.header("X-Trino-User", "test")
			.header("X-Trino-Catalog", "memory")
			.header("X-Trino-Schema", "default")
			.body(sql.to_string())
			.send()
			.await
			.unwrap_or_else(|e| panic!("trino POST failed: {e}\nSQL: {sql}"));

		let body: serde_json::Value = resp.json().await.unwrap();

		// Follow nextUri until query completes
		let mut next_uri = body.get("nextUri").and_then(|v| v.as_str()).map(String::from);
		while let Some(uri) = next_uri {
			tokio::time::sleep(Duration::from_millis(200)).await;
			let resp = client
				.get(&uri)
				.header("X-Trino-User", "test")
				.send()
				.await
				.unwrap();
			let body: serde_json::Value = resp.json().await.unwrap();
			if let Some(err) = body.get("error") {
				panic!("trino query error: {err}\nSQL: {sql}");
			}
			next_uri = body.get("nextUri").and_then(|v| v.as_str()).map(String::from);
		}
	}
}
