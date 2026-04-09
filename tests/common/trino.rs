use std::time::Duration;

use tokio::sync::OnceCell;

use super::stack::{env_var, retry_async, unique_name};

struct SharedTrinoEndpoint {
	url: String,
	user: String,
	catalog: String,
}

static SHARED_TRINO: OnceCell<SharedTrinoEndpoint> = OnceCell::const_new();

async fn shared_trino() -> &'static SharedTrinoEndpoint {
	SHARED_TRINO
		.get_or_init(|| async {
			let url = env_var("OVERSYNC_TEST_TRINO_URL", "http://127.0.0.1:58080");
			let user = env_var("OVERSYNC_TEST_TRINO_USER", "test");
			let catalog = env_var("OVERSYNC_TEST_TRINO_CATALOG", "memory");

			retry_async("trino", 90, Duration::from_secs(2), || {
				let url = url.clone();
				async move {
					let body = reqwest::Client::new()
						.get(format!("{url}/v1/info"))
						.send()
						.await
						.map_err(|e| e.to_string())?
						.json::<serde_json::Value>()
						.await
						.map_err(|e| e.to_string())?;

					if body.get("starting") == Some(&serde_json::json!(false)) {
						Ok::<_, String>(())
					} else {
						Err(format!("trino still starting: {body}"))
					}
				}
			})
			.await;

			SharedTrinoEndpoint { url, user, catalog }
		})
		.await
}

async fn run_statement(url: &str, user: &str, catalog: &str, schema: &str, sql: &str) {
	let client = reqwest::Client::new();
	let resp = client
		.post(format!("{url}/v1/statement"))
		.header("X-Trino-User", user)
		.header("X-Trino-Catalog", catalog)
		.header("X-Trino-Schema", schema)
		.body(sql.to_string())
		.send()
		.await
		.unwrap_or_else(|e| panic!("trino POST failed: {e}\nSQL: {sql}"));

	let body: serde_json::Value = resp.json().await.unwrap();
	if let Some(err) = body.get("error") {
		panic!("trino query error: {err}\nSQL: {sql}");
	}

	let mut next_uri = body
		.get("nextUri")
		.and_then(|v| v.as_str())
		.map(String::from);
	while let Some(uri) = next_uri {
		tokio::time::sleep(Duration::from_millis(200)).await;
		let resp = client
			.get(&uri)
			.header("X-Trino-User", user)
			.send()
			.await
			.unwrap();
		let body: serde_json::Value = resp.json().await.unwrap();
		if let Some(err) = body.get("error") {
			panic!("trino query error: {err}\nSQL: {sql}");
		}
		next_uri = body
			.get("nextUri")
			.and_then(|v| v.as_str())
			.map(String::from);
	}
}

pub struct TestTrino {
	pub url: String,
	pub schema: String,
	user: String,
	catalog: String,
}

impl TestTrino {
	pub async fn new() -> Self {
		let shared = shared_trino().await;
		let instance = Self {
			url: shared.url.clone(),
			schema: unique_name("test"),
			user: shared.user.clone(),
			catalog: shared.catalog.clone(),
		};

		run_statement(
			&instance.url,
			&instance.user,
			&instance.catalog,
			"default",
			&format!(
				"CREATE SCHEMA IF NOT EXISTS {}.{}",
				instance.catalog, instance.schema
			),
		)
		.await;

		instance
	}

	pub async fn run_sql(&self, sql: &str) {
		run_statement(&self.url, &self.user, &self.catalog, &self.schema, sql).await;
	}

	pub fn table(&self, table: &str) -> String {
		format!("{}.{}.{}", self.catalog, self.schema, table)
	}

	pub fn config(&self) -> serde_json::Value {
		serde_json::json!({
			"dsn": &self.url,
			"user": &self.user,
			"catalog": &self.catalog,
			"schema": &self.schema
		})
	}
}
