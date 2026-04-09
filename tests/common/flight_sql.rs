use super::trino::TestTrino;

pub struct TestFlightSql {
	pub flight_endpoint: String,
	pub http_endpoint: String,
	trino: TestTrino,
}

impl TestFlightSql {
	pub async fn new() -> Self {
		let trino = TestTrino::new().await;
		Self {
			flight_endpoint: trino.url.clone(),
			http_endpoint: trino.url.clone(),
			trino,
		}
	}

	pub async fn run_sql(&self, sql: &str) {
		self.trino.run_sql(sql).await;
	}
}
