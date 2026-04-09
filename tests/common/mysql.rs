use std::time::Duration;

use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;
use tokio::sync::OnceCell;

use super::stack::{env_var, retry_async};

struct SharedMysqlEndpoint {
	url: String,
}

static SHARED_MYSQL: OnceCell<SharedMysqlEndpoint> = OnceCell::const_new();

async fn shared_mysql() -> &'static SharedMysqlEndpoint {
	SHARED_MYSQL
		.get_or_init(|| async {
			let url = env_var(
				"OVERSYNC_TEST_MYSQL_DSN",
				"mysql://root:root@127.0.0.1:53306/test",
			);

			retry_async("mysql", 30, Duration::from_secs(1), || {
				let url = url.clone();
				async move {
					let pool = MySqlPoolOptions::new()
						.max_connections(1)
						.connect(&url)
						.await?;
					sqlx::query("SELECT 1").execute(&pool).await?;
					pool.close().await;
					Ok::<_, sqlx::Error>(())
				}
			})
			.await;

			SharedMysqlEndpoint { url }
		})
		.await
}

pub struct TestMysql {
	pub pool: MySqlPool,
	pub db_name: String,
	pub dsn: String,
}

impl TestMysql {
	pub async fn new() -> Self {
		let shared = shared_mysql().await;
		let test_id = uuid::Uuid::new_v4().to_string().replace('-', "");
		let db_name = format!("test_{}", &test_id[..8]);

		let bootstrap = MySqlPool::connect(&shared.url)
			.await
			.expect("failed to connect for bootstrap");
		sqlx::query(&format!("CREATE DATABASE `{db_name}`"))
			.execute(&bootstrap)
			.await
			.expect("failed to create database");
		bootstrap.close().await;

		let base_dsn = shared
			.url
			.rsplit_once('/')
			.map(|(base, _)| base.to_string())
			.unwrap_or_else(|| {
				panic!(
					"mysql DSN must include a bootstrap database: {}",
					shared.url
				)
			});
		let dsn = format!("{base_dsn}/{db_name}");
		let pool = MySqlPoolOptions::new()
			.max_connections(2)
			.connect(&dsn)
			.await
			.expect("failed to connect to mysql test db");

		Self { pool, db_name, dsn }
	}

	pub async fn run_sql(&self, sql: &str) {
		sqlx::query(sql)
			.execute(&self.pool)
			.await
			.unwrap_or_else(|e| panic!("sql failed: {e}\nSQL: {sql}"));
	}
}
