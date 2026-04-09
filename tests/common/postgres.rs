use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::OnceCell;

use super::stack::{env_var, retry_async};

struct SharedPgEndpoint {
	url: String,
}

static SHARED_PG: OnceCell<SharedPgEndpoint> = OnceCell::const_new();

async fn shared_pg() -> &'static SharedPgEndpoint {
	SHARED_PG
		.get_or_init(|| async {
			let url = env_var(
				"OVERSYNC_TEST_POSTGRES_DSN",
				"postgres://postgres:postgres@127.0.0.1:55432/postgres",
			);

			retry_async("postgres", 30, Duration::from_secs(1), || {
				let url = url.clone();
				async move {
					let pool = PgPoolOptions::new()
						.max_connections(1)
						.connect(&url)
						.await?;
					sqlx::query("SELECT 1").execute(&pool).await?;
					pool.close().await;
					Ok::<_, sqlx::Error>(())
				}
			})
			.await;

			SharedPgEndpoint { url }
		})
		.await
}

pub struct TestPostgres {
	pub pool: PgPool,
	pub schema: String,
	pub dsn: String,
}

impl TestPostgres {
	pub async fn new() -> Self {
		let shared = shared_pg().await;
		let test_id = uuid::Uuid::new_v4().to_string().replace('-', "");
		let schema = format!("test_{}", &test_id[..8]);

		let bootstrap = PgPool::connect(&shared.url)
			.await
			.expect("failed to connect for bootstrap");
		sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
			.execute(&bootstrap)
			.await
			.expect("failed to create schema");
		bootstrap.close().await;

		let schema_clone = schema.clone();
		let pool = PgPoolOptions::new()
			.max_connections(2)
			.after_connect(move |conn, _meta| {
				let s = schema_clone.clone();
				Box::pin(async move {
					sqlx::query(&format!("SET search_path TO \"{s}\""))
						.execute(&mut *conn)
						.await?;
					Ok(())
				})
			})
			.connect(&shared.url)
			.await
			.expect("failed to connect to postgres");

		Self {
			pool,
			schema,
			dsn: shared.url.clone(),
		}
	}

	pub async fn run_sql(&self, sql: &str) {
		sqlx::query(sql)
			.execute(&self.pool)
			.await
			.unwrap_or_else(|e| panic!("sql failed: {e}\nSQL: {sql}"));
	}
}
