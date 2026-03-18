use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

struct SharedPgContainer {
	url: String,
	_container: testcontainers::ContainerAsync<Postgres>,
}

static SHARED_PG: OnceCell<SharedPgContainer> = OnceCell::const_new();

async fn shared_pg() -> &'static SharedPgContainer {
	SHARED_PG
		.get_or_init(|| async {
			let container = Postgres::default()
				.with_tag("11-alpine")
				.start()
				.await
				.expect("failed to start postgres container");

			let host = container.get_host().await.expect("pg host");
			let port = container.get_host_port_ipv4(5432).await.expect("pg port");

			SharedPgContainer {
				url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
				_container: container,
			}
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

		// Create schema using a one-off connection
		let bootstrap = PgPool::connect(&shared.url)
			.await
			.expect("failed to connect for bootstrap");
		sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
			.execute(&bootstrap)
			.await
			.expect("failed to create schema");
		bootstrap.close().await;

		// Build pool with search_path set on every connection
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
