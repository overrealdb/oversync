use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;
use tokio::sync::OnceCell;

struct SharedMysqlContainer {
	url: String,
	_container: testcontainers::ContainerAsync<Mysql>,
}

static SHARED_MYSQL: OnceCell<SharedMysqlContainer> = OnceCell::const_new();

async fn shared_mysql() -> &'static SharedMysqlContainer {
	SHARED_MYSQL
		.get_or_init(|| async {
			let container = Mysql::default()
				.with_tag("8.0")
				.start()
				.await
				.expect("failed to start mysql container");

			let host = container.get_host().await.expect("mysql host");
			let port = container
				.get_host_port_ipv4(3306)
				.await
				.expect("mysql port");

			SharedMysqlContainer {
				url: format!("mysql://root@{host}:{port}/test"),
				_container: container,
			}
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

		let dsn = format!("{}/{}", shared.url.trim_end_matches("/test"), db_name);
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
