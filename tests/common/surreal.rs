use surrealdb::engine::any::Any;
use surrealdb::Surreal;

pub struct TestSurreal {
	pub client: Surreal<Any>,
	pub ns: String,
	pub db: String,
}

impl TestSurreal {
	pub async fn new() -> Self {
		let t = Self::new_raw().await;
		oversync_migrate::run_migrations(&t.client)
			.await
			.expect("migrations failed");
		t
	}

	pub async fn new_raw() -> Self {
		let ns = format!("test_{}", uuid::Uuid::new_v4().simple());
		let db = "sync";

		let client = surrealdb::engine::any::connect("mem://")
			.await
			.expect("mem connect failed");
		client
			.use_ns(&ns)
			.use_db(db)
			.await
			.expect("use ns/db failed");

		Self {
			client,
			ns,
			db: db.into(),
		}
	}
}
