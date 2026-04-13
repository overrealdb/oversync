use std::time::Duration;

use oversync_core::runtime_surreal_url;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use tokio::sync::OnceCell;

use super::stack::{env_var, retry_async};

struct SharedContainer {
	url: String,
	username: String,
	password: String,
}

static SHARED: OnceCell<SharedContainer> = OnceCell::const_new();

async fn shared_container() -> &'static SharedContainer {
	SHARED
		.get_or_init(|| async {
			let url = env_var("OVERSYNC_TEST_SURREAL_URL", "ws://127.0.0.1:58000");
			let username = env_var("OVERSYNC_TEST_SURREAL_USERNAME", "root");
			let password = env_var("OVERSYNC_TEST_SURREAL_PASSWORD", "root");

			retry_async("surrealdb", 30, Duration::from_secs(1), || {
				let url = url.clone();
				let username = username.clone();
				let password = password.clone();
				async move {
					let runtime_url = runtime_surreal_url(&url);
					let client = surrealdb::engine::any::connect(runtime_url.as_ref())
						.await
						.map_err(|e| e.to_string())?;
					client
						.signin(Root { username, password })
						.await
						.map_err(|e| e.to_string())?;
					Ok::<_, String>(())
				}
			})
			.await;

			SharedContainer {
				url,
				username,
				password,
			}
		})
		.await
}

pub struct TestSurrealContainer {
	pub client: Surreal<Any>,
	pub ns: String,
	pub db: String,
}

impl TestSurrealContainer {
	pub async fn new() -> Self {
		let t = Self::new_raw().await;
		let mut manifest = overshift::Manifest::load("crates/oversync-queries/surql/")
			.expect("manifest load failed");
		manifest.meta.ns = t.ns.clone();
		manifest.meta.db = t.db.clone();
		let plan = overshift::plan(&t.client, &manifest)
			.await
			.expect("plan failed");
		plan.apply(&t.client).await.expect("apply failed");
		t
	}

	pub async fn url() -> String {
		shared_container().await.url.clone()
	}

	pub async fn new_raw() -> Self {
		let shared = shared_container().await;

		let test_id = uuid::Uuid::new_v4().to_string().replace('-', "");
		let test_id = &test_id[..8];
		let ns = format!("test_{test_id}");
		let db = format!("sync_{test_id}");

		let runtime_url = runtime_surreal_url(&shared.url);
		let client = surrealdb::engine::any::connect(runtime_url.as_ref())
			.await
			.unwrap_or_else(|e| panic!("failed to connect to SurrealDB at {}: {e}", shared.url));

		client
			.signin(Root {
				username: shared.username.clone(),
				password: shared.password.clone(),
			})
			.await
			.expect("failed to signin");

		client
			.use_ns(&ns)
			.use_db(&db)
			.await
			.expect("failed to select ns/db");

		Self { client, ns, db }
	}
}
