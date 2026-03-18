use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::surrealdb::SurrealDb;
use tokio::sync::OnceCell;

struct SharedContainer {
	url: String,
	_container: ContainerAsync<SurrealDb>,
}

static SHARED: OnceCell<SharedContainer> = OnceCell::const_new();

async fn shared_container() -> &'static SharedContainer {
	SHARED
		.get_or_init(|| async {
			let surreal = SurrealDb::default();

			let container = if let Ok(image) = std::env::var("TESTCONTAINERS_SURREAL_IMAGE") {
				surreal.with_name(image).start().await
			} else {
				surreal.with_tag("v3").start().await
			}
			.expect("failed to start SurrealDB container");

			let host = container
				.get_host()
				.await
				.expect("failed to get container host");
			let port = container
				.get_host_port_ipv4(8000)
				.await
				.expect("failed to get container port");

			SharedContainer {
				url: format!("http://{host}:{port}"),
				_container: container,
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
		let mut manifest = overshift::Manifest::load("surql/").expect("manifest load failed");
		manifest.meta.ns = t.ns.clone();
		manifest.meta.db = t.db.clone();
		let plan = overshift::plan(&t.client, &manifest)
			.await
			.expect("plan failed");
		plan.apply(&t.client).await.expect("apply failed");
		t
	}

	pub async fn new_raw() -> Self {
		let shared = shared_container().await;

		let test_id = uuid::Uuid::new_v4().to_string().replace('-', "");
		let test_id = &test_id[..8];
		let ns = format!("test_{test_id}");
		let db = format!("sync_{test_id}");

		let client = surrealdb::engine::any::connect(&shared.url)
			.await
			.unwrap_or_else(|e| panic!("failed to connect to SurrealDB at {}: {e}", shared.url));

		client
			.signin(Root {
				username: "root".to_string(),
				password: "root".to_string(),
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
