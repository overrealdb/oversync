mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;
use oversync::config::{
	DeltaDef, OriginDef, PipeConfig, PipeRecipeDef, PipeRecipeType, RetryDef, ScheduleDef,
	SurrealDbDef, SyncConfig,
};
use oversync::embedded::EmbeddedSync;
use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;
use tokio::sync::Mutex;

struct RecordingSink {
	events: Arc<Mutex<Vec<EventEnvelope>>>,
}

#[async_trait]
impl Sink for RecordingSink {
	fn name(&self) -> &str {
		"recorder"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		self.events.lock().await.push(envelope.clone());
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

fn make_recipe_pipe(dsn: &str, schema: &str) -> PipeConfig {
	let config = SyncConfig {
		surrealdb: SurrealDbDef {
			url: "mem://".into(),
			username: String::new(),
			password: String::new(),
			namespace: "oversync".into(),
			database: "sync".into(),
			snapshot: None,
		},
		sources: vec![],
		sinks: vec![],
		pipes: vec![PipeConfig {
			name: "postgresdl".into(),
			origin: OriginDef {
				connector: "postgres".into(),
				dsn: dsn.into(),
				credential: None,
				trino_url: None,
				config: serde_json::Value::Null,
			},
			targets: vec!["recorder".into()],
			queries: vec![],
			schedule: ScheduleDef {
				interval_secs: 900,
				..ScheduleDef::default()
			},
			delta: DeltaDef {
				diff_mode: oversync::config::DiffMode::Db,
				fail_safe_threshold: 95.0,
			},
			retry: RetryDef::default(),
			recipe: Some(PipeRecipeDef {
				recipe_type: PipeRecipeType::PostgresMetadata,
				prefix: "postgresdl".into(),
				entity_type_id: None,
				schema_id: "table".into(),
				schemas: vec![schema.into()],
			}),
			filters: vec![],
			transforms: vec![],
			links: vec![],
			alert_webhook: None,
			enabled: true,
		}],
	};

	config
		.effective_pipes()
		.into_iter()
		.next()
		.expect("expanded recipe pipe")
}

async fn create_tables(pg: &TestPostgres, count: usize) {
	for i in 0..count {
		pg.run_sql(&format!(
			r#"CREATE TABLE "{schema}"."table_{i:03}" (
				id TEXT PRIMARY KEY,
				payload TEXT,
				amount INT
			)"#,
			schema = pg.schema
		))
		.await;
		pg.run_sql(&format!(
			r#"COMMENT ON COLUMN "{schema}"."table_{i:03}"."payload" IS 'payload {i:03}'"#,
			schema = pg.schema
		))
		.await;
	}
}

async fn mutate_half_tables(pg: &TestPostgres, count: usize) {
	for i in 0..(count / 2) {
		pg.run_sql(&format!(
			r#"ALTER TABLE "{schema}"."table_{i:03}" RENAME COLUMN "payload" TO "payload_v2""#,
			schema = pg.schema
		))
		.await;
		pg.run_sql(&format!(
			r#"COMMENT ON COLUMN "{schema}"."table_{i:03}"."payload_v2" IS 'renamed {i:03}'"#,
			schema = pg.schema
		))
		.await;
	}
}

async fn run_recipe_cycle(
	sync: &EmbeddedSync,
	events: &Arc<Mutex<Vec<EventEnvelope>>>,
) -> Vec<EventEnvelope> {
	events.lock().await.clear();

	let entity = sync.run_once("postgresdl", "entity").await.unwrap();
	let aspect = sync.run_once("postgresdl", "aspect-table").await.unwrap();

	assert_eq!(
		entity.created.len()
			+ entity.updated.len()
			+ entity.deleted.len()
			+ aspect.created.len()
			+ aspect.updated.len()
			+ aspect.deleted.len(),
		events.lock().await.len(),
		"recorded sink events should match diff totals"
	);

	events.lock().await.clone()
}

#[tokio::test]
async fn postgres_metadata_recipe_tracks_only_changed_tables() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;
	let events = Arc::new(Mutex::new(Vec::new()));
	let table_count = 20usize;

	create_tables(&pg, table_count).await;

	let sync = EmbeddedSync::builder()
		.state_db(surreal.client.clone())
		.skip_schema()
		.add_sink(
			"recorder",
			Arc::new(RecordingSink {
				events: events.clone(),
			}),
		)
		.add_pipe(make_recipe_pipe(&pg.dsn, &pg.schema))
		.build()
		.await
		.unwrap();

	let first_cycle = run_recipe_cycle(&sync, &events).await;
	assert_eq!(first_cycle.len(), table_count * 2, "entity + aspect-table");
	assert_eq!(
		first_cycle
			.iter()
			.filter(|event| event.meta.query_id == "entity")
			.count(),
		table_count
	);
	assert_eq!(
		first_cycle
			.iter()
			.filter(|event| event.meta.query_id == "aspect-table")
			.count(),
		table_count
	);
	assert!(
		first_cycle
			.iter()
			.all(|event| { event.meta.op == oversync_core::model::OpType::Created })
	);

	let second_cycle = run_recipe_cycle(&sync, &events).await;
	assert!(
		second_cycle.is_empty(),
		"unchanged metadata should produce no events"
	);

	mutate_half_tables(&pg, table_count).await;

	let third_cycle = run_recipe_cycle(&sync, &events).await;
	assert_eq!(third_cycle.len(), table_count / 2);
	assert!(third_cycle.iter().all(|event| {
		event.meta.query_id == "aspect-table"
			&& event.meta.op == oversync_core::model::OpType::Updated
	}));

	let mut touched: Vec<String> = third_cycle
		.iter()
		.map(|event| {
			event.data["entityId"]
				.as_str()
				.expect("entityId")
				.to_string()
		})
		.collect();
	touched.sort();

	let mut expected: Vec<String> = (0..(table_count / 2))
		.map(|i| format!("postgresdl.{}.table_{i:03}", pg.schema))
		.collect();
	expected.sort();

	assert_eq!(touched, expected);
}
