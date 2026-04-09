mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;
use oversync::config::{
	DeltaDef, DiffMode, OriginDef, PipeConfig, PipeRecipeDef, PipeRecipeType, RetryDef, ScheduleDef,
};
use oversync::embedded::EmbeddedSync;
use oversync::recipes::expand_runtime_pipe;
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, OpType};
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
	PipeConfig {
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
			diff_mode: DiffMode::Db,
			fail_safe_threshold: 95.0,
		},
		retry: RetryDef::default(),
		recipe: Some(PipeRecipeDef {
			recipe_type: PipeRecipeType::PostgresSnapshot,
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
	}
}

async fn create_tables(pg: &TestPostgres, count: usize) {
	for i in 0..count {
		pg.run_sql(&format!(
			r#"CREATE TABLE "{schema}"."table_{i:03}" (
				id INT PRIMARY KEY,
				payload TEXT,
				amount INT
			)"#,
			schema = pg.schema
		))
		.await;
		pg.run_sql(&format!(
			r#"INSERT INTO "{schema}"."table_{i:03}" (id, payload, amount) VALUES ({i}, 'payload {i:03}', {i})"#,
			schema = pg.schema
		))
		.await;
	}
}

async fn mutate_half_tables(pg: &TestPostgres, count: usize) {
	for i in 0..(count / 2) {
		pg.run_sql(&format!(
			r#"UPDATE "{schema}"."table_{i:03}"
SET payload = 'updated {i:03}',
    amount = amount + 1000
WHERE id = {i}"#,
			schema = pg.schema
		))
		.await;
	}
}

async fn run_recipe_cycle(
	sync: &EmbeddedSync,
	query_ids: &[String],
	events: &Arc<Mutex<Vec<EventEnvelope>>>,
) -> Vec<EventEnvelope> {
	events.lock().await.clear();

	for query_id in query_ids {
		sync.run_once("postgresdl", query_id).await.unwrap();
	}

	events.lock().await.clone()
}

#[tokio::test]
async fn postgres_snapshot_recipe_tracks_only_changed_tables() {
	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;
	let events = Arc::new(Mutex::new(Vec::new()));
	let table_count = 12usize;

	create_tables(&pg, table_count).await;

	let expanded = expand_runtime_pipe(make_recipe_pipe(&pg.dsn, &pg.schema))
		.await
		.unwrap();
	let query_ids: Vec<String> = expanded
		.queries
		.iter()
		.map(|query| query.id.clone())
		.collect();
	assert_eq!(query_ids.len(), table_count);

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

	let first_cycle = run_recipe_cycle(&sync, &query_ids, &events).await;
	assert_eq!(first_cycle.len(), table_count);
	assert!(
		first_cycle
			.iter()
			.all(|event| event.meta.op == OpType::Created)
	);

	let second_cycle = run_recipe_cycle(&sync, &query_ids, &events).await;
	assert!(second_cycle.is_empty());

	mutate_half_tables(&pg, table_count).await;

	let third_cycle = run_recipe_cycle(&sync, &query_ids, &events).await;
	assert_eq!(third_cycle.len(), table_count / 2);
	assert!(
		third_cycle
			.iter()
			.all(|event| event.meta.op == OpType::Updated)
	);

	let mut touched: Vec<String> = third_cycle
		.iter()
		.map(|event| event.meta.query_id.clone())
		.collect();
	touched.sort();

	let mut expected: Vec<String> = (0..(table_count / 2))
		.map(|i| format!("postgresdl.{}.table_{i:03}", pg.schema))
		.collect();
	expected.sort();

	assert_eq!(touched, expected);
	assert!(third_cycle.iter().all(|event| {
		event.data["payload"]
			.as_str()
			.is_some_and(|payload| payload.starts_with("updated "))
	}));
}
