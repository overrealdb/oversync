mod common;

use oversync_core::model::{EventEnvelope, EventMeta, OpType};
use oversync_core::traits::Sink;
use oversync_sinks::SurrealDbSink;

use common::surreal::TestSurrealContainer;

fn make_envelope(key: &str, op: OpType, data: serde_json::Value) -> EventEnvelope {
	EventEnvelope {
		meta: EventMeta {
			op,
			origin_id: "test-src".into(),
			query_id: "test-q".into(),
			key: key.into(),
			hash: format!("hash_{key}"),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data,
	}
}

#[tokio::test]
async fn surrealdb_sink_upserts_created_event() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client("test", surreal.client.clone(), "synced_items");

	let envelope = make_envelope(
		"item_1",
		OpType::Created,
		serde_json::json!({"name": "alpha", "val": 42}),
	);
	sink.send_event(&envelope).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM synced_items WHERE id = type::record('synced_items', 'item_1')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["data"]["name"], "alpha");
	assert_eq!(rows[0]["data"]["val"], 42);
	assert_eq!(rows[0]["_meta"]["op"], "created");
	assert_eq!(rows[0]["_meta"]["origin_id"], "test-src");
}

#[tokio::test]
async fn surrealdb_sink_upserts_updated_event() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client("test", surreal.client.clone(), "synced_items");

	let create = make_envelope(
		"item_1",
		OpType::Created,
		serde_json::json!({"name": "alpha"}),
	);
	sink.send_event(&create).await.unwrap();

	let update = make_envelope(
		"item_1",
		OpType::Updated,
		serde_json::json!({"name": "alpha_v2"}),
	);
	sink.send_event(&update).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM synced_items WHERE id = type::record('synced_items', 'item_1')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["data"]["name"], "alpha_v2");
	assert_eq!(rows[0]["_meta"]["op"], "updated");
}

#[tokio::test]
async fn surrealdb_sink_handles_deleted_event() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client("test", surreal.client.clone(), "synced_items");

	let create = make_envelope(
		"item_1",
		OpType::Created,
		serde_json::json!({"name": "alpha"}),
	);
	sink.send_event(&create).await.unwrap();

	let delete = make_envelope("item_1", OpType::Deleted, serde_json::Value::Null);
	sink.send_event(&delete).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM synced_items WHERE id = type::record('synced_items', 'item_1')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["_meta"]["op"], "deleted");
}

#[tokio::test]
async fn surrealdb_sink_batch_upserts() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client("test", surreal.client.clone(), "batch_t");

	let envelopes: Vec<EventEnvelope> = (0..10)
		.map(|i| {
			make_envelope(
				&format!("r{i}"),
				OpType::Created,
				serde_json::json!({"idx": i}),
			)
		})
		.collect();

	sink.send_batch(&envelopes).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT count() FROM batch_t GROUP ALL")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows[0]["count"], 10);
}

#[tokio::test]
async fn surrealdb_sink_test_connection() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client("test", surreal.client.clone(), "t");
	sink.test_connection().await.unwrap();
}

#[tokio::test]
async fn surrealdb_sink_name() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client("my-surreal", surreal.client.clone(), "t");
	assert_eq!(sink.name(), "my-surreal");
}
