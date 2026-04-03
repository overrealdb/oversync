mod common;

use oversync_core::model::{EventEnvelope, EventMeta, OpType};
use oversync_core::traits::Sink;
use oversync_sinks::{SinkMode, SurrealDbSink};

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
	let sink = SurrealDbSink::from_client(
		"test",
		surreal.client.clone(),
		"synced_items",
		SinkMode::Envelope,
		None,
	);

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
	let sink = SurrealDbSink::from_client(
		"test",
		surreal.client.clone(),
		"synced_items",
		SinkMode::Envelope,
		None,
	);

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
	let sink = SurrealDbSink::from_client(
		"test",
		surreal.client.clone(),
		"synced_items",
		SinkMode::Envelope,
		None,
	);

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
	let sink = SurrealDbSink::from_client(
		"test",
		surreal.client.clone(),
		"batch_t",
		SinkMode::Envelope,
		None,
	);

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
	let sink = SurrealDbSink::from_client(
		"test",
		surreal.client.clone(),
		"t",
		SinkMode::Envelope,
		None,
	);
	sink.test_connection().await.unwrap();
}

#[tokio::test]
async fn surrealdb_sink_name() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client(
		"my-surreal",
		surreal.client.clone(),
		"t",
		SinkMode::Envelope,
		None,
	);
	assert_eq!(sink.name(), "my-surreal");
}

// ── Document mode ─────────────────────────────────────────────

#[tokio::test]
async fn document_mode_merges_data_as_top_level_fields() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client(
		"doc-test",
		surreal.client.clone(),
		"entity",
		SinkMode::Document,
		None,
	);

	let envelope = make_envelope(
		"my_table",
		OpType::Created,
		serde_json::json!({
			"urn": "pg.public.my_table",
			"name": "my_table",
			"entity_type": "table",
			"description": "A test table",
		}),
	);
	sink.send_event(&envelope).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM entity WHERE id = type::record('entity', 'my_table')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	// Fields are top-level, not nested under data
	assert_eq!(rows[0]["urn"], "pg.public.my_table");
	assert_eq!(rows[0]["name"], "my_table");
	assert_eq!(rows[0]["entity_type"], "table");
	assert_eq!(rows[0]["description"], "A test table");
	// _meta is present
	assert_eq!(rows[0]["_meta"]["op"], "created");
	// No nested data field
	assert!(rows[0]["data"].is_null());
	// Timestamps
	assert!(!rows[0]["created_at"].is_null());
	assert!(!rows[0]["updated_at"].is_null());
}

#[tokio::test]
async fn document_mode_preserves_created_at_on_update() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client(
		"doc-test",
		surreal.client.clone(),
		"entity",
		SinkMode::Document,
		None,
	);

	let create = make_envelope("tbl", OpType::Created, serde_json::json!({"name": "v1"}));
	sink.send_event(&create).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT created_at FROM entity WHERE id = type::record('entity', 'tbl')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	let created_at = rows[0]["created_at"].clone();

	// Small delay to ensure updated_at differs
	tokio::time::sleep(std::time::Duration::from_millis(10)).await;

	let update = make_envelope("tbl", OpType::Updated, serde_json::json!({"name": "v2"}));
	sink.send_event(&update).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT * FROM entity WHERE id = type::record('entity', 'tbl')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows[0]["name"], "v2");
	assert_eq!(rows[0]["created_at"], created_at); // preserved
}

#[tokio::test]
async fn document_mode_with_key_field() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client(
		"doc-test",
		surreal.client.clone(),
		"entity",
		SinkMode::Document,
		Some("urn".into()),
	);

	let envelope = make_envelope(
		"ignored_key",
		OpType::Created,
		serde_json::json!({
			"urn": "pg.public.users",
			"name": "users",
		}),
	);
	sink.send_event(&envelope).await.unwrap();

	// Record ID should use urn value, not meta.key
	let mut res = surreal
		.client
		.query("SELECT * FROM entity WHERE id = type::record('entity', 'pg.public.users')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["name"], "users");
}

#[tokio::test]
async fn document_mode_batch() {
	let surreal = TestSurrealContainer::new_raw().await;
	let sink = SurrealDbSink::from_client(
		"doc-batch",
		surreal.client.clone(),
		"entity",
		SinkMode::Document,
		Some("urn".into()),
	);

	let envelopes: Vec<EventEnvelope> = (0..5)
		.map(|i| {
			make_envelope(
				&format!("key_{i}"),
				OpType::Created,
				serde_json::json!({
					"urn": format!("pg.public.table_{i}"),
					"name": format!("table_{i}"),
					"entity_type": "table",
				}),
			)
		})
		.collect();

	sink.send_batch(&envelopes).await.unwrap();

	let mut res = surreal
		.client
		.query("SELECT count() FROM entity GROUP ALL")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows[0]["count"], 5);

	// Verify key_field was used
	let mut res = surreal
		.client
		.query("SELECT * FROM entity WHERE id = type::record('entity', 'pg.public.table_2')")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["name"], "table_2");
	assert!(!rows[0]["created_at"].is_null());
}
