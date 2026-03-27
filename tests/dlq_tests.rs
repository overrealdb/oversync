mod common;

use common::surreal::TestSurrealContainer;
use oversync::dlq::{delete_dlq_entry, list_dlq, send_to_dlq};
use oversync_core::model::{EventEnvelope, EventMeta, OpType};

fn test_envelopes() -> Vec<EventEnvelope> {
	vec![
		EventEnvelope {
			meta: EventMeta {
				op: OpType::Created,
				origin_id: "pipe-a".into(),
				query_id: "q1".into(),
				key: "1".into(),
				hash: "h1".into(),
				cycle_id: 1,
				timestamp: chrono::Utc::now(),
			},
			data: serde_json::json!({"name": "alice"}),
		},
		EventEnvelope {
			meta: EventMeta {
				op: OpType::Updated,
				origin_id: "pipe-a".into(),
				query_id: "q1".into(),
				key: "2".into(),
				hash: "h2".into(),
				cycle_id: 1,
				timestamp: chrono::Utc::now(),
			},
			data: serde_json::json!({"name": "bob"}),
		},
	]
}

#[tokio::test]
async fn send_and_list_dlq() {
	let surreal = TestSurrealContainer::new().await;

	send_to_dlq(
		&surreal.client,
		"pipe-a",
		"q1",
		1,
		&test_envelopes(),
		"connection refused",
		3,
	)
	.await
	.unwrap();

	let entries = list_dlq(&surreal.client).await.unwrap();
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].pipe, "pipe-a");
	assert_eq!(entries[0].query, "q1");
	assert_eq!(entries[0].events_count, 2);
	assert_eq!(entries[0].error, "connection refused");
	assert_eq!(entries[0].attempts, 3);
}

#[tokio::test]
async fn delete_dlq_entry_removes() {
	let surreal = TestSurrealContainer::new().await;

	send_to_dlq(
		&surreal.client,
		"pipe-a",
		"q1",
		1,
		&test_envelopes(),
		"timeout",
		1,
	)
	.await
	.unwrap();

	let entries = list_dlq(&surreal.client).await.unwrap();
	assert_eq!(entries.len(), 1);

	delete_dlq_entry(&surreal.client, &entries[0].id)
		.await
		.unwrap();

	let after = list_dlq(&surreal.client).await.unwrap();
	assert!(after.is_empty());
}

#[tokio::test]
async fn dlq_multiple_entries() {
	let surreal = TestSurrealContainer::new().await;

	for i in 0..3 {
		send_to_dlq(
			&surreal.client,
			&format!("pipe-{i}"),
			"q1",
			i as i64,
			&test_envelopes(),
			&format!("error {i}"),
			1,
		)
		.await
		.unwrap();
	}

	let entries = list_dlq(&surreal.client).await.unwrap();
	assert_eq!(entries.len(), 3);
}

#[tokio::test]
async fn dlq_empty_list() {
	let surreal = TestSurrealContainer::new().await;
	let entries = list_dlq(&surreal.client).await.unwrap();
	assert!(entries.is_empty());
}
