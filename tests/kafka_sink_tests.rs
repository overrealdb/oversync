mod common;

use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::Message;
use tokio_stream::StreamExt;

use oversync_core::model::{EventEnvelope, EventMeta, OpType};
use oversync_core::traits::Sink;
use oversync_sinks::KafkaSink;

use common::kafka::TestKafka;

fn make_envelope(key: &str, op: OpType) -> EventEnvelope {
	EventEnvelope {
		meta: EventMeta {
			op,
			source_id: "test-src".into(),
			query_id: "test-q".into(),
			key: key.into(),
			hash: format!("hash_{key}"),
			cycle_id: 1,
			timestamp: chrono::Utc::now(),
		},
		data: serde_json::json!({"key": key, "value": format!("data_{key}")}),
	}
}

async fn make_consumer(broker: &str, topic: &str) -> StreamConsumer {
	let consumer: StreamConsumer = ClientConfig::new()
		.set("bootstrap.servers", broker)
		.set("group.id", "test-consumer")
		.set("auto.offset.reset", "earliest")
		.set("enable.auto.commit", "false")
		.create()
		.expect("consumer creation failed");

	consumer.subscribe(&[topic]).expect("subscribe failed");
	consumer
}

#[tokio::test]
async fn kafka_sink_produces_single_event() {
	let kf = TestKafka::new().await;
	let topic = "test_single_event";

	let sink = KafkaSink::new(&kf.broker, topic).unwrap();
	let envelope = make_envelope("row_1", OpType::Created);
	sink.send_event(&envelope).await.unwrap();

	// Consume and verify
	let consumer = make_consumer(&kf.broker, topic).await;
	let msg = tokio::time::timeout(
		Duration::from_secs(10),
		consumer.stream().next(),
	)
	.await
	.expect("timeout waiting for message")
	.expect("stream ended")
	.expect("consume error");

	let payload = msg.payload().expect("no payload");
	let received: EventEnvelope = serde_json::from_slice(payload).unwrap();

	assert_eq!(received.meta.key, "row_1");
	assert_eq!(received.meta.op, OpType::Created);
	assert_eq!(received.meta.source_id, "test-src");
	assert_eq!(received.data["key"], "row_1");

	let key = std::str::from_utf8(msg.key().unwrap()).unwrap();
	assert_eq!(key, "row_1");
}

#[tokio::test]
async fn kafka_sink_produces_batch() {
	let kf = TestKafka::new().await;
	let topic = "test_batch";

	let sink = KafkaSink::new(&kf.broker, topic).unwrap();
	let envelopes = vec![
		make_envelope("a", OpType::Created),
		make_envelope("b", OpType::Updated),
		make_envelope("c", OpType::Deleted),
	];
	sink.send_batch(&envelopes).await.unwrap();

	let consumer = make_consumer(&kf.broker, topic).await;
	let mut received = Vec::new();

	for _ in 0..3 {
		let msg = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
			.await
			.expect("timeout")
			.expect("stream ended")
			.expect("consume error");

		let payload = msg.payload().expect("no payload");
		let env: EventEnvelope = serde_json::from_slice(payload).unwrap();
		received.push(env);
	}

	assert_eq!(received.len(), 3);
	let mut keys: Vec<String> = received.iter().map(|e| e.meta.key.clone()).collect();
	keys.sort();
	assert_eq!(keys, vec!["a", "b", "c"]);

	let ops: Vec<OpType> = received.iter().map(|e| e.meta.op).collect();
	assert!(ops.contains(&OpType::Created));
	assert!(ops.contains(&OpType::Updated));
	assert!(ops.contains(&OpType::Deleted));
}

#[tokio::test]
async fn kafka_sink_envelope_format() {
	let kf = TestKafka::new().await;
	let topic = "test_format";

	let sink = KafkaSink::new(&kf.broker, topic).unwrap();
	sink.send_event(&make_envelope("x", OpType::Updated))
		.await
		.unwrap();

	let consumer = make_consumer(&kf.broker, topic).await;
	let msg = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
		.await
		.unwrap()
		.unwrap()
		.unwrap();

	let json: serde_json::Value = serde_json::from_slice(msg.payload().unwrap()).unwrap();

	// Verify envelope structure: { meta: {...}, data: {...} }
	assert!(json.get("meta").is_some(), "must have meta");
	assert!(json.get("data").is_some(), "must have data");
	assert_eq!(json["meta"]["op"], "updated");
	assert_eq!(json["meta"]["key"], "x");
	assert_eq!(json["meta"]["source_id"], "test-src");
	assert_eq!(json["data"]["key"], "x");
}

#[tokio::test]
async fn kafka_sink_test_connection() {
	let kf = TestKafka::new().await;
	let sink = KafkaSink::new(&kf.broker, "test_health").unwrap();
	sink.test_connection().await.unwrap();
}

#[tokio::test]
async fn kafka_sink_name() {
	let kf = TestKafka::new().await;
	let sink = KafkaSink::new(&kf.broker, "t").unwrap();
	assert_eq!(sink.name(), "kafka");
}

#[tokio::test]
async fn kafka_sink_many_events() {
	let kf = TestKafka::new().await;
	let uid = uuid::Uuid::new_v4().to_string().replace('-', "");
	let topic = format!("test_many_{}", &uid[..8]);

	let sink = KafkaSink::new(&kf.broker, &topic).unwrap();

	// Produce 500 events
	for i in 0..500 {
		let env = make_envelope(&format!("r{i:04}"), OpType::Created);
		sink.send_event(&env).await.unwrap();
	}

	// Consume all — wait for partition assignment first
	let consumer = make_consumer(&kf.broker, &topic).await;
	tokio::time::sleep(Duration::from_secs(2)).await;

	let mut count = 0;
	loop {
		match tokio::time::timeout(Duration::from_secs(10), consumer.stream().next()).await {
			Ok(Some(Ok(_))) => count += 1,
			_ => break,
		}
		if count >= 500 {
			break;
		}
	}

	assert_eq!(count, 500, "all 500 events consumed");
}
