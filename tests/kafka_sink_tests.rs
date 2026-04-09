mod common;

use std::time::Duration;

use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio_stream::StreamExt;

use oversync_core::model::{EventEnvelope, EventMeta, OpType};
use oversync_core::traits::{Sink, TargetFactory};
use oversync_sinks::{
	KafkaKeyFormat, KafkaSink, KafkaSinkFormat, KafkaTargetFactory, KafkaValueFormat,
};

use common::kafka::TestKafka;

fn unique_topic(prefix: &str) -> String {
	let uid = uuid::Uuid::new_v4().to_string().replace('-', "");
	format!("test_{}_{}", prefix, &uid[..8])
}

fn make_envelope(key: &str, op: OpType) -> EventEnvelope {
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
		data: serde_json::json!({"key": key, "value": format!("data_{key}")}),
	}
}

fn make_envelope_at(
	key: &str,
	op: OpType,
	timestamp: chrono::DateTime<chrono::Utc>,
) -> EventEnvelope {
	let mut envelope = make_envelope(key, op);
	envelope.meta.timestamp = timestamp;
	envelope
}

async fn make_consumer(broker: &str, topic: &str) -> StreamConsumer {
	let group_id = unique_topic("consumer");
	let consumer: StreamConsumer = ClientConfig::new()
		.set("bootstrap.servers", broker)
		.set("group.id", &group_id)
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
	let topic = unique_topic("single_event");

	let sink = KafkaSink::new(&kf.broker, &topic).unwrap();
	let envelope = make_envelope("row_1", OpType::Created);
	sink.send_event(&envelope).await.unwrap();

	// Consume and verify
	let consumer = make_consumer(&kf.broker, &topic).await;
	let msg = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
		.await
		.expect("timeout waiting for message")
		.expect("stream ended")
		.expect("consume error");

	let payload = msg.payload().expect("no payload");
	let received: EventEnvelope = serde_json::from_slice(payload).unwrap();

	assert_eq!(received.meta.key, "row_1");
	assert_eq!(received.meta.op, OpType::Created);
	assert_eq!(received.meta.origin_id, "test-src");
	assert_eq!(received.data["key"], "row_1");

	let key = std::str::from_utf8(msg.key().unwrap()).unwrap();
	assert_eq!(key, "row_1");
}

#[tokio::test]
async fn kafka_sink_produces_batch() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("batch");

	let sink = KafkaSink::new(&kf.broker, &topic).unwrap();
	let envelopes = vec![
		make_envelope("a", OpType::Created),
		make_envelope("b", OpType::Updated),
		make_envelope("c", OpType::Deleted),
	];
	sink.send_batch(&envelopes).await.unwrap();

	let consumer = make_consumer(&kf.broker, &topic).await;
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
	let topic = unique_topic("format");

	let sink = KafkaSink::new(&kf.broker, &topic).unwrap();
	sink.send_event(&make_envelope("x", OpType::Updated))
		.await
		.unwrap();

	let consumer = make_consumer(&kf.broker, &topic).await;
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
	assert_eq!(json["meta"]["origin_id"], "test-src");
	assert_eq!(json["data"]["key"], "x");
}

#[tokio::test]
async fn kafka_sink_compact_format_with_json_object_key() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("compact_json_key");
	let timestamp = chrono::DateTime::parse_from_rfc3339("2026-04-09T06:27:15.926912Z")
		.unwrap()
		.with_timezone(&chrono::Utc);

	let sink = KafkaSink::with_auth_and_format(
		&kf.broker,
		&topic,
		None,
		KafkaSinkFormat {
			key_format: KafkaKeyFormat::JsonObject,
			key_field: "entityId".into(),
			value_format: KafkaValueFormat::Compact,
			created_change_type: "created".into(),
		},
	)
	.unwrap();
	sink.send_event(&make_envelope_at("row_1", OpType::Updated, timestamp))
		.await
		.unwrap();

	let consumer = make_consumer(&kf.broker, &topic).await;
	let msg = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
		.await
		.unwrap()
		.unwrap()
		.unwrap();

	let key: serde_json::Value = serde_json::from_slice(msg.key().unwrap()).unwrap();
	assert_eq!(key, serde_json::json!({"entityId": "row_1"}));

	let value: serde_json::Value = serde_json::from_slice(msg.payload().unwrap()).unwrap();
	assert_eq!(value["meta"]["dateTime"], "2026-04-09T06:27:15.926912Z");
	assert_eq!(value["meta"]["changeType"], "updated");
	assert_eq!(value["data"]["key"], "row_1");
	assert_eq!(value["data"]["value"], "data_row_1");
	assert!(value["meta"].get("op").is_none());
	assert!(value["meta"].get("key").is_none());
}

#[tokio::test]
async fn kafka_sink_test_connection() {
	let kf = TestKafka::new().await;
	let uid = uuid::Uuid::new_v4().to_string().replace('-', "");
	let topic = format!("test_health_{}", &uid[..8]);
	let sink = KafkaSink::new(&kf.broker, &topic).unwrap();
	sink.test_connection().await.unwrap();

	let consumer = make_consumer(&kf.broker, &topic).await;
	let result = tokio::time::timeout(Duration::from_secs(2), consumer.stream().next()).await;
	assert!(
		result.is_err(),
		"test_connection should not publish a health-check message"
	);
}

#[tokio::test]
async fn kafka_sink_name() {
	let kf = TestKafka::new().await;
	let sink = KafkaSink::new(&kf.broker, "t").unwrap();
	assert_eq!(sink.name(), "kafka:t");
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

// ── Factory tests ───────────────────────────────────────────

#[tokio::test]
async fn kafka_factory_creates_working_sink() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("factory");

	let factory = KafkaTargetFactory;
	assert_eq!(factory.sink_type(), "kafka");

	let config = serde_json::json!({"brokers": kf.broker, "topic": &topic});
	let sink = factory.create("my-kafka", &config).await.unwrap();
	assert!(sink.name().starts_with("kafka:"));

	sink.send_event(&make_envelope("fac_1", OpType::Created))
		.await
		.unwrap();

	let consumer = make_consumer(&kf.broker, &topic).await;
	let msg = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
		.await
		.expect("timeout")
		.expect("stream ended")
		.expect("consume error");

	let received: EventEnvelope = serde_json::from_slice(msg.payload().unwrap()).unwrap();
	assert_eq!(received.meta.key, "fac_1");
}

#[tokio::test]
async fn kafka_factory_supports_compact_wire_contract() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("factory_compact");

	let factory = KafkaTargetFactory;
	let config = serde_json::json!({
		"brokers": kf.broker,
		"topic": &topic,
		"key_format": "json_object",
		"key_field": "entityId",
		"value_format": "compact"
	});
	let sink = factory.create("my-kafka", &config).await.unwrap();
	sink.send_event(&make_envelope("fac_2", OpType::Deleted))
		.await
		.unwrap();

	let consumer = make_consumer(&kf.broker, &topic).await;
	let msg = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
		.await
		.expect("timeout")
		.expect("stream ended")
		.expect("consume error");

	let key: serde_json::Value = serde_json::from_slice(msg.key().unwrap()).unwrap();
	assert_eq!(key["entityId"], "fac_2");

	let value: serde_json::Value = serde_json::from_slice(msg.payload().unwrap()).unwrap();
	assert_eq!(value["meta"]["changeType"], "deleted");
	assert_eq!(value["data"]["key"], "fac_2");
	assert!(value["meta"].get("origin_id").is_none());
}

#[tokio::test]
async fn kafka_factory_can_map_created_events_to_updated() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("factory_compact_created_as_updated");

	let factory = KafkaTargetFactory;
	let config = serde_json::json!({
		"brokers": kf.broker,
		"topic": &topic,
		"key_format": "json_object",
		"key_field": "entityId",
		"value_format": "compact",
		"created_change_type": "updated"
	});
	let sink = factory.create("my-kafka", &config).await.unwrap();
	sink.send_event(&make_envelope("fac_3", OpType::Created))
		.await
		.unwrap();

	let consumer = make_consumer(&kf.broker, &topic).await;
	let msg = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
		.await
		.expect("timeout")
		.expect("stream ended")
		.expect("consume error");

	let value: serde_json::Value = serde_json::from_slice(msg.payload().unwrap()).unwrap();
	assert_eq!(value["meta"]["changeType"], "updated");
	assert_eq!(value["data"]["key"], "fac_3");
}

#[tokio::test]
async fn kafka_factory_missing_brokers_errors() {
	let factory = KafkaTargetFactory;
	let config = serde_json::json!({"topic": "t"});
	let result = factory.create("x", &config).await;
	let err = result.err().expect("should error");
	assert!(err.to_string().contains("brokers"));
}

#[tokio::test]
async fn kafka_factory_missing_topic_errors() {
	let factory = KafkaTargetFactory;
	let config = serde_json::json!({"brokers": "localhost:9092"});
	let result = factory.create("x", &config).await;
	let err = result.err().expect("should error");
	assert!(err.to_string().contains("topic"));
}
