mod common;

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use oversync_connectors::{KafkaOriginFactory, KafkaSourceConnector};
use oversync_core::traits::{OriginConnector, OriginFactory};

use common::kafka::TestKafka;

fn unique_topic(prefix: &str) -> String {
	let uid = uuid::Uuid::new_v4().to_string().replace('-', "");
	format!("test_{}_{}", prefix, &uid[..8])
}

async fn produce(broker: &str, topic: &str, key: &str, payload: &str) {
	let producer: FutureProducer = ClientConfig::new()
		.set("bootstrap.servers", broker)
		.set("message.timeout.ms", "5000")
		.create()
		.unwrap();
	let record = FutureRecord::to(topic).key(key).payload(payload);
	producer.send(record, Duration::from_secs(5)).await.unwrap();
}

#[tokio::test]
async fn kafka_source_fetches_single_message() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("single");

	produce(&kf.broker, &topic, "k1", r#"{"id":"1","name":"alice"}"#).await;

	tokio::time::sleep(Duration::from_secs(2)).await;

	let connector =
		KafkaSourceConnector::new("test", &kf.broker, &topic, "grp_single", None).unwrap();

	let rows = connector.fetch_all("", "id").await.unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "1");
	assert_eq!(rows[0].row_data["name"], "alice");
}

#[tokio::test]
async fn kafka_source_fetches_batch() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("batch");

	for i in 0..5 {
		produce(
			&kf.broker,
			&topic,
			&format!("k{i}"),
			&format!(r#"{{"id":"{i}","val":"v{i}"}}"#),
		)
		.await;
	}

	tokio::time::sleep(Duration::from_secs(2)).await;

	let connector =
		KafkaSourceConnector::new("test", &kf.broker, &topic, "grp_batch", None).unwrap();

	let rows = connector.fetch_all("", "id").await.unwrap();
	assert_eq!(rows.len(), 5);

	let mut keys: Vec<String> = rows.iter().map(|r| r.row_key.clone()).collect();
	keys.sort();
	assert_eq!(keys, vec!["0", "1", "2", "3", "4"]);
}

#[tokio::test]
async fn kafka_source_extracts_key_from_json_field() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("jsonkey");

	produce(
		&kf.broker,
		&topic,
		"irrelevant",
		r#"{"id":"abc","color":"red"}"#,
	)
	.await;

	tokio::time::sleep(Duration::from_secs(2)).await;

	let connector =
		KafkaSourceConnector::new("test", &kf.broker, &topic, "grp_jsonkey", None).unwrap();

	let rows = connector.fetch_all("", "id").await.unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "abc");
}

#[tokio::test]
async fn kafka_source_extracts_kafka_message_key() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("msgkey");

	produce(&kf.broker, &topic, "mk1", r#"{"name":"bob"}"#).await;

	tokio::time::sleep(Duration::from_secs(2)).await;

	let connector =
		KafkaSourceConnector::new("test", &kf.broker, &topic, "grp_msgkey", None).unwrap();

	let rows = connector.fetch_all("", "_key").await.unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "mk1");
}

#[tokio::test]
async fn kafka_source_test_connection() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("health");

	let connector =
		KafkaSourceConnector::new("test", &kf.broker, &topic, "grp_health", None).unwrap();

	connector.test_connection().await.unwrap();
}

#[tokio::test]
async fn kafka_source_factory_creates_connector() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("factory");

	produce(&kf.broker, &topic, "fk1", r#"{"id":"f1","x":99}"#).await;

	tokio::time::sleep(Duration::from_secs(2)).await;

	let factory = KafkaOriginFactory;
	assert_eq!(factory.connector_type(), "kafka");

	let config = serde_json::json!({
		"brokers": kf.broker,
		"topic": topic,
		"group_id": "grp_factory",
	});
	let connector = factory.create("kafka-src", &config).await.unwrap();

	let rows = connector.fetch_all("", "id").await.unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].row_key, "f1");
	assert_eq!(rows[0].row_data["x"], 99);
}

#[tokio::test]
async fn kafka_source_empty_topic_returns_empty() {
	let kf = TestKafka::new().await;
	let topic = unique_topic("empty");

	let connector =
		KafkaSourceConnector::new("test", &kf.broker, &topic, "grp_empty", None).unwrap();

	tokio::time::sleep(Duration::from_secs(2)).await;

	let rows = connector.fetch_all("", "id").await.unwrap();
	assert!(rows.is_empty());
}
