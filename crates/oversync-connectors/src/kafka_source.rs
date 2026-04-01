use std::time::Duration;

use async_trait::async_trait;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use tracing::{debug, info};

use oversync_core::error::OversyncError;
use oversync_core::model::{KafkaAuth, RawRow};
use oversync_core::traits::OriginConnector;

pub struct KafkaSourceConnector {
	consumer: StreamConsumer,
	topic: String,
	source_name: String,
}

impl KafkaSourceConnector {
	pub fn new(
		name: &str,
		brokers: &str,
		topic: &str,
		group_id: &str,
		auto_offset_reset: Option<&str>,
	) -> Result<Self, OversyncError> {
		Self::with_auth(name, brokers, topic, group_id, auto_offset_reset, None)
	}

	pub fn with_auth(
		name: &str,
		brokers: &str,
		topic: &str,
		group_id: &str,
		auto_offset_reset: Option<&str>,
		auth: Option<&KafkaAuth>,
	) -> Result<Self, OversyncError> {
		let mut config = ClientConfig::new();
		config
			.set("bootstrap.servers", brokers)
			.set("group.id", group_id)
			.set("enable.auto.commit", "false")
			.set("auto.offset.reset", auto_offset_reset.unwrap_or("earliest"));

		if let Some(auth) = auth {
			apply_kafka_auth(&mut config, auth);
		}

		let consumer: StreamConsumer = config
			.create()
			.map_err(|e| OversyncError::Connector(format!("kafka consumer create: {e}")))?;

		consumer
			.subscribe(&[topic])
			.map_err(|e| OversyncError::Connector(format!("kafka subscribe: {e}")))?;

		info!(topic, group_id, "kafka source subscribed");

		Ok(Self {
			consumer,
			topic: topic.to_string(),
			source_name: name.to_string(),
		})
	}
}

fn apply_kafka_auth(config: &mut ClientConfig, auth: &KafkaAuth) {
	config.set("security.protocol", &auth.security_protocol);

	if let Some(ref mechanism) = auth.sasl_mechanism {
		config.set("sasl.mechanism", mechanism);
	}
	if let Some(ref username) = auth.sasl_username {
		config.set("sasl.username", username);
	}
	if let Some(ref password) = auth.sasl_password {
		config.set("sasl.password", password);
	}
	if let Some(ref keytab) = auth.sasl_kerberos_keytab {
		config.set("sasl.kerberos.keytab", keytab);
	}
	if let Some(ref principal) = auth.sasl_kerberos_principal {
		config.set("sasl.kerberos.principal", principal);
	}
	if let Some(ref ca) = auth.ssl_ca_location {
		config.set("ssl.ca.location", ca);
	}
	if let Some(ref cert) = auth.ssl_certificate_location {
		config.set("ssl.certificate.location", cert);
	}
	if let Some(ref key) = auth.ssl_key_location {
		config.set("ssl.key.location", key);
	}
}

#[async_trait]
impl OriginConnector for KafkaSourceConnector {
	fn name(&self) -> &str {
		&self.source_name
	}

	async fn fetch_all(&self, _sql: &str, key_column: &str) -> Result<Vec<RawRow>, OversyncError> {
		let mut rows = Vec::new();
		// Longer timeout for the first recv to allow for group rebalance + partition assignment.
		let initial_timeout = Duration::from_secs(10);
		let batch_timeout = Duration::from_secs(2);

		loop {
			let timeout = if rows.is_empty() {
				initial_timeout
			} else {
				batch_timeout
			};
			match tokio::time::timeout(timeout, self.consumer.recv()).await {
				Ok(Ok(msg)) => {
					let Some(payload) = msg.payload() else {
						continue;
					};
					let json: serde_json::Value = serde_json::from_slice(payload)
						.map_err(|e| OversyncError::Connector(format!("kafka json parse: {e}")))?;

					let kafka_key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
					let key = extract_key(&json, key_column, kafka_key.as_deref());
					rows.push(RawRow {
						row_key: key,
						row_data: json,
					});
				}
				Ok(Err(e)) => {
					let err_str = e.to_string();
					if err_str.contains("UnknownTopicOrPartition") {
						debug!(topic = %self.topic, "topic not found, returning empty");
						break;
					}
					return Err(OversyncError::Connector(format!("kafka consume: {e}")));
				}
				Err(_) => break,
			}
		}

		self.consumer
			.commit_consumer_state(CommitMode::Async)
			.map_err(|e| OversyncError::Connector(format!("kafka commit: {e}")))?;

		debug!(count = rows.len(), topic = %self.topic, "fetched messages from kafka");
		Ok(rows)
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.consumer
			.fetch_metadata(Some(&self.topic), Duration::from_secs(5))
			.map_err(|e| OversyncError::Connector(format!("kafka metadata: {e}")))?;
		Ok(())
	}
}

fn extract_key(json: &serde_json::Value, key_column: &str, kafka_key: Option<&str>) -> String {
	if key_column == "_key" {
		return kafka_key.unwrap_or_default().to_string();
	}

	match json.get(key_column) {
		Some(serde_json::Value::String(s)) => s.clone(),
		Some(serde_json::Value::Number(n)) => n.to_string(),
		Some(serde_json::Value::Bool(b)) => b.to_string(),
		Some(v) => v.to_string(),
		None => String::new(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extract_key_from_json_string_field() {
		let json = serde_json::json!({"id": "abc-123", "name": "test"});
		assert_eq!(extract_key(&json, "id", None), "abc-123");
	}

	#[test]
	fn extract_key_from_json_number_field() {
		let json = serde_json::json!({"id": 42, "name": "test"});
		assert_eq!(extract_key(&json, "id", None), "42");
	}

	#[test]
	fn extract_key_from_json_bool_field() {
		let json = serde_json::json!({"active": true});
		assert_eq!(extract_key(&json, "active", None), "true");
	}

	#[test]
	fn extract_key_missing_field_returns_empty() {
		let json = serde_json::json!({"name": "test"});
		assert_eq!(extract_key(&json, "id", None), "");
	}

	#[test]
	fn extract_key_uses_kafka_message_key() {
		let json = serde_json::json!({"name": "test"});
		assert_eq!(extract_key(&json, "_key", Some("msg-key-1")), "msg-key-1");
	}

	#[test]
	fn extract_key_kafka_key_missing_returns_empty() {
		let json = serde_json::json!({"name": "test"});
		assert_eq!(extract_key(&json, "_key", None), "");
	}

	#[test]
	fn extract_key_prefers_kafka_key_over_json_when_underscore_key() {
		let json = serde_json::json!({"_key": "json-key"});
		assert_eq!(extract_key(&json, "_key", Some("kafka-key")), "kafka-key");
	}

	#[test]
	fn factory_rejects_missing_brokers() {
		let config = serde_json::json!({"topic": "t", "group_id": "g"});
		let factory = super::super::factory::KafkaOriginFactory;
		let rt = tokio::runtime::Runtime::new().unwrap();
		let result = rt.block_on(
			<super::super::factory::KafkaOriginFactory as oversync_core::traits::OriginFactory>::create(&factory, "test", &config),
		);
		match result {
			Err(e) => assert!(
				e.to_string().contains("brokers"),
				"expected brokers error, got: {e}"
			),
			Ok(_) => panic!("expected error"),
		}
	}

	#[test]
	fn factory_rejects_missing_topic() {
		let config = serde_json::json!({"brokers": "localhost:9092", "group_id": "g"});
		let factory = super::super::factory::KafkaOriginFactory;
		let rt = tokio::runtime::Runtime::new().unwrap();
		let result = rt.block_on(
			<super::super::factory::KafkaOriginFactory as oversync_core::traits::OriginFactory>::create(&factory, "test", &config),
		);
		match result {
			Err(e) => assert!(
				e.to_string().contains("topic"),
				"expected topic error, got: {e}"
			),
			Ok(_) => panic!("expected error"),
		}
	}

	#[test]
	fn factory_rejects_missing_group_id() {
		let config = serde_json::json!({"brokers": "localhost:9092", "topic": "t"});
		let factory = super::super::factory::KafkaOriginFactory;
		let rt = tokio::runtime::Runtime::new().unwrap();
		let result = rt.block_on(
			<super::super::factory::KafkaOriginFactory as oversync_core::traits::OriginFactory>::create(&factory, "test", &config),
		);
		match result {
			Err(e) => assert!(
				e.to_string().contains("group_id"),
				"expected group_id error, got: {e}"
			),
			Ok(_) => panic!("expected error"),
		}
	}
}
