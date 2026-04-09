use std::time::Duration;

use async_trait::async_trait;
use chrono::SecondsFormat;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, KafkaAuth, OpType};
use oversync_core::traits::Sink;

pub struct KafkaSink {
	producer: FutureProducer,
	topic: String,
	sink_name: String,
	format: KafkaSinkFormat,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaKeyFormat {
	#[default]
	String,
	JsonObject,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaValueFormat {
	#[default]
	Envelope,
	Compact,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct KafkaSinkFormat {
	pub key_format: KafkaKeyFormat,
	pub key_field: String,
	pub value_format: KafkaValueFormat,
	pub created_change_type: String,
}

impl Default for KafkaSinkFormat {
	fn default() -> Self {
		Self {
			key_format: KafkaKeyFormat::String,
			key_field: default_key_field(),
			value_format: KafkaValueFormat::Envelope,
			created_change_type: "created".into(),
		}
	}
}

pub(crate) fn default_key_field() -> String {
	"key".into()
}

#[derive(Serialize)]
struct CompactMeta {
	#[serde(rename = "dateTime")]
	date_time: String,
	#[serde(rename = "changeType")]
	change_type: String,
}

#[derive(Serialize)]
struct CompactPayload<'a> {
	meta: CompactMeta,
	data: &'a serde_json::Value,
}

impl KafkaSink {
	pub fn new(brokers: &str, topic: &str) -> Result<Self, OversyncError> {
		Self::with_auth_and_format(brokers, topic, None, KafkaSinkFormat::default())
	}

	pub fn with_auth(
		brokers: &str,
		topic: &str,
		auth: Option<&KafkaAuth>,
	) -> Result<Self, OversyncError> {
		Self::with_auth_and_format(brokers, topic, auth, KafkaSinkFormat::default())
	}

	pub fn with_auth_and_format(
		brokers: &str,
		topic: &str,
		auth: Option<&KafkaAuth>,
		format: KafkaSinkFormat,
	) -> Result<Self, OversyncError> {
		let mut config = ClientConfig::new();
		config
			.set("bootstrap.servers", brokers)
			.set("message.timeout.ms", "5000");

		if let Some(auth) = auth {
			apply_kafka_auth(&mut config, auth);
		}

		let producer: FutureProducer = config
			.create()
			.map_err(|e| OversyncError::Sink(format!("kafka producer create: {e}")))?;

		Ok(Self {
			producer,
			topic: topic.to_string(),
			sink_name: format!("kafka:{topic}"),
			format,
		})
	}

	fn serialize_key(&self, envelope: &EventEnvelope) -> Result<Vec<u8>, OversyncError> {
		match self.format.key_format {
			KafkaKeyFormat::String => Ok(envelope.meta.key.as_bytes().to_vec()),
			KafkaKeyFormat::JsonObject => serde_json::to_vec(&serde_json::json!({
				(self.format.key_field.clone()): envelope.meta.key
			}))
			.map_err(|e| OversyncError::Sink(format!("serialize kafka key: {e}"))),
		}
	}

	fn serialize_payload(&self, envelope: &EventEnvelope) -> Result<Vec<u8>, OversyncError> {
		match self.format.value_format {
			KafkaValueFormat::Envelope => serde_json::to_vec(envelope)
				.map_err(|e| OversyncError::Sink(format!("serialize: {e}"))),
			KafkaValueFormat::Compact => serde_json::to_vec(&CompactPayload {
				meta: CompactMeta {
					date_time: envelope
						.meta
						.timestamp
						.to_rfc3339_opts(SecondsFormat::Micros, true),
					change_type: match envelope.meta.op {
						OpType::Created => self.format.created_change_type.clone(),
						OpType::Updated => "updated".into(),
						OpType::Deleted => "deleted".into(),
					},
				},
				data: &envelope.data,
			})
			.map_err(|e| OversyncError::Sink(format!("serialize: {e}"))),
		}
	}
}

pub(crate) fn apply_kafka_auth(config: &mut ClientConfig, auth: &KafkaAuth) {
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
impl Sink for KafkaSink {
	fn name(&self) -> &str {
		&self.sink_name
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let key = self.serialize_key(envelope)?;
		let payload = self.serialize_payload(envelope)?;

		let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);

		self.producer
			.send(record, Duration::from_secs(5))
			.await
			.map_err(|(e, _)| OversyncError::Sink(format!("kafka produce: {e}")))?;

		debug!(topic = %self.topic, key = %envelope.meta.key, "produced event");
		Ok(())
	}

	async fn send_batch(&self, envelopes: &[EventEnvelope]) -> Result<(), OversyncError> {
		let mut deliveries = Vec::with_capacity(envelopes.len());
		for envelope in envelopes {
			let log_key = envelope.meta.key.clone();
			let key = self.serialize_key(envelope)?;
			let payload = self.serialize_payload(envelope)?;
			let record = FutureRecord::to(&self.topic).key(&key).payload(&payload);
			let delivery = self
				.producer
				.send_result(record)
				.map_err(|(e, _)| OversyncError::Sink(format!("kafka produce: {e}")))?;
			deliveries.push((log_key, delivery));
		}

		for (key, delivery) in deliveries {
			delivery
				.await
				.map_err(|_| OversyncError::Sink("kafka produce canceled".into()))?
				.map_err(|(e, _)| OversyncError::Sink(format!("kafka produce: {e}")))?;
			debug!(topic = %self.topic, key = %key, "produced event");
		}

		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		self.producer
			.client()
			.fetch_metadata(Some(&self.topic), Duration::from_secs(5))
			.map_err(|e| OversyncError::Sink(format!("kafka metadata: {e}")))?;

		Ok(())
	}
}
