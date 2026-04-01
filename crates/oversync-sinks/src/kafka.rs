use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, KafkaAuth};
use oversync_core::traits::Sink;

pub struct KafkaSink {
	producer: FutureProducer,
	topic: String,
	sink_name: String,
}

impl KafkaSink {
	pub fn new(brokers: &str, topic: &str) -> Result<Self, OversyncError> {
		Self::with_auth(brokers, topic, None)
	}

	pub fn with_auth(
		brokers: &str,
		topic: &str,
		auth: Option<&KafkaAuth>,
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
		})
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
		let key = &envelope.meta.key;
		let payload = serde_json::to_string(envelope)
			.map_err(|e| OversyncError::Sink(format!("serialize: {e}")))?;

		let record = FutureRecord::to(&self.topic).key(key).payload(&payload);

		self.producer
			.send(record, Duration::from_secs(5))
			.await
			.map_err(|(e, _)| OversyncError::Sink(format!("kafka produce: {e}")))?;

		debug!(topic = %self.topic, key = %key, "produced event");
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		let record = FutureRecord::to(&self.topic)
			.key("__oversync_health_check")
			.payload("{}");

		self.producer
			.send(record, Duration::from_secs(5))
			.await
			.map_err(|(e, _)| OversyncError::Sink(format!("kafka test: {e}")))?;

		Ok(())
	}
}
