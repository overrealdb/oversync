use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::debug;

use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;

pub struct KafkaSink {
	producer: FutureProducer,
	topic: String,
}

impl KafkaSink {
	pub fn new(brokers: &str, topic: &str) -> Result<Self, OversyncError> {
		let producer: FutureProducer = ClientConfig::new()
			.set("bootstrap.servers", brokers)
			.set("message.timeout.ms", "5000")
			.create()
			.map_err(|e| OversyncError::Sink(format!("kafka producer create: {e}")))?;

		Ok(Self {
			producer,
			topic: topic.to_string(),
		})
	}
}

#[async_trait]
impl Sink for KafkaSink {
	fn name(&self) -> &str {
		"kafka"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let key = &envelope.meta.key;
		let payload = serde_json::to_string(envelope)
			.map_err(|e| OversyncError::Sink(format!("serialize: {e}")))?;

		let record = FutureRecord::to(&self.topic)
			.key(key)
			.payload(&payload);

		self.producer
			.send(record, Duration::from_secs(5))
			.await
			.map_err(|(e, _)| OversyncError::Sink(format!("kafka produce: {e}")))?;

		debug!(topic = %self.topic, key = %key, "produced event");
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		// Produce a test message to verify connectivity
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
