use std::collections::HashMap;

use oversync_core::error::OversyncError;
use oversync_core::traits::{Sink, SinkFactory, SourceConnector, SourceFactory};

pub struct PluginRegistry {
	sources: HashMap<String, Box<dyn SourceFactory>>,
	sinks: HashMap<String, Box<dyn SinkFactory>>,
}

impl PluginRegistry {
	pub fn new() -> Self {
		Self {
			sources: HashMap::new(),
			sinks: HashMap::new(),
		}
	}

	pub fn register_source(&mut self, factory: Box<dyn SourceFactory>) {
		self.sources
			.insert(factory.connector_type().to_string(), factory);
	}

	pub fn register_sink(&mut self, factory: Box<dyn SinkFactory>) {
		self.sinks.insert(factory.sink_type().to_string(), factory);
	}

	pub async fn create_source(
		&self,
		connector_type: &str,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn SourceConnector>, OversyncError> {
		let factory = self.sources.get(connector_type).ok_or_else(|| {
			OversyncError::Plugin(format!("unknown source type: {connector_type}"))
		})?;
		factory.create(name, config).await
	}

	pub async fn create_sink(
		&self,
		sink_type: &str,
		name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let factory = self
			.sinks
			.get(sink_type)
			.ok_or_else(|| OversyncError::Plugin(format!("unknown sink type: {sink_type}")))?;
		factory.create(name, config).await
	}
}

impl Default for PluginRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use async_trait::async_trait;
	use oversync_core::model::{EventEnvelope, RawRow};

	struct MockConnector;

	#[async_trait]
	impl SourceConnector for MockConnector {
		fn name(&self) -> &str {
			"mock"
		}
		async fn fetch_all(&self, _sql: &str, _key: &str) -> Result<Vec<RawRow>, OversyncError> {
			Ok(vec![])
		}
		async fn test_connection(&self) -> Result<(), OversyncError> {
			Ok(())
		}
	}

	struct MockSourceFactory;

	#[async_trait]
	impl SourceFactory for MockSourceFactory {
		fn connector_type(&self) -> &str {
			"mock"
		}
		async fn create(
			&self,
			_name: &str,
			_config: &serde_json::Value,
		) -> Result<Box<dyn SourceConnector>, OversyncError> {
			Ok(Box::new(MockConnector))
		}
	}

	struct MockSink;

	#[async_trait]
	impl Sink for MockSink {
		fn name(&self) -> &str {
			"mock"
		}
		async fn send_event(&self, _: &EventEnvelope) -> Result<(), OversyncError> {
			Ok(())
		}
		async fn test_connection(&self) -> Result<(), OversyncError> {
			Ok(())
		}
	}

	struct MockSinkFactory;

	#[async_trait]
	impl SinkFactory for MockSinkFactory {
		fn sink_type(&self) -> &str {
			"mock"
		}
		async fn create(
			&self,
			_name: &str,
			_config: &serde_json::Value,
		) -> Result<Box<dyn Sink>, OversyncError> {
			Ok(Box::new(MockSink))
		}
	}

	#[tokio::test]
	async fn registry_creates_registered_source() {
		let mut r = PluginRegistry::new();
		r.register_source(Box::new(MockSourceFactory));
		let src = r
			.create_source("mock", "test", &serde_json::json!({}))
			.await;
		assert!(src.is_ok());
		assert_eq!(src.unwrap().name(), "mock");
	}

	#[tokio::test]
	async fn registry_creates_registered_sink() {
		let mut r = PluginRegistry::new();
		r.register_sink(Box::new(MockSinkFactory));
		let sink = r.create_sink("mock", "test", &serde_json::json!({})).await;
		assert!(sink.is_ok());
		assert_eq!(sink.unwrap().name(), "mock");
	}

	#[tokio::test]
	async fn registry_errors_on_unknown_source() {
		let r = PluginRegistry::new();
		let result = r
			.create_source("nonexistent", "x", &serde_json::json!({}))
			.await;
		let err = result.err().expect("should error");
		assert!(err.to_string().contains("unknown source type"));
	}

	#[tokio::test]
	async fn registry_errors_on_unknown_sink() {
		let r = PluginRegistry::new();
		let result = r
			.create_sink("nonexistent", "x", &serde_json::json!({}))
			.await;
		let err = result.err().expect("should error");
		assert!(err.to_string().contains("unknown sink type"));
	}

	#[tokio::test]
	async fn registry_default_is_empty() {
		let r = PluginRegistry::default();
		assert!(
			r.create_source("any", "x", &serde_json::json!({}))
				.await
				.is_err()
		);
	}
}
