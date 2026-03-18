use std::collections::HashMap;

use oversync_core::error::OversyncError;
use oversync_core::traits::{
	Sink, SinkFactory, SourceConnector, SourceFactory, Transform, TransformFactory,
};

pub struct PluginRegistry {
	sources: HashMap<String, Box<dyn SourceFactory>>,
	sinks: HashMap<String, Box<dyn SinkFactory>>,
	transforms: HashMap<String, Box<dyn TransformFactory>>,
}

impl PluginRegistry {
	pub fn new() -> Self {
		Self {
			sources: HashMap::new(),
			sinks: HashMap::new(),
			transforms: HashMap::new(),
		}
	}

	pub fn register_source(&mut self, factory: Box<dyn SourceFactory>) {
		self.sources
			.insert(factory.connector_type().to_string(), factory);
	}

	pub fn register_sink(&mut self, factory: Box<dyn SinkFactory>) {
		self.sinks.insert(factory.sink_type().to_string(), factory);
	}

	pub fn register_transform(&mut self, factory: Box<dyn TransformFactory>) {
		self.transforms
			.insert(factory.transform_type().to_string(), factory);
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

	pub async fn create_transform(
		&self,
		transform_type: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Transform>, OversyncError> {
		let factory = self.transforms.get(transform_type).ok_or_else(|| {
			OversyncError::Plugin(format!("unknown transform type: {transform_type}"))
		})?;
		factory.create(config).await
	}

	pub fn source_types(&self) -> Vec<&str> {
		self.sources.keys().map(|s| s.as_str()).collect()
	}

	pub fn sink_types(&self) -> Vec<&str> {
		self.sinks.keys().map(|s| s.as_str()).collect()
	}

	pub fn transform_types(&self) -> Vec<&str> {
		self.transforms.keys().map(|s| s.as_str()).collect()
	}
}

impl Default for PluginRegistry {
	fn default() -> Self {
		Self::new()
	}
}
