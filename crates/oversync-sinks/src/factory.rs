use async_trait::async_trait;

use oversync_core::error::OversyncError;
use oversync_core::traits::{Sink, SinkFactory};

use crate::stdout::StdoutSink;

pub struct StdoutSinkFactory;

#[async_trait]
impl SinkFactory for StdoutSinkFactory {
	fn sink_type(&self) -> &str {
		"stdout"
	}

	async fn create(
		&self,
		_name: &str,
		config: &serde_json::Value,
	) -> Result<Box<dyn Sink>, OversyncError> {
		let pretty = config
			.get("pretty")
			.and_then(|v| v.as_bool())
			.unwrap_or(false);
		Ok(Box::new(StdoutSink::new(pretty)))
	}
}
