use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::Sink;

pub struct StdoutSink {
	pretty: bool,
	sent: Arc<Mutex<Vec<EventEnvelope>>>,
}

impl StdoutSink {
	pub fn new(pretty: bool) -> Self {
		Self {
			pretty,
			sent: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn sent_events(&self) -> Vec<EventEnvelope> {
		self.sent.lock().unwrap().clone()
	}
}

#[async_trait]
impl Sink for StdoutSink {
	fn name(&self) -> &str {
		"stdout"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		let json = if self.pretty {
			serde_json::to_string_pretty(envelope)
		} else {
			serde_json::to_string(envelope)
		}
		.map_err(|e| OversyncError::Sink(format!("json serialize: {e}")))?;

		println!("{json}");
		self.sent.lock().unwrap().push(envelope.clone());
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::Utc;
	use oversync_core::model::{EventMeta, OpType};

	fn make_envelope(key: &str) -> EventEnvelope {
		EventEnvelope {
			meta: EventMeta {
				op: OpType::Created,
				source_id: "src".into(),
				query_id: "q".into(),
				key: key.into(),
				hash: "h".into(),
				cycle_id: 1,
				timestamp: Utc::now(),
			},
			data: serde_json::json!({"k": key}),
		}
	}

	#[tokio::test]
	async fn test_connection_always_ok() {
		let sink = StdoutSink::new(false);
		assert!(sink.test_connection().await.is_ok());
	}

	#[test]
	fn name_is_stdout() {
		let sink = StdoutSink::new(false);
		assert_eq!(sink.name(), "stdout");
	}

	#[tokio::test]
	async fn send_event_records() {
		let sink = StdoutSink::new(false);
		sink.send_event(&make_envelope("a")).await.unwrap();
		let sent = sink.sent_events();
		assert_eq!(sent.len(), 1);
		assert_eq!(sent[0].meta.key, "a");
	}

	#[tokio::test]
	async fn send_batch_records_all() {
		let sink = StdoutSink::new(false);
		let envelopes = vec![make_envelope("a"), make_envelope("b")];
		sink.send_batch(&envelopes).await.unwrap();
		let sent = sink.sent_events();
		assert_eq!(sent.len(), 2);
		assert_eq!(sent[0].meta.key, "a");
		assert_eq!(sent[1].meta.key, "b");
	}

	#[tokio::test]
	async fn send_batch_empty() {
		let sink = StdoutSink::new(true);
		assert!(sink.send_batch(&[]).await.is_ok());
		assert!(sink.sent_events().is_empty());
	}

	#[test]
	fn sent_events_starts_empty() {
		let sink = StdoutSink::new(false);
		assert!(sink.sent_events().is_empty());
	}

	#[tokio::test]
	async fn send_batch_accumulates() {
		let sink = StdoutSink::new(false);
		sink.send_event(&make_envelope("a")).await.unwrap();
		sink.send_batch(&[make_envelope("b"), make_envelope("c")])
			.await
			.unwrap();
		let sent = sink.sent_events();
		assert_eq!(sent.len(), 3);
	}

	#[tokio::test]
	async fn envelope_format_has_meta_and_data() {
		let sink = StdoutSink::new(false);
		let env = make_envelope("x");
		sink.send_event(&env).await.unwrap();

		let json = serde_json::to_value(&env).unwrap();
		assert!(json.get("meta").is_some());
		assert!(json.get("data").is_some());
		assert_eq!(json["meta"]["op"], "created");
		assert_eq!(json["meta"]["key"], "x");
	}
}
