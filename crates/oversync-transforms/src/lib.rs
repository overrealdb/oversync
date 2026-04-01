#[cfg(feature = "js")]
pub mod js;
mod parse;
pub mod steps;
#[cfg(feature = "wasm")]
pub mod wasm;

use async_trait::async_trait;
use oversync_core::error::OversyncError;
use oversync_core::model::EventEnvelope;
use oversync_core::traits::TransformHook;

pub use parse::parse_steps;
pub use steps::*;

/// A single transform operation applied to one record's data.
///
/// Each step receives a mutable reference to the record's JSON `data` field
/// and returns `Ok(true)` to keep the record or `Ok(false)` to filter it out.
pub trait TransformStep: Send + Sync {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError>;

	fn step_name(&self) -> &str;
}

/// Ordered chain of [`TransformStep`]s applied sequentially to each record.
///
/// If any step returns `Ok(false)`, the record is dropped from the output.
/// If any step returns `Err`, the entire chain short-circuits.
pub struct StepChain {
	steps: Vec<Box<dyn TransformStep>>,
}

impl std::fmt::Debug for StepChain {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let names: Vec<&str> = self.steps.iter().map(|s| s.step_name()).collect();
		f.debug_struct("StepChain").field("steps", &names).finish()
	}
}

impl StepChain {
	pub fn new(steps: Vec<Box<dyn TransformStep>>) -> Self {
		Self { steps }
	}

	pub fn apply_one(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		for step in &self.steps {
			if !step.apply(data)? {
				return Ok(false);
			}
		}
		Ok(true)
	}

	pub fn len(&self) -> usize {
		self.steps.len()
	}

	pub fn is_empty(&self) -> bool {
		self.steps.is_empty()
	}

	/// Filter rows pre-delta: applies steps to each RawRow.row_data,
	/// keeping only rows where all steps return true.
	pub fn filter_rows(
		&self,
		rows: Vec<oversync_core::model::RawRow>,
	) -> Result<Vec<oversync_core::model::RawRow>, OversyncError> {
		let mut kept = Vec::with_capacity(rows.len());
		for mut row in rows {
			if self.apply_one(&mut row.row_data)? {
				kept.push(row);
			}
		}
		Ok(kept)
	}
}

#[async_trait]
impl TransformHook for StepChain {
	async fn transform(
		&self,
		envelopes: Vec<EventEnvelope>,
	) -> Result<Vec<EventEnvelope>, OversyncError> {
		let mut output = Vec::with_capacity(envelopes.len());
		for mut envelope in envelopes {
			if self.apply_one(&mut envelope.data)? {
				output.push(envelope);
			}
		}
		Ok(output)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use oversync_core::model::{EventMeta, OpType};

	fn test_envelope(data: serde_json::Value) -> EventEnvelope {
		EventEnvelope {
			meta: EventMeta {
				op: OpType::Created,
				origin_id: "test".into(),
				query_id: "q".into(),
				key: "k".into(),
				hash: "h".into(),
				cycle_id: 1,
				timestamp: chrono::Utc::now(),
			},
			data,
		}
	}

	struct AlwaysKeep;
	impl TransformStep for AlwaysKeep {
		fn apply(&self, _data: &mut serde_json::Value) -> Result<bool, OversyncError> {
			Ok(true)
		}
		fn step_name(&self) -> &str {
			"always_keep"
		}
	}

	struct AlwaysDrop;
	impl TransformStep for AlwaysDrop {
		fn apply(&self, _data: &mut serde_json::Value) -> Result<bool, OversyncError> {
			Ok(false)
		}
		fn step_name(&self) -> &str {
			"always_drop"
		}
	}

	struct FailStep;
	impl TransformStep for FailStep {
		fn apply(&self, _data: &mut serde_json::Value) -> Result<bool, OversyncError> {
			Err(OversyncError::Internal("step failed".into()))
		}
		fn step_name(&self) -> &str {
			"fail"
		}
	}

	#[test]
	fn empty_chain_is_passthrough() {
		let chain = StepChain::new(vec![]);
		assert!(chain.is_empty());
		let mut data = serde_json::json!({"x": 1});
		assert!(chain.apply_one(&mut data).unwrap());
		assert_eq!(data, serde_json::json!({"x": 1}));
	}

	#[test]
	fn chain_keeps_record() {
		let chain = StepChain::new(vec![Box::new(AlwaysKeep)]);
		let mut data = serde_json::json!({"x": 1});
		assert!(chain.apply_one(&mut data).unwrap());
	}

	#[test]
	fn chain_drops_record() {
		let chain = StepChain::new(vec![Box::new(AlwaysDrop)]);
		let mut data = serde_json::json!({"x": 1});
		assert!(!chain.apply_one(&mut data).unwrap());
	}

	#[test]
	fn chain_short_circuits_on_drop() {
		let chain = StepChain::new(vec![Box::new(AlwaysDrop), Box::new(FailStep)]);
		let mut data = serde_json::json!({});
		assert!(!chain.apply_one(&mut data).unwrap());
	}

	#[test]
	fn chain_short_circuits_on_error() {
		let chain = StepChain::new(vec![Box::new(FailStep), Box::new(AlwaysKeep)]);
		let mut data = serde_json::json!({});
		assert!(chain.apply_one(&mut data).is_err());
	}

	#[tokio::test]
	async fn chain_as_transform_hook() {
		let chain = StepChain::new(vec![Box::new(AlwaysKeep)]);
		let input = vec![
			test_envelope(serde_json::json!({"a": 1})),
			test_envelope(serde_json::json!({"b": 2})),
		];
		let output = chain.transform(input).await.unwrap();
		assert_eq!(output.len(), 2);
	}

	#[tokio::test]
	async fn chain_hook_filters_records() {
		let chain = StepChain::new(vec![Box::new(AlwaysDrop)]);
		let input = vec![
			test_envelope(serde_json::json!({"a": 1})),
			test_envelope(serde_json::json!({"b": 2})),
		];
		let output = chain.transform(input).await.unwrap();
		assert!(output.is_empty());
	}

	#[tokio::test]
	async fn chain_hook_propagates_error() {
		let chain = StepChain::new(vec![Box::new(FailStep)]);
		let input = vec![test_envelope(serde_json::json!({}))];
		let result = chain.transform(input).await;
		assert!(result.is_err());
	}

	// ── filter_rows tests ───────────────────────────────────────

	use oversync_core::model::RawRow;

	fn test_rows() -> Vec<RawRow> {
		vec![
			RawRow {
				row_key: "1".into(),
				row_data: serde_json::json!({"name": "alice"}),
			},
			RawRow {
				row_key: "2".into(),
				row_data: serde_json::json!({"name": "bob"}),
			},
			RawRow {
				row_key: "3".into(),
				row_data: serde_json::json!({"name": "charlie"}),
			},
		]
	}

	#[test]
	fn filter_rows_keeps_all() {
		let chain = StepChain::new(vec![Box::new(AlwaysKeep)]);
		let result = chain.filter_rows(test_rows()).unwrap();
		assert_eq!(result.len(), 3);
	}

	#[test]
	fn filter_rows_drops_all() {
		let chain = StepChain::new(vec![Box::new(AlwaysDrop)]);
		let result = chain.filter_rows(test_rows()).unwrap();
		assert!(result.is_empty());
	}

	#[test]
	fn filter_rows_empty_chain_keeps_all() {
		let chain = StepChain::new(vec![]);
		let result = chain.filter_rows(test_rows()).unwrap();
		assert_eq!(result.len(), 3);
	}

	#[test]
	fn filter_rows_error_propagates() {
		let chain = StepChain::new(vec![Box::new(FailStep)]);
		let result = chain.filter_rows(test_rows());
		assert!(result.is_err());
	}

	#[test]
	fn filter_rows_partial() {
		use crate::steps::{Filter, FilterOp};
		let chain = StepChain::new(vec![Box::new(Filter {
			field: "name".into(),
			op: FilterOp::Eq,
			value: serde_json::json!("alice"),
		})]);
		let result = chain.filter_rows(test_rows()).unwrap();
		assert_eq!(result.len(), 1);
		assert_eq!(result[0].row_key, "1");
	}
}
