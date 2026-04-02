use oversync_core::RawRow;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkRule {
	pub name: String,
	pub left_field: String,
	pub right_field: String,
	pub strategy: MatchStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchStrategy {
	Exact,
	Normalized,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkMatch {
	pub left_key: String,
	pub right_key: String,
	pub rule_name: String,
	pub confidence: f64,
}

impl LinkRule {
	pub fn exact(name: &str, left_field: &str, right_field: &str) -> Self {
		Self {
			name: name.to_string(),
			left_field: left_field.to_string(),
			right_field: right_field.to_string(),
			strategy: MatchStrategy::Exact,
		}
	}

	pub fn normalized(name: &str, left_field: &str, right_field: &str) -> Self {
		Self {
			name: name.to_string(),
			left_field: left_field.to_string(),
			right_field: right_field.to_string(),
			strategy: MatchStrategy::Normalized,
		}
	}

	/// Evaluate this rule against two rows. Returns `Some(confidence)` on match.
	pub fn evaluate(&self, left: &RawRow, right: &RawRow) -> Option<f64> {
		let lv = left.row_data.get(&self.left_field)?;
		let rv = right.row_data.get(&self.right_field)?;

		if lv.is_null() || rv.is_null() {
			return None;
		}

		match self.strategy {
			MatchStrategy::Exact => {
				if lv == rv {
					Some(1.0)
				} else {
					None
				}
			}
			MatchStrategy::Normalized => {
				let ls = normalize(lv)?;
				let rs = normalize(rv)?;
				if ls == rs { Some(1.0) } else { None }
			}
		}
	}
}

fn normalize(value: &serde_json::Value) -> Option<String> {
	let s = value.as_str()?;
	let lowered = s.to_lowercase();
	let collapsed: String = lowered.split_whitespace().collect::<Vec<_>>().join(" ");
	Some(collapsed)
}
