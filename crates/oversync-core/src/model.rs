use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawRow {
	pub row_key: String,
	pub row_data: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OpType {
	Created,
	Updated,
	Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeltaEvent {
	pub op: OpType,
	pub source_id: String,
	pub query_id: String,
	pub row_key: String,
	pub row_data: serde_json::Value,
	pub row_hash: String,
	pub cycle_id: u64,
	pub timestamp: DateTime<Utc>,
}

pub fn hash_row_data(data: &serde_json::Value) -> String {
	let serialized = serde_json::to_string(data).unwrap_or_default();
	let mut hasher = Sha256::new();
	hasher.update(serialized.as_bytes());
	hex::encode(hasher.finalize())
}

impl std::fmt::Display for OpType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			OpType::Created => write!(f, "created"),
			OpType::Updated => write!(f, "updated"),
			OpType::Deleted => write!(f, "deleted"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn hash_is_deterministic() {
		let data = serde_json::json!({"name": "alice", "age": 30});
		assert_eq!(hash_row_data(&data), hash_row_data(&data));
	}

	#[test]
	fn hash_differs_for_different_data() {
		let a = serde_json::json!({"name": "alice"});
		let b = serde_json::json!({"name": "bob"});
		assert_ne!(hash_row_data(&a), hash_row_data(&b));
	}

	#[test]
	fn hash_is_64_char_hex() {
		let hash = hash_row_data(&serde_json::json!({}));
		assert_eq!(hash.len(), 64);
		assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
	}

	#[test]
	fn op_type_display() {
		assert_eq!(OpType::Created.to_string(), "created");
		assert_eq!(OpType::Updated.to_string(), "updated");
		assert_eq!(OpType::Deleted.to_string(), "deleted");
	}

	#[test]
	fn raw_row_roundtrip_json() {
		let row = RawRow {
			row_key: "pk-1".into(),
			row_data: serde_json::json!({"col": "val"}),
		};
		let json = serde_json::to_string(&row).unwrap();
		let back: RawRow = serde_json::from_str(&json).unwrap();
		assert_eq!(row, back);
	}

	#[test]
	fn delta_event_roundtrip_json() {
		let evt = DeltaEvent {
			op: OpType::Created,
			source_id: "pg-prod".into(),
			query_id: "q1".into(),
			row_key: "pk-1".into(),
			row_data: serde_json::json!({"x": 1}),
			row_hash: hash_row_data(&serde_json::json!({"x": 1})),
			cycle_id: 42,
			timestamp: chrono::Utc::now(),
		};
		let json = serde_json::to_string(&evt).unwrap();
		let back: DeltaEvent = serde_json::from_str(&json).unwrap();
		assert_eq!(evt.op, back.op);
		assert_eq!(evt.row_key, back.row_key);
		assert_eq!(evt.cycle_id, back.cycle_id);
	}

	#[test]
	fn op_type_serde_snake_case() {
		let json = serde_json::to_string(&OpType::Created).unwrap();
		assert_eq!(json, "\"created\"");
		let back: OpType = serde_json::from_str("\"updated\"").unwrap();
		assert_eq!(back, OpType::Updated);
	}
}
