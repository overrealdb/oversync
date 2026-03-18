use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawRow {
	pub row_key: String,
	pub row_data: serde_json::Value,
}

#[derive(Debug, Clone, Default)]
pub struct DeltaResult {
	pub created: Vec<DeltaEvent>,
	pub updated: Vec<DeltaEvent>,
	pub deleted: Vec<DeltaEvent>,
}

impl DeltaResult {
	pub fn total(&self) -> usize {
		self.created.len() + self.updated.len() + self.deleted.len()
	}

	pub fn is_empty(&self) -> bool {
		self.total() == 0
	}
}

/// Wire format for sink delivery. Each event is one message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
	pub meta: EventMeta,
	pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventMeta {
	pub op: OpType,
	pub source_id: String,
	pub query_id: String,
	pub key: String,
	pub hash: String,
	pub cycle_id: u64,
	pub timestamp: DateTime<Utc>,
}

impl From<&DeltaEvent> for EventEnvelope {
	fn from(e: &DeltaEvent) -> Self {
		Self {
			meta: EventMeta {
				op: e.op,
				source_id: e.source_id.clone(),
				query_id: e.query_id.clone(),
				key: e.row_key.clone(),
				hash: e.row_hash.clone(),
				cycle_id: e.cycle_id,
				timestamp: e.timestamp,
			},
			data: e.row_data.clone(),
		}
	}
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CycleStatus {
	Running,
	Success,
	Failed,
	Aborted,
}

impl std::fmt::Display for CycleStatus {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Running => write!(f, "running"),
			Self::Success => write!(f, "success"),
			Self::Failed => write!(f, "failed"),
			Self::Aborted => write!(f, "aborted"),
		}
	}
}

pub fn compute_diff(
	previous: &HashMap<String, String>,
	current: &[RawRow],
	source_id: &str,
	query_id: &str,
	cycle_id: u64,
) -> DeltaResult {
	let now = Utc::now();
	let mut result = DeltaResult::default();
	let mut seen_keys = std::collections::HashSet::new();

	for row in current {
		seen_keys.insert(row.row_key.clone());
		let row_hash = hash_row_data(&row.row_data);

		match previous.get(&row.row_key) {
			None => {
				result.created.push(DeltaEvent {
					op: OpType::Created,
					source_id: source_id.into(),
					query_id: query_id.into(),
					row_key: row.row_key.clone(),
					row_data: row.row_data.clone(),
					row_hash,
					cycle_id,
					timestamp: now,
				});
			}
			Some(prev_hash) if *prev_hash != row_hash => {
				result.updated.push(DeltaEvent {
					op: OpType::Updated,
					source_id: source_id.into(),
					query_id: query_id.into(),
					row_key: row.row_key.clone(),
					row_data: row.row_data.clone(),
					row_hash,
					cycle_id,
					timestamp: now,
				});
			}
			_ => {}
		}
	}

	for (key, hash) in previous {
		if !seen_keys.contains(key) {
			result.deleted.push(DeltaEvent {
				op: OpType::Deleted,
				source_id: source_id.into(),
				query_id: query_id.into(),
				row_key: key.clone(),
				row_data: serde_json::Value::Null,
				row_hash: hash.clone(),
				cycle_id,
				timestamp: now,
			});
		}
	}

	result
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

	// ── DeltaResult tests ──────────────────────────────────────────

	#[test]
	fn delta_result_empty_by_default() {
		let r = DeltaResult::default();
		assert!(r.is_empty());
		assert_eq!(r.total(), 0);
	}

	#[test]
	fn delta_result_total_counts_all() {
		let evt = || DeltaEvent {
			op: OpType::Created,
			source_id: "s".into(),
			query_id: "q".into(),
			row_key: "k".into(),
			row_data: serde_json::json!({}),
			row_hash: "h".into(),
			cycle_id: 1,
			timestamp: Utc::now(),
		};
		let r = DeltaResult {
			created: vec![evt()],
			updated: vec![evt(), evt()],
			deleted: vec![evt()],
		};
		assert_eq!(r.total(), 4);
		assert!(!r.is_empty());
	}

	// ── CycleStatus tests ──────────────────────────────────────────

	#[test]
	fn cycle_status_display() {
		assert_eq!(CycleStatus::Running.to_string(), "running");
		assert_eq!(CycleStatus::Success.to_string(), "success");
		assert_eq!(CycleStatus::Failed.to_string(), "failed");
		assert_eq!(CycleStatus::Aborted.to_string(), "aborted");
	}

	#[test]
	fn cycle_status_serde() {
		let json = serde_json::to_string(&CycleStatus::Aborted).unwrap();
		assert_eq!(json, "\"aborted\"");
		let back: CycleStatus = serde_json::from_str("\"success\"").unwrap();
		assert_eq!(back, CycleStatus::Success);
	}

	// ── compute_diff tests ─────────────────────────────────────────

	#[test]
	fn diff_all_created_when_no_previous() {
		let prev = HashMap::new();
		let rows = vec![
			RawRow {
				row_key: "a".into(),
				row_data: serde_json::json!({"v": 1}),
			},
			RawRow {
				row_key: "b".into(),
				row_data: serde_json::json!({"v": 2}),
			},
		];
		let r = compute_diff(&prev, &rows, "src", "q", 1);
		assert_eq!(r.created.len(), 2);
		assert!(r.updated.is_empty());
		assert!(r.deleted.is_empty());
		assert!(r.created.iter().all(|e| e.op == OpType::Created));
	}

	#[test]
	fn diff_all_deleted_when_no_current() {
		let prev = HashMap::from([
			("a".into(), hash_row_data(&serde_json::json!({"v": 1}))),
			("b".into(), hash_row_data(&serde_json::json!({"v": 2}))),
		]);
		let r = compute_diff(&prev, &[], "src", "q", 2);
		assert!(r.created.is_empty());
		assert!(r.updated.is_empty());
		assert_eq!(r.deleted.len(), 2);
		assert!(r.deleted.iter().all(|e| e.op == OpType::Deleted));
	}

	#[test]
	fn diff_detects_update() {
		let data_v1 = serde_json::json!({"name": "alice"});
		let data_v2 = serde_json::json!({"name": "alice_updated"});
		let prev = HashMap::from([("a".into(), hash_row_data(&data_v1))]);
		let rows = vec![RawRow {
			row_key: "a".into(),
			row_data: data_v2,
		}];
		let r = compute_diff(&prev, &rows, "src", "q", 2);
		assert!(r.created.is_empty());
		assert_eq!(r.updated.len(), 1);
		assert!(r.deleted.is_empty());
		assert_eq!(r.updated[0].row_key, "a");
	}

	#[test]
	fn diff_no_change_when_hash_matches() {
		let data = serde_json::json!({"name": "alice"});
		let prev = HashMap::from([("a".into(), hash_row_data(&data))]);
		let rows = vec![RawRow {
			row_key: "a".into(),
			row_data: data,
		}];
		let r = compute_diff(&prev, &rows, "src", "q", 2);
		assert!(r.is_empty());
	}

	#[test]
	fn diff_mixed_operations() {
		let data_a = serde_json::json!({"v": 1});
		let data_b = serde_json::json!({"v": 2});
		let data_b2 = serde_json::json!({"v": 3});
		let prev = HashMap::from([
			("a".into(), hash_row_data(&data_a)),
			("b".into(), hash_row_data(&data_b)),
			("c".into(), hash_row_data(&serde_json::json!({"v": 99}))),
		]);
		let rows = vec![
			RawRow {
				row_key: "a".into(),
				row_data: data_a,
			},
			RawRow {
				row_key: "b".into(),
				row_data: data_b2,
			},
			RawRow {
				row_key: "d".into(),
				row_data: serde_json::json!({"v": 4}),
			},
		];
		let r = compute_diff(&prev, &rows, "src", "q", 3);
		assert_eq!(r.created.len(), 1);
		assert_eq!(r.created[0].row_key, "d");
		assert_eq!(r.updated.len(), 1);
		assert_eq!(r.updated[0].row_key, "b");
		assert_eq!(r.deleted.len(), 1);
		assert_eq!(r.deleted[0].row_key, "c");
	}

	#[test]
	fn diff_preserves_source_and_query_ids() {
		let rows = vec![RawRow {
			row_key: "x".into(),
			row_data: serde_json::json!({}),
		}];
		let r = compute_diff(&HashMap::new(), &rows, "my-src", "my-q", 7);
		assert_eq!(r.created[0].source_id, "my-src");
		assert_eq!(r.created[0].query_id, "my-q");
		assert_eq!(r.created[0].cycle_id, 7);
	}

	#[test]
	fn diff_deleted_events_carry_previous_hash() {
		let data = serde_json::json!({"k": 1});
		let h = hash_row_data(&data);
		let prev = HashMap::from([("gone".into(), h.clone())]);
		let r = compute_diff(&prev, &[], "s", "q", 5);
		assert_eq!(r.deleted[0].row_hash, h);
		assert_eq!(r.deleted[0].row_data, serde_json::Value::Null);
	}
}
