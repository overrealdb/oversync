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
	pub origin_id: String,
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
				origin_id: e.origin_id.clone(),
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
	origin_id: &str,
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
					origin_id: origin_id.into(),
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
					origin_id: origin_id.into(),
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
				origin_id: origin_id.into(),
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
	pub origin_id: String,
	pub query_id: String,
	pub row_key: String,
	pub row_data: serde_json::Value,
	pub row_hash: String,
	pub cycle_id: u64,
	pub timestamp: DateTime<Utc>,
}

pub fn hash_row_data(data: &serde_json::Value) -> String {
	let serialized = serde_json::to_string(data).expect("serde_json::Value is always serializable");
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

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
	Bearer { token: String },
	Header { name: String, value: String },
	Basic { username: String, password: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaAuth {
	#[serde(default = "default_security_protocol")]
	pub security_protocol: String,
	pub sasl_mechanism: Option<String>,
	pub sasl_username: Option<String>,
	pub sasl_password: Option<String>,
	pub sasl_kerberos_keytab: Option<String>,
	pub sasl_kerberos_principal: Option<String>,
	pub ssl_ca_location: Option<String>,
	pub ssl_certificate_location: Option<String>,
	pub ssl_key_location: Option<String>,
}

fn default_security_protocol() -> String {
	"PLAINTEXT".to_string()
}

impl Default for KafkaAuth {
	fn default() -> Self {
		Self {
			security_protocol: default_security_protocol(),
			sasl_mechanism: None,
			sasl_username: None,
			sasl_password: None,
			sasl_kerberos_keytab: None,
			sasl_kerberos_principal: None,
			ssl_ca_location: None,
			ssl_certificate_location: None,
			ssl_key_location: None,
		}
	}
}

impl KafkaAuth {
	pub fn is_plaintext(&self) -> bool {
		self.security_protocol == "PLAINTEXT"
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
			origin_id: "pg-prod".into(),
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
			origin_id: "s".into(),
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
		assert_eq!(r.created[0].origin_id, "my-src");
		assert_eq!(r.created[0].query_id, "my-q");
		assert_eq!(r.created[0].cycle_id, 7);
	}

	#[test]
	fn kafka_auth_default_is_plaintext() {
		let auth = KafkaAuth::default();
		assert_eq!(auth.security_protocol, "PLAINTEXT");
		assert!(auth.is_plaintext());
		assert!(auth.sasl_mechanism.is_none());
	}

	#[test]
	fn kafka_auth_deserializes_sasl_plain() {
		let json = serde_json::json!({
			"security_protocol": "SASL_SSL",
			"sasl_mechanism": "PLAIN",
			"sasl_username": "user",
			"sasl_password": "pass",
			"ssl_ca_location": "/path/to/ca.pem"
		});
		let auth: KafkaAuth = serde_json::from_value(json).unwrap();
		assert_eq!(auth.security_protocol, "SASL_SSL");
		assert_eq!(auth.sasl_mechanism.as_deref(), Some("PLAIN"));
		assert_eq!(auth.sasl_username.as_deref(), Some("user"));
		assert_eq!(auth.sasl_password.as_deref(), Some("pass"));
		assert_eq!(auth.ssl_ca_location.as_deref(), Some("/path/to/ca.pem"));
		assert!(!auth.is_plaintext());
	}

	#[test]
	fn kafka_auth_deserializes_kerberos() {
		let json = serde_json::json!({
			"security_protocol": "SASL_PLAINTEXT",
			"sasl_mechanism": "GSSAPI",
			"sasl_kerberos_keytab": "/etc/krb5.keytab",
			"sasl_kerberos_principal": "kafka/broker@REALM"
		});
		let auth: KafkaAuth = serde_json::from_value(json).unwrap();
		assert_eq!(auth.sasl_mechanism.as_deref(), Some("GSSAPI"));
		assert_eq!(
			auth.sasl_kerberos_keytab.as_deref(),
			Some("/etc/krb5.keytab")
		);
		assert_eq!(
			auth.sasl_kerberos_principal.as_deref(),
			Some("kafka/broker@REALM")
		);
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

	// ── Edge case tests ───────────────────────────────────────────

	#[test]
	fn diff_empty_previous_empty_current() {
		let r = compute_diff(&HashMap::new(), &[], "s", "q", 1);
		assert!(r.is_empty());
	}

	#[test]
	fn diff_unicode_keys() {
		let rows = vec![
			RawRow {
				row_key: "日本語キー".into(),
				row_data: serde_json::json!({"emoji": "🎉"}),
			},
			RawRow {
				row_key: "clé-français".into(),
				row_data: serde_json::json!({"val": "été"}),
			},
		];
		let r = compute_diff(&HashMap::new(), &rows, "s", "q", 1);
		assert_eq!(r.created.len(), 2);
		let keys: Vec<&str> = r.created.iter().map(|e| e.row_key.as_str()).collect();
		assert!(keys.contains(&"日本語キー"));
		assert!(keys.contains(&"clé-français"));
	}

	#[test]
	fn diff_null_row_data() {
		let rows = vec![RawRow {
			row_key: "k1".into(),
			row_data: serde_json::Value::Null,
		}];
		let r = compute_diff(&HashMap::new(), &rows, "s", "q", 1);
		assert_eq!(r.created.len(), 1);
		assert_eq!(r.created[0].row_data, serde_json::Value::Null);
	}

	#[test]
	fn diff_empty_string_key() {
		let rows = vec![RawRow {
			row_key: "".into(),
			row_data: serde_json::json!({"v": 1}),
		}];
		let r = compute_diff(&HashMap::new(), &rows, "s", "q", 1);
		assert_eq!(r.created.len(), 1);
		assert_eq!(r.created[0].row_key, "");
	}

	#[test]
	fn diff_duplicate_keys_emit_multiple_created_events() {
		let prev = HashMap::new();
		let rows = vec![
			RawRow {
				row_key: "dup".into(),
				row_data: serde_json::json!({"v": 1}),
			},
			RawRow {
				row_key: "dup".into(),
				row_data: serde_json::json!({"v": 2}),
			},
		];
		let r = compute_diff(&prev, &rows, "s", "q", 1);
		assert_eq!(r.created.len(), 2);
	}

	#[test]
	fn diff_large_json_data() {
		let big_obj: serde_json::Value = (0..100)
			.map(|i| (format!("field_{i}"), serde_json::json!(i)))
			.collect::<serde_json::Map<String, serde_json::Value>>()
			.into();
		let rows = vec![RawRow {
			row_key: "big".into(),
			row_data: big_obj.clone(),
		}];
		let r = compute_diff(&HashMap::new(), &rows, "s", "q", 1);
		assert_eq!(r.created.len(), 1);
		assert_eq!(r.created[0].row_hash, hash_row_data(&big_obj));
	}

	#[test]
	fn hash_null_value() {
		let h = hash_row_data(&serde_json::Value::Null);
		assert_eq!(h.len(), 64);
	}

	#[test]
	fn hash_nested_objects_differ() {
		let a = serde_json::json!({"nested": {"a": 1}});
		let b = serde_json::json!({"nested": {"a": 2}});
		assert_ne!(hash_row_data(&a), hash_row_data(&b));
	}

	#[test]
	fn hash_array_values() {
		let data = serde_json::json!({"items": [1, 2, 3]});
		let h = hash_row_data(&data);
		assert_eq!(h.len(), 64);
		assert_ne!(h, hash_row_data(&serde_json::json!({"items": [1, 2, 4]})));
	}

	// ── EventEnvelope conversion ──────────────────────────────────

	#[test]
	fn event_envelope_from_delta_event() {
		let evt = DeltaEvent {
			op: OpType::Updated,
			origin_id: "pg".into(),
			query_id: "q1".into(),
			row_key: "pk-42".into(),
			row_data: serde_json::json!({"name": "test"}),
			row_hash: "abc123".into(),
			cycle_id: 10,
			timestamp: Utc::now(),
		};
		let envelope = EventEnvelope::from(&evt);
		assert_eq!(envelope.meta.op, OpType::Updated);
		assert_eq!(envelope.meta.origin_id, "pg");
		assert_eq!(envelope.meta.query_id, "q1");
		assert_eq!(envelope.meta.key, "pk-42");
		assert_eq!(envelope.meta.hash, "abc123");
		assert_eq!(envelope.meta.cycle_id, 10);
		assert_eq!(envelope.data, serde_json::json!({"name": "test"}));
	}

	#[test]
	fn event_envelope_roundtrip_json() {
		let evt = DeltaEvent {
			op: OpType::Created,
			origin_id: "s".into(),
			query_id: "q".into(),
			row_key: "k".into(),
			row_data: serde_json::json!(null),
			row_hash: "h".into(),
			cycle_id: 1,
			timestamp: Utc::now(),
		};
		let envelope = EventEnvelope::from(&evt);
		let json = serde_json::to_string(&envelope).unwrap();
		let back: EventEnvelope = serde_json::from_str(&json).unwrap();
		assert_eq!(envelope, back);
	}

	// ── AuthConfig tests ──────────────────────────────────────────

	#[test]
	fn auth_config_bearer() {
		let json = serde_json::json!({"type": "bearer", "token": "tok123"});
		let auth: AuthConfig = serde_json::from_value(json).unwrap();
		assert!(matches!(auth, AuthConfig::Bearer { token } if token == "tok123"));
	}

	#[test]
	fn auth_config_header() {
		let json = serde_json::json!({"type": "header", "name": "X-Api-Key", "value": "secret"});
		let auth: AuthConfig = serde_json::from_value(json).unwrap();
		assert!(
			matches!(auth, AuthConfig::Header { name, value } if name == "X-Api-Key" && value == "secret")
		);
	}

	#[test]
	fn auth_config_basic() {
		let json = serde_json::json!({"type": "basic", "username": "user", "password": "pass"});
		let auth: AuthConfig = serde_json::from_value(json).unwrap();
		assert!(
			matches!(auth, AuthConfig::Basic { username, password } if username == "user" && password == "pass")
		);
	}

	#[test]
	fn auth_config_unknown_type_fails() {
		let json = serde_json::json!({"type": "oauth2", "token": "t"});
		assert!(serde_json::from_value::<AuthConfig>(json).is_err());
	}

	#[test]
	fn kafka_auth_ssl_only() {
		let json = serde_json::json!({
			"security_protocol": "SSL",
			"ssl_ca_location": "/ca.pem",
			"ssl_certificate_location": "/cert.pem",
			"ssl_key_location": "/key.pem"
		});
		let auth: KafkaAuth = serde_json::from_value(json).unwrap();
		assert_eq!(auth.security_protocol, "SSL");
		assert!(!auth.is_plaintext());
		assert!(auth.sasl_mechanism.is_none());
		assert_eq!(auth.ssl_ca_location.as_deref(), Some("/ca.pem"));
		assert_eq!(auth.ssl_certificate_location.as_deref(), Some("/cert.pem"));
		assert_eq!(auth.ssl_key_location.as_deref(), Some("/key.pem"));
	}

	#[test]
	fn kafka_auth_minimal_deserialize() {
		let json = serde_json::json!({});
		let auth: KafkaAuth = serde_json::from_value(json).unwrap();
		assert_eq!(auth.security_protocol, "PLAINTEXT");
		assert!(auth.is_plaintext());
	}
}
