use oversync_core::error::OversyncError;

use crate::TransformStep;

/// Rename a field: moves value from `from` to `to`.
pub struct Rename {
	pub from: String,
	pub to: String,
}

impl TransformStep for Rename {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(val) = obj.remove(&self.from) {
				obj.insert(self.to.clone(), val);
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"rename"
	}
}

/// Set a field to a constant value (overwrites if exists).
pub struct Set {
	pub field: String,
	pub value: serde_json::Value,
}

impl TransformStep for Set {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			obj.insert(self.field.clone(), self.value.clone());
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"set"
	}
}

/// Convert a string field to uppercase.
pub struct Upper {
	pub field: String,
}

impl TransformStep for Upper {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(val) = obj.get_mut(&self.field) {
				if let Some(s) = val.as_str() {
					*val = serde_json::Value::String(s.to_uppercase());
				}
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"upper"
	}
}

/// Convert a string field to lowercase.
pub struct Lower {
	pub field: String,
}

impl TransformStep for Lower {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(val) = obj.get_mut(&self.field) {
				if let Some(s) = val.as_str() {
					*val = serde_json::Value::String(s.to_lowercase());
				}
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"lower"
	}
}

/// Remove a field from the record.
pub struct Remove {
	pub field: String,
}

impl TransformStep for Remove {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			obj.remove(&self.field);
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"remove"
	}
}

/// Copy value from one field to another (keeps original).
pub struct Copy {
	pub from: String,
	pub to: String,
}

impl TransformStep for Copy {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(val) = obj.get(&self.from).cloned() {
				obj.insert(self.to.clone(), val);
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"copy"
	}
}

/// Set a field only if it doesn't exist or is null.
pub struct Default {
	pub field: String,
	pub value: serde_json::Value,
}

impl TransformStep for Default {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			let needs_default = match obj.get(&self.field) {
				None => true,
				Some(v) => v.is_null(),
			};
			if needs_default {
				obj.insert(self.field.clone(), self.value.clone());
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"default"
	}
}

/// Filter records by comparing a field value. Returns false to drop.
pub struct Filter {
	pub field: String,
	pub op: FilterOp,
	pub value: serde_json::Value,
}

#[derive(Debug, Clone)]
pub enum FilterOp {
	Eq,
	Ne,
	Gt,
	Gte,
	Lt,
	Lte,
	Contains,
	Exists,
}

impl TransformStep for Filter {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		let obj = match data.as_object() {
			Some(o) => o,
			None => return Ok(true),
		};

		if matches!(self.op, FilterOp::Exists) {
			return Ok(obj.contains_key(&self.field));
		}

		let field_val = match obj.get(&self.field) {
			Some(v) => v,
			None => return Ok(false),
		};

		let keep = match self.op {
			FilterOp::Eq => field_val == &self.value,
			FilterOp::Ne => field_val != &self.value,
			FilterOp::Gt => json_cmp(field_val, &self.value).is_some_and(|o| o.is_gt()),
			FilterOp::Gte => json_cmp(field_val, &self.value).is_some_and(|o| o.is_ge()),
			FilterOp::Lt => json_cmp(field_val, &self.value).is_some_and(|o| o.is_lt()),
			FilterOp::Lte => json_cmp(field_val, &self.value).is_some_and(|o| o.is_le()),
			FilterOp::Contains => {
				if let (Some(haystack), Some(needle)) = (field_val.as_str(), self.value.as_str()) {
					haystack.contains(needle)
				} else {
					false
				}
			}
			FilterOp::Exists => unreachable!(),
		};
		Ok(keep)
	}

	fn step_name(&self) -> &str {
		"filter"
	}
}

/// Replace field value using a lookup mapping. Unmapped values stay unchanged.
pub struct MapValue {
	pub field: String,
	pub mapping: serde_json::Map<String, serde_json::Value>,
}

impl TransformStep for MapValue {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(val) = obj.get(&self.field) {
				let key = match val {
					serde_json::Value::String(s) => s.clone(),
					other => other.to_string(),
				};
				if let Some(replacement) = self.mapping.get(&key) {
					obj.insert(self.field.clone(), replacement.clone());
				}
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"map_value"
	}
}

/// Truncate a string field to max_len characters.
pub struct Truncate {
	pub field: String,
	pub max_len: usize,
}

impl TransformStep for Truncate {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(val) = obj.get_mut(&self.field) {
				if let Some(s) = val.as_str() {
					if s.chars().count() > self.max_len {
						let truncated: String = s.chars().take(self.max_len).collect();
						*val = serde_json::Value::String(truncated);
					}
				}
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"truncate"
	}
}

/// Nest multiple fields into a sub-object.
pub struct Nest {
	pub fields: Vec<String>,
	pub into: String,
}

impl TransformStep for Nest {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			let mut nested = serde_json::Map::new();
			for field in &self.fields {
				if let Some(val) = obj.remove(field) {
					nested.insert(field.clone(), val);
				}
			}
			if !nested.is_empty() {
				obj.insert(self.into.clone(), serde_json::Value::Object(nested));
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"nest"
	}
}

/// Flatten a sub-object's fields into the parent.
pub struct Flatten {
	pub field: String,
}

impl TransformStep for Flatten {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(serde_json::Value::Object(nested)) = obj.remove(&self.field) {
				for (k, v) in nested {
					obj.insert(k, v);
				}
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"flatten"
	}
}

/// Hash a field value with SHA-256, replacing it with the hex digest.
pub struct Hash {
	pub field: String,
}

impl TransformStep for Hash {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			if let Some(val) = obj.get(&self.field) {
				let input = match val {
					serde_json::Value::String(s) => s.clone(),
					other => other.to_string(),
				};
				use sha2::{Digest, Sha256};
				let hash = hex::encode(Sha256::digest(input.as_bytes()));
				obj.insert(self.field.clone(), serde_json::Value::String(hash));
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"hash"
	}
}

/// Take the first non-null value from a list of fields and write to target.
pub struct Coalesce {
	pub fields: Vec<String>,
	pub into: String,
}

impl TransformStep for Coalesce {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		if let Some(obj) = data.as_object_mut() {
			for field in &self.fields {
				if let Some(val) = obj.get(field) {
					if !val.is_null() {
						let v = val.clone();
						obj.insert(self.into.clone(), v);
						return Ok(true);
					}
				}
			}
		}
		Ok(true)
	}

	fn step_name(&self) -> &str {
		"coalesce"
	}
}

fn json_cmp(
	a: &serde_json::Value,
	b: &serde_json::Value,
) -> Option<std::cmp::Ordering> {
	match (a, b) {
		(serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
			a.as_f64()?.partial_cmp(&b.as_f64()?)
		}
		(serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn rename_moves_field() {
		let mut data = serde_json::json!({"old_name": "alice"});
		let step = Rename { from: "old_name".into(), to: "new_name".into() };
		assert!(step.apply(&mut data).unwrap());
		assert_eq!(data, serde_json::json!({"new_name": "alice"}));
	}

	#[test]
	fn rename_missing_field_is_noop() {
		let mut data = serde_json::json!({"x": 1});
		let step = Rename { from: "missing".into(), to: "y".into() };
		assert!(step.apply(&mut data).unwrap());
		assert_eq!(data, serde_json::json!({"x": 1}));
	}

	#[test]
	fn set_adds_field() {
		let mut data = serde_json::json!({"x": 1});
		let step = Set { field: "version".into(), value: serde_json::json!(2) };
		step.apply(&mut data).unwrap();
		assert_eq!(data["version"], 2);
	}

	#[test]
	fn set_overwrites_existing() {
		let mut data = serde_json::json!({"x": 1});
		let step = Set { field: "x".into(), value: serde_json::json!(99) };
		step.apply(&mut data).unwrap();
		assert_eq!(data["x"], 99);
	}

	#[test]
	fn upper_converts_string() {
		let mut data = serde_json::json!({"name": "alice"});
		Upper { field: "name".into() }.apply(&mut data).unwrap();
		assert_eq!(data["name"], "ALICE");
	}

	#[test]
	fn upper_ignores_non_string() {
		let mut data = serde_json::json!({"count": 42});
		Upper { field: "count".into() }.apply(&mut data).unwrap();
		assert_eq!(data["count"], 42);
	}

	#[test]
	fn lower_converts_string() {
		let mut data = serde_json::json!({"name": "ALICE"});
		Lower { field: "name".into() }.apply(&mut data).unwrap();
		assert_eq!(data["name"], "alice");
	}

	#[test]
	fn remove_deletes_field() {
		let mut data = serde_json::json!({"x": 1, "secret": "password"});
		Remove { field: "secret".into() }.apply(&mut data).unwrap();
		assert_eq!(data, serde_json::json!({"x": 1}));
	}

	#[test]
	fn remove_missing_is_noop() {
		let mut data = serde_json::json!({"x": 1});
		Remove { field: "missing".into() }.apply(&mut data).unwrap();
		assert_eq!(data, serde_json::json!({"x": 1}));
	}

	#[test]
	fn copy_duplicates_value() {
		let mut data = serde_json::json!({"src": "hello"});
		Copy { from: "src".into(), to: "dst".into() }.apply(&mut data).unwrap();
		assert_eq!(data["src"], "hello");
		assert_eq!(data["dst"], "hello");
	}

	#[test]
	fn copy_missing_source_is_noop() {
		let mut data = serde_json::json!({"x": 1});
		Copy { from: "missing".into(), to: "dst".into() }.apply(&mut data).unwrap();
		assert!(!data.as_object().unwrap().contains_key("dst"));
	}

	#[test]
	fn default_sets_when_absent() {
		let mut data = serde_json::json!({"x": 1});
		Default { field: "y".into(), value: serde_json::json!(42) }.apply(&mut data).unwrap();
		assert_eq!(data["y"], 42);
	}

	#[test]
	fn default_sets_when_null() {
		let mut data = serde_json::json!({"x": null});
		Default { field: "x".into(), value: serde_json::json!(0) }.apply(&mut data).unwrap();
		assert_eq!(data["x"], 0);
	}

	#[test]
	fn default_skips_when_present() {
		let mut data = serde_json::json!({"x": 99});
		Default { field: "x".into(), value: serde_json::json!(0) }.apply(&mut data).unwrap();
		assert_eq!(data["x"], 99);
	}

	#[test]
	fn filter_eq_keeps() {
		let mut data = serde_json::json!({"status": "active"});
		let step = Filter { field: "status".into(), op: FilterOp::Eq, value: serde_json::json!("active") };
		assert!(step.apply(&mut data).unwrap());
	}

	#[test]
	fn filter_eq_drops() {
		let mut data = serde_json::json!({"status": "inactive"});
		let step = Filter { field: "status".into(), op: FilterOp::Eq, value: serde_json::json!("active") };
		assert!(!step.apply(&mut data).unwrap());
	}

	#[test]
	fn filter_ne() {
		let mut data = serde_json::json!({"status": "active"});
		let step = Filter { field: "status".into(), op: FilterOp::Ne, value: serde_json::json!("deleted") };
		assert!(step.apply(&mut data).unwrap());
	}

	#[test]
	fn filter_gt_numeric() {
		let mut data = serde_json::json!({"score": 80});
		let step = Filter { field: "score".into(), op: FilterOp::Gt, value: serde_json::json!(50) };
		assert!(step.apply(&mut data).unwrap());

		let mut data2 = serde_json::json!({"score": 30});
		assert!(!step.apply(&mut data2).unwrap());
	}

	#[test]
	fn filter_contains_string() {
		let mut data = serde_json::json!({"email": "alice@example.com"});
		let step = Filter { field: "email".into(), op: FilterOp::Contains, value: serde_json::json!("@example") };
		assert!(step.apply(&mut data).unwrap());
	}

	#[test]
	fn filter_exists() {
		let mut data = serde_json::json!({"name": "alice"});
		let step = Filter { field: "name".into(), op: FilterOp::Exists, value: serde_json::json!(null) };
		assert!(step.apply(&mut data).unwrap());

		let mut data2 = serde_json::json!({"other": 1});
		assert!(!step.apply(&mut data2).unwrap());
	}

	#[test]
	fn filter_missing_field_drops() {
		let mut data = serde_json::json!({"x": 1});
		let step = Filter { field: "y".into(), op: FilterOp::Eq, value: serde_json::json!(1) };
		assert!(!step.apply(&mut data).unwrap());
	}

	#[test]
	fn map_value_replaces() {
		let mut data = serde_json::json!({"op_type": "D"});
		let mut mapping = serde_json::Map::new();
		mapping.insert("D".into(), serde_json::json!("deleted"));
		mapping.insert("U".into(), serde_json::json!("updated"));
		mapping.insert("I".into(), serde_json::json!("inserted"));
		let step = MapValue { field: "op_type".into(), mapping };
		step.apply(&mut data).unwrap();
		assert_eq!(data["op_type"], "deleted");
	}

	#[test]
	fn map_value_unmapped_unchanged() {
		let mut data = serde_json::json!({"op_type": "X"});
		let mut mapping = serde_json::Map::new();
		mapping.insert("D".into(), serde_json::json!("deleted"));
		let step = MapValue { field: "op_type".into(), mapping };
		step.apply(&mut data).unwrap();
		assert_eq!(data["op_type"], "X");
	}

	#[test]
	fn truncate_shortens() {
		let mut data = serde_json::json!({"desc": "a very long description text here"});
		Truncate { field: "desc".into(), max_len: 10 }.apply(&mut data).unwrap();
		assert_eq!(data["desc"], "a very lon");
	}

	#[test]
	fn truncate_short_string_unchanged() {
		let mut data = serde_json::json!({"desc": "short"});
		Truncate { field: "desc".into(), max_len: 100 }.apply(&mut data).unwrap();
		assert_eq!(data["desc"], "short");
	}

	#[test]
	fn nest_groups_fields() {
		let mut data = serde_json::json!({"city": "NYC", "zip": "10001", "name": "alice"});
		Nest { fields: vec!["city".into(), "zip".into()], into: "address".into() }
			.apply(&mut data)
			.unwrap();
		assert_eq!(data["address"]["city"], "NYC");
		assert_eq!(data["address"]["zip"], "10001");
		assert_eq!(data["name"], "alice");
		assert!(!data.as_object().unwrap().contains_key("city"));
	}

	#[test]
	fn nest_partial_fields() {
		let mut data = serde_json::json!({"city": "NYC"});
		Nest { fields: vec!["city".into(), "missing".into()], into: "addr".into() }
			.apply(&mut data)
			.unwrap();
		assert_eq!(data["addr"]["city"], "NYC");
		assert!(!data["addr"].as_object().unwrap().contains_key("missing"));
	}

	#[test]
	fn flatten_inlines_nested() {
		let mut data = serde_json::json!({"meta": {"source": "pg", "version": 2}, "id": 1});
		Flatten { field: "meta".into() }.apply(&mut data).unwrap();
		assert_eq!(data["source"], "pg");
		assert_eq!(data["version"], 2);
		assert_eq!(data["id"], 1);
		assert!(!data.as_object().unwrap().contains_key("meta"));
	}

	#[test]
	fn flatten_missing_is_noop() {
		let mut data = serde_json::json!({"id": 1});
		Flatten { field: "meta".into() }.apply(&mut data).unwrap();
		assert_eq!(data, serde_json::json!({"id": 1}));
	}

	#[test]
	fn hash_sha256() {
		let mut data = serde_json::json!({"email": "alice@example.com"});
		Hash { field: "email".into() }.apply(&mut data).unwrap();
		let hashed = data["email"].as_str().unwrap();
		assert_eq!(hashed.len(), 64);
		assert!(hashed.chars().all(|c| c.is_ascii_hexdigit()));
		assert_ne!(hashed, "alice@example.com");
	}

	#[test]
	fn hash_missing_is_noop() {
		let mut data = serde_json::json!({"x": 1});
		Hash { field: "missing".into() }.apply(&mut data).unwrap();
		assert_eq!(data, serde_json::json!({"x": 1}));
	}

	#[test]
	fn coalesce_takes_first_non_null() {
		let mut data = serde_json::json!({"a": null, "b": "found", "c": "also"});
		Coalesce { fields: vec!["a".into(), "b".into(), "c".into()], into: "result".into() }
			.apply(&mut data)
			.unwrap();
		assert_eq!(data["result"], "found");
	}

	#[test]
	fn coalesce_all_null_no_write() {
		let mut data = serde_json::json!({"a": null, "b": null});
		Coalesce { fields: vec!["a".into(), "b".into()], into: "result".into() }
			.apply(&mut data)
			.unwrap();
		assert!(!data.as_object().unwrap().contains_key("result"));
	}

	#[test]
	fn coalesce_missing_field_skipped() {
		let mut data = serde_json::json!({"b": 42});
		Coalesce { fields: vec!["missing".into(), "b".into()], into: "out".into() }
			.apply(&mut data)
			.unwrap();
		assert_eq!(data["out"], 42);
	}
}
