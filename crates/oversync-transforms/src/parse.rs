use oversync_core::error::OversyncError;

use crate::steps::*;
use crate::{StepChain, TransformStep};

/// Parse a JSON array of step definitions into a [`StepChain`].
///
/// Each element must be an object with a `type` field and step-specific parameters.
///
/// # Supported types
///
/// | type | params |
/// |------|--------|
/// | `rename` | `from`, `to` |
/// | `set` | `field`, `value` |
/// | `upper` | `field` |
/// | `lower` | `field` |
/// | `remove` | `field` |
/// | `copy` | `from`, `to` |
/// | `default` | `field`, `value` |
/// | `filter` | `field`, `op`, `value` |
/// | `map_value` | `field`, `mapping` |
/// | `truncate` | `field`, `max_len` |
/// | `nest` | `fields`, `into` |
/// | `flatten` | `field` |
/// | `hash` | `field` |
/// | `coalesce` | `fields`, `into` |
pub fn parse_steps(defs: &[serde_json::Value]) -> Result<StepChain, OversyncError> {
	let mut steps: Vec<Box<dyn TransformStep>> = Vec::with_capacity(defs.len());

	for (i, def) in defs.iter().enumerate() {
		let obj = def.as_object().ok_or_else(|| {
			OversyncError::Config(format!("transform step {i}: expected object"))
		})?;

		let step_type = obj
			.get("type")
			.and_then(|v| v.as_str())
			.ok_or_else(|| {
				OversyncError::Config(format!("transform step {i}: missing 'type'"))
			})?;

		let step: Box<dyn TransformStep> = match step_type {
			"rename" => Box::new(Rename {
				from: req_str(obj, "from", i)?,
				to: req_str(obj, "to", i)?,
			}),
			"set" => Box::new(Set {
				field: req_str(obj, "field", i)?,
				value: req_val(obj, "value", i)?,
			}),
			"upper" => Box::new(Upper {
				field: req_str(obj, "field", i)?,
			}),
			"lower" => Box::new(Lower {
				field: req_str(obj, "field", i)?,
			}),
			"remove" => Box::new(Remove {
				field: req_str(obj, "field", i)?,
			}),
			"copy" => Box::new(Copy {
				from: req_str(obj, "from", i)?,
				to: req_str(obj, "to", i)?,
			}),
			"default" => Box::new(Default {
				field: req_str(obj, "field", i)?,
				value: req_val(obj, "value", i)?,
			}),
			"filter" => {
				let op_str = req_str(obj, "op", i)?;
				let op = match op_str.as_str() {
					"eq" => FilterOp::Eq,
					"ne" => FilterOp::Ne,
					"gt" => FilterOp::Gt,
					"gte" => FilterOp::Gte,
					"lt" => FilterOp::Lt,
					"lte" => FilterOp::Lte,
					"contains" => FilterOp::Contains,
					"exists" => FilterOp::Exists,
					other => {
						return Err(OversyncError::Config(format!(
							"transform step {i}: unknown filter op '{other}'"
						)));
					}
				};
				Box::new(Filter {
					field: req_str(obj, "field", i)?,
					op,
					value: obj
						.get("value")
						.cloned()
						.unwrap_or(serde_json::Value::Null),
				})
			}
			"map_value" => {
				let mapping = obj
					.get("mapping")
					.and_then(|v| v.as_object())
					.ok_or_else(|| {
						OversyncError::Config(format!(
							"transform step {i}: 'map_value' requires 'mapping' object"
						))
					})?
					.clone();
				Box::new(MapValue {
					field: req_str(obj, "field", i)?,
					mapping,
				})
			}
			"truncate" => {
				let max_len = obj
					.get("max_len")
					.and_then(|v| v.as_u64())
					.ok_or_else(|| {
						OversyncError::Config(format!(
							"transform step {i}: 'truncate' requires 'max_len' (integer)"
						))
					})? as usize;
				Box::new(Truncate {
					field: req_str(obj, "field", i)?,
					max_len,
				})
			}
			"nest" => Box::new(Nest {
				fields: req_str_array(obj, "fields", i)?,
				into: req_str(obj, "into", i)?,
			}),
			"flatten" => Box::new(Flatten {
				field: req_str(obj, "field", i)?,
			}),
			"hash" => Box::new(Hash {
				field: req_str(obj, "field", i)?,
			}),
			"coalesce" => Box::new(Coalesce {
				fields: req_str_array(obj, "fields", i)?,
				into: req_str(obj, "into", i)?,
			}),
			other => {
				return Err(OversyncError::Config(format!(
					"transform step {i}: unknown type '{other}'"
				)));
			}
		};

		steps.push(step);
	}

	Ok(StepChain::new(steps))
}

fn req_str(
	obj: &serde_json::Map<String, serde_json::Value>,
	key: &str,
	idx: usize,
) -> Result<String, OversyncError> {
	obj.get(key)
		.and_then(|v| v.as_str())
		.map(String::from)
		.ok_or_else(|| {
			OversyncError::Config(format!("transform step {idx}: missing '{key}' (string)"))
		})
}

fn req_val(
	obj: &serde_json::Map<String, serde_json::Value>,
	key: &str,
	idx: usize,
) -> Result<serde_json::Value, OversyncError> {
	obj.get(key).cloned().ok_or_else(|| {
		OversyncError::Config(format!("transform step {idx}: missing '{key}'"))
	})
}

fn req_str_array(
	obj: &serde_json::Map<String, serde_json::Value>,
	key: &str,
	idx: usize,
) -> Result<Vec<String>, OversyncError> {
	obj.get(key)
		.and_then(|v| v.as_array())
		.map(|arr| {
			arr.iter()
				.filter_map(|v| v.as_str().map(String::from))
				.collect()
		})
		.ok_or_else(|| {
			OversyncError::Config(format!(
				"transform step {idx}: missing '{key}' (string array)"
			))
		})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_empty_array() {
		let chain = parse_steps(&[]).unwrap();
		assert!(chain.is_empty());
	}

	#[test]
	fn parse_rename_step() {
		let defs = vec![serde_json::json!({"type": "rename", "from": "old", "to": "new"})];
		let chain = parse_steps(&defs).unwrap();
		assert_eq!(chain.len(), 1);

		let mut data = serde_json::json!({"old": "value"});
		chain.apply_one(&mut data).unwrap();
		assert_eq!(data, serde_json::json!({"new": "value"}));
	}

	#[test]
	fn parse_filter_step() {
		let defs = vec![serde_json::json!({
			"type": "filter",
			"field": "status",
			"op": "eq",
			"value": "active"
		})];
		let chain = parse_steps(&defs).unwrap();

		let mut keep = serde_json::json!({"status": "active"});
		assert!(chain.apply_one(&mut keep).unwrap());

		let mut drop = serde_json::json!({"status": "deleted"});
		assert!(!chain.apply_one(&mut drop).unwrap());
	}

	#[test]
	fn parse_multi_step_chain() {
		let defs = vec![
			serde_json::json!({"type": "rename", "from": "entity_id", "to": "id"}),
			serde_json::json!({"type": "upper", "field": "name"}),
			serde_json::json!({"type": "set", "field": "version", "value": 1}),
			serde_json::json!({"type": "remove", "field": "secret"}),
		];
		let chain = parse_steps(&defs).unwrap();
		assert_eq!(chain.len(), 4);

		let mut data = serde_json::json!({"entity_id": "123", "name": "alice", "secret": "pw"});
		chain.apply_one(&mut data).unwrap();
		assert_eq!(data, serde_json::json!({"id": "123", "name": "ALICE", "version": 1}));
	}

	#[test]
	fn parse_map_value_step() {
		let defs = vec![serde_json::json!({
			"type": "map_value",
			"field": "op",
			"mapping": {"D": "deleted", "U": "updated", "I": "inserted"}
		})];
		let chain = parse_steps(&defs).unwrap();
		let mut data = serde_json::json!({"op": "D"});
		chain.apply_one(&mut data).unwrap();
		assert_eq!(data["op"], "deleted");
	}

	#[test]
	fn parse_truncate_step() {
		let defs = vec![serde_json::json!({"type": "truncate", "field": "desc", "max_len": 5})];
		let chain = parse_steps(&defs).unwrap();
		let mut data = serde_json::json!({"desc": "hello world"});
		chain.apply_one(&mut data).unwrap();
		assert_eq!(data["desc"], "hello");
	}

	#[test]
	fn parse_nest_step() {
		let defs = vec![serde_json::json!({
			"type": "nest",
			"fields": ["city", "zip"],
			"into": "address"
		})];
		let chain = parse_steps(&defs).unwrap();
		let mut data = serde_json::json!({"city": "NYC", "zip": "10001", "name": "alice"});
		chain.apply_one(&mut data).unwrap();
		assert_eq!(data["address"]["city"], "NYC");
		assert!(!data.as_object().unwrap().contains_key("city"));
	}

	#[test]
	fn parse_coalesce_step() {
		let defs = vec![serde_json::json!({
			"type": "coalesce",
			"fields": ["a", "b"],
			"into": "result"
		})];
		let chain = parse_steps(&defs).unwrap();
		let mut data = serde_json::json!({"a": null, "b": 42});
		chain.apply_one(&mut data).unwrap();
		assert_eq!(data["result"], 42);
	}

	#[test]
	fn parse_unknown_type_errors() {
		let defs = vec![serde_json::json!({"type": "bogus"})];
		let err = parse_steps(&defs).unwrap_err();
		assert!(err.to_string().contains("unknown type 'bogus'"));
	}

	#[test]
	fn parse_missing_type_errors() {
		let defs = vec![serde_json::json!({"field": "x"})];
		let err = parse_steps(&defs).unwrap_err();
		assert!(err.to_string().contains("missing 'type'"));
	}

	#[test]
	fn parse_missing_required_field_errors() {
		let defs = vec![serde_json::json!({"type": "rename", "from": "x"})];
		let err = parse_steps(&defs).unwrap_err();
		assert!(err.to_string().contains("missing 'to'"));
	}

	#[test]
	fn parse_unknown_filter_op_errors() {
		let defs = vec![serde_json::json!({
			"type": "filter",
			"field": "x",
			"op": "bogus",
			"value": 1
		})];
		let err = parse_steps(&defs).unwrap_err();
		assert!(err.to_string().contains("unknown filter op 'bogus'"));
	}

	#[test]
	fn parse_all_step_types() {
		let defs = vec![
			serde_json::json!({"type": "rename", "from": "a", "to": "b"}),
			serde_json::json!({"type": "set", "field": "x", "value": 1}),
			serde_json::json!({"type": "upper", "field": "x"}),
			serde_json::json!({"type": "lower", "field": "x"}),
			serde_json::json!({"type": "remove", "field": "x"}),
			serde_json::json!({"type": "copy", "from": "a", "to": "b"}),
			serde_json::json!({"type": "default", "field": "x", "value": 0}),
			serde_json::json!({"type": "filter", "field": "x", "op": "exists"}),
			serde_json::json!({"type": "map_value", "field": "x", "mapping": {"a": "b"}}),
			serde_json::json!({"type": "truncate", "field": "x", "max_len": 10}),
			serde_json::json!({"type": "nest", "fields": ["a"], "into": "b"}),
			serde_json::json!({"type": "flatten", "field": "x"}),
			serde_json::json!({"type": "hash", "field": "x"}),
			serde_json::json!({"type": "coalesce", "fields": ["a", "b"], "into": "c"}),
		];
		let chain = parse_steps(&defs).unwrap();
		assert_eq!(chain.len(), 14);
	}
}
