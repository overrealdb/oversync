use std::env;
use std::fs::{self, File};
use std::path::Path;

use serde_json::Value;

fn is_null_schema(value: &Value) -> bool {
	matches!(
		value,
		Value::Object(map)
			if matches!(map.get("type"), Some(Value::String(ty)) if ty == "null")
	)
}

fn normalize_schema_types(value: &mut Value) {
	match value {
		Value::Object(map) => {
			for keyword in ["oneOf", "anyOf"] {
				if let Some(Value::Array(variants)) = map.get_mut(keyword) {
					let mut non_null = variants
						.iter()
						.filter(|entry| !is_null_schema(entry))
						.cloned()
						.collect::<Vec<_>>();
					let has_null = variants.iter().any(is_null_schema);
					if has_null {
						match non_null.len() {
							0 => {
								map.remove(keyword);
								map.insert("nullable".into(), Value::Bool(true));
							}
							1 => {
								let replacement = non_null.pop().expect("single non-null schema");
								map.remove(keyword);
								match replacement {
									Value::Object(replacement_map) => {
										for (key, value) in replacement_map {
											map.insert(key, value);
										}
									}
									other => {
										map.insert(keyword.into(), Value::Array(vec![other]));
									}
								}
								map.insert("nullable".into(), Value::Bool(true));
							}
							_ => {
								*variants = non_null;
								map.insert("nullable".into(), Value::Bool(true));
							}
						}
					}
				}
			}

			if let Some(type_value) = map.get_mut("type")
				&& let Value::Array(types) = type_value
			{
				let mut non_null = types
					.iter()
					.filter_map(|entry| match entry {
						Value::String(name) if name != "null" => Some(name.clone()),
						_ => None,
					})
					.collect::<Vec<_>>();
				let has_null = types.iter().any(|entry| entry == "null");
				if has_null && non_null.len() == 1 {
					*type_value = Value::String(non_null.remove(0));
					map.insert("nullable".into(), Value::Bool(true));
				}
			}

			if matches!(map.get("type"), Some(Value::String(ty)) if ty == "null") {
				map.remove("type");
				map.insert("nullable".into(), Value::Bool(true));
			}

			for child in map.values_mut() {
				normalize_schema_types(child);
			}
		}
		Value::Array(items) => {
			for item in items {
				normalize_schema_types(item);
			}
		}
		_ => {}
	}
}

fn main() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let local_spec = manifest_dir.join("openapi.json");
	let workspace_spec = manifest_dir.join("../../ui/openapi.json");
	println!("cargo:rerun-if-changed={}", local_spec.display());
	println!("cargo:rerun-if-changed={}", workspace_spec.display());
	let spec_path = if workspace_spec.exists() {
		workspace_spec
	} else {
		local_spec
	};

	let file = File::open(&spec_path).expect("open oversync OpenAPI spec");
	let mut spec: Value = serde_json::from_reader(file).expect("parse oversync OpenAPI spec");
	if let Value::Object(root) = &mut spec {
		root.insert("openapi".into(), Value::String("3.0.3".into()));
	}
	normalize_schema_types(&mut spec);
	let spec: openapiv3::OpenAPI =
		serde_json::from_value(spec).expect("convert OpenAPI 3.1 schema to 3.0-compatible form");
	let tokens = progenitor::Generator::default()
		.generate_tokens(&spec)
		.expect("generate Rust client from OpenAPI");
	let ast = syn::parse2(tokens).expect("parse generated Rust client");
	let content = prettyplease::unparse(&ast);

	let out_file = Path::new(&env::var("OUT_DIR").expect("OUT_DIR")).join("generated.rs");
	fs::write(out_file, content).expect("write generated Rust client");
}
