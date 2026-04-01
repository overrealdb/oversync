use oversync_core::error::OversyncError;

use crate::TransformStep;

/// A JavaScript transform step powered by QuickJS (via rquickjs).
///
/// The user provides a JS function body that receives a row object and returns
/// a transformed object (or `null`/`undefined` to filter the record out).
///
/// ```json
/// {
///   "type": "js",
///   "function": "function transform(row) { return { ...row, total: row.price * row.qty } }"
/// }
/// ```
///
/// The function is compiled once at parse time. Each `apply()` call serializes
/// the row to JS, invokes the function, and deserializes the result back.
pub struct JsStep {
	name: String,
	ctx: rquickjs::Context,
}

impl std::fmt::Debug for JsStep {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("JsStep").field("name", &self.name).finish()
	}
}

impl JsStep {
	/// Create a new JS transform from a function source string.
	///
	/// The source must define a global `transform` function that accepts one argument.
	pub fn new(name: &str, function_source: &str) -> Result<Self, OversyncError> {
		let rt = rquickjs::Runtime::new()
			.map_err(|e| OversyncError::Plugin(format!("js runtime init '{name}': {e}")))?;

		// Memory limit: 32MB to prevent runaway scripts
		rt.set_memory_limit(32 * 1024 * 1024);
		// Max stack size: 1MB
		rt.set_max_stack_size(1024 * 1024);

		let ctx = rquickjs::Context::full(&rt)
			.map_err(|e| OversyncError::Plugin(format!("js context init '{name}': {e}")))?;

		// Compile the user's function
		ctx.with(|ctx| {
			ctx.eval::<(), _>(function_source)
				.map_err(|e| OversyncError::Config(format!("js compile '{name}': {e}")))?;

			// Verify 'transform' function exists
			let globals = ctx.globals();
			let has_transform: bool = globals
				.get::<_, rquickjs::Value>("transform")
				.map(|v| v.is_function())
				.unwrap_or(false);

			if !has_transform {
				return Err(OversyncError::Config(format!(
					"js '{name}': source must define a 'transform' function"
				)));
			}

			Ok(())
		})?;

		Ok(Self {
			name: name.to_string(),
			ctx,
		})
	}
}

impl TransformStep for JsStep {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		self.ctx.with(|ctx| {
			// Serialize row to JS value
			let js_input = rquickjs_serde::to_value(ctx.clone(), &*data)
				.map_err(|e| OversyncError::Plugin(format!("js serialize: {e}")))?;

			// Get transform function from globals
			let transform: rquickjs::Function = ctx
				.globals()
				.get("transform")
				.map_err(|e| OversyncError::Plugin(format!("js get transform: {e}")))?;

			// Call transform(row)
			let result: rquickjs::Value = transform
				.call((js_input,))
				.map_err(|e| OversyncError::Plugin(format!("js transform: {e}")))?;

			// null/undefined → filter out
			if result.is_null() || result.is_undefined() {
				return Ok(false);
			}

			// Deserialize result back to serde_json::Value
			let output: serde_json::Value = rquickjs_serde::from_value(result)
				.map_err(|e| OversyncError::Plugin(format!("js deserialize: {e}")))?;

			*data = output;
			Ok(true)
		})
	}

	fn step_name(&self) -> &str {
		&self.name
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn passthrough() {
		let step = JsStep::new("test", "function transform(row) { return row; }").unwrap();
		let mut data = serde_json::json!({"name": "alice", "age": 30});
		assert!(step.apply(&mut data).unwrap());
		assert_eq!(data, serde_json::json!({"name": "alice", "age": 30}));
	}

	#[test]
	fn add_computed_field() {
		let step = JsStep::new(
			"test",
			"function transform(row) { return { ...row, total: row.price * row.qty } }",
		)
		.unwrap();
		let mut data = serde_json::json!({"price": 10.5, "qty": 3});
		assert!(step.apply(&mut data).unwrap());
		assert_eq!(data["total"], 31.5);
		assert_eq!(data["price"], 10.5);
	}

	#[test]
	fn filter_by_returning_null() {
		let step = JsStep::new(
			"test",
			"function transform(row) { return row.active ? row : null; }",
		)
		.unwrap();

		let mut keep = serde_json::json!({"active": true, "name": "alice"});
		assert!(step.apply(&mut keep).unwrap());

		let mut drop = serde_json::json!({"active": false, "name": "bob"});
		assert!(!step.apply(&mut drop).unwrap());
	}

	#[test]
	fn reshape_jsonb() {
		let step = JsStep::new(
			"test",
			r#"function transform(row) {
				var meta = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata;
				return {
					id: row.id,
					email: meta.contact.email,
					tier: meta.subscription.tier,
					active: meta.subscription.active
				};
			}"#,
		)
		.unwrap();

		let mut data = serde_json::json!({
			"id": "u1",
			"metadata": {
				"contact": {"email": "a@b.com", "phone": "555"},
				"subscription": {"tier": "pro", "active": true}
			}
		});
		assert!(step.apply(&mut data).unwrap());
		assert_eq!(
			data,
			serde_json::json!({
				"id": "u1",
				"email": "a@b.com",
				"tier": "pro",
				"active": true
			})
		);
	}

	#[test]
	fn multiple_calls_reuse_context() {
		let step = JsStep::new(
			"test",
			"var count = 0; function transform(row) { count++; row.seq = count; return row; }",
		)
		.unwrap();

		let mut d1 = serde_json::json!({"x": 1});
		step.apply(&mut d1).unwrap();
		assert_eq!(d1["seq"], 1);

		let mut d2 = serde_json::json!({"x": 2});
		step.apply(&mut d2).unwrap();
		assert_eq!(d2["seq"], 2);
	}

	#[test]
	fn missing_transform_function_errors() {
		let err = JsStep::new("test", "function foo(row) { return row; }").unwrap_err();
		assert!(
			err.to_string()
				.contains("must define a 'transform' function")
		);
	}

	#[test]
	fn syntax_error_in_source() {
		let err = JsStep::new("test", "function transform(row { return row; }").unwrap_err();
		assert!(err.to_string().contains("js compile"));
	}

	#[test]
	fn runtime_error_propagates() {
		let step = JsStep::new(
			"test",
			"function transform(row) { throw new Error('boom'); }",
		)
		.unwrap();
		let mut data = serde_json::json!({});
		let err = step.apply(&mut data).unwrap_err();
		assert!(err.to_string().contains("js transform"), "got: {err}");
	}

	#[test]
	fn return_undefined_filters() {
		let step = JsStep::new("test", "function transform(row) { }").unwrap();
		let mut data = serde_json::json!({"x": 1});
		assert!(!step.apply(&mut data).unwrap());
	}

	#[test]
	fn array_and_nested_objects() {
		let step = JsStep::new(
			"test",
			r#"function transform(row) {
				return { tags: row.tags.map(function(t) { return t.toUpperCase(); }), count: row.tags.length };
			}"#,
		)
		.unwrap();
		let mut data = serde_json::json!({"tags": ["foo", "bar"]});
		step.apply(&mut data).unwrap();
		assert_eq!(data["tags"], serde_json::json!(["FOO", "BAR"]));
		assert_eq!(data["count"], 2);
	}
}
