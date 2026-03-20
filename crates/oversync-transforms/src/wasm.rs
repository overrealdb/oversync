use oversync_core::error::OversyncError;

use crate::TransformStep;

/// A WASM-based transform step loaded from a `.wasm` file.
///
/// The WASM module must export:
/// - `alloc(size: i32) -> i32` — allocate `size` bytes, return pointer
/// - `dealloc(ptr: i32, size: i32)` — free allocated memory
/// - `transform(ptr: i32, len: i32) -> i64` — transform JSON bytes at `ptr`,
///   returns `(new_ptr << 32) | new_len` packed into i64.
///   Return 0 to filter out the record.
///
/// Input/output format: JSON bytes (UTF-8 encoded `serde_json::Value`).
pub struct WasmStep {
	name: String,
	store: std::sync::Mutex<wasmtime::Store<()>>,
	instance: wasmtime::Instance,
	memory: wasmtime::Memory,
}

impl std::fmt::Debug for WasmStep {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("WasmStep")
			.field("name", &self.name)
			.finish()
	}
}

impl WasmStep {
	/// Load a WASM transform from bytes (compiled `.wasm`).
	pub fn from_bytes(name: &str, wasm_bytes: &[u8]) -> Result<Self, OversyncError> {
		let engine = wasmtime::Engine::default();
		let module = wasmtime::Module::new(&engine, wasm_bytes)
			.map_err(|e| OversyncError::Plugin(format!("wasm compile '{name}': {e}")))?;

		let mut store = wasmtime::Store::new(&engine, ());
		let instance = wasmtime::Instance::new(&mut store, &module, &[])
			.map_err(|e| OversyncError::Plugin(format!("wasm instantiate '{name}': {e}")))?;

		let memory = instance
			.get_memory(&mut store, "memory")
			.ok_or_else(|| {
				OversyncError::Plugin(format!("wasm '{name}': missing 'memory' export"))
			})?;

		Ok(Self {
			name: name.to_string(),
			store: std::sync::Mutex::new(store),
			instance,
			memory,
		})
	}

	/// Load a WASM transform from a file path.
	pub fn from_file(name: &str, path: &std::path::Path) -> Result<Self, OversyncError> {
		let bytes = std::fs::read(path)
			.map_err(|e| OversyncError::Plugin(format!("read wasm '{name}': {e}")))?;
		Self::from_bytes(name, &bytes)
	}
}

impl TransformStep for WasmStep {
	fn apply(&self, data: &mut serde_json::Value) -> Result<bool, OversyncError> {
		let input = serde_json::to_vec(data)
			.map_err(|e| OversyncError::Plugin(format!("wasm serialize: {e}")))?;

		let mut store = self.store.lock().map_err(|e| {
			OversyncError::Plugin(format!("wasm store lock: {e}"))
		})?;

		// Allocate input buffer in WASM memory
		let alloc = self
			.instance
			.get_typed_func::<i32, i32>(&mut *store, "alloc")
			.map_err(|e| OversyncError::Plugin(format!("wasm '{0}': missing 'alloc': {e}", self.name)))?;

		let input_ptr = alloc
			.call(&mut *store, input.len() as i32)
			.map_err(|e| OversyncError::Plugin(format!("wasm alloc: {e}")))?;

		// Write input to WASM memory
		let mem_data = self.memory.data_mut(&mut *store);
		let start = input_ptr as usize;
		let end = start + input.len();
		if end > mem_data.len() {
			return Err(OversyncError::Plugin("wasm: alloc returned out-of-bounds pointer".into()));
		}
		mem_data[start..end].copy_from_slice(&input);

		// Call transform
		let transform = self
			.instance
			.get_typed_func::<(i32, i32), i64>(&mut *store, "transform")
			.map_err(|e| OversyncError::Plugin(format!("wasm '{0}': missing 'transform': {e}", self.name)))?;

		let result = transform
			.call(&mut *store, (input_ptr, input.len() as i32))
			.map_err(|e| OversyncError::Plugin(format!("wasm transform: {e}")))?;

		// result = 0 means filter out
		if result == 0 {
			return Ok(false);
		}

		// Unpack result: high 32 bits = ptr, low 32 bits = len
		let out_ptr = (result >> 32) as usize;
		let out_len = (result & 0xFFFFFFFF) as usize;

		let mem_data = self.memory.data(&*store);
		if out_ptr + out_len > mem_data.len() {
			return Err(OversyncError::Plugin("wasm: transform returned out-of-bounds result".into()));
		}

		let output_bytes = &mem_data[out_ptr..out_ptr + out_len];
		let output: serde_json::Value = serde_json::from_slice(output_bytes)
			.map_err(|e| OversyncError::Plugin(format!("wasm output parse: {e}")))?;

		// Free WASM memory
		if let Ok(dealloc) = self.instance.get_typed_func::<(i32, i32), ()>(&mut *store, "dealloc") {
			let _ = dealloc.call(&mut *store, (out_ptr as i32, out_len as i32));
		}

		*data = output;
		Ok(true)
	}

	fn step_name(&self) -> &str {
		&self.name
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// Minimal WAT (WebAssembly Text) that exports memory + alloc + transform.
	// This transform is a passthrough: returns the same data it received.
	const PASSTHROUGH_WAT: &str = r#"
		(module
			(memory (export "memory") 1)
			(global $offset (mut i32) (i32.const 1024))

			(func (export "alloc") (param $size i32) (result i32)
				(local $ptr i32)
				(local.set $ptr (global.get $offset))
				(global.set $offset (i32.add (global.get $offset) (local.get $size)))
				(local.get $ptr)
			)

			(func (export "dealloc") (param $ptr i32) (param $size i32)
				;; no-op for simplicity
			)

			(func (export "transform") (param $ptr i32) (param $len i32) (result i64)
				;; Return the same ptr/len (passthrough)
				(i64.or
					(i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
					(i64.extend_i32_u (local.get $len))
				)
			)
		)
	"#;

	// WAT that always returns 0 (filter out)
	const FILTER_WAT: &str = r#"
		(module
			(memory (export "memory") 1)
			(func (export "alloc") (param $size i32) (result i32) (i32.const 1024))
			(func (export "dealloc") (param $ptr i32) (param $size i32))
			(func (export "transform") (param $ptr i32) (param $len i32) (result i64)
				(i64.const 0)
			)
		)
	"#;

	fn compile_wat(wat: &str) -> Vec<u8> {
		wat::parse_str(wat).expect("invalid WAT")
	}

	#[test]
	fn wasm_passthrough() {
		let wasm = compile_wat(PASSTHROUGH_WAT);
		let step = WasmStep::from_bytes("passthrough", &wasm).unwrap();
		let mut data = serde_json::json!({"name": "alice", "age": 30});
		let keep = step.apply(&mut data).unwrap();
		assert!(keep);
		assert_eq!(data["name"], "alice");
		assert_eq!(data["age"], 30);
	}

	#[test]
	fn wasm_filter_drops() {
		let wasm = compile_wat(FILTER_WAT);
		let step = WasmStep::from_bytes("filter", &wasm).unwrap();
		let mut data = serde_json::json!({"x": 1});
		let keep = step.apply(&mut data).unwrap();
		assert!(!keep);
	}

	#[test]
	fn wasm_missing_memory_errors() {
		let wat = r#"
			(module
				(func (export "alloc") (param i32) (result i32) (i32.const 0))
				(func (export "transform") (param i32) (param i32) (result i64) (i64.const 0))
			)
		"#;
		let wasm = compile_wat(wat);
		let err = WasmStep::from_bytes("no-mem", &wasm).unwrap_err();
		assert!(err.to_string().contains("missing 'memory'"));
	}

	#[test]
	fn wasm_step_name() {
		let wasm = compile_wat(PASSTHROUGH_WAT);
		let step = WasmStep::from_bytes("my-plugin", &wasm).unwrap();
		assert_eq!(step.step_name(), "my-plugin");
	}
}
