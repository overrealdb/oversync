# oversync-transforms

Declarative transform step library for the oversync data sync engine.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **`TransformStep` trait** -- interface for a single operation on a record's JSON data, returning keep/drop
- **`StepChain`** -- ordered chain of steps applied sequentially; implements `TransformHook` for use in the sync pipeline
- **Built-in steps** -- `Rename`, `Set`, `Filter` (with `Eq`/`Ne`/`Contains` operators), and more
- **`parse_steps`** -- parse a declarative JSON step definition into a `StepChain`
- **Optional WASM support** -- enable the `wasm` feature for wasmtime-based transform steps

## Usage

```rust
use oversync_transforms::{StepChain, steps::{Rename, Filter, FilterOp}};

let chain = StepChain::new(vec![
    Box::new(Rename { from: "old_name".into(), to: "new_name".into() }),
    Box::new(Filter {
        field: "status".into(),
        op: FilterOp::Eq,
        value: serde_json::json!("active"),
    }),
]);

let mut data = serde_json::json!({"old_name": "val", "status": "active"});
let keep = chain.apply_one(&mut data)?;
assert!(keep);
assert_eq!(data["new_name"], "val");
```

## Features

- `wasm` -- enables wasmtime-based WASM transform steps

## License

Apache-2.0
