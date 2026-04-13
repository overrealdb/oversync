//! Pipe config with transforms: shows TOML parsing of [[pipes]] with transforms.
//!
//! Run: `cargo run --example pipes_with_transforms`

use oversync::config::SyncConfig;

fn main() {
	let toml = r#"
[surrealdb]
url = "ws://localhost:8000"

[[sinks]]
name = "kafka"
type = "kafka"

[sinks.config]
brokers = "kafka:9092"
topic = "sync-events"

[[pipes]]
name = "catalog-sync"
targets = ["kafka"]

[pipes.origin]
connector = "postgres"
dsn = "postgres://readonly:pass@db:5432/app"

[pipes.schedule]
interval_secs = 60

[pipes.delta]
diff_mode = "memory"
fail_safe_threshold = 25.0

[[pipes.transforms]]
type = "rename"
from = "entity_id"
to = "id"

[[pipes.transforms]]
type = "upper"
field = "name"

[[pipes.transforms]]
type = "set"
field = "source"
value = "catalog-sync"

[[pipes.queries]]
id = "tables"
sql = "SELECT oid::text AS entity_id, relname AS name FROM pg_class"
key_column = "entity_id"

[[pipes.queries]]
id = "columns"
sql = "SELECT attrelid::text || '.' || attnum::text AS id, attname FROM pg_attribute"
key_column = "id"
"#;

	let config = SyncConfig::from_str(toml).unwrap();
	let pipes = config.effective_pipes();

	println!(
		"Parsed {} pipe(s), {} sink(s)\n",
		pipes.len(),
		config.sinks.len()
	);

	for pipe in &pipes {
		println!("Pipe: {}", pipe.name);
		println!("  Origin: {} @ {}", pipe.origin.connector, pipe.origin.dsn);
		println!("  Targets: {:?}", pipe.targets);
		println!("  Schedule: {}s", pipe.schedule.interval_secs);
		println!("  Transforms: {}", pipe.transforms.len());
		for t in &pipe.transforms {
			println!("    - {}: {:?}", t["type"].as_str().unwrap_or("?"), t);
		}
		println!("  Queries: {}", pipe.queries.len());
		for q in &pipe.queries {
			println!("    - {} (key: {})", q.id, q.key_column);
		}
		println!();
	}
}
