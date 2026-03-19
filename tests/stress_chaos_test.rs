//! Stress + chaos test: two tables with JOIN, 100K rows, source/sink outages,
//! mutations, table drops, full lifecycle verification.
//!
//! Default: 100_000 rows. Override with STRESS_ROWS=10000 for faster CI.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use oversync::cycle::{CycleConfig, CycleRunner};
use oversync_core::error::OversyncError;
use oversync_core::model::{EventEnvelope, OpType};
use oversync_core::traits::Sink;
use oversync_delta::DeltaEngine;

use common::postgres::TestPostgres;
use common::surreal::TestSurrealContainer;

fn row_count() -> usize {
	std::env::var("STRESS_ROWS")
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(100_000)
}

// ── Controllable sink that records all events ────────────────

struct VerifySink {
	available: Arc<AtomicBool>,
	events: std::sync::Mutex<Vec<EventEnvelope>>,
	batch_count: AtomicUsize,
}

impl VerifySink {
	fn new(available: Arc<AtomicBool>) -> Self {
		Self {
			available,
			events: std::sync::Mutex::new(Vec::new()),
			batch_count: AtomicUsize::new(0),
		}
	}

	fn events(&self) -> Vec<EventEnvelope> {
		self.events.lock().unwrap().clone()
	}

	fn count_by_op(&self, op: OpType) -> usize {
		self.events
			.lock()
			.unwrap()
			.iter()
			.filter(|e| e.meta.op == op)
			.count()
	}

	fn clear(&self) {
		self.events.lock().unwrap().clear();
		self.batch_count.store(0, Ordering::SeqCst);
	}

	fn total(&self) -> usize {
		self.events.lock().unwrap().len()
	}
}

#[async_trait::async_trait]
impl Sink for VerifySink {
	fn name(&self) -> &str {
		"verify-sink"
	}

	async fn send_event(&self, envelope: &EventEnvelope) -> Result<(), OversyncError> {
		if !self.available.load(Ordering::SeqCst) {
			return Err(OversyncError::Sink("sink unavailable".into()));
		}
		self.events.lock().unwrap().push(envelope.clone());
		self.batch_count.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}

	async fn test_connection(&self) -> Result<(), OversyncError> {
		Ok(())
	}
}

async fn count_snapshot(
	surreal: &common::surreal::TestSurrealContainer,
	source: &str,
	query: &str,
) -> usize {
	let mut res = surreal
		.client
		.query(
			"SELECT count() AS c FROM snapshot WHERE source_id = $src AND query_id = $qid GROUP ALL",
		)
		.bind(("src", source.to_string()))
		.bind(("qid", query.to_string()))
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	rows.first().and_then(|r| r["c"].as_u64()).unwrap_or(0) as usize
}

fn make_config(schema: &str, threshold: f64) -> CycleConfig {
	CycleConfig {
		source_id: "stress".into(),
		query_id: "product_catalog".into(),
		sql: format!(
			"SELECT p.id, p.name, p.price::text AS price, p.metadata, c.name AS category_name \
			 FROM {schema}.products p \
			 JOIN {schema}.categories c ON p.category_id = c.id \
			 ORDER BY p.id"
		),
		key_column: "id".into(),
		fail_safe_threshold: threshold,
		diff_mode: oversync::config::DiffMode::Db,
			missed_tick_policy: Default::default(),
		transform: None,
	}
}

// ── The test ─────────────────────────────────────────────────

#[tokio::test]
async fn stress_two_tables_join_full_lifecycle() {
	let _ = tracing_subscriber::fmt()
		.with_env_filter("oversync=info")
		.with_test_writer()
		.try_init();

	let n = row_count();
	let cat_count = (n / 100).max(10);
	eprintln!("=== stress: {n} products, {cat_count} categories ===");

	let surreal = TestSurrealContainer::new().await;
	let pg = TestPostgres::new().await;
	let s = &pg.schema;

	let engine = DeltaEngine::single(surreal.client.clone());
	let connector = oversync_connectors::PostgresConnector::from_pool("stress-pg", pg.pool.clone());

	let sink_on = Arc::new(AtomicBool::new(true));
	let sink = Arc::new(VerifySink::new(sink_on.clone()));
	// Need to share sink between the sinks vec and our reference
	let sink2 = Arc::new(VerifySink::new(sink_on.clone()));
	let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(VerifySink::new(sink_on.clone()))];

	let config = make_config(s, 30.0);

	// ── Setup: create tables with bulk data ─────────────────
	eprintln!("setup: creating tables...");

	pg.run_sql(&format!(
		"CREATE TABLE categories (id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT)"
	))
	.await;

	pg.run_sql(&format!(
		"INSERT INTO categories (id, name, description) \
		 SELECT 'c' || lpad(g::text, 5, '0'), \
		        'Category ' || g, \
		        'Description for category ' || g \
		 FROM generate_series(1, {cat_count}) g"
	))
	.await;

	pg.run_sql(&format!(
		"CREATE TABLE products (\
		   id TEXT PRIMARY KEY, \
		   name TEXT NOT NULL, \
		   price NUMERIC(10,2), \
		   category_id TEXT REFERENCES categories(id), \
		   metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb \
		 )"
	))
	.await;

	// Insert in chunks of 100K to avoid OOM in postgres container
	let chunk_size = 100_000;
	for start in (0..n).step_by(chunk_size) {
		let end = (start + chunk_size).min(n);
		pg.run_sql(&format!(
			"INSERT INTO products (id, name, price, category_id, metadata) \
			 SELECT \
			   'p' || lpad(g::text, 7, '0'), \
			   'Product ' || g, \
			   (random() * 1000)::numeric(10,2), \
			   'c' || lpad(((g % {cat_count}) + 1)::text, 5, '0'), \
			   jsonb_build_object(\
			     'index', g, \
			     'tags', jsonb_build_array('tag_' || (g % 50), 'group_' || (g % 10)), \
			     'specs', jsonb_build_object('weight', g * 0.5, 'color', 'color_' || (g % 20)) \
			   ) \
			 FROM generate_series({}, {end}) g",
			start + 1
		))
		.await;
		if n > chunk_size {
			eprintln!("  inserted {end}/{n} products...");
		}
	}

	// Verify data
	let mut res = pg.pool.acquire().await.unwrap();
	let row: (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM {s}.products"))
		.fetch_one(&mut *res)
		.await
		.unwrap();
	assert_eq!(row.0 as usize, n, "products count");
	drop(res);

	eprintln!("setup: {n} products + {cat_count} categories created");

	// ── Phase 1: Initial sync (JOIN query) ──────────────────
	eprintln!("phase 1: initial sync ({n} rows via JOIN)...");
	let t0 = std::time::Instant::now();

	let runner = CycleRunner::new(&engine, &connector, &sinks);
	let diff1 = runner.run(&config).await.unwrap();

	assert_eq!(diff1.created.len(), n, "all products should be created");
	assert!(diff1.updated.is_empty());
	assert!(diff1.deleted.is_empty());

	// Verify JSONB data made it through
	let sample = diff1
		.created
		.iter()
		.find(|e| e.row_key == "p0000001")
		.unwrap();
	assert!(
		sample.row_data.get("metadata").is_some(),
		"JSONB metadata present"
	);
	assert!(
		sample.row_data.get("category_name").is_some(),
		"JOIN column present"
	);

	eprintln!(
		"phase 1: OK — {n} created in {:.1}s",
		t0.elapsed().as_secs_f64()
	);

	// ── Phase 2: Mutations across both tables ───────────────
	let update_count = n / 10;
	let delete_count = n / 20;
	let insert_count = n / 20;

	eprintln!(
		"phase 2: {update_count} price updates, {delete_count} deletes, \
		 {insert_count} new products, category rename..."
	);

	// Update prices on first 10%
	pg.run_sql(&format!(
		"UPDATE products SET price = price + 100, \
		   metadata = metadata || jsonb_build_object('price_bumped', true) \
		 WHERE id IN (SELECT id FROM {s}.products ORDER BY id LIMIT {update_count})"
	))
	.await;

	// Delete last 5%
	pg.run_sql(&format!(
		"DELETE FROM products \
		 WHERE id IN (SELECT id FROM {s}.products ORDER BY id DESC LIMIT {delete_count})"
	))
	.await;

	// Insert 5% new products
	pg.run_sql(&format!(
		"INSERT INTO products (id, name, price, category_id, metadata) \
		 SELECT \
		   'p' || lpad((g + {n})::text, 7, '0'), \
		   'New Product ' || g, \
		   (random() * 500)::numeric(10,2), \
		   'c' || lpad(((g % {cat_count}) + 1)::text, 5, '0'), \
		   jsonb_build_object('index', g + {n}, 'new', true) \
		 FROM generate_series(1, {insert_count}) g"
	))
	.await;

	// Rename a category (affects JOIN output for related products)
	pg.run_sql("UPDATE categories SET name = 'RENAMED_CATEGORY' WHERE id = 'c00001'")
		.await;

	let t2 = std::time::Instant::now();
	let diff2 = runner.run(&config).await.unwrap();

	// Debug: check snapshot state
	let snap_count = count_snapshot(&surreal, "stress", "product_catalog").await;
	eprintln!(
		"  snapshot has {} rows (expected ~{})",
		snap_count,
		n - delete_count + insert_count
	);
	eprintln!(
		"  diff: created={}, updated={}, deleted={}",
		diff2.created.len(),
		diff2.updated.len(),
		diff2.deleted.len()
	);

	// Debug: check prev_hash on a sample of "new" rows
	let sample_new_id = format!("p{:07}", n + 1); // first new row
	let mut dbg = surreal.client.query(
		"SELECT row_key, prev_hash, cycle_id FROM snapshot WHERE source_id = 'stress' AND row_key = $key"
	).bind(("key", sample_new_id.clone())).await.unwrap();
	let dbg_rows: Vec<serde_json::Value> = dbg.take(0).unwrap();
	eprintln!("  sample new row {sample_new_id}: {:?}", dbg_rows);

	// Count rows with prev_hash IS NONE
	let mut cnt = surreal.client.query(
		"SELECT count() AS c FROM snapshot WHERE source_id = 'stress' AND query_id = 'product_catalog' AND prev_hash IS NONE GROUP ALL"
	).await.unwrap();
	let cnt_rows: Vec<serde_json::Value> = cnt.take(0).unwrap();
	eprintln!("  rows with prev_hash IS NONE: {:?}", cnt_rows);

	// Direct find_created test without pagination
	let mut direct = surreal.client.query(
		"SELECT row_key FROM snapshot WHERE source_id = 'stress' AND query_id = 'product_catalog' AND cycle_id = 2 AND prev_hash IS NONE LIMIT 10"
	).await.unwrap();
	let direct_rows: Vec<serde_json::Value> = direct.take(0).unwrap();
	eprintln!(
		"  direct find_created (limit 10): {} rows",
		direct_rows.len()
	);

	// Test with bound params like paginated_query uses
	let mut bound = surreal.client.query(
		"SELECT row_key FROM snapshot WHERE source_id = $source_id AND query_id = $query_id AND cycle_id = $cycle_id AND prev_hash IS NONE LIMIT $page_size START $offset"
	).bind(("source_id", "stress".to_string()))
	.bind(("query_id", "product_catalog".to_string()))
	.bind(("cycle_id", 2i64))
	.bind(("page_size", 5000i64))
	.bind(("offset", 0i64))
	.await.unwrap();
	let bound_rows: Vec<serde_json::Value> = bound.take(0).unwrap();
	eprintln!(
		"  bound find_created (page_size=5000, offset=0): {} rows",
		bound_rows.len()
	);

	assert_eq!(diff2.created.len(), insert_count, "new products");
	assert_eq!(diff2.deleted.len(), delete_count, "deleted products");
	// Updated count includes price bumps + category rename affecting joined rows
	assert!(
		diff2.updated.len() >= update_count,
		"expected >= {update_count} updates, got {}",
		diff2.updated.len()
	);

	let expected_total = n - delete_count + insert_count;
	let count2 = count_snapshot(&surreal, "stress", "product_catalog").await;
	assert_eq!(count2, expected_total, "snapshot count after mutations");

	eprintln!(
		"phase 2: OK — {} created, {} updated, {} deleted in {:.1}s",
		diff2.created.len(),
		diff2.updated.len(),
		diff2.deleted.len(),
		t2.elapsed().as_secs_f64()
	);

	// ── Phase 3: No-change cycle (measures full-scan overhead) ──
	eprintln!("phase 3: no-change cycle (full scan, 0 diffs)...");
	let t3 = std::time::Instant::now();
	let diff3 = runner.run(&config).await.unwrap();
	assert!(diff3.is_empty(), "no changes = empty diff");
	eprintln!(
		"phase 3: OK — {:.1}s (this is the baseline full-scan cost)",
		t3.elapsed().as_secs_f64()
	);

	// ── Phase 3b: Micro-change (1 row out of N) ────────────
	eprintln!("phase 3b: micro-change (1 row updated)...");
	pg.run_sql("UPDATE products SET name = 'MICRO_CHANGE' WHERE id = 'p0000001'")
		.await;
	let t3b = std::time::Instant::now();
	let diff3b = runner.run(&config).await.unwrap();
	assert_eq!(diff3b.updated.len(), 1, "exactly 1 update");
	assert_eq!(diff3b.updated[0].row_key, "p0000001");
	assert!(diff3b.created.is_empty());
	assert!(diff3b.deleted.is_empty());
	eprintln!(
		"phase 3b: OK — 1 update detected in {:.1}s (same full-scan cost)",
		t3b.elapsed().as_secs_f64()
	);

	// ── Phase 4: Sink outage during mutations ───────────────
	eprintln!("phase 4: sink DOWN + mutations...");
	sink_on.store(false, Ordering::SeqCst);

	pg.run_sql(&format!(
		"UPDATE products SET name = name || ' (v2)' \
		 WHERE id IN (SELECT id FROM {s}.products LIMIT 500)"
	))
	.await;

	let result4 = runner.run(&config).await;
	assert!(result4.is_err(), "cycle should fail with sink down");

	let pending = engine
		.read_pending_events("stress", "product_catalog")
		.await
		.unwrap();
	assert!(!pending.is_empty(), "events in outbox");
	let pending_count: usize = pending.iter().map(|(_, e)| e.len()).sum();
	assert_eq!(pending_count, 500, "500 updated events pending");
	eprintln!("phase 4: OK — {pending_count} events in outbox");

	// ── Phase 5: Sink recovery ──────────────────────────────
	eprintln!("phase 5: sink UP, recovery...");
	sink_on.store(true, Ordering::SeqCst);

	let diff5 = runner.run(&config).await.unwrap();

	let pending_after = engine
		.read_pending_events("stress", "product_catalog")
		.await
		.unwrap();
	assert!(pending_after.is_empty(), "outbox drained");
	eprintln!(
		"phase 5: OK — outbox drained, diff: {} c / {} u / {} d",
		diff5.created.len(),
		diff5.updated.len(),
		diff5.deleted.len()
	);

	// ── Phase 6: Source tables dropped → mass DELETE ─────────
	eprintln!("phase 6: dropping source data (mass delete)...");

	let pre_drop_count = count_snapshot(&surreal, "stress", "product_catalog").await;

	// Delete all products (simulating table drop)
	pg.run_sql("DELETE FROM products").await;

	// Use 100% threshold to allow mass deletion
	let config_permissive = make_config(s, 100.0);
	let diff6 = runner.run(&config_permissive).await.unwrap();

	assert_eq!(
		diff6.deleted.len(),
		pre_drop_count,
		"all rows should be deleted"
	);
	assert!(diff6.created.is_empty());
	assert!(diff6.updated.is_empty());

	// Snapshot should be empty
	let final_count = count_snapshot(&surreal, "stress", "product_catalog").await;
	assert_eq!(final_count, 0, "snapshot empty after source drop");

	// Verify DELETE events have correct keys
	let deleted_keys: Vec<&str> = diff6.deleted.iter().map(|e| e.row_key.as_str()).collect();
	assert!(
		deleted_keys.iter().any(|k| k.starts_with("p")),
		"delete events have product keys"
	);

	eprintln!(
		"phase 6: OK — {} DELETE events sent, snapshot empty",
		diff6.deleted.len()
	);

	// ── Phase 7: Verify cycle_log audit trail ───────────────
	let mut res = surreal
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'stress' ORDER BY cycle_id")
		.await
		.unwrap();
	let logs: Vec<serde_json::Value> = res.take(0).unwrap();

	let statuses: Vec<&str> = logs
		.iter()
		.map(|l| l["status"].as_str().unwrap_or("?"))
		.collect();
	eprintln!("cycle log ({} cycles): {:?}", logs.len(), statuses);

	assert!(logs.len() >= 6, "expected >=6 cycles, got {}", logs.len());
	assert!(statuses.contains(&"success"));
	assert!(
		statuses.contains(&"failed"),
		"should have a failed cycle from sink outage"
	);

	// Phase 1 cycle should show N created
	let first_success = logs.iter().find(|l| l["status"] == "success").unwrap();
	assert_eq!(
		first_success["rows_created"].as_i64().unwrap_or(0) as usize,
		n
	);

	eprintln!(
		"=== STRESS TEST PASSED: {n} rows, {} cycles ===",
		logs.len()
	);
}
