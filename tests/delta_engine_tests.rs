mod common;

use oversync_core::model::{CycleStatus, RawRow, compute_diff, hash_row_data};
use oversync_delta::DeltaEngine;

use common::surreal::TestSurrealContainer;

#[tokio::test]
async fn next_cycle_id_starts_at_1() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());
	let id = engine.next_cycle_id("src", "q").await.unwrap();
	assert_eq!(id, 1);
}

#[tokio::test]
async fn next_cycle_id_increments_after_cycle_log() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	engine.log_cycle_start("src", "q", 1).await.unwrap();
	engine
		.log_cycle_finish("src", "q", 1, CycleStatus::Success, 10, 10, 0, 0)
		.await
		.unwrap();

	let id = engine.next_cycle_id("src", "q").await.unwrap();
	assert_eq!(id, 2);
}

#[tokio::test]
async fn read_snapshot_keys_empty_initially() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());
	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert!(keys.is_empty());
}

#[tokio::test]
async fn upsert_batch_inserts_rows() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let rows = vec![
		RawRow {
			row_key: "a".into(),
			row_data: serde_json::json!({"name": "alice"}),
		},
		RawRow {
			row_key: "b".into(),
			row_data: serde_json::json!({"name": "bob"}),
		},
	];

	let count = engine.upsert_batch("src", "q", 1, &rows).await.unwrap();
	assert_eq!(count, 2);

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 2);
	assert_eq!(keys["a"], hash_row_data(&serde_json::json!({"name": "alice"})));
}

#[tokio::test]
async fn upsert_batch_updates_existing_row() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let v1 = vec![RawRow {
		row_key: "a".into(),
		row_data: serde_json::json!({"v": 1}),
	}];
	engine.upsert_batch("src", "q", 1, &v1).await.unwrap();

	let v2 = vec![RawRow {
		row_key: "a".into(),
		row_data: serde_json::json!({"v": 2}),
	}];
	engine.upsert_batch("src", "q", 2, &v2).await.unwrap();

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 1);
	assert_eq!(keys["a"], hash_row_data(&serde_json::json!({"v": 2})));
}

#[tokio::test]
async fn delete_stale_removes_old_cycle_rows() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

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
	engine.upsert_batch("src", "q", 1, &rows).await.unwrap();

	// Only upsert "a" in cycle 2
	let rows_c2 = vec![RawRow {
		row_key: "a".into(),
		row_data: serde_json::json!({"v": 1}),
	}];
	engine.upsert_batch("src", "q", 2, &rows_c2).await.unwrap();

	// Delete stale (cycle < 2) — "b" still has cycle_id=1
	engine.delete_stale("src", "q", 2).await.unwrap();

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 1);
	assert!(keys.contains_key("a"));
}

#[tokio::test]
async fn full_cycle_first_run_all_created() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let cycle_id = engine.next_cycle_id("src", "q").await.unwrap();
	assert_eq!(cycle_id, 1);

	engine.log_cycle_start("src", "q", cycle_id).await.unwrap();

	let previous = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert!(previous.is_empty());

	let rows = vec![
		RawRow {
			row_key: "a".into(),
			row_data: serde_json::json!({"name": "alice"}),
		},
		RawRow {
			row_key: "b".into(),
			row_data: serde_json::json!({"name": "bob"}),
		},
	];

	let diff = compute_diff(&previous, &rows, "src", "q", cycle_id as u64);
	assert_eq!(diff.created.len(), 2);
	assert!(diff.updated.is_empty());
	assert!(diff.deleted.is_empty());

	engine
		.upsert_batch("src", "q", cycle_id, &rows)
		.await
		.unwrap();

	engine
		.log_cycle_finish(
			"src",
			"q",
			cycle_id,
			CycleStatus::Success,
			rows.len() as i64,
			diff.created.len() as i64,
			0,
			0,
		)
		.await
		.unwrap();
}

#[tokio::test]
async fn full_cycle_detects_update_and_delete() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: insert a, b, c, e, f (5 rows so 1 deletion = 20% < 30%)
	let rows_c1 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
		RawRow { row_key: "c".into(), row_data: serde_json::json!({"v": 3}) },
		RawRow { row_key: "e".into(), row_data: serde_json::json!({"v": 5}) },
		RawRow { row_key: "f".into(), row_data: serde_json::json!({"v": 6}) },
	];
	engine.log_cycle_start("src", "q", 1).await.unwrap();
	engine.upsert_batch("src", "q", 1, &rows_c1).await.unwrap();
	engine
		.log_cycle_finish("src", "q", 1, CycleStatus::Success, 5, 5, 0, 0)
		.await
		.unwrap();

	// Cycle 2: a unchanged, b updated, c gone, d new, e+f unchanged
	let previous = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(previous.len(), 5);

	let rows_c2 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 999}) },
		RawRow { row_key: "d".into(), row_data: serde_json::json!({"v": 4}) },
		RawRow { row_key: "e".into(), row_data: serde_json::json!({"v": 5}) },
		RawRow { row_key: "f".into(), row_data: serde_json::json!({"v": 6}) },
	];

	let diff = compute_diff(&previous, &rows_c2, "src", "q", 2);
	assert_eq!(diff.created.len(), 1);
	assert_eq!(diff.created[0].row_key, "d");
	assert_eq!(diff.updated.len(), 1);
	assert_eq!(diff.updated[0].row_key, "b");
	assert_eq!(diff.deleted.len(), 1);
	assert_eq!(diff.deleted[0].row_key, "c");

	// Fail-safe: 1 deleted / 5 previous = 20% < 30% threshold
	assert!(oversync_delta::check_fail_safe(
		previous.len(),
		diff.deleted.len(),
		30.0
	));

	engine.upsert_batch("src", "q", 2, &rows_c2).await.unwrap();
	engine.delete_stale("src", "q", 2).await.unwrap();

	let final_keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(final_keys.len(), 5);
	assert!(final_keys.contains_key("a"));
	assert!(final_keys.contains_key("b"));
	assert!(final_keys.contains_key("d"));
	assert!(final_keys.contains_key("e"));
	assert!(final_keys.contains_key("f"));
}

#[tokio::test]
async fn fail_safe_aborts_on_mass_deletion() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: insert 10 rows
	let rows_c1: Vec<RawRow> = (0..10)
		.map(|i| RawRow {
			row_key: format!("row_{i}"),
			row_data: serde_json::json!({"i": i}),
		})
		.collect();
	engine.log_cycle_start("src", "q", 1).await.unwrap();
	engine.upsert_batch("src", "q", 1, &rows_c1).await.unwrap();
	engine
		.log_cycle_finish("src", "q", 1, CycleStatus::Success, 10, 10, 0, 0)
		.await
		.unwrap();

	// Cycle 2: source returns only 2 rows (8 deleted = 80%)
	let previous = engine.read_snapshot_keys("src", "q").await.unwrap();
	let rows_c2 = vec![
		RawRow {
			row_key: "row_0".into(),
			row_data: serde_json::json!({"i": 0}),
		},
		RawRow {
			row_key: "row_1".into(),
			row_data: serde_json::json!({"i": 1}),
		},
	];

	let diff = compute_diff(&previous, &rows_c2, "src", "q", 2);
	assert_eq!(diff.deleted.len(), 8);

	// Fail-safe should REJECT (80% > 30% threshold)
	assert!(!oversync_delta::check_fail_safe(
		previous.len(),
		diff.deleted.len(),
		30.0
	));

	// Log as aborted — do NOT upsert or delete
	engine.log_cycle_start("src", "q", 2).await.unwrap();
	engine
		.log_cycle_finish("src", "q", 2, CycleStatus::Aborted, 2, 0, 0, 0)
		.await
		.unwrap();

	// Snapshot unchanged — all 10 rows still there
	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 10);
}

#[tokio::test]
async fn snapshot_isolation_between_sources() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let row = vec![RawRow {
		row_key: "x".into(),
		row_data: serde_json::json!({"v": 1}),
	}];

	engine.upsert_batch("src_a", "q1", 1, &row).await.unwrap();
	engine.upsert_batch("src_b", "q1", 1, &row).await.unwrap();

	let keys_a = engine.read_snapshot_keys("src_a", "q1").await.unwrap();
	let keys_b = engine.read_snapshot_keys("src_b", "q1").await.unwrap();
	let keys_c = engine.read_snapshot_keys("src_c", "q1").await.unwrap();

	assert_eq!(keys_a.len(), 1);
	assert_eq!(keys_b.len(), 1);
	assert!(keys_c.is_empty());
}

#[tokio::test]
async fn cycle_log_records_status() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	engine.log_cycle_start("src", "q", 1).await.unwrap();
	engine
		.log_cycle_finish("src", "q", 1, CycleStatus::Success, 5, 3, 1, 1)
		.await
		.unwrap();

	// Verify via raw query
	let mut res = t
		.client
		.query("SELECT * FROM cycle_log WHERE source_id = 'src'")
		.await
		.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0]["status"], "success");
	assert_eq!(rows[0]["rows_fetched"], 5);
	assert_eq!(rows[0]["rows_created"], 3);
	assert_eq!(rows[0]["rows_updated"], 1);
	assert_eq!(rows[0]["rows_deleted"], 1);
}

// ── compute_delta_from_db tests ────────────────────────────────

#[tokio::test]
async fn db_delta_first_cycle_all_created() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let rows = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
	];
	engine.upsert_batch("src", "q", 1, &rows).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 1).await.unwrap();
	assert_eq!(diff.created.len(), 2, "expected 2 created, got {:?}", diff);
	assert!(diff.updated.is_empty());
	assert!(diff.deleted.is_empty());
}

#[tokio::test]
async fn db_delta_detects_update() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1
	let rows_c1 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
	];
	engine.upsert_batch("src", "q", 1, &rows_c1).await.unwrap();

	// Cycle 2: 'b' updated
	let rows_c2 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 999}) },
	];
	engine.upsert_batch("src", "q", 2, &rows_c2).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();

	// Debug: check prev_hash via raw query
	let mut res = t.client.query(
		"SELECT row_key, row_hash, prev_hash, cycle_id FROM snapshot WHERE source_id = 'src'"
	).await.unwrap();
	let raw: Vec<serde_json::Value> = res.take(0).unwrap();
	eprintln!("snapshot state: {}", serde_json::to_string_pretty(&raw).unwrap());

	assert_eq!(diff.updated.len(), 1, "expected 1 updated");
	assert_eq!(diff.updated[0].row_key, "b");
}

#[tokio::test]
async fn db_delta_detects_delete() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: insert a, b
	let rows_c1 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
	];
	engine.upsert_batch("src", "q", 1, &rows_c1).await.unwrap();

	// Cycle 2: only insert a (b is gone)
	let rows_c2 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
	];
	engine.upsert_batch("src", "q", 2, &rows_c2).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();
	assert_eq!(diff.deleted.len(), 1, "expected 1 deleted, got {:?}", diff);
	assert_eq!(diff.deleted[0].row_key, "b");
	assert!(diff.created.is_empty());
}

#[tokio::test]
async fn db_delta_mixed_operations() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: insert a, b, c, d, e
	let rows_c1 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
		RawRow { row_key: "c".into(), row_data: serde_json::json!({"v": 3}) },
		RawRow { row_key: "d".into(), row_data: serde_json::json!({"v": 4}) },
		RawRow { row_key: "e".into(), row_data: serde_json::json!({"v": 5}) },
	];
	engine.upsert_batch("src", "q", 1, &rows_c1).await.unwrap();

	// Cycle 2: a unchanged, b updated, c gone, d unchanged, f new
	let rows_c2 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 999}) },
		RawRow { row_key: "d".into(), row_data: serde_json::json!({"v": 4}) },
		RawRow { row_key: "e".into(), row_data: serde_json::json!({"v": 5}) },
		RawRow { row_key: "f".into(), row_data: serde_json::json!({"v": 6}) },
	];
	engine.upsert_batch("src", "q", 2, &rows_c2).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();
	assert_eq!(diff.created.len(), 1, "expected 1 created: {:?}", diff.created);
	assert_eq!(diff.created[0].row_key, "f");
	assert_eq!(diff.updated.len(), 1, "expected 1 updated: {:?}", diff.updated);
	assert_eq!(diff.updated[0].row_key, "b");
	assert_eq!(diff.deleted.len(), 1, "expected 1 deleted: {:?}", diff.deleted);
	assert_eq!(diff.deleted[0].row_key, "c");
}

#[tokio::test]
async fn db_delta_no_changes() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: insert a, b
	let rows_c1 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
	];
	engine.upsert_batch("src", "q", 1, &rows_c1).await.unwrap();

	// Cycle 2: same a, b
	let rows_c2 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
	];
	engine.upsert_batch("src", "q", 2, &rows_c2).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();
	assert!(diff.is_empty(), "expected empty diff, got {:?}", diff);
}

#[tokio::test]
async fn db_delta_empty_first_cycle() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	engine.upsert_batch("src", "q", 1, &[]).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 1).await.unwrap();
	assert!(diff.is_empty(), "expected empty diff, got {:?}", diff);
}

// ── Batch upsert edge-case tests ──────────────────────────────

#[tokio::test]
async fn upsert_batch_empty() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let count = engine.upsert_batch("src", "q", 1, &[]).await.unwrap();
	assert_eq!(count, 0);
}

#[tokio::test]
async fn upsert_batch_single_row() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let rows = vec![
		RawRow { row_key: "only".into(), row_data: serde_json::json!({"v": 42}) },
	];
	let count = engine.upsert_batch("src", "q", 1, &rows).await.unwrap();
	assert_eq!(count, 1);

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 1);
	assert!(keys.contains_key("only"));
}

#[tokio::test]
async fn upsert_batch_exactly_batch_size() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let rows: Vec<RawRow> = (0..500)
		.map(|i| RawRow {
			row_key: format!("row_{i:04}"),
			row_data: serde_json::json!({"i": i}),
		})
		.collect();

	let count = engine.upsert_batch("src", "q", 1, &rows).await.unwrap();
	assert_eq!(count, 500);

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 500);
}

#[tokio::test]
async fn upsert_batch_one_over_batch_size() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let rows: Vec<RawRow> = (0..501)
		.map(|i| RawRow {
			row_key: format!("row_{i:04}"),
			row_data: serde_json::json!({"i": i}),
		})
		.collect();

	let count = engine.upsert_batch("src", "q", 1, &rows).await.unwrap();
	assert_eq!(count, 501);

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 501);
}

// ── Batch upsert tests ────────────────────────────────────────

#[tokio::test]
async fn batch_upsert_large_dataset() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// 1200 rows — will be split into 3 chunks (500+500+200)
	let rows: Vec<RawRow> = (0..1200)
		.map(|i| RawRow {
			row_key: format!("row_{i:04}"),
			row_data: serde_json::json!({"i": i, "name": format!("item_{i}")}),
		})
		.collect();

	let count = engine.upsert_batch("src", "q", 1, &rows).await.unwrap();
	assert_eq!(count, 1200);

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 1200);
}

#[tokio::test]
async fn batch_upsert_updates_existing_rows() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let rows_v1: Vec<RawRow> = (0..10)
		.map(|i| RawRow {
			row_key: format!("k{i}"),
			row_data: serde_json::json!({"v": 1}),
		})
		.collect();
	engine.upsert_batch("src", "q", 1, &rows_v1).await.unwrap();

	// Update half of them
	let rows_v2: Vec<RawRow> = (0..10)
		.map(|i| RawRow {
			row_key: format!("k{i}"),
			row_data: if i < 5 {
				serde_json::json!({"v": 2})
			} else {
				serde_json::json!({"v": 1})
			},
		})
		.collect();
	engine.upsert_batch("src", "q", 2, &rows_v2).await.unwrap();

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 10);
	// First 5 should have new hash
	assert_eq!(keys["k0"], hash_row_data(&serde_json::json!({"v": 2})));
	// Last 5 unchanged
	assert_eq!(keys["k5"], hash_row_data(&serde_json::json!({"v": 1})));
}

// ── commit_cycle (atomic upsert+delete) tests ──────────────────

#[tokio::test]
async fn commit_cycle_inserts_and_cleans_stale() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: insert 5 rows
	let rows_c1: Vec<RawRow> = (0..5)
		.map(|i| RawRow {
			row_key: format!("r{i}"),
			row_data: serde_json::json!({"v": i}),
		})
		.collect();
	engine.commit_cycle("src", "q", 1, &rows_c1).await.unwrap();
	assert_eq!(engine.read_snapshot_keys("src", "q").await.unwrap().len(), 5);

	// Cycle 2: only 3 rows (r0, r1, r2). r3, r4 should be deleted.
	let rows_c2: Vec<RawRow> = (0..3)
		.map(|i| RawRow {
			row_key: format!("r{i}"),
			row_data: serde_json::json!({"v": i}),
		})
		.collect();
	engine.commit_cycle("src", "q", 2, &rows_c2).await.unwrap();

	let keys = engine.read_snapshot_keys("src", "q").await.unwrap();
	assert_eq!(keys.len(), 3);
	assert!(keys.contains_key("r0"));
	assert!(keys.contains_key("r1"));
	assert!(keys.contains_key("r2"));
	assert!(!keys.contains_key("r3"));
	assert!(!keys.contains_key("r4"));
}

#[tokio::test]
async fn commit_cycle_large_dataset_chunked() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// >500 rows triggers chunked path
	let rows: Vec<RawRow> = (0..700)
		.map(|i| RawRow {
			row_key: format!("r{i:04}"),
			row_data: serde_json::json!({"i": i}),
		})
		.collect();
	engine.commit_cycle("src", "q", 1, &rows).await.unwrap();
	assert_eq!(engine.read_snapshot_keys("src", "q").await.unwrap().len(), 700);

	// Cycle 2: shrink to 600
	let rows_c2: Vec<RawRow> = (0..600)
		.map(|i| RawRow {
			row_key: format!("r{i:04}"),
			row_data: serde_json::json!({"i": i}),
		})
		.collect();
	engine.commit_cycle("src", "q", 2, &rows_c2).await.unwrap();
	assert_eq!(engine.read_snapshot_keys("src", "q").await.unwrap().len(), 600);
}

// ── upsert_batch_raw + prep_prev_hash tests ─────────────────

#[tokio::test]
async fn upsert_batch_raw_without_prep_does_not_set_prev_hash() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1
	let rows = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
	];
	engine.upsert_batch_raw("src", "q", 1, &rows).await.unwrap();

	// Cycle 2 with raw (no prep) — prev_hash stays NONE
	let rows2 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 2}) },
	];
	engine.upsert_batch_raw("src", "q", 2, &rows2).await.unwrap();

	// compute_delta sees "a" as CREATED (prev_hash is NONE), not UPDATED
	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();
	assert_eq!(diff.created.len(), 1);
	assert!(diff.updated.is_empty());
}

#[tokio::test]
async fn prep_then_raw_correctly_tracks_updates() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1
	let rows = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
	];
	engine.upsert_batch("src", "q", 1, &rows).await.unwrap();

	// Cycle 2: prep first, then raw upsert (simulating streaming path)
	engine.prep_prev_hash("src", "q").await.unwrap();

	let rows2 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 999}) },
		RawRow { row_key: "c".into(), row_data: serde_json::json!({"v": 3}) },
	];
	engine.upsert_batch_raw("src", "q", 2, &rows2).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();
	assert_eq!(diff.created.len(), 1, "c is new");
	assert_eq!(diff.created[0].row_key, "c");
	assert_eq!(diff.updated.len(), 1, "b changed");
	assert_eq!(diff.updated[0].row_key, "b");
	assert!(diff.deleted.is_empty(), "nothing deleted");
}

#[tokio::test]
async fn prep_prev_hash_idempotent() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	let rows = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
	];
	engine.upsert_batch("src", "q", 1, &rows).await.unwrap();

	// Call prep twice — should be safe
	engine.prep_prev_hash("src", "q").await.unwrap();
	engine.prep_prev_hash("src", "q").await.unwrap();

	// Upsert with same data
	engine.upsert_batch_raw("src", "q", 2, &rows).await.unwrap();

	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();
	assert!(diff.is_empty(), "no changes");
}

#[tokio::test]
async fn streaming_multi_batch_upsert_with_delta() {
	let t = TestSurrealContainer::new().await;
	let engine = DeltaEngine::single(t.client.clone());

	// Cycle 1: 3 rows
	let c1 = vec![
		RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 2}) },
		RawRow { row_key: "c".into(), row_data: serde_json::json!({"v": 3}) },
	];
	engine.upsert_batch("src", "q", 1, &c1).await.unwrap();

	// Cycle 2: simulate streaming — prep once, then multiple raw batches
	engine.prep_prev_hash("src", "q").await.unwrap();

	// Batch 1: a unchanged
	engine
		.upsert_batch_raw("src", "q", 2, &[
			RawRow { row_key: "a".into(), row_data: serde_json::json!({"v": 1}) },
		])
		.await
		.unwrap();

	// Batch 2: b updated, d new
	engine
		.upsert_batch_raw("src", "q", 2, &[
			RawRow { row_key: "b".into(), row_data: serde_json::json!({"v": 999}) },
			RawRow { row_key: "d".into(), row_data: serde_json::json!({"v": 4}) },
		])
		.await
		.unwrap();

	// c not seen → deleted
	let diff = engine.compute_delta_from_db("src", "q", 2).await.unwrap();
	assert_eq!(diff.created.len(), 1);
	assert_eq!(diff.created[0].row_key, "d");
	assert_eq!(diff.updated.len(), 1);
	assert_eq!(diff.updated[0].row_key, "b");
	assert_eq!(diff.deleted.len(), 1);
	assert_eq!(diff.deleted[0].row_key, "c");
}
