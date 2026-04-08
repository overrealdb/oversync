use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oversync_core::model::{RawRow, compute_diff, hash_row_data, hash_rows};

fn make_rows(n: usize) -> Vec<RawRow> {
	(0..n)
		.map(|i| RawRow {
			row_key: format!("key-{i}"),
			row_data: serde_json::json!({
				"id": i,
				"name": format!("name-{i}"),
				"value": i * 17,
				"active": i % 2 == 0,
			}),
		})
		.collect()
}

fn make_realistic_rows(n: usize) -> Vec<RawRow> {
	(0..n)
		.map(|i| RawRow {
			row_key: format!("row-{i:06}"),
			row_data: serde_json::json!({
				"id": i,
				"first_name": format!("User{i}"),
				"last_name": format!("Surname{i}"),
				"email": format!("user{i}@example.com"),
				"phone": format!("+1-555-{:04}", i % 10000),
				"status": if i % 3 == 0 { "active" } else if i % 3 == 1 { "inactive" } else { "pending" },
				"score": i * 7,
				"balance": (i as f64) * 99.95,
				"created_at": "2025-01-15T10:30:00Z",
				"updated_at": "2026-03-20T14:22:33Z",
				"is_verified": i % 5 != 0,
				"tags": ["customer", format!("tier-{}", i % 4), format!("region-{}", i % 8)],
				"metadata": {
					"source": "postgres",
					"version": 3,
					"flags": [i % 2 == 0, i % 3 == 0],
				},
				"address": {
					"city": format!("City{}", i % 100),
					"zip": format!("{:05}", i % 99999),
					"country": "US",
				},
			}),
		})
		.collect()
}

fn memory_diff(rows: &[RawRow], previous: &HashMap<String, String>) -> (usize, usize, usize) {
	let current: HashMap<String, String> = rows
		.iter()
		.map(|r| (r.row_key.clone(), hash_row_data(&r.row_data)))
		.collect();

	let mut created = 0usize;
	let mut updated = 0usize;
	let mut deleted = 0usize;

	for (key, hash) in &current {
		match previous.get(key) {
			None => created += 1,
			Some(prev_hash) if prev_hash != hash => updated += 1,
			_ => {}
		}
	}
	for key in previous.keys() {
		if !current.contains_key(key) {
			deleted += 1;
		}
	}

	(created, updated, deleted)
}

fn bench_delta(c: &mut Criterion) {
	let mut group = c.benchmark_group("delta/memory_diff");

	for n in [1_000usize, 10_000, 100_000] {
		let rows = make_rows(n);
		// Snapshot: first half already present (unchanged), second half absent (→ created)
		let previous: HashMap<String, String> = rows[..n / 2]
			.iter()
			.map(|r| (r.row_key.clone(), hash_row_data(&r.row_data)))
			.collect();

		group.throughput(criterion::Throughput::Elements(n as u64));
		group.bench_with_input(
			BenchmarkId::from_parameter(n),
			&(&rows, &previous),
			|b, (rows, prev)| {
				b.iter(|| memory_diff(black_box(rows), black_box(prev)));
			},
		);
	}

	group.finish();
}

fn bench_hash(c: &mut Criterion) {
	let mut group = c.benchmark_group("delta/hash_row");

	for n in [1_000usize, 10_000, 100_000] {
		let rows = make_rows(n);

		group.throughput(criterion::Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::from_parameter(n), &rows, |b, rows| {
			b.iter(|| {
				for row in rows {
					black_box(hash_row_data(&row.row_data));
				}
			});
		});
	}

	group.finish();
}

fn bench_hash_realistic(c: &mut Criterion) {
	let mut group = c.benchmark_group("delta/hash_row_realistic");

	for n in [1_000usize, 10_000, 100_000] {
		let rows = make_realistic_rows(n);

		group.throughput(criterion::Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::from_parameter(n), &rows, |b, rows| {
			b.iter(|| {
				for row in rows {
					black_box(hash_row_data(&row.row_data));
				}
			});
		});
	}

	group.finish();
}

fn bench_hash_single(c: &mut Criterion) {
	let mut group = c.benchmark_group("delta/hash_single");

	let small = serde_json::json!({"id": 1, "name": "alice", "active": true});
	let medium = make_realistic_rows(1)[0].row_data.clone();
	let large = serde_json::json!({
		"id": 1,
		"data": (0..50).map(|i| (format!("field_{i}"), serde_json::json!({
			"value": i * 42,
			"label": format!("item-{i}"),
			"nested": {"a": i, "b": format!("x{i}")},
		}))).collect::<serde_json::Map<String, serde_json::Value>>(),
	});

	group.bench_function("small_3f", |b| {
		b.iter(|| black_box(hash_row_data(black_box(&small))));
	});
	group.bench_function("medium_15f", |b| {
		b.iter(|| black_box(hash_row_data(black_box(&medium))));
	});
	group.bench_function("large_50f", |b| {
		b.iter(|| black_box(hash_row_data(black_box(&large))));
	});

	group.finish();
}

fn bench_compute_diff(c: &mut Criterion) {
	let mut group = c.benchmark_group("delta/compute_diff");

	for n in [1_000usize, 10_000, 100_000] {
		let rows = make_realistic_rows(n);

		// Scenario 1: all created (empty previous)
		let empty_prev = HashMap::new();
		group.throughput(criterion::Throughput::Elements(n as u64));
		group.bench_with_input(
			BenchmarkId::new("all_created", n),
			&(&rows, &empty_prev),
			|b, (rows, prev)| {
				b.iter(|| compute_diff(black_box(prev), black_box(rows), "src", "q1", 1));
			},
		);

		// Scenario 2: no changes (steady state — previous matches current exactly)
		let full_prev: HashMap<String, String> = rows
			.iter()
			.map(|r| (r.row_key.clone(), hash_row_data(&r.row_data)))
			.collect();
		group.bench_with_input(
			BenchmarkId::new("no_change", n),
			&(&rows, &full_prev),
			|b, (rows, prev)| {
				b.iter(|| compute_diff(black_box(prev), black_box(rows), "src", "q1", 2));
			},
		);

		// Scenario 3: mixed — 50% unchanged, 25% updated (different data), 25% created + some deleted
		let mut mixed_rows = Vec::with_capacity(n);
		let mut mixed_prev: HashMap<String, String> = HashMap::with_capacity(n);

		for (i, row) in rows.iter().enumerate() {
			if i < n / 2 {
				// Unchanged
				mixed_rows.push(row.clone());
				mixed_prev.insert(row.row_key.clone(), hash_row_data(&row.row_data));
			} else if i < 3 * n / 4 {
				// Updated (same key, different data)
				let mut updated = row.clone();
				updated.row_data["score"] = serde_json::json!(9999);
				mixed_rows.push(updated);
				mixed_prev.insert(row.row_key.clone(), hash_row_data(&row.row_data));
			} else {
				// Created (new key, old key becomes deleted)
				mixed_rows.push(RawRow {
					row_key: format!("new-{i}"),
					row_data: row.row_data.clone(),
				});
				mixed_prev.insert(row.row_key.clone(), hash_row_data(&row.row_data));
			}
		}

		group.bench_with_input(
			BenchmarkId::new("mixed", n),
			&(&mixed_rows, &mixed_prev),
			|b, (rows, prev)| {
				b.iter(|| compute_diff(black_box(prev), black_box(rows), "src", "q1", 3));
			},
		);
	}

	group.finish();
}

fn bench_hash_rows(c: &mut Criterion) {
	let mut group = c.benchmark_group("delta/hash_rows");

	for n in [1_000usize, 10_000, 100_000] {
		let rows = make_realistic_rows(n);

		group.throughput(criterion::Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::from_parameter(n), &rows, |b, rows| {
			b.iter(|| black_box(hash_rows(black_box(rows))));
		});
	}

	group.finish();
}

criterion_group!(
	benches,
	bench_delta,
	bench_hash,
	bench_hash_realistic,
	bench_hash_single,
	bench_compute_diff,
	bench_hash_rows
);
criterion_main!(benches);
