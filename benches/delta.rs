use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oversync_core::model::{RawRow, hash_row_data};

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

/// Simulate the in-memory diff: hash each row, then compare against a previous snapshot.
/// Half the rows are new (created), half are unchanged. Mirrors cycle.rs run_memory_diff.
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
        group.bench_with_input(BenchmarkId::from_parameter(n), &(&rows, &previous), |b, (rows, prev)| {
            b.iter(|| memory_diff(black_box(rows), black_box(prev)));
        });
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

criterion_group!(benches, bench_delta, bench_hash);
criterion_main!(benches);
