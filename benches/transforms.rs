use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oversync_core::model::RawRow;
use oversync_transforms::StepChain;
use oversync_transforms::steps::{Filter, FilterOp, Lower, Remove, Rename, Set, Upper};

fn make_rows(n: usize) -> Vec<RawRow> {
    (0..n)
        .map(|i| RawRow {
            row_key: format!("key-{i}"),
            row_data: serde_json::json!({
                "id": i,
                "first_name": format!("User{i}"),
                "last_name": format!("Surname{i}"),
                "status": if i % 3 == 0 { "active" } else { "inactive" },
                "score": i * 7,
            }),
        })
        .collect()
}

fn make_chain() -> StepChain {
    StepChain::new(vec![
        Box::new(Upper { field: "first_name".into() }),
        Box::new(Lower { field: "last_name".into() }),
        Box::new(Rename { from: "first_name".into(), to: "name".into() }),
        Box::new(Set { field: "version".into(), value: serde_json::json!(1) }),
        Box::new(Remove { field: "last_name".into() }),
    ])
}

fn make_filter_chain() -> StepChain {
    StepChain::new(vec![
        Box::new(Filter {
            field: "status".into(),
            op: FilterOp::Eq,
            value: serde_json::json!("active"),
        }),
        Box::new(Upper { field: "first_name".into() }),
        Box::new(Lower { field: "last_name".into() }),
        Box::new(Rename { from: "first_name".into(), to: "name".into() }),
        Box::new(Set { field: "version".into(), value: serde_json::json!(2) }),
    ])
}

fn bench_transforms(c: &mut Criterion) {
    let mut group = c.benchmark_group("transforms/chain_5");

    for n in [1_000usize, 10_000] {
        let rows = make_rows(n);
        let chain = make_chain();

        group.throughput(criterion::Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &rows, |b, rows| {
            b.iter(|| chain.filter_rows(black_box(rows.clone())).unwrap());
        });
    }

    group.finish();
}

fn bench_transforms_with_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("transforms/chain_5_with_filter");

    for n in [1_000usize, 10_000] {
        let rows = make_rows(n);
        let chain = make_filter_chain();

        group.throughput(criterion::Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &rows, |b, rows| {
            b.iter(|| chain.filter_rows(black_box(rows.clone())).unwrap());
        });
    }

    group.finish();
}

criterion_group!(benches, bench_transforms, bench_transforms_with_filter);
criterion_main!(benches);
