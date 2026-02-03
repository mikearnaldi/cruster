//! Benchmarks for shard assignment using rendezvous hashing.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use cruster::runner::Runner;
use cruster::shard_assigner::{ShardAssigner, ShardAssignmentStrategy};
use cruster::types::RunnerAddress;

fn bench_compute_assignments(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_assignment");
    let strategy = ShardAssignmentStrategy::default();

    for num_runners in [3, 10, 100, 1000] {
        let runners: Vec<Runner> = (0..num_runners)
            .map(|i| Runner::new(RunnerAddress::new(format!("host{i}"), 9000), 1))
            .collect();
        let shard_groups = vec!["default".to_string()];

        group.bench_with_input(
            BenchmarkId::new("rendezvous", num_runners),
            &num_runners,
            |b, _| {
                b.iter(|| {
                    ShardAssigner::compute_assignments(&runners, &shard_groups, 2048, &strategy)
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_compute_assignments);
criterion_main!(benches);
