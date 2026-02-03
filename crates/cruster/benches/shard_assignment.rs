//! Benchmarks for shard assignment strategies.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use cruster::runner::Runner;
use cruster::shard_assigner::{ShardAssigner, ShardAssignmentStrategy};
use cruster::types::RunnerAddress;

fn bench_compute_assignments(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_assignment");
    let rendezvous_strategy = ShardAssignmentStrategy::Rendezvous;

    for num_runners in [3, 10, 100, 1000] {
        let runners: Vec<Runner> = (0..num_runners)
            .map(|i| Runner::new(RunnerAddress::new(format!("host{i}"), 9000), 1))
            .collect();
        let shard_groups = vec!["default".to_string()];

        // Benchmark rendezvous hashing
        group.bench_with_input(
            BenchmarkId::new("rendezvous", num_runners),
            &num_runners,
            |b, _| {
                b.iter(|| {
                    ShardAssigner::compute_assignments(
                        &runners,
                        &shard_groups,
                        2048,
                        &rendezvous_strategy,
                    )
                })
            },
        );

        // Benchmark parallel rendezvous hashing (when feature is enabled)
        #[cfg(feature = "parallel")]
        {
            let parallel_strategy = ShardAssignmentStrategy::RendezvousParallel;
            group.bench_with_input(
                BenchmarkId::new("rendezvous_parallel", num_runners),
                &num_runners,
                |b, _| {
                    b.iter(|| {
                        ShardAssigner::compute_assignments(
                            &runners,
                            &shard_groups,
                            2048,
                            &parallel_strategy,
                        )
                    })
                },
            );
        }

        // Benchmark consistent hashing (when feature is enabled)
        #[cfg(feature = "consistent-hash")]
        {
            let consistent_hash_strategy = ShardAssignmentStrategy::ConsistentHash {
                vnodes_per_weight: 150,
            };
            group.bench_with_input(
                BenchmarkId::new("consistent_hash", num_runners),
                &num_runners,
                |b, _| {
                    b.iter(|| {
                        ShardAssigner::compute_assignments(
                            &runners,
                            &shard_groups,
                            2048,
                            &consistent_hash_strategy,
                        )
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_compute_assignments);
criterion_main!(benches);
