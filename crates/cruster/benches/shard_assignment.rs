//! Benchmarks for shard assignment strategies.
//!
//! Compares performance of different strategies for assigning 2048 shards across 100 nodes.

use criterion::{criterion_group, criterion_main, Criterion};
use cruster::runner::Runner;
use cruster::shard_assigner::{ShardAssigner, ShardAssignmentStrategy};
use cruster::types::RunnerAddress;

fn bench_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_assignment_100_nodes");

    // Configure for faster benchmarks
    group.sample_size(50);

    // Setup: 100 nodes, 2048 shards
    let runners: Vec<Runner> = (0..100)
        .map(|i| Runner::new(RunnerAddress::new(format!("host{i}"), 9000), 1))
        .collect();
    let shard_groups = vec!["default".to_string()];

    // 1. Rendezvous (sequential)
    group.bench_function("rendezvous", |b| {
        let strategy = ShardAssignmentStrategy::Rendezvous;
        b.iter(|| ShardAssigner::compute_assignments(&runners, &shard_groups, 2048, &strategy))
    });

    // 2. Rendezvous Parallel
    #[cfg(feature = "parallel")]
    group.bench_function("rendezvous_parallel", |b| {
        let strategy = ShardAssignmentStrategy::RendezvousParallel;
        b.iter(|| ShardAssigner::compute_assignments(&runners, &shard_groups, 2048, &strategy))
    });

    // 3. Consistent Hash
    #[cfg(feature = "consistent-hash")]
    group.bench_function("consistent_hash", |b| {
        let strategy = ShardAssignmentStrategy::ConsistentHash {
            vnodes_per_weight: 150,
        };
        b.iter(|| ShardAssigner::compute_assignments(&runners, &shard_groups, 2048, &strategy))
    });

    group.finish();
}

criterion_group!(benches, bench_strategies);
criterion_main!(benches);
