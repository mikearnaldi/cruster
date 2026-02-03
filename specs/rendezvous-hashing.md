# Rendezvous Hashing Implementation Plan

## Problem Statement

The current consistent hashing implementation using the `hashring` crate with 100 virtual nodes per runner produces uneven shard distribution:

- 2048 shards across 3 nodes
- Expected: ~682-683 each
- Actual: 655, 662, 731 (variance of -4%, -3%, +7%)

This variance is inherent to ring-based consistent hashing with a limited number of virtual nodes.

## Proposed Solution

Replace the ring-based consistent hashing with **Rendezvous Hashing** (also known as Highest Random Weight / HRW hashing).

### Algorithm

For each shard, compute a hash combining the shard key with each candidate runner. Assign the shard to the runner with the highest hash value:

```
assign(shard) = argmax over runners r: hash(shard, r)
```

### Properties

| Property | Guarantee |
|----------|-----------|
| Distribution | Near-perfect (each node gets exactly 1/n shards, ±1) |
| Node departure | Only shards on departed node move (~1/n) |
| Node addition | New node claims ~1/(n+1) shards evenly from all nodes |
| Determinism | Same inputs always produce same assignments |
| Weighted nodes | Supported via multiple hash computations per weight unit |

## Implementation Plan

### Phase 1: Core Algorithm [COMPLETE]

**File**: `crates/cruster/src/shard_assigner.rs`

1. ~~Remove the `hashring` crate dependency~~
2. ~~Replace `RunnerNode` struct with a simpler weight expansion approach~~
3. ~~Implement rendezvous assignment~~

**Implementation note**: Used a two-phase hash with MurmurHash3-style bit mixing for better distribution. The shard and runner keys are hashed separately with djb2, then combined with XOR and mixed using the MurmurHash3 finalizer for excellent avalanche properties.

Original pseudocode:

```rust
use crate::hash::djb2_hash64_with_seed;

impl ShardAssigner {
    pub fn compute_assignments(
        runners: &[Runner],
        shard_groups: &[String],
        shards_per_group: i32,
    ) -> HashMap<ShardId, RunnerAddress> {
        let mut assignments = HashMap::new();

        let healthy_runners: Vec<&Runner> = runners
            .iter()
            .filter(|r| r.healthy && r.weight > 0)
            .collect();

        if healthy_runners.is_empty() {
            return assignments;
        }

        for group in shard_groups {
            for id in 0..shards_per_group {
                let shard_key = format!("{group}:{id}");
                
                if let Some(runner) = select_runner_rendezvous(&shard_key, &healthy_runners) {
                    assignments.insert(ShardId::new(group, id), runner.address.clone());
                }
            }
        }

        assignments
    }
}

/// Select the runner with the highest hash score for the given shard key.
/// Weights are handled by computing multiple hashes per runner (one per weight unit)
/// and using the maximum.
fn select_runner_rendezvous<'a>(
    shard_key: &str,
    runners: &[&'a Runner],
) -> Option<&'a Runner> {
    runners
        .iter()
        .max_by_key(|runner| compute_runner_score(shard_key, runner))
        .copied()
}

/// Compute the rendezvous score for a runner.
/// For weighted runners, we compute `weight` hashes and take the maximum,
/// which statistically gives weighted runners proportionally more wins.
fn compute_runner_score(shard_key: &str, runner: &Runner) -> u64 {
    let runner_key = format!("{}:{}", runner.address.host, runner.address.port);
    
    (0..runner.weight)
        .map(|w| {
            let combined = format!("{shard_key}:{runner_key}:{w}");
            djb2_hash64(combined.as_bytes())
        })
        .max()
        .unwrap_or(0)
}
```

### Phase 2: Hash Function [COMPLETE]

**File**: `crates/cruster/src/hash.rs`

The existing `djb2_hash64` function is used but combined with a MurmurHash3-style bit mixing function (`mix64`) in `shard_assigner.rs` to achieve better avalanche properties for rendezvous hashing. Original djb2 alone had weak distribution when runner keys differed only slightly.

Original spec:

```rust
/// DJB2 hash (64-bit variant) - used for rendezvous hashing
pub fn djb2_hash64(data: &[u8]) -> u64 {
    let mut hash: u64 = 5381;
    for &byte in data {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    hash
}
```

Consider whether to use a stronger hash (xxhash, seahash) if distribution testing reveals issues with djb2 for this use case.

### Phase 3: Testing [COMPLETE]

**File**: `crates/cruster/src/shard_assigner.rs` (test module)

All tests pass with the rendezvous implementation. Added tests:

1. **Distribution uniformity test** - verify near-perfect distribution:

```rust
#[test]
fn rendezvous_distribution_uniformity() {
    let runners = vec![
        Runner::new(RunnerAddress::new("host1", 9000), 1),
        Runner::new(RunnerAddress::new("host2", 9000), 1),
        Runner::new(RunnerAddress::new("host3", 9000), 1),
    ];
    let groups = vec!["default".to_string()];
    let assignments = ShardAssigner::compute_assignments(&runners, &groups, 2048);

    let count = |host: &str| assignments.values().filter(|a| a.host == host).count();
    let h1 = count("host1");
    let h2 = count("host2");
    let h3 = count("host3");

    // With rendezvous, expect very tight distribution
    // 2048 / 3 = 682.67, so expect 682 or 683 each
    let expected = 2048 / 3; // 682
    let tolerance = 5; // Much tighter than the 20% we had before
    
    assert!(h1.abs_diff(expected) <= tolerance, "host1: {h1}");
    assert!(h2.abs_diff(expected) <= tolerance, "host2: {h2}");
    assert!(h3.abs_diff(expected) <= tolerance, "host3: {h3}");
}
```

2. **Minimal movement on node removal**:

```rust
#[test]
fn minimal_movement_on_node_removal() {
    let runners_3 = vec![
        Runner::new(RunnerAddress::new("host1", 9000), 1),
        Runner::new(RunnerAddress::new("host2", 9000), 1),
        Runner::new(RunnerAddress::new("host3", 9000), 1),
    ];
    let runners_2 = vec![
        Runner::new(RunnerAddress::new("host1", 9000), 1),
        Runner::new(RunnerAddress::new("host2", 9000), 1),
        // host3 removed
    ];
    
    let groups = vec!["default".to_string()];
    let before = ShardAssigner::compute_assignments(&runners_3, &groups, 2048);
    let after = ShardAssigner::compute_assignments(&runners_2, &groups, 2048);

    let moved: usize = before
        .iter()
        .filter(|(shard, addr)| after.get(*shard) != Some(*addr))
        .count();

    // Only shards from host3 should move (~1/3 of total)
    let host3_shards = before.values().filter(|a| a.host == "host3").count();
    assert_eq!(moved, host3_shards, "only host3 shards should move");
    
    // Verify it's roughly 1/3
    assert!(moved > 600 && moved < 750, "expected ~683 moves, got {moved}");
}
```

3. **Minimal movement on node addition**:

```rust
#[test]
fn minimal_movement_on_node_addition() {
    let runners_3 = vec![
        Runner::new(RunnerAddress::new("host1", 9000), 1),
        Runner::new(RunnerAddress::new("host2", 9000), 1),
        Runner::new(RunnerAddress::new("host3", 9000), 1),
    ];
    let runners_4 = vec![
        Runner::new(RunnerAddress::new("host1", 9000), 1),
        Runner::new(RunnerAddress::new("host2", 9000), 1),
        Runner::new(RunnerAddress::new("host3", 9000), 1),
        Runner::new(RunnerAddress::new("host4", 9000), 1), // new
    ];
    
    let groups = vec!["default".to_string()];
    let before = ShardAssigner::compute_assignments(&runners_3, &groups, 2048);
    let after = ShardAssigner::compute_assignments(&runners_4, &groups, 2048);

    let moved: usize = before
        .iter()
        .filter(|(shard, addr)| after.get(*shard) != Some(*addr))
        .count();

    // New node should claim ~1/4 of shards
    let host4_shards = after.values().filter(|a| a.host == "host4").count();
    assert_eq!(moved, host4_shards, "moves should equal host4's new shards");
    
    // Verify it's roughly 1/4
    assert!(moved > 450 && moved < 560, "expected ~512 moves, got {moved}");
}
```

4. **Weighted distribution**:

```rust
#[test]
fn weighted_distribution() {
    let runners = vec![
        Runner::new(RunnerAddress::new("host1", 9000), 3), // 3x weight
        Runner::new(RunnerAddress::new("host2", 9000), 1),
    ];
    let groups = vec!["default".to_string()];
    let assignments = ShardAssigner::compute_assignments(&runners, &groups, 2048);

    let h1 = assignments.values().filter(|a| a.host == "host1").count();
    let h2 = assignments.values().filter(|a| a.host == "host2").count();

    // host1 (weight 3) should get ~75%, host2 (weight 1) should get ~25%
    // 2048 * 0.75 = 1536, 2048 * 0.25 = 512
    assert!(h1 > 1450 && h1 < 1620, "host1 (w=3): expected ~1536, got {h1}");
    assert!(h2 > 430 && h2 < 600, "host2 (w=1): expected ~512, got {h2}");
}
```

### Phase 4: Benchmarking [COMPLETE]

**File**: `crates/cruster/benches/shard_assignment.rs`

Benchmark added using criterion. Results with 2048 shards:

| Runners | Time |
|---------|------|
| 3 | 0.63ms |
| 10 | 1.38ms |
| 100 | 11.1ms |
| 1000 | 107ms |

Performance is O(shards * runners) which is expected for rendezvous hashing. For typical production clusters (< 100 nodes), performance is excellent. Large clusters (1000+ nodes) may benefit from the optimizations described in Future Considerations.

Run with: `cargo bench --bench shard_assignment`

### Phase 5: Cleanup [COMPLETE]

1. ~~Remove `hashring` from `Cargo.toml` dependencies~~
2. ~~Remove unused `RunnerNode` struct~~
3. Documentation updated: ShardAssigner docstring now describes rendezvous hashing properties

### Phase 6: Configurable Hashing Strategy

Make the shard assignment strategy configurable so users can choose between different algorithms based on their needs.

#### Strategy Options

| Strategy | Distribution | Rebalance Cost | Complexity | Best For |
|----------|--------------|----------------|------------|----------|
| `Rendezvous` | Near-perfect | Optimal (1/n) | O(shards × nodes) | Small-medium clusters (< 1000 nodes) |
| `ConsistentHash` | Good (with vnodes) | Optimal (1/n) | O(shards × log(vnodes)) | Large clusters, legacy compatibility |

#### API Design

```rust
/// Strategy for assigning shards to runners.
#[derive(Debug, Clone, Default)]
pub enum ShardAssignmentStrategy {
    /// Rendezvous hashing (HRW) - best distribution, O(n) per shard.
    /// Recommended for clusters with < 1000 nodes.
    #[default]
    Rendezvous,
    
    /// Consistent hashing with virtual nodes.
    /// Better performance for very large clusters, slightly less uniform distribution.
    ConsistentHash {
        /// Virtual nodes per weight unit. Higher = better distribution, more memory.
        /// Default: 150
        vnodes_per_weight: u32,
    },
}

impl ShardAssigner {
    pub fn compute_assignments(
        runners: &[Runner],
        shard_groups: &[String],
        shards_per_group: i32,
        strategy: &ShardAssignmentStrategy,
    ) -> HashMap<ShardId, RunnerAddress> {
        match strategy {
            ShardAssignmentStrategy::Rendezvous => {
                Self::compute_rendezvous(runners, shard_groups, shards_per_group)
            }
            ShardAssignmentStrategy::ConsistentHash { vnodes_per_weight } => {
                Self::compute_consistent_hash(runners, shard_groups, shards_per_group, *vnodes_per_weight)
            }
        }
    }
}
```

#### Configuration

The strategy should be configurable at the `Sharding` level:

```rust
// In ShardingConfig or similar
pub struct ShardingConfig {
    pub shards_per_group: i32,
    pub assignment_strategy: ShardAssignmentStrategy,
    // ... other fields
}
```

#### Implementation Tasks

- [x] Define `ShardAssignmentStrategy` enum
- [x] Refactor `compute_assignments` to accept strategy parameter
- [x] Extract current rendezvous logic into `compute_rendezvous` method
- [x] Re-add consistent hash implementation as `compute_consistent_hash` method (bring back `hashring` as optional)
- [x] Add strategy to `ShardingConfig` / cluster configuration
- [x] Update `ShardingImpl` to use configured strategy
- [x] Add tests for strategy selection
- [x] Update benchmarks to compare strategies

#### Migration Notes

- Default strategy is `Rendezvous` (current behavior)
- Existing deployments continue unchanged
- Users can opt into `ConsistentHash` if they have very large clusters or need compatibility

### Phase 7: Parallel Rendezvous Hashing

For large shard counts, parallelize the rendezvous computation using Rayon to leverage multiple CPU cores.

#### Motivation

The rendezvous algorithm is embarrassingly parallel - each shard's assignment is independent. With 2048+ shards across 100+ nodes, parallelization can provide significant speedups.

#### API Design

```rust
/// Strategy for assigning shards to runners.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ShardAssignmentStrategy {
    /// Rendezvous hashing (HRW) - best distribution, O(shards × nodes) complexity.
    /// Single-threaded computation.
    #[default]
    Rendezvous,

    /// Parallel rendezvous hashing using Rayon.
    /// Same algorithm as Rendezvous but parallelized across shards.
    /// Recommended for large shard counts (1000+) with many nodes (100+).
    #[cfg(feature = "parallel")]
    RendezvousParallel,

    /// Consistent hashing with virtual nodes.
    #[cfg(feature = "consistent-hash")]
    ConsistentHash { vnodes_per_weight: u32 },
}
```

#### Implementation

```rust
use rayon::prelude::*;

fn compute_rendezvous_parallel(
    runners: &[Runner],
    shard_groups: &[String],
    shards_per_group: i32,
) -> HashMap<ShardId, RunnerAddress> {
    let healthy_runners: Vec<&Runner> = runners
        .iter()
        .filter(|r| r.healthy && r.weight > 0)
        .collect();

    if healthy_runners.is_empty() {
        return HashMap::new();
    }

    // Generate all (group, shard_id) pairs
    let shards: Vec<(String, i32)> = shard_groups
        .iter()
        .flat_map(|group| (0..shards_per_group).map(move |id| (group.clone(), id)))
        .collect();

    // Parallel computation
    shards
        .par_iter()
        .filter_map(|(group, id)| {
            let shard_key = format!("{group}:{id}");
            select_runner_rendezvous(&shard_key, &healthy_runners)
                .map(|runner| (ShardId::new(group, *id), runner.address.clone()))
        })
        .collect()
}
```

#### Implementation Tasks

- [ ] Add `rayon` dependency (with optional feature flag `parallel`)
- [ ] Implement `RendezvousParallel` strategy variant
- [ ] Add `compute_rendezvous_parallel` method
- [ ] Add benchmark comparing sequential vs parallel performance
- [ ] Add tests for parallel strategy correctness (same results as sequential)
- [ ] Document when to use parallel vs sequential

#### Expected Performance

| Runners | Shards | Sequential | Parallel (8 cores) | Speedup |
|---------|--------|------------|-------------------|---------|
| 100     | 2048   | ~11ms      | ~2ms              | ~5.5x   |
| 1000    | 2048   | ~107ms     | ~15ms             | ~7x     |
| 100     | 10000  | ~55ms      | ~8ms              | ~7x     |

Note: Parallel overhead means sequential may be faster for small inputs (< 100 shards or < 10 nodes).

## Migration

No migration needed - the assignment computation is stateless and deterministic. When the new code deploys:

1. Runners will compute new assignments using rendezvous
2. Shards will rebalance to their new assignments
3. One-time movement of shards (full rebalance, not incremental)

This is acceptable because:
- It's a one-time event
- The shard handoff mechanism already handles rebalancing gracefully
- The new distribution will be better immediately

## Future Considerations

### Large Cluster Optimization

If we ever reach 10,000+ nodes where O(n) per shard becomes noticeable:

1. **Hierarchical rendezvous**: Assign shards to zones first, then to nodes within zones
2. **Candidate filtering**: Use a cheap hash to select K candidates, then rendezvous among those
3. **Parallel computation**: The per-shard computation is embarrassingly parallel

### Alternative Hash Functions

If distribution testing reveals issues with djb2:

- `xxhash` - very fast, excellent distribution
- `seahash` - Rust-native, good performance
- `ahash` - Rust's fast HashMap hasher

## Acceptance Criteria

### Core Implementation (Complete)

- [x] All existing tests pass (with updated tolerance expectations)
- [x] Distribution test shows variance < 20% for equal-weight nodes (existing test)
- [x] Distribution test shows variance < 5% for equal-weight nodes at scale (2048 shards, 3 nodes)
- [x] Node add/remove tests confirm O(1/n) movement
- [x] Weighted distribution test confirms proportional assignment (at scale)
- [x] Benchmark shows acceptable performance for typical clusters (< 15ms for 100 nodes, 2048 shards)
- [x] `hashring` crate removed from dependencies

### Configurable Strategy (Phase 6)

- [x] `ShardAssignmentStrategy` enum defined with `Rendezvous` and `ConsistentHash` variants
- [x] `compute_assignments` accepts strategy parameter
- [x] Rendezvous strategy produces correct, deterministic assignments
- [x] Rendezvous strategy passes distribution and rebalance tests
- [x] Strategy is configurable via `ShardingConfig`
- [x] Benchmarks updated to use strategy API
- [x] ConsistentHash strategy implementation (behind `consistent-hash` feature flag)
- [x] Documentation explains when to use each strategy (module-level doc in shard_assigner.rs)

### Parallel Rendezvous (Phase 7)

- [ ] `rayon` dependency added with `parallel` feature flag
- [ ] `RendezvousParallel` strategy variant implemented
- [ ] Parallel and sequential produce identical results
- [ ] Benchmark shows speedup for large inputs (100+ nodes, 2048+ shards)
- [ ] Documentation explains when to use parallel vs sequential
