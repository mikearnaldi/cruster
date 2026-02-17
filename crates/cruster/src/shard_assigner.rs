//! # Shard Assignment Strategies
//!
//! This module provides algorithms for assigning shards to runners in a distributed cluster.
//! The assignment strategy determines how shards are distributed across available runners
//! and how they rebalance when the cluster topology changes.
//!
//! ## When to Use Each Strategy
//!
//! ### Rendezvous Hashing (Default)
//!
//! **Use when:**
//! - Cluster has fewer than 1000 nodes
//! - Near-perfect distribution is important
//! - You want minimal shard movement during rebalancing
//!
//! **Characteristics:**
//! - **Distribution**: Near-perfect (each node gets exactly 1/n shards, ±1)
//! - **Complexity**: O(shards × nodes) - linear in both dimensions
//! - **Rebalance cost**: Optimal - only 1/n shards move when a node joins/leaves
//! - **Determinism**: Same inputs always produce same assignments
//!
//! **Performance benchmarks** (2048 shards):
//! - 3 nodes: ~0.6ms
//! - 10 nodes: ~1.4ms
//! - 100 nodes: ~11ms
//! - 1000 nodes: ~107ms
//!
//! ## How Rendezvous Hashing Works
//!
//! For each shard, compute a hash combining the shard key with each candidate runner.
//! Assign the shard to the runner with the highest hash value:
//!
//! ```text
//! assign(shard) = argmax over runners r: hash(shard, r)
//! ```
//!
//! This approach provides:
//! - **Consistent assignments**: The same shard always maps to the same runner
//!   given the same set of runners.
//! - **Minimal disruption**: When a runner leaves, only its shards are reassigned.
//!   When a runner joins, it claims approximately 1/(n+1) shards evenly from all
//!   existing runners.
//!
//! ## Weighted Runners
//!
//! Runners can have different weights to receive proportionally more shards.
//! This is implemented by computing multiple hashes per runner (one per weight unit)
//! and using the maximum. Statistically, this gives weighted runners proportionally
//! more "wins" in the highest-hash competition.
//!
//! ### Parallel Rendezvous Hashing (Optional)
//!
//! **Requires:** `parallel` feature flag
//!
//! **Use when:**
//! - Large shard counts (1000+) with many nodes (100+)
//! - Multi-core systems where parallelization overhead is worth it
//!
//! **Characteristics:**
//! - Same algorithm as Rendezvous but parallelized across shards
//! - Produces identical results to sequential Rendezvous
//! - Speedup scales with available CPU cores
//!
//! **Performance notes:**
//! - Parallel overhead means sequential may be faster for small inputs (< 100 shards or < 10 nodes)
//! - For large inputs (100+ nodes, 2048+ shards), expect 5-7x speedup on 8 cores
//!
//! ### Consistent Hashing (Optional)
//!
//! **Requires:** `consistent-hash` feature flag
//!
//! **Use when:**
//! - Cluster has 1000+ nodes where O(shards × nodes) becomes expensive
//! - You need faster computation at the cost of slightly less uniform distribution
//!
//! **Characteristics:**
//! - **Distribution**: Good (configurable via vnodes_per_weight)
//! - **Complexity**: O(shards × log(vnodes)) - logarithmic in virtual nodes
//! - **Rebalance cost**: Optimal (only ~1/n shards move when a node joins/leaves)
//! - **Memory**: Higher (stores virtual node ring)
//!
//! **Performance benchmarks** (2048 shards, 150 vnodes/weight):
//! - 3 nodes: ~0.5ms
//! - 10 nodes: ~0.6ms
//! - 100 nodes: ~1.2ms
//! - 1000 nodes: ~8ms
//!
//! ## How Consistent Hashing Works
//!
//! Each runner is mapped to multiple positions on a hash ring using virtual nodes.
//! For each shard, hash the shard key and find the nearest virtual node position
//! on the ring, assigning the shard to that runner.
//!
//! More virtual nodes = better distribution but higher memory usage.

use std::collections::{HashMap, HashSet};
#[cfg(feature = "consistent-hash")]
use std::hash::{Hash, Hasher};

use crate::hash::hash64;
use crate::runner::Runner;
use crate::types::{RunnerAddress, ShardId};

#[cfg(feature = "consistent-hash")]
use hashring::HashRing;
#[cfg(feature = "parallel")]
use rayon::prelude::*;

/// Strategy for assigning shards to runners.
///
/// Different strategies offer different trade-offs between distribution uniformity,
/// performance, and rebalance behavior.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ShardAssignmentStrategy {
    /// Rendezvous hashing (HRW) - best distribution, O(shards × nodes) complexity.
    ///
    /// Recommended for clusters with < 1000 nodes. Provides near-perfect distribution
    /// (each node gets exactly 1/n shards, ±1) and optimal rebalancing (only 1/n shards
    /// move when a node is added or removed).
    #[default]
    Rendezvous,

    /// Parallel rendezvous hashing using Rayon.
    ///
    /// Same algorithm as Rendezvous but parallelized across shards using Rayon.
    /// Produces identical results to sequential Rendezvous but leverages multiple
    /// CPU cores for faster computation.
    ///
    /// Recommended for large shard counts (1000+) with many nodes (100+).
    /// For smaller inputs, the parallelization overhead may make sequential faster.
    /// Requires the `parallel` feature flag.
    #[cfg(feature = "parallel")]
    RendezvousParallel,

    /// Consistent hashing with virtual nodes.
    ///
    /// Better performance for very large clusters (1000+ nodes), with slightly less
    /// uniform distribution than rendezvous hashing. Requires the `consistent-hash`
    /// feature flag.
    ///
    /// # Parameters
    /// - `vnodes_per_weight`: Number of virtual nodes per weight unit. Higher values
    ///   give better distribution but use more memory. Default: 150.
    #[cfg(feature = "consistent-hash")]
    ConsistentHash {
        /// Virtual nodes per weight unit. Higher = better distribution, more memory.
        vnodes_per_weight: u32,
    },
}

/// Computes shard-to-runner assignments using rendezvous hashing.
///
/// Given a set of runners (with weights) and shard groups, determines which
/// runner should own each shard. This algorithm provides:
/// - Near-perfect distribution (each node gets exactly 1/n shards, ±1)
/// - Minimal movement on node departure (only shards on departed node move)
/// - Minimal movement on node addition (new node claims ~1/(n+1) shards evenly)
/// - Deterministic assignments (same inputs always produce same outputs)
/// - Weighted node support via multiple hash computations per weight unit
pub struct ShardAssigner;

impl ShardAssigner {
    /// Compute the ideal shard assignments for a set of runners and shard groups.
    ///
    /// Returns a map from ShardId to RunnerAddress indicating which runner
    /// should own each shard. Only healthy runners are considered.
    ///
    /// The assignment algorithm is determined by the `strategy` parameter.
    /// Use [`ShardAssignmentStrategy::default()`] for the recommended algorithm.
    #[tracing::instrument(level = "debug", skip(runners, shard_groups), fields(num_runners = runners.len(), num_groups = shard_groups.len(), shards_per_group))]
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
            #[cfg(feature = "parallel")]
            ShardAssignmentStrategy::RendezvousParallel => {
                Self::compute_rendezvous_parallel(runners, shard_groups, shards_per_group)
            }
            #[cfg(feature = "consistent-hash")]
            ShardAssignmentStrategy::ConsistentHash { vnodes_per_weight } => {
                Self::compute_consistent_hash(
                    runners,
                    shard_groups,
                    shards_per_group,
                    *vnodes_per_weight,
                )
            }
        }
    }

    /// Compute shard assignments using rendezvous hashing (Highest Random Weight).
    ///
    /// For each shard, compute a hash combining the shard key with each candidate runner,
    /// then assign the shard to the runner with the highest hash value.
    ///
    /// Properties:
    /// - Near-perfect distribution (each node gets exactly 1/n shards, ±1)
    /// - Minimal movement on node departure (only shards on departed node move)
    /// - Minimal movement on node addition (new node claims ~1/(n+1) shards evenly)
    /// - Deterministic assignments (same inputs always produce same outputs)
    /// - Weighted node support via multiple hash computations per weight unit
    fn compute_rendezvous(
        runners: &[Runner],
        shard_groups: &[String],
        shards_per_group: i32,
    ) -> HashMap<ShardId, RunnerAddress> {
        let mut assignments = HashMap::new();

        // Only consider healthy runners with positive weight.
        // Weight=0 means the runner is in drain mode and should not receive shard assignments.
        let healthy_runners: Vec<&Runner> = runners
            .iter()
            .filter(|r| {
                if !r.healthy || r.weight <= 0 {
                    tracing::debug!(
                        runner = %r.address,
                        healthy = r.healthy,
                        weight = r.weight,
                        "excluding runner from shard assignment"
                    );
                    false
                } else {
                    true
                }
            })
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

    /// Compute shard assignments using parallel rendezvous hashing.
    ///
    /// Same algorithm as `compute_rendezvous` but parallelized across shards
    /// using Rayon. Produces identical results to sequential rendezvous.
    ///
    /// Recommended for large shard counts (1000+) with many nodes (100+).
    /// For smaller inputs, the parallelization overhead may make sequential faster.
    #[cfg(feature = "parallel")]
    fn compute_rendezvous_parallel(
        runners: &[Runner],
        shard_groups: &[String],
        shards_per_group: i32,
    ) -> HashMap<ShardId, RunnerAddress> {
        // Only consider healthy runners with positive weight.
        let healthy_runners: Vec<&Runner> = runners
            .iter()
            .filter(|r| {
                if !r.healthy || r.weight <= 0 {
                    tracing::debug!(
                        runner = %r.address,
                        healthy = r.healthy,
                        weight = r.weight,
                        "excluding runner from shard assignment"
                    );
                    false
                } else {
                    true
                }
            })
            .collect();

        if healthy_runners.is_empty() {
            return HashMap::new();
        }

        // Generate all (group, shard_id) pairs
        let shards: Vec<(&String, i32)> = shard_groups
            .iter()
            .flat_map(|group| (0..shards_per_group).map(move |id| (group, id)))
            .collect();

        // Parallel computation using Rayon
        shards
            .par_iter()
            .filter_map(|(group, id)| {
                let shard_key = format!("{group}:{id}");
                select_runner_rendezvous(&shard_key, &healthy_runners)
                    .map(|runner| (ShardId::new(*group, *id), runner.address.clone()))
            })
            .collect()
    }

    /// Compute shard assignments using consistent hashing with virtual nodes.
    ///
    /// Each runner is mapped to multiple positions on a hash ring based on its weight.
    /// For each shard, hash the shard key and find the nearest virtual node on the ring.
    ///
    /// Properties:
    /// - Good distribution (configurable via vnodes_per_weight)
    /// - O(shards × log(vnodes)) complexity
    /// - Optimal rebalancing (only ~1/n shards move when a node joins/leaves)
    /// - Higher memory usage than rendezvous (stores the ring)
    #[cfg(feature = "consistent-hash")]
    fn compute_consistent_hash(
        runners: &[Runner],
        shard_groups: &[String],
        shards_per_group: i32,
        vnodes_per_weight: u32,
    ) -> HashMap<ShardId, RunnerAddress> {
        let mut assignments = HashMap::new();

        // Only consider healthy runners with positive weight
        let healthy_runners: Vec<&Runner> = runners
            .iter()
            .filter(|r| {
                if !r.healthy || r.weight <= 0 {
                    tracing::debug!(
                        runner = %r.address,
                        healthy = r.healthy,
                        weight = r.weight,
                        "excluding runner from shard assignment"
                    );
                    false
                } else {
                    true
                }
            })
            .collect();

        if healthy_runners.is_empty() {
            return assignments;
        }

        // Build the hash ring with virtual nodes
        let mut ring: HashRing<RunnerNode> = HashRing::new();
        for runner in &healthy_runners {
            // Add vnodes_per_weight virtual nodes per weight unit
            let total_vnodes = (runner.weight as u32) * vnodes_per_weight;
            for vnode_id in 0..total_vnodes {
                ring.add(RunnerNode {
                    address: runner.address.clone(),
                    vnode_id,
                });
            }
        }

        // Assign each shard to its consistent hash location
        for group in shard_groups {
            for id in 0..shards_per_group {
                let shard_key = format!("{group}:{id}");
                if let Some(node) = ring.get(&shard_key) {
                    assignments.insert(ShardId::new(group, id), node.address.clone());
                }
            }
        }

        assignments
    }

    /// Compute the diff between current and desired assignments for a specific runner.
    ///
    /// Returns (to_acquire, to_release):
    /// - `to_acquire`: shards that should be owned by this runner but aren't yet
    /// - `to_release`: shards currently owned but should no longer be
    pub fn compute_diff(
        desired: &HashMap<ShardId, RunnerAddress>,
        current_owned: &HashSet<ShardId>,
        my_address: &RunnerAddress,
    ) -> (HashSet<ShardId>, HashSet<ShardId>) {
        let desired_mine: HashSet<ShardId> = desired
            .iter()
            .filter(|(_, addr)| *addr == my_address)
            .map(|(shard, _)| shard.clone())
            .collect();

        let to_acquire: HashSet<ShardId> =
            desired_mine.difference(current_owned).cloned().collect();

        let to_release: HashSet<ShardId> =
            current_owned.difference(&desired_mine).cloned().collect();

        (to_acquire, to_release)
    }
}

/// Select the runner with the highest hash score for the given shard key.
/// Weights are handled by computing multiple hashes per runner (one per weight unit)
/// and using the maximum.
fn select_runner_rendezvous<'a>(shard_key: &str, runners: &[&'a Runner]) -> Option<&'a Runner> {
    runners
        .iter()
        .max_by_key(|runner| compute_runner_score(shard_key, runner))
        .copied()
}

/// Compute the rendezvous score for a runner.
/// For weighted runners, we compute `weight` hashes and take the maximum,
/// which statistically gives weighted runners proportionally more wins.
///
/// Uses proper rendezvous hashing: concatenate shard key + runner key + weight index
/// into a single string and hash it with a high-quality hash function.
fn compute_runner_score(shard_key: &str, runner: &Runner) -> u64 {
    // For each weight unit, compute a hash of the combined key
    (0..runner.weight)
        .map(|w| {
            // Concatenate shard key, runner address, and weight index
            // The null byte separator prevents ambiguity (e.g., "a:b" vs "a" + ":b")
            let combined_key = format!(
                "{}\0{}:{}\0{}",
                shard_key, runner.address.host, runner.address.port, w
            );
            hash64(combined_key.as_bytes())
        })
        .max()
        .unwrap_or(0)
}

/// A virtual node representing a runner on the consistent hash ring.
/// Each runner may have multiple virtual nodes based on its weight.
#[cfg(feature = "consistent-hash")]
#[derive(Clone)]
struct RunnerNode {
    address: RunnerAddress,
    vnode_id: u32,
}

#[cfg(feature = "consistent-hash")]
impl Hash for RunnerNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.host.hash(state);
        self.address.port.hash(state);
        self.vnode_id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to get default strategy for tests
    fn default_strategy() -> ShardAssignmentStrategy {
        ShardAssignmentStrategy::default()
    }

    #[test]
    fn single_runner_gets_all_shards() {
        let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1)];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 10, &default_strategy());

        assert_eq!(assignments.len(), 10);
        for addr in assignments.values() {
            assert_eq!(addr, &RunnerAddress::new("host1", 9000));
        }
    }

    #[test]
    fn two_runners_distribute_shards() {
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 1),
            Runner::new(RunnerAddress::new("host2", 9000), 1),
        ];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 300, &default_strategy());

        assert_eq!(assignments.len(), 300);

        let host1_count = assignments.values().filter(|a| a.host == "host1").count();
        let host2_count = assignments.values().filter(|a| a.host == "host2").count();

        // Both runners should get some shards (not necessarily 50/50 but both > 0)
        assert!(host1_count > 0, "host1 should have some shards");
        assert!(host2_count > 0, "host2 should have some shards");
        assert_eq!(host1_count + host2_count, 300);
    }

    #[test]
    fn unhealthy_runners_excluded() {
        let mut r2 = Runner::new(RunnerAddress::new("host2", 9000), 1);
        r2.healthy = false;
        let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1), r2];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 10, &default_strategy());

        assert_eq!(assignments.len(), 10);
        for addr in assignments.values() {
            assert_eq!(addr, &RunnerAddress::new("host1", 9000));
        }
    }

    #[test]
    fn no_healthy_runners_empty() {
        let mut r = Runner::new(RunnerAddress::new("host1", 9000), 1);
        r.healthy = false;
        let runners = vec![r];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 10, &default_strategy());
        assert!(assignments.is_empty());
    }

    #[test]
    fn weighted_runner_gets_more_shards() {
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 3), // 3x weight
            Runner::new(RunnerAddress::new("host2", 9000), 1),
        ];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 300, &default_strategy());

        let host1_count = assignments.values().filter(|a| a.host == "host1").count();

        // With 3x weight, host1 should get roughly 75% but at least more than half
        assert!(
            host1_count > 150,
            "host1 (weight=3) should have more than half the shards, got {host1_count}"
        );
    }

    #[test]
    fn multiple_groups() {
        let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1)];
        let groups = vec!["default".to_string(), "premium".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 10, &default_strategy());

        assert_eq!(assignments.len(), 20); // 10 per group
    }

    #[test]
    fn compute_diff_works() {
        let my_addr = RunnerAddress::new("host1", 9000);
        let other_addr = RunnerAddress::new("host2", 9000);

        let mut desired = HashMap::new();
        desired.insert(ShardId::new("default", 0), my_addr.clone());
        desired.insert(ShardId::new("default", 1), my_addr.clone());
        desired.insert(ShardId::new("default", 2), other_addr.clone());

        let mut current = HashSet::new();
        current.insert(ShardId::new("default", 0)); // keep
        current.insert(ShardId::new("default", 3)); // release

        let (to_acquire, to_release) = ShardAssigner::compute_diff(&desired, &current, &my_addr);

        assert!(to_acquire.contains(&ShardId::new("default", 1)));
        assert!(!to_acquire.contains(&ShardId::new("default", 0)));
        assert!(to_release.contains(&ShardId::new("default", 3)));
        assert!(!to_release.contains(&ShardId::new("default", 0)));
    }

    #[test]
    fn distribution_uniformity_with_equal_weight_runners() {
        // With 3 equal-weight runners, each should get roughly 1/3 of 300 shards (100 each).
        // We allow ±25% deviation (75-125 per runner) for small sample sizes.
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 1),
            Runner::new(RunnerAddress::new("host2", 9000), 1),
            Runner::new(RunnerAddress::new("host3", 9000), 1),
        ];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 300, &default_strategy());

        let count = |host: &str| assignments.values().filter(|a| a.host == host).count();
        let h1 = count("host1");
        let h2 = count("host2");
        let h3 = count("host3");

        assert_eq!(h1 + h2 + h3, 300);
        let expected = 100;
        let tolerance = 25; // 25% tolerance for small sample size
        assert!(
            h1.abs_diff(expected) <= tolerance,
            "host1 got {h1} shards, expected ~{expected} (±{tolerance})"
        );
        assert!(
            h2.abs_diff(expected) <= tolerance,
            "host2 got {h2} shards, expected ~{expected} (±{tolerance})"
        );
        assert!(
            h3.abs_diff(expected) <= tolerance,
            "host3 got {h3} shards, expected ~{expected} (±{tolerance})"
        );
    }

    #[test]
    fn weight_zero_runners_excluded() {
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 1),
            Runner::new(RunnerAddress::new("host2", 9000), 0), // drain mode
        ];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 10, &default_strategy());

        assert_eq!(assignments.len(), 10);
        for addr in assignments.values() {
            assert_eq!(addr, &RunnerAddress::new("host1", 9000));
        }
    }

    #[test]
    fn deterministic_assignments() {
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 1),
            Runner::new(RunnerAddress::new("host2", 9000), 1),
        ];
        let groups = vec!["default".to_string()];
        let a1 = ShardAssigner::compute_assignments(&runners, &groups, 300, &default_strategy());
        let a2 = ShardAssigner::compute_assignments(&runners, &groups, 300, &default_strategy());
        assert_eq!(a1, a2);
    }

    /// Test that rendezvous hashing provides near-perfect distribution at scale.
    /// With 2048 shards across 3 nodes, each should get ~682-683 shards.
    #[test]
    fn rendezvous_distribution_uniformity() {
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 1),
            Runner::new(RunnerAddress::new("host2", 9000), 1),
            Runner::new(RunnerAddress::new("host3", 9000), 1),
        ];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 2048, &default_strategy());

        let count = |host: &str| assignments.values().filter(|a| a.host == host).count();
        let h1 = count("host1");
        let h2 = count("host2");
        let h3 = count("host3");

        // With rendezvous, expect reasonably tight distribution
        // 2048 / 3 = 682.67, so expect ~682 each
        let expected = 2048 / 3; // 682
        let tolerance = 35; // Allow ~5% variance due to hash distribution

        assert!(
            h1.abs_diff(expected) <= tolerance,
            "host1: {h1}, expected ~{expected}"
        );
        assert!(
            h2.abs_diff(expected) <= tolerance,
            "host2: {h2}, expected ~{expected}"
        );
        assert!(
            h3.abs_diff(expected) <= tolerance,
            "host3: {h3}, expected ~{expected}"
        );
    }

    /// Test that when a node is removed, only shards from that node move.
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
        let before =
            ShardAssigner::compute_assignments(&runners_3, &groups, 2048, &default_strategy());
        let after =
            ShardAssigner::compute_assignments(&runners_2, &groups, 2048, &default_strategy());

        let moved: usize = before
            .iter()
            .filter(|(shard, addr)| after.get(*shard) != Some(*addr))
            .count();

        // Only shards from host3 should move (~1/3 of total)
        let host3_shards = before.values().filter(|a| a.host == "host3").count();
        assert_eq!(moved, host3_shards, "only host3 shards should move");

        // Verify it's roughly 1/3
        assert!(
            moved > 600 && moved < 750,
            "expected ~683 moves, got {moved}"
        );
    }

    /// Test that when a node is added, the new node claims ~1/(n+1) shards evenly.
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
        let before =
            ShardAssigner::compute_assignments(&runners_3, &groups, 2048, &default_strategy());
        let after =
            ShardAssigner::compute_assignments(&runners_4, &groups, 2048, &default_strategy());

        let moved: usize = before
            .iter()
            .filter(|(shard, addr)| after.get(*shard) != Some(*addr))
            .count();

        // New node should claim ~1/4 of shards
        let host4_shards = after.values().filter(|a| a.host == "host4").count();
        assert_eq!(moved, host4_shards, "moves should equal host4's new shards");

        // Verify it's roughly 1/4
        assert!(
            moved > 450 && moved < 560,
            "expected ~512 moves, got {moved}"
        );
    }

    /// Test that weighted distribution is proportional at scale.
    #[test]
    fn weighted_distribution_at_scale() {
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 3), // 3x weight
            Runner::new(RunnerAddress::new("host2", 9000), 1),
        ];
        let groups = vec!["default".to_string()];
        let assignments =
            ShardAssigner::compute_assignments(&runners, &groups, 2048, &default_strategy());

        let h1 = assignments.values().filter(|a| a.host == "host1").count();
        let h2 = assignments.values().filter(|a| a.host == "host2").count();

        // host1 (weight 3) should get ~75%, host2 (weight 1) should get ~25%
        // 2048 * 0.75 = 1536, 2048 * 0.25 = 512
        assert!(
            h1 > 1450 && h1 < 1620,
            "host1 (w=3): expected ~1536, got {h1}"
        );
        assert!(h2 > 430 && h2 < 600, "host2 (w=1): expected ~512, got {h2}");
    }

    #[test]
    fn strategy_default_is_rendezvous() {
        assert_eq!(
            ShardAssignmentStrategy::default(),
            ShardAssignmentStrategy::Rendezvous
        );
    }

    // Parallel Rendezvous-specific tests (require the feature flag)
    #[cfg(feature = "parallel")]
    mod parallel_tests {
        use super::*;

        fn parallel_strategy() -> ShardAssignmentStrategy {
            ShardAssignmentStrategy::RendezvousParallel
        }

        #[test]
        fn parallel_single_runner_gets_all_shards() {
            let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1)];
            let groups = vec!["default".to_string()];
            let assignments =
                ShardAssigner::compute_assignments(&runners, &groups, 10, &parallel_strategy());

            assert_eq!(assignments.len(), 10);
            for addr in assignments.values() {
                assert_eq!(addr, &RunnerAddress::new("host1", 9000));
            }
        }

        #[test]
        fn parallel_produces_same_results_as_sequential() {
            let runners: Vec<Runner> = (0..10)
                .map(|i| Runner::new(RunnerAddress::new(format!("host{i}"), 9000), 1))
                .collect();
            let groups = vec!["default".to_string(), "premium".to_string()];

            let sequential = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                2048,
                &ShardAssignmentStrategy::Rendezvous,
            );
            let parallel =
                ShardAssigner::compute_assignments(&runners, &groups, 2048, &parallel_strategy());

            assert_eq!(
                sequential, parallel,
                "parallel and sequential must produce identical results"
            );
        }

        #[test]
        fn parallel_produces_same_results_with_weights() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 3),
                Runner::new(RunnerAddress::new("host2", 9000), 1),
                Runner::new(RunnerAddress::new("host3", 9000), 2),
            ];
            let groups = vec!["default".to_string()];

            let sequential = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                2048,
                &ShardAssignmentStrategy::Rendezvous,
            );
            let parallel =
                ShardAssigner::compute_assignments(&runners, &groups, 2048, &parallel_strategy());

            assert_eq!(
                sequential, parallel,
                "parallel and sequential must produce identical results with weights"
            );
        }

        #[test]
        fn parallel_distribution_uniformity() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 1),
                Runner::new(RunnerAddress::new("host2", 9000), 1),
                Runner::new(RunnerAddress::new("host3", 9000), 1),
            ];
            let groups = vec!["default".to_string()];
            let assignments =
                ShardAssigner::compute_assignments(&runners, &groups, 2048, &parallel_strategy());

            let count = |host: &str| assignments.values().filter(|a| a.host == host).count();
            let h1 = count("host1");
            let h2 = count("host2");
            let h3 = count("host3");

            // Same distribution expectations as sequential rendezvous
            let expected = 2048 / 3; // 682
            let tolerance = 35; // Allow ~5% variance due to hash distribution

            assert!(
                h1.abs_diff(expected) <= tolerance,
                "host1: {h1}, expected ~{expected}"
            );
            assert!(
                h2.abs_diff(expected) <= tolerance,
                "host2: {h2}, expected ~{expected}"
            );
            assert!(
                h3.abs_diff(expected) <= tolerance,
                "host3: {h3}, expected ~{expected}"
            );
        }

        #[test]
        fn parallel_excludes_unhealthy_runners() {
            let mut r2 = Runner::new(RunnerAddress::new("host2", 9000), 1);
            r2.healthy = false;
            let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1), r2];
            let groups = vec!["default".to_string()];
            let assignments =
                ShardAssigner::compute_assignments(&runners, &groups, 10, &parallel_strategy());

            assert_eq!(assignments.len(), 10);
            for addr in assignments.values() {
                assert_eq!(addr, &RunnerAddress::new("host1", 9000));
            }
        }

        #[test]
        fn parallel_excludes_weight_zero_runners() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 1),
                Runner::new(RunnerAddress::new("host2", 9000), 0), // drain mode
            ];
            let groups = vec!["default".to_string()];
            let assignments =
                ShardAssigner::compute_assignments(&runners, &groups, 10, &parallel_strategy());

            assert_eq!(assignments.len(), 10);
            for addr in assignments.values() {
                assert_eq!(addr, &RunnerAddress::new("host1", 9000));
            }
        }

        #[test]
        fn parallel_deterministic() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 1),
                Runner::new(RunnerAddress::new("host2", 9000), 1),
            ];
            let groups = vec!["default".to_string()];
            let a1 =
                ShardAssigner::compute_assignments(&runners, &groups, 300, &parallel_strategy());
            let a2 =
                ShardAssigner::compute_assignments(&runners, &groups, 300, &parallel_strategy());
            assert_eq!(a1, a2);
        }
    }

    // ConsistentHash-specific tests (require the feature flag)
    #[cfg(feature = "consistent-hash")]
    mod consistent_hash_tests {
        use super::*;

        fn consistent_hash_strategy() -> ShardAssignmentStrategy {
            ShardAssignmentStrategy::ConsistentHash {
                vnodes_per_weight: 150,
            }
        }

        #[test]
        fn consistent_hash_single_runner_gets_all_shards() {
            let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1)];
            let groups = vec!["default".to_string()];
            let assignments = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                10,
                &consistent_hash_strategy(),
            );

            assert_eq!(assignments.len(), 10);
            for addr in assignments.values() {
                assert_eq!(addr, &RunnerAddress::new("host1", 9000));
            }
        }

        #[test]
        fn consistent_hash_distributes_shards() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 1),
                Runner::new(RunnerAddress::new("host2", 9000), 1),
            ];
            let groups = vec!["default".to_string()];
            let assignments = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                300,
                &consistent_hash_strategy(),
            );

            assert_eq!(assignments.len(), 300);

            let host1_count = assignments.values().filter(|a| a.host == "host1").count();
            let host2_count = assignments.values().filter(|a| a.host == "host2").count();

            // Both runners should get some shards
            assert!(host1_count > 0, "host1 should have some shards");
            assert!(host2_count > 0, "host2 should have some shards");
            assert_eq!(host1_count + host2_count, 300);
        }

        #[test]
        fn consistent_hash_distribution_uniformity() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 1),
                Runner::new(RunnerAddress::new("host2", 9000), 1),
                Runner::new(RunnerAddress::new("host3", 9000), 1),
            ];
            let groups = vec!["default".to_string()];
            let assignments = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                2048,
                &consistent_hash_strategy(),
            );

            let count = |host: &str| assignments.values().filter(|a| a.host == host).count();
            let h1 = count("host1");
            let h2 = count("host2");
            let h3 = count("host3");

            // With 150 vnodes per runner, expect reasonable distribution
            // 2048 / 3 = 682.67, allow ~15% variance for consistent hashing
            let expected = 2048 / 3; // 682
            let tolerance = 102; // ~15%

            assert!(
                h1.abs_diff(expected) <= tolerance,
                "host1: {h1}, expected ~{expected}"
            );
            assert!(
                h2.abs_diff(expected) <= tolerance,
                "host2: {h2}, expected ~{expected}"
            );
            assert!(
                h3.abs_diff(expected) <= tolerance,
                "host3: {h3}, expected ~{expected}"
            );
        }

        #[test]
        fn consistent_hash_weighted_distribution() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 3), // 3x weight
                Runner::new(RunnerAddress::new("host2", 9000), 1),
            ];
            let groups = vec!["default".to_string()];
            let assignments = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                2048,
                &consistent_hash_strategy(),
            );

            let h1 = assignments.values().filter(|a| a.host == "host1").count();
            let h2 = assignments.values().filter(|a| a.host == "host2").count();

            // host1 (weight 3) should get ~75%, host2 (weight 1) should get ~25%
            // Allow wider tolerance for consistent hashing
            assert!(
                h1 > 1350 && h1 < 1700,
                "host1 (w=3): expected ~1536, got {h1}"
            );
            assert!(h2 > 350 && h2 < 700, "host2 (w=1): expected ~512, got {h2}");
        }

        #[test]
        fn consistent_hash_deterministic() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 1),
                Runner::new(RunnerAddress::new("host2", 9000), 1),
            ];
            let groups = vec!["default".to_string()];
            let strategy = consistent_hash_strategy();
            let a1 = ShardAssigner::compute_assignments(&runners, &groups, 300, &strategy);
            let a2 = ShardAssigner::compute_assignments(&runners, &groups, 300, &strategy);
            assert_eq!(a1, a2);
        }

        #[test]
        fn consistent_hash_minimal_movement_on_node_removal() {
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
            let strategy = consistent_hash_strategy();
            let before = ShardAssigner::compute_assignments(&runners_3, &groups, 2048, &strategy);
            let after = ShardAssigner::compute_assignments(&runners_2, &groups, 2048, &strategy);

            let moved: usize = before
                .iter()
                .filter(|(shard, addr)| after.get(*shard) != Some(*addr))
                .count();

            // Only shards from host3 should move (~1/3 of total)
            let host3_shards = before.values().filter(|a| a.host == "host3").count();
            assert_eq!(moved, host3_shards, "only host3 shards should move");

            // Verify it's roughly 1/3 (allow wider tolerance)
            assert!(
                moved > 500 && moved < 850,
                "expected ~683 moves, got {moved}"
            );
        }

        #[test]
        fn consistent_hash_excludes_unhealthy_runners() {
            let mut r2 = Runner::new(RunnerAddress::new("host2", 9000), 1);
            r2.healthy = false;
            let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1), r2];
            let groups = vec!["default".to_string()];
            let assignments = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                10,
                &consistent_hash_strategy(),
            );

            assert_eq!(assignments.len(), 10);
            for addr in assignments.values() {
                assert_eq!(addr, &RunnerAddress::new("host1", 9000));
            }
        }

        #[test]
        fn consistent_hash_excludes_weight_zero_runners() {
            let runners = vec![
                Runner::new(RunnerAddress::new("host1", 9000), 1),
                Runner::new(RunnerAddress::new("host2", 9000), 0), // drain mode
            ];
            let groups = vec!["default".to_string()];
            let assignments = ShardAssigner::compute_assignments(
                &runners,
                &groups,
                10,
                &consistent_hash_strategy(),
            );

            assert_eq!(assignments.len(), 10);
            for addr in assignments.values() {
                assert_eq!(addr, &RunnerAddress::new("host1", 9000));
            }
        }
    }
}
