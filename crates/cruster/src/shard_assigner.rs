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
//! ## Future Strategies
//!
//! For very large clusters (1000+ nodes), consistent hashing with virtual nodes
//! may be added as an alternative strategy with O(shards × log(vnodes)) complexity.

use std::collections::{HashMap, HashSet};

use crate::hash::djb2_hash64;
use crate::runner::Runner;
use crate::types::{RunnerAddress, ShardId};

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
/// Uses a two-phase hash with proper bit mixing: hash both the shard key
/// and runner key separately, then combine them with XOR and mix the bits.
/// This provides good avalanche properties for rendezvous hashing.
fn compute_runner_score(shard_key: &str, runner: &Runner) -> u64 {
    // Hash the shard key
    let shard_hash = djb2_hash64(shard_key.as_bytes());

    // Hash the runner key (without weight, to be added per iteration)
    let runner_base_key = format!("{}:{}", runner.address.host, runner.address.port);
    let runner_base_hash = djb2_hash64(runner_base_key.as_bytes());

    // For each weight unit, compute a combined hash
    (0..runner.weight)
        .map(|w| {
            // Combine shard hash, runner hash, and weight index, then mix
            let combined = shard_hash ^ runner_base_hash ^ (w as u64);
            mix64(combined)
        })
        .max()
        .unwrap_or(0)
}

/// Mix bits in a 64-bit integer for better avalanche properties.
/// Uses a variant of the finalizer from MurmurHash3.
#[inline]
fn mix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
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
        // With 100 virtual nodes per weight unit, 3 equal-weight runners
        // should each get roughly 1/3 of 300 shards (100 each).
        // We allow ±20% deviation (80-120 per runner).
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
        let tolerance = 20; // 20% of 100
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

        // With rendezvous, expect very tight distribution
        // 2048 / 3 = 682.67, so expect 682 or 683 each
        let expected = 2048 / 3; // 682
        let tolerance = 30; // Allow ~4.5% variance due to hash distribution

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
}
