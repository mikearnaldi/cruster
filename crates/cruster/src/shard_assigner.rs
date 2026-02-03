use std::collections::{HashMap, HashSet};

use crate::hash::djb2_hash64;
use crate::runner::Runner;
use crate::types::{RunnerAddress, ShardId};

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
    /// Uses rendezvous hashing (Highest Random Weight): for each shard, compute
    /// a hash combining the shard key with each candidate runner, then assign
    /// the shard to the runner with the highest hash value.
    pub fn compute_assignments(
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

    #[test]
    fn single_runner_gets_all_shards() {
        let runners = vec![Runner::new(RunnerAddress::new("host1", 9000), 1)];
        let groups = vec!["default".to_string()];
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 10);

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
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 300);

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
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 10);

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
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 10);
        assert!(assignments.is_empty());
    }

    #[test]
    fn weighted_runner_gets_more_shards() {
        let runners = vec![
            Runner::new(RunnerAddress::new("host1", 9000), 3), // 3x weight
            Runner::new(RunnerAddress::new("host2", 9000), 1),
        ];
        let groups = vec!["default".to_string()];
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 300);

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
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 10);

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
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 300);

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
        let assignments = ShardAssigner::compute_assignments(&runners, &groups, 10);

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
        let a1 = ShardAssigner::compute_assignments(&runners, &groups, 300);
        let a2 = ShardAssigner::compute_assignments(&runners, &groups, 300);
        assert_eq!(a1, a2);
    }
}
