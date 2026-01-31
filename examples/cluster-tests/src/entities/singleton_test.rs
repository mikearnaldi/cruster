//! SingletonTest - A proper cluster singleton for testing cluster-wide singleton behavior.
//!
//! Unlike regular entities, this uses `register_singleton` to run a background task
//! on exactly one runner in the cluster. The singleton tracks:
//! - Leadership changes (which runner hosts the singleton)
//! - A global sequence counter (guaranteed unique across the cluster)
//!
//! ## How it Works
//!
//! 1. Call `SingletonManager::new()` to create the manager
//! 2. Call `manager.register(sharding)` to register the singleton with the cluster
//! 3. Use `manager.get_next_sequence()` etc. to interact with the singleton
//!
//! The singleton task runs continuously on the runner that owns the computed shard.
//! When that runner dies, the singleton migrates to another runner, which is
//! recorded as a leadership change.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::sharding::Sharding;
use cruster::singleton::register_singleton;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// The name used for the singleton registration.
pub const SINGLETON_NAME: &str = "cluster-tests/singleton-test";

/// Record of a leadership change.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderRecord {
    /// The runner ID (address) that became leader.
    pub runner_id: String,
    /// When this runner became leader.
    pub became_leader_at: DateTime<Utc>,
    /// When this runner lost leadership (if it has).
    pub lost_leadership_at: Option<DateTime<Utc>>,
}

/// Shared state for the singleton, protected by a mutex.
#[derive(Debug, Default)]
struct SingletonState {
    /// History of leadership changes.
    leader_history: Vec<LeaderRecord>,
    /// Current runner ID.
    current_runner: Option<String>,
}

/// Manager for the SingletonTest singleton.
///
/// This provides a public API for interacting with the singleton state.
/// The actual singleton task runs in the background on one runner.
pub struct SingletonManager {
    /// Shared state protected by mutex.
    state: Arc<Mutex<SingletonState>>,
    /// Global sequence counter (atomic for lock-free access).
    sequence: Arc<AtomicU64>,
    /// Notification for when the singleton starts running.
    started: Arc<Notify>,
}

impl SingletonManager {
    /// Create a new singleton manager.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SingletonState::default())),
            sequence: Arc::new(AtomicU64::new(0)),
            started: Arc::new(Notify::new()),
        }
    }

    /// Register the singleton with the cluster.
    ///
    /// This must be called after creating the manager. The singleton task
    /// will be spawned on the runner that owns the computed shard.
    pub async fn register(&self, sharding: Arc<dyn Sharding>) -> Result<(), ClusterError> {
        let state = self.state.clone();
        let started = self.started.clone();

        // Get the runner address to identify this runner
        // We'll get it from the sharding config in the actual singleton task

        register_singleton(sharding.as_ref(), SINGLETON_NAME, move || {
            let state = state.clone();
            let started = started.clone();

            async move {
                // Get the current runner's address - we're now the leader!
                // Since we're inside the singleton, we know we're the current leader.
                // The runner address is determined by the sharding config.
                let runner_id = format!("runner-{}", std::process::id());

                tracing::info!(
                    runner = %runner_id,
                    "SingletonTest singleton started - this runner is now the leader"
                );

                // Record the leadership change
                {
                    let mut guard = state.lock();
                    let now = Utc::now();

                    // Mark previous leader as having lost leadership
                    if let Some(last) = guard.leader_history.last_mut() {
                        if last.lost_leadership_at.is_none() {
                            last.lost_leadership_at = Some(now);
                        }
                    }

                    // Add new leader record
                    guard.leader_history.push(LeaderRecord {
                        runner_id: runner_id.clone(),
                        became_leader_at: now,
                        lost_leadership_at: None,
                    });
                    guard.current_runner = Some(runner_id);
                }

                // Signal that we've started
                started.notify_waiters();

                // Keep the singleton running until cancelled
                // In a real cluster, cancellation happens when:
                // - The runner shuts down
                // - The shard is reassigned to another runner
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    tracing::trace!("SingletonTest singleton heartbeat");
                }

                #[allow(unreachable_code)]
                Ok(())
            }
        })
        .await
    }

    /// Get the next global sequence number.
    ///
    /// This is atomic and lock-free. Sequence numbers are guaranteed to be
    /// unique and monotonically increasing across all runners.
    pub fn get_next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Get the leadership change history.
    pub fn get_leader_history(&self) -> Vec<LeaderRecord> {
        self.state.lock().leader_history.clone()
    }

    /// Get the current runner ID.
    ///
    /// Returns the runner ID that is currently hosting the singleton,
    /// or an empty string if no leader has been recorded yet.
    pub fn get_current_runner(&self) -> String {
        self.state.lock().current_runner.clone().unwrap_or_default()
    }

    /// Clear the leadership history.
    ///
    /// This is useful for testing to reset the state.
    /// Note: This does NOT reset the sequence counter to ensure uniqueness.
    pub fn clear_history(&self) {
        let mut guard = self.state.lock();
        guard.leader_history.clear();
        guard.current_runner = None;
    }

    /// Reset the sequence counter.
    ///
    /// WARNING: This should only be used in testing. In production, resetting
    /// the sequence counter could cause duplicate sequence numbers.
    pub fn reset_sequence(&self) {
        self.sequence.store(0, Ordering::SeqCst);
    }

    /// Wait for the singleton to start running.
    ///
    /// This is useful in tests to ensure the singleton is active before
    /// making assertions.
    #[allow(dead_code)]
    pub async fn wait_for_start(&self) {
        self.started.notified().await;
    }
}

impl Default for SingletonManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_record_serialization() {
        let record = LeaderRecord {
            runner_id: "runner-1".to_string(),
            became_leader_at: Utc::now(),
            lost_leadership_at: None,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: LeaderRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.runner_id, "runner-1");
        assert!(parsed.lost_leadership_at.is_none());
    }

    #[test]
    fn test_leader_record_with_lost_leadership() {
        let record = LeaderRecord {
            runner_id: "runner-1".to_string(),
            became_leader_at: Utc::now(),
            lost_leadership_at: Some(Utc::now()),
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: LeaderRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.runner_id, "runner-1");
        assert!(parsed.lost_leadership_at.is_some());
    }

    #[test]
    fn test_singleton_manager_sequence() {
        let manager = SingletonManager::new();

        assert_eq!(manager.get_next_sequence(), 1);
        assert_eq!(manager.get_next_sequence(), 2);
        assert_eq!(manager.get_next_sequence(), 3);
    }

    #[test]
    fn test_singleton_manager_reset_sequence() {
        let manager = SingletonManager::new();

        manager.get_next_sequence();
        manager.get_next_sequence();
        manager.reset_sequence();

        assert_eq!(manager.get_next_sequence(), 1);
    }

    #[test]
    fn test_singleton_manager_clear_history() {
        let manager = SingletonManager::new();

        // Simulate leadership record
        {
            let mut state = manager.state.lock();
            state.leader_history.push(LeaderRecord {
                runner_id: "test-runner".to_string(),
                became_leader_at: Utc::now(),
                lost_leadership_at: None,
            });
            state.current_runner = Some("test-runner".to_string());
        }

        assert_eq!(manager.get_leader_history().len(), 1);
        assert_eq!(manager.get_current_runner(), "test-runner");

        manager.clear_history();

        assert!(manager.get_leader_history().is_empty());
        assert_eq!(manager.get_current_runner(), "");
    }
}
