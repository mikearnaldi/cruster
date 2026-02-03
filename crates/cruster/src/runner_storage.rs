use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::broadcast;

use crate::error::ClusterError;
use crate::runner::Runner;
use crate::types::{MachineId, RunnerAddress, ShardId};

/// Health status of the runner's lease/registration keep-alive.
///
/// Published by `RunnerStorage` implementations to allow sharding to monitor
/// lease health and trigger detachment when connectivity is degraded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHealth {
    /// Whether the last keep-alive attempt was successful.
    pub healthy: bool,
    /// Number of consecutive keep-alive failures.
    pub failure_streak: u32,
}

/// Result of a batch shard lock refresh, reporting successes, lost locks, and per-shard errors.
#[derive(Debug)]
pub struct BatchRefreshResult {
    /// Shard IDs that were successfully refreshed.
    pub refreshed: Vec<ShardId>,
    /// Shard IDs where the lock was no longer held (refresh returned `false`).
    pub lost: Vec<ShardId>,
    /// Per-shard errors for shards that failed to refresh due to storage errors.
    pub failures: Vec<(ShardId, ClusterError)>,
}

/// Result of a batch shard acquisition, reporting both successes and per-shard errors.
#[derive(Debug)]
pub struct BatchAcquireResult {
    /// Shard IDs that were successfully acquired.
    pub acquired: Vec<ShardId>,
    /// Per-shard errors for shards that failed to acquire due to storage errors
    /// (as opposed to being held by another runner, which is a normal `Ok(false)`).
    pub failures: Vec<(ShardId, ClusterError)>,
}

/// Storage backend for runner registration and shard locking.
#[async_trait]
pub trait RunnerStorage: Send + Sync {
    /// Register this runner. Returns assigned machine ID.
    async fn register(&self, runner: &Runner) -> Result<MachineId, ClusterError>;

    /// Unregister this runner.
    async fn unregister(&self, address: &RunnerAddress) -> Result<(), ClusterError>;

    /// Get all registered runners.
    async fn get_runners(&self) -> Result<Vec<Runner>, ClusterError>;

    /// Set runner health status.
    async fn set_runner_health(
        &self,
        address: &RunnerAddress,
        healthy: bool,
    ) -> Result<(), ClusterError>;

    /// Try to acquire a shard lock.
    async fn acquire(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<bool, ClusterError>;

    /// Refresh shard lock TTL.
    async fn refresh(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<bool, ClusterError>;

    /// Release a shard lock.
    async fn release(&self, shard_id: &ShardId, runner: &RunnerAddress)
        -> Result<(), ClusterError>;

    /// Release all locks held by this runner.
    async fn release_all(&self, runner: &RunnerAddress) -> Result<(), ClusterError>;

    /// Watch for changes to runner registrations.
    async fn watch_runners(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Vec<Runner>> + Send>>, ClusterError>;

    /// Refresh multiple shard lock TTLs in a batch. Returns the list of
    /// shard IDs that were successfully refreshed. Shards where the lock was
    /// not held (refresh returned `false`) are not included in the result.
    ///
    /// The default implementation calls `refresh` in a loop. Backends may
    /// override this for better performance (e.g., fewer mutex acquisitions).
    async fn refresh_batch(
        &self,
        shard_ids: &[ShardId],
        runner: &RunnerAddress,
    ) -> Result<BatchRefreshResult, ClusterError> {
        let mut refreshed = Vec::new();
        let mut lost = Vec::new();
        let mut failures = Vec::new();
        for shard_id in shard_ids {
            match self.refresh(shard_id, runner).await {
                Ok(true) => refreshed.push(shard_id.clone()),
                Ok(false) => lost.push(shard_id.clone()),
                Err(e) => {
                    tracing::warn!(shard_id = %shard_id, error = %e, "failed to refresh shard in batch");
                    failures.push((shard_id.clone(), e));
                }
            }
        }
        Ok(BatchRefreshResult {
            refreshed,
            lost,
            failures,
        })
    }

    /// Try to acquire multiple shard locks in a batch. Returns the list of
    /// shard IDs that were successfully acquired.
    ///
    /// The default implementation calls `acquire` in a loop. Backends may
    /// override this for better performance (e.g., etcd multi-key transactions).
    async fn acquire_batch(
        &self,
        shard_ids: &[ShardId],
        runner: &RunnerAddress,
    ) -> Result<BatchAcquireResult, ClusterError> {
        let mut acquired = Vec::new();
        let mut failures = Vec::new();
        for shard_id in shard_ids {
            match self.acquire(shard_id, runner).await {
                Ok(true) => acquired.push(shard_id.clone()),
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(shard_id = %shard_id, error = %e, "failed to acquire shard in batch");
                    failures.push((shard_id.clone(), e));
                }
            }
        }
        Ok(BatchAcquireResult { acquired, failures })
    }

    /// Subscribe to lease health updates.
    ///
    /// The returned receiver will receive [`LeaseHealth`] updates whenever the
    /// keep-alive status changes (success or failure). Sharding uses these
    /// updates to detect connectivity degradation and trigger detachment.
    ///
    /// The default implementation returns `None`, indicating the backend does
    /// not support health notifications (e.g., in-memory implementations).
    /// Backends that use leases (etcd, etc.) should override this.
    fn lease_health_receiver(&self) -> Option<broadcast::Receiver<LeaseHealth>> {
        None
    }
}
