use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, EventType, GetOptions, LeaseGrantOptions, PutOptions, Txn, TxnOp,
    WatchOptions,
};
use futures::Stream;
use tokio::sync::{broadcast, Mutex};

use crate::error::ClusterError;
use crate::runner::Runner;
use crate::runner_health::RunnerHealth;
use crate::runner_storage::{BatchAcquireResult, BatchRefreshResult, LeaseHealth, RunnerStorage};
use crate::types::{MachineId, RunnerAddress, ShardId};

/// etcd-backed runner storage using leases for registration and
/// transactions for shard lock acquisition.
pub struct EtcdRunnerStorage {
    client: Arc<Mutex<Client>>,
    prefix: String,
    lease_ttl: i64,
    /// Lease ID for this runner's registration, set after `register`.
    lease_id: Mutex<Option<i64>>,
    /// Handle to the lease keep-alive background task.
    /// Uses `parking_lot::Mutex` (non-async) so `Drop` can always access it
    /// synchronously without contention with async code paths.
    keep_alive_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Broadcast channel for publishing lease health updates.
    /// Sharding subscribes to this to monitor keep-alive health and trigger
    /// detachment when connectivity is degraded.
    lease_health_tx: broadcast::Sender<LeaseHealth>,
}

impl EtcdRunnerStorage {
    /// Create a new etcd runner storage.
    ///
    /// - `client`: Connected etcd client.
    /// - `prefix`: Key prefix for all cluster keys (e.g., "/cluster/").
    /// - `lease_ttl`: TTL in seconds for runner registration leases.
    pub fn new(client: Client, prefix: impl Into<String>, lease_ttl: i64) -> Self {
        assert!(lease_ttl > 0, "lease_ttl must be positive, got {lease_ttl}");
        // Buffer size of 16 allows for brief subscriber lag without blocking the keep-alive loop.
        let (lease_health_tx, _) = broadcast::channel(16);
        Self {
            client: Arc::new(Mutex::new(client)),
            prefix: prefix.into(),
            lease_ttl,
            lease_id: Mutex::new(None),
            keep_alive_handle: parking_lot::Mutex::new(None),
            lease_health_tx,
        }
    }

    fn runner_key(&self, address: &RunnerAddress) -> String {
        format!("{}runners/{}:{}", self.prefix, address.host, address.port)
    }

    fn runners_prefix(&self) -> String {
        format!("{}runners/", self.prefix)
    }

    fn shard_key(&self, shard_id: &ShardId) -> String {
        format!("{}shards/{}:{}", self.prefix, shard_id.group, shard_id.id)
    }

    fn machine_id_key(&self) -> String {
        format!("{}machine_id_counter", self.prefix)
    }

    fn map_err(e: etcd_client::Error) -> ClusterError {
        ClusterError::PersistenceError {
            reason: e.to_string(),
            source: Some(Box::new(e)),
        }
    }
}

/// Guard that revokes an etcd lease on drop unless disarmed.
/// Used during `register()` to prevent orphaned leases if any step
/// after lease grant fails.
struct LeaseGuard {
    client: Arc<Mutex<Client>>,
    lease_id: i64,
    disarmed: bool,
}

impl LeaseGuard {
    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        let client = self.client.clone();
        let lease_id = self.lease_id;
        // Spawn a task to revoke the lease asynchronously since Drop is sync.
        tokio::spawn(async move {
            let mut c = client.lock().await;
            if let Err(e) = c.lease_revoke(lease_id).await {
                tracing::warn!(
                    lease_id,
                    error = %e,
                    "failed to revoke orphaned lease during registration cleanup — lease will expire via TTL"
                );
            } else {
                tracing::debug!(
                    lease_id,
                    "revoked orphaned lease after registration failure"
                );
            }
        });
    }
}

#[async_trait]
impl RunnerStorage for EtcdRunnerStorage {
    async fn register(&self, runner: &Runner) -> Result<MachineId, ClusterError> {
        // Phase 1: Grant lease and put runner key.
        // Acquire client mutex only for the duration of each individual operation,
        // releasing it between steps to avoid blocking other etcd operations.
        let lease_id = {
            let mut client = self.client.lock().await;
            let lease = client
                .lease_grant(self.lease_ttl, None::<LeaseGrantOptions>)
                .await
                .map_err(Self::map_err)?;
            lease.id()
        };

        // Guard: revoke the lease if any subsequent phase fails, preventing
        // orphaned leases that consume server-side resources until TTL expiry.
        let mut lease_guard = LeaseGuard {
            client: self.client.clone(),
            lease_id,
            disarmed: false,
        };

        // Serialize runner data.
        let data = serde_json::to_vec(runner).map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to serialize runner: {e}"),
            source: Some(Box::new(e)),
        })?;

        // Put runner key with lease attachment.
        {
            let key = self.runner_key(&runner.address);
            let mut client = self.client.lock().await;
            client
                .put(key, data, Some(PutOptions::new().with_lease(lease_id)))
                .await
                .map_err(Self::map_err)?;
        }

        // Phase 2: Atomically increment machine ID counter using etcd CAS transaction.
        // Each CAS attempt acquires and releases the client mutex independently,
        // so other etcd operations (acquire, refresh, health checks) can proceed
        // between retries.
        let mid_key = self.machine_id_key();
        let next;
        const MAX_CAS_RETRIES: u32 = 100;
        let mut cas_attempt = 0u32;
        loop {
            cas_attempt += 1;
            if cas_attempt > MAX_CAS_RETRIES {
                return Err(ClusterError::PersistenceError {
                    reason: format!(
                        "machine ID CAS failed after {MAX_CAS_RETRIES} retries — extreme contention on key '{mid_key}'"
                    ),
                    source: None,
                });
            }

            let cas_result = {
                let mut client = self.client.lock().await;
                let resp = client
                    .get(mid_key.as_bytes(), None)
                    .await
                    .map_err(Self::map_err)?;
                let (current, create_revision, mod_revision) = if let Some(kv) = resp.kvs().first()
                {
                    let val = std::str::from_utf8(kv.value())
                        .map_err(|e| ClusterError::PersistenceError {
                            reason: format!(
                                "machine ID counter at key '{}' contains invalid UTF-8: {e}",
                                mid_key
                            ),
                            source: Some(Box::new(e)),
                        })?
                        .parse::<i32>()
                        .map_err(|e| ClusterError::PersistenceError {
                            reason: format!(
                                "machine ID counter at key '{}' contains non-integer value: {e}",
                                mid_key
                            ),
                            source: Some(Box::new(e)),
                        })?;
                    (val, kv.create_revision(), kv.mod_revision())
                } else {
                    (0, 0, 0)
                };
                let candidate = current + 1;

                let txn = if create_revision == 0 {
                    // Key doesn't exist yet — only create if still absent.
                    Txn::new()
                        .when([Compare::create_revision(
                            mid_key.clone(),
                            CompareOp::Equal,
                            0,
                        )])
                        .and_then([TxnOp::put(mid_key.clone(), candidate.to_string(), None)])
                } else {
                    // Key exists — only update if mod_revision hasn't changed.
                    Txn::new()
                        .when([Compare::mod_revision(
                            mid_key.clone(),
                            CompareOp::Equal,
                            mod_revision,
                        )])
                        .and_then([TxnOp::put(mid_key.clone(), candidate.to_string(), None)])
                };

                let txn_resp = client.txn(txn).await.map_err(Self::map_err)?;
                if txn_resp.succeeded() {
                    Some(candidate)
                } else {
                    None
                }
                // client mutex released here
            };

            if let Some(candidate) = cas_result {
                next = candidate;
                break;
            }

            // CAS failed — another runner incremented concurrently. Retry with backoff.
            tracing::debug!(
                attempt = cas_attempt,
                "etcd machine ID CAS conflict, retrying"
            );
            // Exponential backoff: 1ms, 2ms, 4ms, ... capped at 100ms.
            let backoff_ms = (1u64 << cas_attempt.min(6)).min(100);
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        }

        // Phase 3: Start keep-alive for the lease.
        let client_clone = self.client.clone();
        let keep_alive_interval = (self.lease_ttl as u64).max(3) / 3;
        let health_tx = self.lease_health_tx.clone();
        let handle = tokio::spawn(async move {
            // Maximum consecutive keep-alive failures before the task gives up.
            // At the default keep-alive interval (~10s for a 30s TTL), 100 failures
            // means ~16 minutes of sustained etcd unreachability before giving up.
            const MAX_CONSECUTIVE_FAILURES: u32 = 100;
            let mut consecutive_failures: u32 = 0;

            // Publish health update to subscribers.
            fn publish_health(tx: &broadcast::Sender<LeaseHealth>, healthy: bool, failures: u32) {
                // Ignore send errors — no subscribers means no one cares.
                let _ = tx.send(LeaseHealth {
                    healthy,
                    failure_streak: failures,
                });
            }

            loop {
                let result = {
                    let mut c = client_clone.lock().await;
                    c.lease_keep_alive(lease_id).await
                };
                match result {
                    Ok((mut keeper, mut stream)) => {
                        // Successful initialization resets the failure counter.
                        let previous_failures = consecutive_failures;
                        if previous_failures > 0 {
                            tracing::info!(
                                lease_id,
                                previous_failures,
                                "etcd keep-alive recovered"
                            );
                        }
                        consecutive_failures = 0;
                        publish_health(&health_tx, true, 0);

                        loop {
                            tokio::time::sleep(std::time::Duration::from_secs(keep_alive_interval))
                                .await;
                            if let Err(e) = keeper.keep_alive().await {
                                tracing::warn!(lease_id, error = %e, "etcd lease keep-alive failed, reconnecting");
                                consecutive_failures += 1;
                                publish_health(&health_tx, false, consecutive_failures);
                                break;
                            }
                            // Consume the response.
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                stream.message(),
                            )
                            .await
                            {
                                Ok(Ok(Some(_))) => {
                                    // Successful keep-alive.
                                    if consecutive_failures > 0 {
                                        tracing::info!(
                                            lease_id,
                                            previous_failures = consecutive_failures,
                                            "etcd keep-alive healthy after degradation"
                                        );
                                        consecutive_failures = 0;
                                        publish_health(&health_tx, true, 0);
                                    }
                                }
                                Ok(Ok(None)) => {
                                    tracing::warn!(lease_id, "etcd keep-alive stream ended");
                                    consecutive_failures += 1;
                                    publish_health(&health_tx, false, consecutive_failures);
                                    break;
                                }
                                Ok(Err(e)) => {
                                    tracing::warn!(lease_id, error = %e, "etcd keep-alive stream error");
                                    consecutive_failures += 1;
                                    publish_health(&health_tx, false, consecutive_failures);
                                    break;
                                }
                                Err(_) => {
                                    tracing::warn!(lease_id, "etcd keep-alive response timed out");
                                    consecutive_failures += 1;
                                    publish_health(&health_tx, false, consecutive_failures);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(lease_id, error = %e, "etcd keep-alive initialization failed, retrying in 1s");
                        consecutive_failures += 1;
                        publish_health(&health_tx, false, consecutive_failures);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    tracing::error!(
                        lease_id,
                        consecutive_failures,
                        "etcd keep-alive exhausted after {MAX_CONSECUTIVE_FAILURES} consecutive failures — \
                         lease will expire and runner registration will be lost"
                    );
                    // Final unhealthy notification before giving up.
                    publish_health(&health_tx, false, consecutive_failures);
                    break;
                }
            }
        });

        // Disarm the lease guard — registration succeeded, lease is now managed
        // by the keep-alive task.
        lease_guard.disarm();

        *self.lease_id.lock().await = Some(lease_id);
        *self.keep_alive_handle.lock() = Some(handle);

        Ok(MachineId::wrapping(next))
    }

    async fn unregister(&self, address: &RunnerAddress) -> Result<(), ClusterError> {
        // Revoking the lease deletes the runner key and all shard locks atomically.
        self.release_all(address).await
    }

    async fn get_runners(&self) -> Result<Vec<Runner>, ClusterError> {
        let mut client = self.client.lock().await;
        let prefix = self.runners_prefix();
        let resp = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(Self::map_err)?;

        let mut runners = Vec::new();
        for kv in resp.kvs() {
            match serde_json::from_slice::<Runner>(kv.value()) {
                Ok(runner) => runners.push(runner),
                Err(e) => {
                    tracing::warn!(
                        key = ?String::from_utf8_lossy(kv.key()),
                        error = %e,
                        "skipping malformed runner entry in etcd"
                    );
                }
            }
        }
        Ok(runners)
    }

    /// Set runner health using a CAS (compare-and-swap) transaction to prevent
    /// concurrent `set_runner_health` calls from overwriting each other's changes.
    /// Retries on conflict (another caller updated the same key concurrently).
    async fn set_runner_health(
        &self,
        address: &RunnerAddress,
        healthy: bool,
    ) -> Result<(), ClusterError> {
        let key = self.runner_key(address);
        const MAX_CAS_RETRIES: u32 = 10;

        for attempt in 1..=MAX_CAS_RETRIES {
            let mut client = self.client.lock().await;

            // Get current runner data with mod_revision for CAS.
            let resp = client
                .get(key.as_bytes(), None)
                .await
                .map_err(Self::map_err)?;
            let Some(kv) = resp.kvs().first() else {
                // Runner key doesn't exist — nothing to update.
                return Ok(());
            };

            let mut runner = serde_json::from_slice::<Runner>(kv.value()).map_err(|e| {
                ClusterError::PersistenceError {
                    reason: format!("failed to deserialize runner at key '{key}': {e}"),
                    source: Some(Box::new(e)),
                }
            })?;

            if runner.healthy == healthy {
                // Already at desired state — no update needed.
                return Ok(());
            }

            runner.healthy = healthy;
            let data = serde_json::to_vec(&runner).map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to serialize runner: {e}"),
                source: Some(Box::new(e)),
            })?;

            // Preserve the existing key's lease ID so we don't re-attach it
            // to the calling runner's lease (which would be wrong when setting
            // health for a different runner).
            let existing_lease = kv.lease();
            let opts = if existing_lease != 0 {
                PutOptions::new().with_lease(existing_lease)
            } else {
                PutOptions::default()
            };

            let mod_revision = kv.mod_revision();

            // CAS: only update if mod_revision hasn't changed since our read.
            let txn = Txn::new()
                .when([Compare::mod_revision(
                    key.as_bytes(),
                    CompareOp::Equal,
                    mod_revision,
                )])
                .and_then([TxnOp::put(key.as_bytes(), data, Some(opts))]);

            let txn_resp = client.txn(txn).await.map_err(Self::map_err)?;
            if txn_resp.succeeded() {
                return Ok(());
            }

            // CAS conflict — another caller updated this key concurrently. Retry.
            drop(client);
            tracing::debug!(
                attempt,
                runner_address = %address,
                "set_runner_health CAS conflict, retrying"
            );
            let backoff_ms = (1u64 << attempt.min(5)).min(50);
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        }

        Err(ClusterError::PersistenceError {
            reason: format!(
                "set_runner_health CAS failed after {MAX_CAS_RETRIES} retries for runner {address}"
            ),
            source: None,
        })
    }

    async fn acquire(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<bool, ClusterError> {
        let mut client = self.client.lock().await;
        let key = self.shard_key(shard_id);
        let value = format!("{}:{}", runner.host, runner.port);

        // Attach the runner's lease to shard lock keys so they auto-expire on crash.
        let lease_id = self.lease_id.lock().await;
        let put_opts = lease_id.map(|id| PutOptions::new().with_lease(id));

        // Use a transaction: if key doesn't exist (create_revision == 0), create it.
        // If key exists with our value, it's already ours.
        let txn = Txn::new()
            .when([Compare::create_revision(
                key.as_bytes(),
                CompareOp::Equal,
                0,
            )])
            .and_then([TxnOp::put(key.as_bytes(), value.as_bytes(), put_opts)])
            .or_else([TxnOp::get(key.as_bytes(), None)]);

        let resp = client.txn(txn).await.map_err(Self::map_err)?;

        if resp.succeeded() {
            // Key was created — we acquired the lock.
            return Ok(true);
        }

        // Key already exists. Check if it's ours.
        for op_resp in resp.op_responses() {
            if let etcd_client::TxnOpResponse::Get(get_resp) = op_resp {
                if let Some(kv) = get_resp.kvs().first() {
                    let existing = std::str::from_utf8(kv.value()).map_err(|e| {
                        tracing::warn!(
                            shard = %shard_id,
                            error = %e,
                            "shard lock value contains non-UTF-8 data"
                        );
                        ClusterError::PersistenceError {
                            reason: format!(
                                "shard lock for {shard_id} contains non-UTF-8 value: {e}"
                            ),
                            source: Some(Box::new(e)),
                        }
                    })?;
                    if existing == value {
                        return Ok(true); // Already held by us.
                    }
                }
            }
        }

        Ok(false)
    }

    /// Batch acquire multiple shard locks. Releases mutexes between individual
    /// shard operations to reduce contention — other etcd operations (health
    /// checks, other runners) can proceed between shards.
    async fn acquire_batch(
        &self,
        shard_ids: &[ShardId],
        runner: &RunnerAddress,
    ) -> Result<BatchAcquireResult, ClusterError> {
        if shard_ids.is_empty() {
            return Ok(BatchAcquireResult {
                acquired: Vec::new(),
                failures: Vec::new(),
            });
        }

        let value = format!("{}:{}", runner.host, runner.port);

        let mut acquired = Vec::new();
        let mut failures = Vec::new();
        for shard_id in shard_ids {
            let key = self.shard_key(shard_id);

            // Acquire mutexes per-shard to allow other operations between shards.
            let mut client = self.client.lock().await;
            let lease_id = self.lease_id.lock().await;
            let put_opts = lease_id.map(|id| PutOptions::new().with_lease(id));
            drop(lease_id);

            let txn = Txn::new()
                .when([Compare::create_revision(
                    key.as_bytes(),
                    CompareOp::Equal,
                    0,
                )])
                .and_then([TxnOp::put(key.as_bytes(), value.as_bytes(), put_opts)])
                .or_else([TxnOp::get(key.as_bytes(), None)]);

            match client.txn(txn).await {
                Ok(resp) => {
                    if resp.succeeded() {
                        acquired.push(shard_id.clone());
                    } else {
                        // Check if already held by us
                        let mut is_ours = false;
                        for op_resp in resp.op_responses() {
                            if let etcd_client::TxnOpResponse::Get(get_resp) = op_resp {
                                if let Some(kv) = get_resp.kvs().first() {
                                    match std::str::from_utf8(kv.value()) {
                                        Ok(existing) => {
                                            if existing == value {
                                                is_ours = true;
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                shard_id = %shard_id,
                                                error = %e,
                                                "non-UTF-8 shard lock value in acquire_batch"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        if is_ours {
                            acquired.push(shard_id.clone());
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        shard_id = %shard_id,
                        error = %e,
                        "failed to acquire shard in batch"
                    );
                    failures.push((
                        shard_id.clone(),
                        ClusterError::PersistenceError {
                            reason: format!("etcd acquire failed for shard {shard_id}: {e}"),
                            source: Some(Box::new(e)),
                        },
                    ));
                }
            }
        }

        Ok(BatchAcquireResult { acquired, failures })
    }

    /// Refresh the shard lock by verifying ownership and re-putting the key
    /// with the runner's lease to ensure the TTL is actively maintained.
    /// Returns `true` if the lock is still held by this runner, `false` otherwise.
    async fn refresh(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<bool, ClusterError> {
        let mut client = self.client.lock().await;
        let key = self.shard_key(shard_id);
        let value = format!("{}:{}", runner.host, runner.port);

        // Use a CAS transaction: only re-put if the key's current value matches
        // our runner address. This both verifies ownership and refreshes the
        // lease attachment (extending TTL).
        let lease_id = self.lease_id.lock().await;
        let put_opts = lease_id.map(|id| PutOptions::new().with_lease(id));

        let txn = Txn::new()
            .when([Compare::value(
                key.as_bytes(),
                CompareOp::Equal,
                value.as_bytes(),
            )])
            .and_then([TxnOp::put(key.as_bytes(), value.as_bytes(), put_opts)]);

        let resp = client.txn(txn).await.map_err(Self::map_err)?;
        Ok(resp.succeeded())
    }

    /// Batch refresh: releases mutexes between individual shard operations
    /// to reduce contention — other etcd operations can proceed between shards.
    async fn refresh_batch(
        &self,
        shard_ids: &[ShardId],
        runner: &RunnerAddress,
    ) -> Result<BatchRefreshResult, ClusterError> {
        let value = format!("{}:{}", runner.host, runner.port);

        let mut refreshed = Vec::new();
        let mut lost = Vec::new();
        let mut failures = Vec::new();

        for shard_id in shard_ids {
            let key = self.shard_key(shard_id);

            // Acquire mutexes per-shard to allow other operations between shards.
            let mut client = self.client.lock().await;
            let lease_id = self.lease_id.lock().await;
            let put_opts = lease_id.map(|id| PutOptions::new().with_lease(id));
            drop(lease_id);

            let txn = Txn::new()
                .when([Compare::value(
                    key.as_bytes(),
                    CompareOp::Equal,
                    value.as_bytes(),
                )])
                .and_then([TxnOp::put(key.as_bytes(), value.as_bytes(), put_opts)]);

            match client.txn(txn).await {
                Ok(resp) => {
                    if resp.succeeded() {
                        refreshed.push(shard_id.clone());
                    } else {
                        lost.push(shard_id.clone());
                    }
                }
                Err(e) => {
                    tracing::warn!(shard_id = %shard_id, error = %e, "failed to refresh shard in batch");
                    failures.push((shard_id.clone(), Self::map_err(e)));
                }
            }
        }

        Ok(BatchRefreshResult {
            refreshed,
            lost,
            failures,
        })
    }

    async fn release(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<(), ClusterError> {
        let mut client = self.client.lock().await;
        let key = self.shard_key(shard_id);
        let value = format!("{}:{}", runner.host, runner.port);

        // Only delete if we hold the lock (compare value).
        let txn = Txn::new()
            .when([Compare::value(
                key.as_bytes(),
                CompareOp::Equal,
                value.as_bytes(),
            )])
            .and_then([TxnOp::delete(key.as_bytes(), None)]);

        client.txn(txn).await.map_err(Self::map_err)?;
        Ok(())
    }

    async fn release_all(&self, _runner: &RunnerAddress) -> Result<(), ClusterError> {
        // Cancel keep-alive task first to stop refreshing the lease.
        if let Some(handle) = self.keep_alive_handle.lock().take() {
            handle.abort();
        }

        // Revoke the lease. This atomically deletes ALL keys attached to it:
        // - The runner registration key
        // - All shard lock keys
        // This is O(1) instead of O(shards) and completes in a single etcd operation.
        if let Some(lease_id) = self.lease_id.lock().await.take() {
            let mut client = self.client.lock().await;
            if let Err(e) = client.lease_revoke(lease_id).await {
                tracing::warn!(
                    lease_id,
                    error = %e,
                    "release_all: lease revocation failed — keys will expire via TTL"
                );
            }
        }

        Ok(())
    }

    async fn watch_runners(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Vec<Runner>> + Send>>, ClusterError> {
        let mut client = self.client.lock().await;
        let prefix = self.runners_prefix();

        let (mut watcher, watch_stream) = client
            .watch(prefix.as_bytes(), Some(WatchOptions::new().with_prefix()))
            .await
            .map_err(Self::map_err)?;

        // We need a separate client for fetching all runners on each change.
        let client_clone = self.client.clone();
        let runners_prefix = self.runners_prefix();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Spawn a task that converts watch events into full runner list snapshots.
        // On stream end or error, reconnects with exponential backoff to prevent
        // consumers from operating on stale runner data indefinitely.
        tokio::spawn(async move {
            let mut stream = watch_stream;
            let mut consecutive_failures: u32 = 0;
            /// Maximum consecutive reconnection failures before giving up.
            const MAX_RECONNECT_FAILURES: u32 = 50;
            /// Maximum backoff between reconnection attempts (30 seconds).
            const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

            'outer: loop {
                loop {
                    let msg = stream.message().await;
                    match msg {
                        Ok(Some(resp)) => {
                            // Successful message resets the failure counter.
                            consecutive_failures = 0;

                            // Check if there were any relevant events.
                            let has_events = resp.events().iter().any(|e| {
                                matches!(e.event_type(), EventType::Put | EventType::Delete)
                            });
                            if !has_events {
                                continue;
                            }

                            // Fetch current runners.
                            let mut c = client_clone.lock().await;
                            match c
                                .get(
                                    runners_prefix.as_str(),
                                    Some(GetOptions::new().with_prefix()),
                                )
                                .await
                            {
                                Ok(get_resp) => {
                                    let runners: Vec<Runner> = get_resp
                                        .kvs()
                                        .iter()
                                        .filter_map(|kv| {
                                            match serde_json::from_slice(kv.value()) {
                                                Ok(runner) => Some(runner),
                                                Err(e) => {
                                                    let key =
                                                        kv.key_str().unwrap_or("<non-utf8>");
                                                    tracing::warn!(
                                                        key = %key,
                                                        error = %e,
                                                        "failed to deserialize runner in watch snapshot, skipping"
                                                    );
                                                    None
                                                }
                                            }
                                        })
                                        .collect();
                                    if tx.send(runners).is_err() {
                                        break 'outer;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "etcd get failed after watch event — consumers may see stale runner data"
                                    );
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::warn!("etcd watch stream ended, attempting reconnection");
                            break; // Break inner loop to reconnect
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "etcd watch stream error, attempting reconnection");
                            break; // Break inner loop to reconnect
                        }
                    }
                }

                // Cancel the old watch to prevent server-side resource leaks.
                if let Err(e) = watcher.cancel().await {
                    tracing::warn!(error = %e, "failed to cancel etcd watch before reconnection");
                }

                // Reconnect with exponential backoff.
                consecutive_failures += 1;
                if consecutive_failures >= MAX_RECONNECT_FAILURES {
                    tracing::error!(
                        consecutive_failures,
                        "etcd watch_runners exhausted after {MAX_RECONNECT_FAILURES} consecutive reconnection failures — \
                         consumers will operate on stale runner data"
                    );
                    break 'outer;
                }

                let backoff = std::cmp::min(
                    std::time::Duration::from_millis(
                        500u64.saturating_mul(1u64 << consecutive_failures.min(10)),
                    ),
                    MAX_BACKOFF,
                );
                tracing::info!(
                    attempt = consecutive_failures,
                    backoff_ms = backoff.as_millis() as u64,
                    "reconnecting etcd watch_runners"
                );
                tokio::time::sleep(backoff).await;

                // Re-create the watch.
                let mut c = client_clone.lock().await;
                match c
                    .watch(
                        runners_prefix.as_bytes(),
                        Some(WatchOptions::new().with_prefix()),
                    )
                    .await
                {
                    Ok((new_watcher, new_stream)) => {
                        watcher = new_watcher;
                        stream = new_stream;
                        tracing::info!("etcd watch_runners reconnected successfully");

                        // After reconnection, send an immediate snapshot so consumers
                        // get caught up on any changes missed during the reconnection window.
                        match c
                            .get(
                                runners_prefix.as_str(),
                                Some(GetOptions::new().with_prefix()),
                            )
                            .await
                        {
                            Ok(get_resp) => {
                                let runners: Vec<Runner> = get_resp
                                    .kvs()
                                    .iter()
                                    .filter_map(|kv| {
                                        match serde_json::from_slice(kv.value()) {
                                            Ok(runner) => Some(runner),
                                            Err(e) => {
                                                let key =
                                                    kv.key_str().unwrap_or("<non-utf8>");
                                                tracing::warn!(
                                                    key = %key,
                                                    error = %e,
                                                    "failed to deserialize runner in reconnection snapshot, skipping"
                                                );
                                                None
                                            }
                                        }
                                    })
                                    .collect();
                                if tx.send(runners).is_err() {
                                    break 'outer;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "etcd get failed after watch reconnection — consumers may see stale runner data"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            attempt = consecutive_failures,
                            "etcd watch reconnection failed"
                        );
                        // Continue outer loop to retry with increased backoff
                    }
                }
            }

            // Final cleanup: cancel the server-side watch to prevent resource leaks.
            if let Err(e) = watcher.cancel().await {
                tracing::warn!(error = %e, "failed to cancel etcd watch on task exit");
            }
        });

        Ok(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        ))
    }

    fn lease_health_receiver(&self) -> Option<broadcast::Receiver<LeaseHealth>> {
        Some(self.lease_health_tx.subscribe())
    }
}

#[async_trait]
impl RunnerHealth for EtcdRunnerStorage {
    async fn is_alive(&self, address: &RunnerAddress) -> Result<bool, ClusterError> {
        let mut client = self.client.lock().await;
        let key = self.runner_key(address);
        let resp = client
            .get(key.as_bytes(), None)
            .await
            .map_err(Self::map_err)?;
        // If the key exists, the runner's lease is still alive.
        Ok(!resp.kvs().is_empty())
    }
}

impl Drop for EtcdRunnerStorage {
    fn drop(&mut self) {
        // Abort keep-alive task. `parking_lot::Mutex::lock()` never fails
        // (no poisoning), so this always succeeds — no silent leak.
        if let Some(h) = self.keep_alive_handle.lock().take() {
            h.abort();
        }
    }
}
