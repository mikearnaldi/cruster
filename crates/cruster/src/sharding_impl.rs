use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use futures::future::BoxFuture;
use tokio::sync::{broadcast, mpsc, Notify, RwLock};
use tokio_stream::Stream;
use tracing::instrument;

use crate::config::ShardingConfig;
use crate::detachment::{DetachedState, DetachmentReason};
use crate::durable::{WorkflowEngine, WorkflowStorage, INTERRUPT_SIGNAL};
use crate::entity::Entity;
use crate::entity_client::EntityClient;
use crate::entity_manager::EntityManager;
use crate::entity_reaper::EntityReaper;
use crate::envelope::{AckChunk, Envelope, EnvelopeRequest, Interrupt};
use crate::error::ClusterError;
use crate::hash::shard_for_entity;
use crate::message::{IncomingMessage, ReplyReceiver};
use crate::message_storage::{MessageStorage, SaveResult};
use crate::metrics::ClusterMetrics;
use crate::reply::{
    dead_letter_reply_id, fallback_reply_id, ExitResult, Reply, ReplyWithExit, EXIT_SEQUENCE,
};
use crate::runner::Runner;
use crate::runner_health::RunnerHealth;
use crate::runner_storage::{LeaseHealth, RunnerStorage};
use crate::runners::Runners;
use crate::shard_assigner::ShardAssigner;
use crate::sharding::{Sharding, ShardingRegistrationEvent};
use crate::singleton::SingletonContext;
use crate::snowflake::{Snowflake, SnowflakeGenerator};
use crate::types::{EntityId, EntityType, RunnerAddress, ShardId};

/// Implementation of the core sharding orchestrator.
///
/// Manages shard ownership, entity lifecycle, and message routing.
/// Supports two modes:
/// - **Single-node**: Call `acquire_all_shards()` to immediately own all shards.
/// - **Multi-runner**: Call `start().await` to begin the shard acquisition loop with
///   HashRing-based assignment, lock refresh, and remote routing via `Runners`.
pub struct ShardingImpl {
    config: Arc<ShardingConfig>,
    snowflake: Arc<SnowflakeGenerator>,
    runners: Arc<dyn Runners>,
    runner_storage: Option<Arc<dyn RunnerStorage>>,
    runner_health: Option<Arc<dyn RunnerHealth>>,
    message_storage: Option<Arc<dyn MessageStorage>>,
    /// Optional key-value storage for persisted entity state.
    state_storage: Option<Arc<dyn WorkflowStorage>>,
    /// Optional workflow engine for durable context support.
    workflow_engine: Option<Arc<dyn WorkflowEngine>>,
    metrics: Arc<ClusterMetrics>,

    /// Entity managers keyed by entity type name.
    entity_managers: Arc<DashMap<EntityType, Arc<EntityManager>>>,

    /// Set of shard IDs currently owned (acquired) by this runner.
    owned_shards: Arc<RwLock<HashSet<ShardId>>>,

    /// Shard-to-runner assignments computed from HashRing.
    /// Updated by the shard acquisition loop. Empty in single-node mode.
    shard_assignments: RwLock<HashMap<ShardId, RunnerAddress>>,

    /// Shards currently being released (prevents re-acquisition during release).
    releasing_shards: RwLock<HashSet<ShardId>>,

    /// Entity reaper for idle cleanup.
    reaper: Arc<EntityReaper>,

    /// Registration event broadcaster.
    event_tx: broadcast::Sender<ShardingRegistrationEvent>,

    /// Singleton registry: name -> (cancel token, run factory).
    /// In multi-runner mode, singletons are stored here but only spawned
    /// when their computed shard is owned by this runner.
    singletons: DashMap<String, SingletonEntry>,

    /// Whether shutdown has been initiated.
    shutdown: AtomicBool,

    /// Cancellation for background tasks.
    cancel: tokio_util::sync::CancellationToken,

    /// Notifier to trigger storage polling.
    storage_poll_notify: Arc<Notify>,

    /// Serializes storage polls to prevent concurrent dispatch from the
    /// background poll loop and explicit `poll_storage` calls.
    storage_poll_lock: tokio::sync::Mutex<()>,

    /// Notifier signalled when a new entity type is registered.
    /// Used by `route_local` to wait for late entity registrations during startup.
    entity_registration_notify: Notify,

    /// JoinHandles for background tasks, awaited during shutdown.
    background_tasks: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,

    /// Semaphore (permits=1) coordinating storage poll and resumption tasks.
    /// The poll loop acquires this during dispatch, and resumption tasks acquire
    /// it when checking if remaining == 0 before exiting. This prevents the race
    /// where a poll adds a new message to `resumption_unprocessed` between the
    /// resumption task's emptiness check and its exit, which would orphan the message.
    /// Matches the TS `storageReadLock` (Sharding.ts).
    storage_read_lock: Arc<tokio::sync::Semaphore>,

    /// Per-entity resumption state: tracks request IDs that encountered MailboxFull
    /// during storage polling. A dedicated resumption task per entity re-fetches
    /// and retries delivery with backoff.
    resumption_unprocessed:
        Arc<DashMap<crate::types::EntityAddress, DashSet<crate::snowflake::Snowflake>>>,

    /// Tracks which entity addresses have an active resumption task running.
    /// Prevents spawning duplicate resumption tasks for the same entity.
    resumption_active: Arc<DashSet<crate::types::EntityAddress>>,

    /// JoinHandles for per-entity resumption tasks, awaited during shutdown.
    resumption_handles: Arc<DashMap<crate::types::EntityAddress, tokio::task::JoinHandle<()>>>,

    /// Tracks first-seen time for unprocessed messages with unregistered entity types.
    /// After `entity_registration_timeout` elapses, a failure reply is saved to storage
    /// to prevent permanent message accumulation for removed or mistyped entity types.
    /// Key is (entity_type, request_id) to deduplicate per-message tracking.
    unregistered_first_seen: DashMap<(EntityType, crate::snowflake::Snowflake), Instant>,

    /// Serializes `sync_singletons` and `register_singleton` to prevent races where
    /// concurrent execution can cause missed spawns or brief double-runs.
    /// Matches the TS `withSingletonLock` semaphore (Sharding.ts).
    singleton_lock: tokio::sync::Mutex<()>,

    /// Self-reference for passing to entity managers.
    ///
    /// Initialized immediately after Arc construction. Allows entities to get
    /// access to the sharding interface for inter-entity communication and
    /// scheduled messages.
    self_ref: OnceLock<Weak<ShardingImpl>>,

    /// Detachment state tracking for storage connectivity.
    ///
    /// When detached, the runner clears owned shards and pauses acquisition/refresh
    /// loops to prevent split-brain scenarios where multiple runners execute the
    /// same shard concurrently.
    detached_state: Arc<DetachedState>,
}

/// A reusable singleton factory function that can be called multiple times
/// to (re)start the singleton after shard round-trips.
///
/// The factory receives a [`SingletonContext`] containing a cancellation token
/// for graceful shutdown.
type SingletonFactory =
    Arc<dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>> + Send + Sync>;

/// A registered singleton entry.
struct SingletonEntry {
    /// Cancellation token for the running singleton task (if any).
    cancel: tokio_util::sync::CancellationToken,
    /// Whether this singleton is currently running.
    running: Arc<std::sync::atomic::AtomicBool>,
    /// The factory to spawn the singleton. Reusable across re-spawns.
    factory: SingletonFactory,
    /// JoinHandle for the running singleton task (if any).
    /// Used during shutdown to await task completion.
    handle: Option<tokio::task::JoinHandle<()>>,
    /// Shard group for this singleton. Defaults to "default".
    /// Used to compute the shard ID directly via `shard_for_entity(name, shards_per_group)`
    /// without going through entity manager lookup.
    shard_group: String,
    /// Number of consecutive failures (Err or panic) without a successful run.
    /// Used for exponential backoff before re-spawning failed singletons.
    /// Shared with the spawned task so it can increment on failure.
    consecutive_failures: Arc<std::sync::atomic::AtomicU32>,
    /// Epoch millis of the last failure. 0 = no failure recorded.
    /// Shared with the spawned task so it can record failure time.
    last_failure_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Whether the singleton has opted in to manage its own cancellation.
    /// Shared with the spawned task's SingletonContext.
    /// If true, sync_singletons will await the handle when cancelling.
    managed: Arc<std::sync::atomic::AtomicBool>,
}

impl ShardingImpl {
    /// Create a new ShardingImpl.
    ///
    /// For a single-node setup (e.g. testing), call `acquire_all_shards()` after creation
    /// to immediately own all configured shards.
    ///
    /// For multi-runner mode, provide `runner_storage` and call `start().await` to begin
    /// the shard acquisition loop.
    pub fn new(
        config: Arc<ShardingConfig>,
        runners: Arc<dyn Runners>,
        runner_storage: Option<Arc<dyn RunnerStorage>>,
        runner_health: Option<Arc<dyn RunnerHealth>>,
        message_storage: Option<Arc<dyn MessageStorage>>,
        metrics: Arc<ClusterMetrics>,
    ) -> Result<Arc<Self>, ClusterError> {
        Self::new_with_engines(
            config,
            runners,
            runner_storage,
            runner_health,
            message_storage,
            None,
            None,
            metrics,
        )
    }

    /// Create a new ShardingImpl with optional workflow engine and state storage.
    ///
    /// Use this when you need durable context support or persisted entity state.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_engines(
        config: Arc<ShardingConfig>,
        runners: Arc<dyn Runners>,
        runner_storage: Option<Arc<dyn RunnerStorage>>,
        runner_health: Option<Arc<dyn RunnerHealth>>,
        message_storage: Option<Arc<dyn MessageStorage>>,
        state_storage: Option<Arc<dyn WorkflowStorage>>,
        workflow_engine: Option<Arc<dyn WorkflowEngine>>,
        metrics: Arc<ClusterMetrics>,
    ) -> Result<Arc<Self>, ClusterError> {
        config.validate()?;
        if let Some(storage) = message_storage.as_ref() {
            storage.set_max_retries(config.storage_message_max_retries);
        }
        let (event_tx, _) = broadcast::channel(64);
        let snowflake = Arc::new(SnowflakeGenerator::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let reaper = Arc::new(EntityReaper::new(cancel.clone()));
        let detached_state = Arc::new(DetachedState::new(config.detachment_recover_window));

        let this = Arc::new(Self {
            config,
            snowflake,
            runners,
            runner_storage,
            runner_health,
            message_storage,
            state_storage,
            workflow_engine,
            metrics,
            entity_managers: Arc::new(DashMap::new()),
            owned_shards: Arc::new(RwLock::new(HashSet::new())),
            shard_assignments: RwLock::new(HashMap::new()),
            releasing_shards: RwLock::new(HashSet::new()),
            reaper,
            event_tx,
            singletons: DashMap::new(),
            shutdown: AtomicBool::new(false),
            cancel,
            storage_poll_notify: Arc::new(Notify::new()),
            entity_registration_notify: Notify::new(),
            background_tasks: tokio::sync::Mutex::new(Vec::new()),
            storage_read_lock: Arc::new(tokio::sync::Semaphore::new(1)),
            storage_poll_lock: tokio::sync::Mutex::new(()),
            resumption_unprocessed: Arc::new(DashMap::new()),
            resumption_active: Arc::new(DashSet::new()),
            resumption_handles: Arc::new(DashMap::new()),
            unregistered_first_seen: DashMap::new(),
            singleton_lock: tokio::sync::Mutex::new(()),
            self_ref: OnceLock::new(),
            detached_state,
        });

        // Initialize self-reference for entity managers to access sharding.
        // Ignore error since OnceLock is newly created and cannot fail.
        let _ = this.self_ref.set(Arc::downgrade(&this));

        // Start reaper background task.
        // The reaper dynamically adapts its check interval to the shortest registered
        // idle time (floored at 5s), matching TS entityReaper.ts behavior.
        let reaper_clone = Arc::clone(&this.reaper);
        let reaper_handle = tokio::spawn(async move {
            reaper_clone.run().await;
        });

        // Store handle. try_lock cannot fail here — we just created the mutex and hold the
        // only Arc, so there is no contention. Using expect() instead of silently dropping.
        this.background_tasks
            .try_lock()
            .expect("background_tasks lock should be uncontested during initialization")
            .push(reaper_handle);

        if this.runner_storage.is_none() && this.message_storage.is_some() {
            let this_clone = Arc::clone(&this);
            let poll_handle = tokio::spawn(async move {
                this_clone.storage_poll_loop().await;
            });
            this.background_tasks
                .try_lock()
                .expect("background_tasks lock should be uncontested during initialization")
                .push(poll_handle);
        }

        Ok(this)
    }

    /// Start multi-runner background loops:
    /// - Shard acquisition loop (watches runners, computes HashRing, acquires/releases shards)
    /// - Lock refresh loop (periodically refreshes shard lock TTLs)
    /// - Storage polling loop (polls for unprocessed persisted messages)
    ///
    /// Requires `runner_storage` to be provided at construction.
    /// For single-node mode, use `acquire_all_shards()` instead.
    pub async fn start(self: &Arc<Self>) -> Result<(), ClusterError> {
        let runner_storage = match &self.runner_storage {
            Some(storage) => Arc::clone(storage),
            None => {
                tracing::warn!(
                    "start() called without runner_storage; no background loops will run"
                );
                return Ok(());
            }
        };

        let runner = Runner::new(
            self.config.runner_address.clone(),
            self.config.runner_weight,
        );
        let machine_id = runner_storage.register(&runner).await?;
        self.snowflake.set_machine_id(machine_id);
        tracing::info!(
            runner = %self.config.runner_address,
            machine_id = %machine_id,
            "registered runner"
        );

        // Start shard acquisition loop
        let this = Arc::clone(self);
        let h1 = tokio::spawn(async move {
            this.shard_acquisition_loop().await;
        });

        // Start lock refresh loop
        let this = Arc::clone(self);
        let h2 = tokio::spawn(async move {
            this.lock_refresh_loop().await;
        });

        // Start storage polling loop
        let this = Arc::clone(self);
        let h3 = tokio::spawn(async move {
            this.storage_poll_loop().await;
        });

        // Store background task handles for graceful shutdown.
        {
            let mut tasks = self.background_tasks.try_lock()
                .expect("background_tasks lock uncontested during start() — called immediately after construction");
            tasks.push(h1);
            tasks.push(h2);
            tasks.push(h3);
        }

        // Start lease health monitoring loop if storage supports it.
        if let Some(health_rx) = runner_storage.lease_health_receiver() {
            let this = Arc::clone(self);
            let h4 = tokio::spawn(async move {
                this.lease_health_loop(health_rx).await;
            });
            self.background_tasks
                .try_lock()
                .expect("background_tasks lock uncontested during start()")
                .push(h4);
            tracing::info!("started lease health monitoring loop");
        }

        // Register runner health loop as a cluster singleton so only one runner
        // in the cluster performs health checks at a time (matches TS source:
        // "effect/cluster/Sharding/RunnerHealth" singleton in Sharding.ts:1339).
        if self.runner_health.is_some() {
            let this = Arc::clone(self);
            // We cannot call async register_singleton from a sync fn, so insert directly.
            let cancel = tokio_util::sync::CancellationToken::new();
            let factory: SingletonFactory = Arc::new(move |ctx| {
                let this = Arc::clone(&this);
                let cancel = ctx.cancellation();
                Box::pin(async move { this.runner_health_loop(cancel).await })
            });
            self.singletons.insert(
                "cluster/RunnerHealth".to_string(),
                SingletonEntry {
                    cancel,
                    running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    factory,
                    handle: None,
                    shard_group: "default".to_string(),
                    consecutive_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
                    last_failure_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                    managed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                },
            );
            // sync_singletons() will spawn it when the appropriate shard is acquired.
            tracing::info!("registered runner health loop as singleton");
        }

        Ok(())
    }

    /// Immediately claim ownership of all shards for all configured shard groups.
    /// This is used for single-node / test scenarios.
    pub async fn acquire_all_shards(&self) {
        let mut owned = self.owned_shards.write().await;
        for group in &self.config.shard_groups {
            for id in 0..self.config.shards_per_group {
                owned.insert(ShardId::new(group, id));
            }
        }
        self.metrics.shards.set(owned.len() as i64);
    }

    /// Check if a shard is locally owned (non-async version using try_read).
    ///
    /// Returns `false` during lock contention (e.g., during rebalance writes).
    /// This is a transient false-negative that self-heals on the next poll cycle.
    ///
    /// This method is only used by the sync `Sharding::has_shard_id` trait method.
    /// All hot-path routing code uses `has_shard_async()` instead, so false negatives
    /// here only affect external callers of the sync trait method.
    fn has_shard_sync(&self, shard_id: &ShardId) -> bool {
        match self.owned_shards.try_read() {
            Ok(guard) => guard.contains(shard_id),
            Err(_) => {
                tracing::trace!(shard_id = ?shard_id, "owned_shards lock contended in has_shard_sync, returning false");
                false
            }
        }
    }

    /// Check if a shard is locally owned (async version, blocks until lock available).
    async fn has_shard_async(&self, shard_id: &ShardId) -> bool {
        self.owned_shards.read().await.contains(shard_id)
    }

    /// Look up the runner that owns a shard (async version, blocks until lock available).
    async fn get_shard_owner_async(&self, shard_id: &ShardId) -> Option<RunnerAddress> {
        self.shard_assignments.read().await.get(shard_id).cloned()
    }

    // -------------------------------------------------------------------------
    // Detachment API
    // -------------------------------------------------------------------------

    /// Check if the runner is currently detached from the cluster.
    ///
    /// When detached, the runner should not execute shard entities and should
    /// pause acquisition/refresh loops.
    #[inline]
    pub fn is_detached(&self) -> bool {
        self.config.detachment_enabled && self.detached_state.is_detached()
    }

    /// Detach from the cluster with the given reason.
    ///
    /// This clears owned shards and interrupts all entities to prevent split-brain.
    /// Returns `true` if this call caused the transition from attached to detached.
    pub async fn detach(&self, reason: DetachmentReason) {
        if !self.config.detachment_enabled {
            tracing::debug!(
                reason = %reason,
                "detachment triggered but detachment_enabled=false, ignoring"
            );
            return;
        }

        let transitioned = self.detached_state.detach(reason);
        if transitioned {
            self.handle_detachment().await;
        }
    }

    /// Signal that a healthy storage operation was observed.
    ///
    /// If detached, this may trigger re-attachment if the recovery window has
    /// elapsed with sustained healthy status.
    pub fn signal_healthy(&self) {
        if !self.config.detachment_enabled {
            return;
        }

        if self.detached_state.maybe_reattach() {
            self.metrics.sharding_detached.set(0);
        }
    }

    /// Signal that an unhealthy storage operation was observed while detached.
    ///
    /// This resets the recovery window, requiring another full recovery period
    /// before re-attachment can occur.
    pub fn signal_unhealthy(&self) {
        if self.config.detachment_enabled {
            self.detached_state.reset_healthy_since();
        }
    }

    /// Handle the transition to detached state.
    ///
    /// Clears owned shards and interrupts all entities to prevent split-brain.
    async fn handle_detachment(&self) {
        self.metrics.sharding_detached.set(1);

        // Clear owned shards
        let shards_to_interrupt: Vec<ShardId> = {
            let mut owned = self.owned_shards.write().await;
            let shards: Vec<_> = owned.drain().collect();
            self.metrics.shards.set(0);
            shards
        };

        // Interrupt all entities on the cleared shards
        for shard_id in &shards_to_interrupt {
            for entry in self.entity_managers.iter() {
                entry.value().interrupt_shard(shard_id).await;
            }
        }

        // Sync singletons to stop any running (shards are now empty)
        self.sync_singletons().await;

        tracing::warn!(
            shards_interrupted = shards_to_interrupt.len(),
            "detachment complete: cleared owned shards and interrupted entities"
        );
    }

    /// Whether an error is retryable during shard rebalancing.
    /// `EntityNotAssignedToRunner` and `RunnerUnavailable` are transient
    /// routing failures that may resolve after shards are reassigned.
    fn is_retryable(err: &ClusterError) -> bool {
        matches!(
            err,
            ClusterError::EntityNotAssignedToRunner { .. } | ClusterError::RunnerUnavailable { .. }
        )
    }

    /// Route a message locally to the appropriate entity manager.
    ///
    /// If the entity type is not yet registered, waits up to
    /// `entity_registration_timeout` for it to appear (matching the TS
    /// `waitForEntityManager` pattern in `Sharding.ts:440-476`).
    async fn route_local(
        &self,
        envelope: EnvelopeRequest,
        reply_tx: Option<mpsc::Sender<crate::reply::Reply>>,
    ) -> Result<(), ClusterError> {
        let entity_type = &envelope.address.entity_type;
        let manager = match self.entity_managers.get(entity_type) {
            Some(m) => m,
            None => {
                // Wait for entity registration with timeout
                let timeout = self.config.entity_registration_timeout;
                tracing::debug!(
                    %entity_type,
                    ?timeout,
                    "entity type not registered, waiting for registration"
                );
                let deadline = tokio::time::Instant::now() + timeout;
                loop {
                    let notified = self.entity_registration_notify.notified();
                    // Re-check before awaiting (avoid missed notification)
                    if let Some(m) = self.entity_managers.get(entity_type) {
                        break m;
                    }
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        return Err(ClusterError::MalformedMessage {
                            reason: format!(
                                "no entity registered for type: {entity_type} (waited {timeout:?})"
                            ),
                            source: None,
                        });
                    }
                    // Notification received — check again (might be a different entity type)
                    if let Some(m) = self.entity_managers.get(entity_type) {
                        break m;
                    }
                }
            }
        };

        let msg = match reply_tx {
            Some(tx) => IncomingMessage::Request {
                request: envelope,
                reply_tx: tx,
            },
            None => IncomingMessage::Envelope { envelope },
        };

        manager.send_local(msg).await
    }

    async fn handle_interrupt_local(&self, interrupt: &Interrupt) {
        if let Some(engine) = &self.workflow_engine {
            if let Err(e) = engine
                .resolve_deferred(
                    &interrupt.address.entity_type.0,
                    &interrupt.address.entity_id.0,
                    INTERRUPT_SIGNAL,
                    Vec::new(),
                )
                .await
            {
                tracing::warn!(
                    request_id = %interrupt.request_id,
                    entity_address = %interrupt.address,
                    error = %e,
                    "failed to resolve interrupt signal"
                );
            }
        }

        if let Some(manager) = self.entity_managers.get(&interrupt.address.entity_type) {
            manager.interrupt_entity(&interrupt.address).await;
        }
    }

    /// Background loop that watches for runner changes and rebalances shards.
    ///
    /// Uses `shard_rebalance_retry_interval` as the normal polling interval.
    /// When a rebalance actually acquires or releases shards, applies
    /// `shard_rebalance_debounce` as an additional delay before the next
    /// rebalance cycle to avoid thrashing during rapid topology changes.
    async fn shard_acquisition_loop(&self) {
        let runner_storage = match &self.runner_storage {
            Some(s) => Arc::clone(s),
            None => return,
        };

        loop {
            if self.cancel.is_cancelled() {
                break;
            }

            // Short-circuit while detached — no point in rebalancing when we can't
            // safely execute shards. Sleep and wait for re-attachment.
            if self.is_detached() {
                tracing::trace!("shard_acquisition_loop: detached, skipping rebalance");
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = tokio::time::sleep(self.config.shard_rebalance_retry_interval) => {},
                }
                continue;
            }

            let changed = match self.rebalance_shards(&runner_storage).await {
                Ok(changed) => changed,
                Err(e) => {
                    tracing::error!(error = %e, "shard rebalance failed");
                    false
                }
            };

            // If shards changed, apply debounce delay to avoid thrashing
            // during rapid runner topology changes (e.g., rolling restarts).
            let sleep_duration = if changed {
                self.config.shard_rebalance_debounce
            } else {
                self.config.shard_rebalance_retry_interval
            };

            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(sleep_duration) => {},
            }
        }
    }

    /// Perform one round of shard rebalancing.
    ///
    /// Returns `true` if any shards were acquired or released during this cycle.
    /// Rebalance shard ownership based on the current set of healthy runners.
    ///
    /// Lock ordering invariant (all sequential, never nested):
    ///   1. `shard_assignments` (write) — update routing table
    ///   2. `owned_shards` (read) — compute diff
    ///   3. `releasing_shards` (write) → entity interrupt → `owned_shards` (write) → `releasing_shards` (write) — release phase
    ///   4. `releasing_shards` (read) → `owned_shards` (write) — acquire phase
    ///
    /// This method is only called from the single-threaded `shard_acquisition_loop`,
    /// so there is no risk of concurrent invocations causing deadlocks.
    #[instrument(skip(self, runner_storage))]
    async fn rebalance_shards(
        &self,
        runner_storage: &Arc<dyn RunnerStorage>,
    ) -> Result<bool, ClusterError> {
        // Check cancellation before doing any work
        if self.cancel.is_cancelled() {
            return Ok(false);
        }

        // 1. Get current runners (with cancellation support)
        let runners = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                tracing::debug!("rebalance_shards cancelled during get_runners");
                return Ok(false);
            }
            result = runner_storage.get_runners() => {
                match result {
                    Ok(r) => {
                        // Successful storage call - signal healthy for re-attachment
                        self.signal_healthy();
                        r
                    }
                    Err(e) => {
                        // Storage error during get_runners - trigger detachment
                        tracing::error!(error = %e, "get_runners failed, triggering detachment");
                        self.detach(DetachmentReason::StorageError(format!(
                            "get_runners failed: {}", e
                        ))).await;
                        return Ok(false);
                    }
                }
            },
        };
        self.metrics.runners.set(runners.len() as i64);
        self.metrics
            .runners_healthy
            .set(runners.iter().filter(|r| r.healthy).count() as i64);

        // 2. Compute desired assignments via configured strategy
        let desired = ShardAssigner::compute_assignments(
            &runners,
            &self.config.shard_groups,
            self.config.shards_per_group,
            &self.config.shard_assignment_strategy,
        );

        // Update shard assignments map (used for remote routing)
        {
            let mut assignments = self.shard_assignments.write().await;
            *assignments = desired.clone();
        }

        // 3. Compute diff
        let current_owned = self.owned_shards.read().await.clone();
        let (to_acquire, to_release) =
            ShardAssigner::compute_diff(&desired, &current_owned, &self.config.runner_address);

        // 4. Release phase: release shards no longer assigned to us
        for shard_id in &to_release {
            // Check cancellation before each shard release
            if self.cancel.is_cancelled() {
                tracing::debug!("rebalance_shards cancelled during release phase");
                return Ok(false);
            }

            // Mark as releasing to prevent re-acquisition
            self.releasing_shards.write().await.insert(shard_id.clone());

            // Interrupt entities on this shard
            for entry in self.entity_managers.iter() {
                entry.value().interrupt_shard(shard_id).await;
            }

            // Release lock in storage (with cancellation support)
            let release_result = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    tracing::debug!("rebalance_shards cancelled during shard release");
                    return Ok(false);
                }
                result = runner_storage.release(shard_id, &self.config.runner_address) => result,
            };
            if let Err(e) = release_result {
                tracing::warn!(shard_id = %shard_id, error = %e, "failed to release shard lock, keeping shard in owned set for retry");
                // Keep shard in owned_shards so the next rebalance cycle retries the release.
                // Remove from releasing set so it can be re-added to to_release next cycle.
                self.releasing_shards.write().await.remove(shard_id);
                continue;
            }

            // Remove from owned and releasing sets
            self.owned_shards.write().await.remove(shard_id);
            self.releasing_shards.write().await.remove(shard_id);

            tracing::debug!(shard_id = %shard_id, "released shard");
        }

        // Check cancellation before acquire phase
        if self.cancel.is_cancelled() {
            tracing::debug!("rebalance_shards cancelled before acquire phase");
            return Ok(!to_release.is_empty());
        }

        // 5. Acquire phase: batch acquire newly assigned shards with retry
        // Filter out shards currently being released
        let releasing = self.releasing_shards.read().await;
        let mut pending_acquire: Vec<ShardId> = to_acquire
            .iter()
            .filter(|s| !releasing.contains(s))
            .cloned()
            .collect();
        drop(releasing);

        let mut all_newly_acquired = Vec::new();
        let retry_window = self.config.acquire_retry_window;
        let retry_interval = self.config.acquire_retry_interval;
        let window_start = std::time::Instant::now();
        let mut is_first_attempt = true;

        // Retry loop for acquiring shards held by other runners
        while !pending_acquire.is_empty() {
            // Check cancellation before each attempt
            if self.cancel.is_cancelled() {
                tracing::debug!("rebalance_shards cancelled during acquire_batch");
                return Ok(!to_release.is_empty() || !all_newly_acquired.is_empty());
            }

            // If not first attempt, check window and sleep before retry
            if !is_first_attempt {
                // Check if we've exceeded the retry window
                if window_start.elapsed() >= retry_window {
                    tracing::warn!(
                        remaining_shards = pending_acquire.len(),
                        window_ms = retry_window.as_millis() as u64,
                        "acquire retry window exhausted, giving up on remaining shards"
                    );
                    self.metrics.acquire_retry_window_exhausted.inc();
                    break;
                }

                // Sleep before retry
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => {
                        tracing::debug!("rebalance_shards cancelled during acquire retry sleep");
                        return Ok(!to_release.is_empty() || !all_newly_acquired.is_empty());
                    }
                    _ = tokio::time::sleep(retry_interval) => {},
                }

                self.metrics.acquire_retry_attempts.inc();
                tracing::trace!(
                    pending_shards = pending_acquire.len(),
                    "retrying acquire for shards held by other runners"
                );
            }
            is_first_attempt = false;

            // Acquire shards (with cancellation support)
            let batch_result = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    tracing::debug!("rebalance_shards cancelled during acquire_batch");
                    return Ok(!to_release.is_empty() || !all_newly_acquired.is_empty());
                }
                result = runner_storage.acquire_batch(&pending_acquire, &self.config.runner_address) => {
                    match result {
                        Ok(r) => r,
                        Err(e) => {
                            // Storage error during acquire_batch - trigger detachment
                            tracing::error!(error = %e, "acquire_batch failed, triggering detachment");
                            self.detach(DetachmentReason::StorageError(format!(
                                "acquire_batch failed: {}", e
                            ))).await;
                            return Ok(!to_release.is_empty() || !all_newly_acquired.is_empty());
                        }
                    }
                },
            };

            // Handle storage errors for individual shards - remove them from pending
            // (don't retry shards that failed due to storage errors, only shards held elsewhere)
            let failed_shard_ids: HashSet<_> = batch_result
                .failures
                .iter()
                .map(|(shard_id, _)| shard_id.clone())
                .collect();
            if !failed_shard_ids.is_empty() {
                tracing::warn!(
                    failure_count = failed_shard_ids.len(),
                    "some shards failed to acquire due to storage errors (will not retry)"
                );
            }

            // Add acquired shards to owned_shards and track them
            {
                let mut owned = self.owned_shards.write().await;
                for shard_id in &batch_result.acquired {
                    owned.insert(shard_id.clone());
                    tracing::debug!(shard_id = %shard_id, "acquired shard");
                }
            }
            all_newly_acquired.extend(batch_result.acquired.clone());

            // Remove acquired and failed shards from pending
            let acquired_set: HashSet<_> = batch_result.acquired.into_iter().collect();
            pending_acquire.retain(|s| !acquired_set.contains(s) && !failed_shard_ids.contains(s));

            // If no retries configured, break after first attempt
            if retry_window.is_zero() {
                if !pending_acquire.is_empty() {
                    tracing::trace!(
                        count = pending_acquire.len(),
                        "shards not acquired (held by other runners, no retry configured)"
                    );
                }
                break;
            }
        }

        // Update metrics
        let owned_count = self.owned_shards.read().await.len();
        self.metrics.shards.set(owned_count as i64);
        let newly_acquired = all_newly_acquired;

        // 6. If we acquired new shards, reset them in message storage and trigger poll
        if !newly_acquired.is_empty() && !self.cancel.is_cancelled() {
            if let Some(ref storage) = self.message_storage {
                if let Err(e) = storage.reset_shards(&newly_acquired).await {
                    tracing::warn!(error = %e, "failed to reset shards in message storage");
                }
            }
            self.storage_poll_notify.notify_one();
            self.sync_singletons().await;
        }

        // Also sync singletons if shards were released (skip if cancelled)
        if !to_release.is_empty() && !self.cancel.is_cancelled() {
            self.sync_singletons().await;
        }

        let changed = !newly_acquired.is_empty() || !to_release.is_empty();
        Ok(changed)
    }

    /// Background loop that periodically refreshes shard lock TTLs.
    ///
    /// Tracks consecutive refresh failures per shard. When a shard exceeds
    /// `shard_lock_refresh_max_failures` consecutive failures, it is removed
    /// from owned shards and its entities are interrupted to prevent split-brain
    /// (another runner may have already acquired the expired lock).
    async fn lock_refresh_loop(&self) {
        let runner_storage = match &self.runner_storage {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let max_failures = self.config.shard_lock_refresh_max_failures;
        let mut failure_counts: HashMap<ShardId, u32> = HashMap::new();

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(self.config.runner_lock_refresh_interval) => {},
            }

            // Check cancellation after waking up
            if self.cancel.is_cancelled() {
                break;
            }

            // Short-circuit while detached — owned_shards is empty and we shouldn't
            // be refreshing locks when we can't safely execute shards.
            if self.is_detached() {
                tracing::trace!("lock_refresh_loop: detached, skipping refresh");
                continue;
            }

            let owned: std::collections::HashSet<ShardId> =
                self.owned_shards.read().await.iter().cloned().collect();
            let releasing: std::collections::HashSet<ShardId> =
                self.releasing_shards.read().await.iter().cloned().collect();

            // Clean up failure counts for shards we no longer own.
            failure_counts.retain(|s, _| owned.contains(s));

            // Refresh both owned and releasing shards. Releasing shards still have
            // entities running on them; if their lock expires before release completes,
            // another runner can acquire the shard while this runner still has entities
            // on it, causing split-brain.
            let all_shards: std::collections::HashSet<ShardId> =
                owned.union(&releasing).cloned().collect();

            // Batch refresh all shards in a single storage round-trip (or single
            // mutex hold for etcd), avoiding N sequential network calls.
            let all_shard_vec: Vec<ShardId> = all_shards.iter().cloned().collect();
            let batch_result = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    tracing::debug!("lock_refresh_loop cancelled during refresh_batch");
                    break;
                }
                result = runner_storage.refresh_batch(&all_shard_vec, &self.config.runner_address) => {
                    match result {
                        Ok(r) => r,
                        Err(e) => {
                            // Top-level storage error - trigger detachment and return early
                            tracing::error!(error = %e, "batch refresh failed entirely, triggering detachment");
                            self.detach(DetachmentReason::StorageError(format!(
                                "refresh_batch failed: {}", e
                            ))).await;
                            continue;
                        }
                    }
                }
            };

            // Successful storage call - signal healthy for re-attachment
            self.signal_healthy();

            // Process successfully refreshed shards — reset failure counts.
            for shard_id in &batch_result.refreshed {
                if !releasing.contains(shard_id) {
                    failure_counts.remove(shard_id);
                }
            }

            // Process lost locks (skip if cancelled)
            for shard_id in &batch_result.lost {
                if self.cancel.is_cancelled() {
                    break;
                }
                if releasing.contains(shard_id) {
                    tracing::warn!(shard_id = %shard_id, "releasing shard lock lost during refresh");
                    continue;
                }
                tracing::warn!(shard_id = %shard_id, "shard lock lost during refresh");
                failure_counts.remove(shard_id);
                {
                    let mut owned_shards = self.owned_shards.write().await;
                    owned_shards.remove(shard_id);
                    self.metrics.shards.set(owned_shards.len() as i64);
                }
                for entry in self.entity_managers.iter() {
                    entry.value().interrupt_shard(shard_id).await;
                }
                self.sync_singletons().await;
            }

            // Process per-shard errors (skip if cancelled)
            for (shard_id, e) in &batch_result.failures {
                if self.cancel.is_cancelled() {
                    break;
                }
                if releasing.contains(shard_id) {
                    tracing::warn!(
                        shard_id = %shard_id,
                        error = %e,
                        "failed to refresh releasing shard lock"
                    );
                    continue;
                }
                let count = failure_counts.entry(shard_id.clone()).or_insert(0);
                *count += 1;
                tracing::warn!(
                    shard_id = %shard_id,
                    error = %e,
                    consecutive_failures = *count,
                    max_failures = max_failures,
                    "failed to refresh shard lock"
                );

                if *count >= max_failures {
                    tracing::error!(
                        shard_id = %shard_id,
                        consecutive_failures = *count,
                        "shard lock refresh failed {} consecutive times, releasing shard to prevent split-brain",
                        *count,
                    );
                    failure_counts.remove(shard_id);
                    {
                        let mut owned_shards = self.owned_shards.write().await;
                        owned_shards.remove(shard_id);
                        self.metrics.shards.set(owned_shards.len() as i64);
                    }
                    for entry in self.entity_managers.iter() {
                        entry.value().interrupt_shard(shard_id).await;
                    }
                    self.sync_singletons().await;
                }
            }

            // Periodically re-sync singletons so that normally-completed singletons
            // are re-spawned without waiting for a shard topology change (skip if cancelled)
            if !self.cancel.is_cancelled() {
                self.sync_singletons().await;
            }
        }
    }

    /// Background loop that monitors lease health and triggers detachment.
    ///
    /// Listens to `LeaseHealth` updates from the runner storage and triggers
    /// detachment when the failure streak exceeds `keepalive_failure_threshold`.
    async fn lease_health_loop(
        &self,
        mut health_rx: tokio::sync::broadcast::Receiver<LeaseHealth>,
    ) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                result = health_rx.recv() => {
                    match result {
                        Ok(health) => {
                            // Update metrics.
                            self.metrics.lease_keepalive_failure_streak.set(health.failure_streak as i64);
                            if !health.healthy {
                                self.metrics.lease_keepalive_failures.inc();
                            }

                            if health.healthy {
                                // Signal healthy for re-attachment.
                                self.signal_healthy();
                            } else {
                                // Check if failure streak exceeds threshold.
                                if health.failure_streak >= self.config.keepalive_failure_threshold {
                                    tracing::warn!(
                                        failure_streak = health.failure_streak,
                                        threshold = self.config.keepalive_failure_threshold,
                                        "keep-alive failure streak exceeded threshold, triggering detachment"
                                    );
                                    self.detach(DetachmentReason::KeepAliveFailure {
                                        consecutive_failures: health.failure_streak,
                                    }).await;
                                } else {
                                    // Signal unhealthy to reset recovery window.
                                    self.signal_unhealthy();
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                            // We missed some messages due to slow processing. This is
                            // generally fine — we'll catch up on the next message.
                            tracing::debug!(
                                missed_count = count,
                                "lease health receiver lagged, missed some updates"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // Sender closed — runner storage is shutting down.
                            tracing::info!("lease health channel closed, stopping monitoring loop");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Background loop that polls message storage for unprocessed messages.
    async fn storage_poll_loop(&self) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = self.storage_poll_notify.notified() => {},
                _ = tokio::time::sleep(self.config.storage_poll_interval) => {},
            }

            // Check cancellation after waking up
            if self.cancel.is_cancelled() {
                break;
            }

            if let Err(e) = self.poll_storage_inner().await {
                tracing::warn!(error = %e, "storage poll failed");
            }
        }
    }

    /// Internal implementation of storage polling.
    async fn poll_storage_inner(&self) -> Result<(), ClusterError> {
        // Check cancellation early
        if self.cancel.is_cancelled() {
            return Ok(());
        }

        let _poll_lock = self.storage_poll_lock.lock().await;
        let storage = match &self.message_storage {
            Some(s) => s,
            None => return Ok(()),
        };

        let shard_ids: Vec<ShardId> = {
            let owned = self.owned_shards.read().await;
            owned.iter().cloned().collect()
        };

        if shard_ids.is_empty() {
            return Ok(());
        }

        // Clear processed request ID sets before polling so that
        // messages not yet marked processed in storage can be re-fetched.
        // The dedup set in each EntityManager prevents double-dispatch within
        // a single poll cycle.
        for manager in self.entity_managers.iter() {
            manager.value().clear_processed();
        }

        let mut messages = storage.unprocessed_messages(&shard_ids).await?;

        // Limit the number of messages dispatched per poll cycle to avoid
        // overwhelming entity mailboxes. Remaining messages will be picked
        // up in subsequent poll cycles.
        if self.config.storage_inbox_size > 0 {
            messages.truncate(self.config.storage_inbox_size);
        }

        // Acquire storage_read_lock during dispatch to coordinate with resumption
        // tasks. This prevents the race where a resumption task checks remaining == 0
        // and exits, while we're about to add new MailboxFull entries for the same entity.
        let _lock = self
            .storage_read_lock
            .acquire()
            .await
            .expect("semaphore not closed");

        for envelope in messages {
            // Check cancellation during message dispatch
            if self.cancel.is_cancelled() {
                break;
            }

            let entity_type = &envelope.address.entity_type;

            let manager = match self.entity_managers.get(entity_type) {
                Some(m) => {
                    // Entity type is registered — clean up any first-seen tracking
                    self.unregistered_first_seen
                        .remove(&(entity_type.clone(), envelope.request_id));
                    m
                }
                None => {
                    let key = (entity_type.clone(), envelope.request_id);
                    let first_seen = *self
                        .unregistered_first_seen
                        .entry(key.clone())
                        .or_insert_with(Instant::now);
                    let elapsed = first_seen.elapsed();

                    if elapsed >= self.config.entity_registration_timeout {
                        // Timeout exceeded — save a failure reply to storage so the
                        // message is marked processed and callers get a clear error
                        // instead of the message being re-polled forever.
                        tracing::error!(
                            entity_type = %entity_type,
                            request_id = %envelope.request_id,
                            elapsed_ms = elapsed.as_millis() as u64,
                            "poll_storage: entity type not registered after timeout, saving failure reply"
                        );
                        let reply_id = match self.snowflake.next_async().await {
                            Ok(id) => id,
                            Err(e) => {
                                tracing::error!(
                                    request_id = %envelope.request_id,
                                    error = %e,
                                    "poll_storage: snowflake generation failed, using fallback reply id"
                                );
                                fallback_reply_id(envelope.request_id, EXIT_SEQUENCE)
                            }
                        };
                        let reply = Reply::WithExit(ReplyWithExit {
                            request_id: envelope.request_id,
                            id: reply_id,
                            exit: ExitResult::Failure(format!(
                                "entity type '{}' not registered after {}ms",
                                entity_type,
                                self.config.entity_registration_timeout.as_millis()
                            )),
                        });
                        if let Err(e) = storage.save_reply(&reply).await {
                            tracing::warn!(
                                request_id = %envelope.request_id,
                                error = %e,
                                "poll_storage: failed to save failure reply for unregistered entity type"
                            );
                        }
                        self.unregistered_first_seen.remove(&key);
                    } else {
                        tracing::warn!(
                            entity_type = %entity_type,
                            request_id = %envelope.request_id,
                            elapsed_ms = elapsed.as_millis() as u64,
                            timeout_ms = self.config.entity_registration_timeout.as_millis() as u64,
                            "poll_storage: skipping message for unregistered entity type (waiting for registration)"
                        );
                    }
                    continue;
                }
            };

            if !self.has_shard_async(&envelope.address.shard_id).await {
                continue;
            }

            let address = envelope.address.clone();
            let request_id = envelope.request_id;
            let msg = IncomingMessage::Envelope { envelope };
            match manager.send_local(msg).await {
                Ok(()) => {}
                Err(ClusterError::MailboxFull { .. }) => {
                    // Track for resumption — a dedicated per-entity task will retry.
                    let entry = self
                        .resumption_unprocessed
                        .entry(address.clone())
                        .or_default();
                    entry.value().insert(request_id);

                    // Spawn a resumption task if one isn't already running for this entity.
                    if self.resumption_active.insert(address.clone()) {
                        self.spawn_resumption_task(address);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "poll_storage: failed to dispatch message");
                }
            }
        }

        drop(_lock);

        Ok(())
    }

    /// Spawn a per-entity resumption task that retries delivering messages
    /// that encountered `MailboxFull` during storage polling.
    ///
    /// Matches the TS `resumeEntityFromStorageImpl` (Sharding.ts:612-710):
    /// - Takes batches of unprocessed request IDs for the entity
    /// - Re-fetches messages from storage by ID
    /// - Retries delivery with `send_retry_interval` backoff on `MailboxFull`
    /// - Stops when all tracked IDs are delivered or the shard is no longer owned
    fn spawn_resumption_task(&self, address: crate::types::EntityAddress) {
        let storage = match &self.message_storage {
            Some(s) => Arc::clone(s),
            None => {
                self.resumption_active.remove(&address);
                return;
            }
        };
        let entity_managers = Arc::clone(&self.entity_managers);
        let owned_shards = Arc::clone(&self.owned_shards);
        let cancel = self.cancel.clone();
        let retry_interval = self.config.send_retry_interval;
        let max_retries = self.config.storage_resumption_max_retries;
        let resumption_unprocessed = Arc::clone(&self.resumption_unprocessed);
        let resumption_active = Arc::clone(&self.resumption_active);
        let resumption_handles = Arc::clone(&self.resumption_handles);
        let addr_clone = address.clone();

        let storage_read_lock = Arc::clone(&self.storage_read_lock);

        let handle = tokio::spawn(async move {
            let result = Self::run_resumption(
                &address,
                &storage,
                &entity_managers,
                &owned_shards,
                &cancel,
                retry_interval,
                max_retries,
                &resumption_unprocessed,
                &storage_read_lock,
            )
            .await;
            if let Err(e) = result {
                tracing::warn!(
                    error = %e,
                    entity_address = %address,
                    "resumption task failed"
                );
            }
            // Always clean up
            resumption_unprocessed.remove(&address);
            resumption_active.remove(&address);
            resumption_handles.remove(&address);
        });
        self.resumption_handles.insert(addr_clone, handle);
    }

    /// Internal resumption loop for a single entity.
    #[allow(clippy::too_many_arguments)]
    async fn run_resumption(
        address: &crate::types::EntityAddress,
        storage: &Arc<dyn MessageStorage>,
        entity_managers: &Arc<DashMap<EntityType, Arc<EntityManager>>>,
        owned_shards: &Arc<RwLock<HashSet<ShardId>>>,
        cancel: &tokio_util::sync::CancellationToken,
        retry_interval: std::time::Duration,
        max_retries: u32,
        resumption_unprocessed: &DashMap<
            crate::types::EntityAddress,
            DashSet<crate::snowflake::Snowflake>,
        >,
        storage_read_lock: &tokio::sync::Semaphore,
    ) -> Result<(), ClusterError> {
        loop {
            // Check if shard is still owned
            {
                let owned = owned_shards.read().await;
                if !owned.contains(&address.shard_id) {
                    tracing::debug!(
                        entity_address = %address,
                        "resumption: shard no longer owned, stopping"
                    );
                    return Ok(());
                }
            }

            if cancel.is_cancelled() {
                return Ok(());
            }

            // Take a batch of unprocessed IDs (up to 1024)
            let ids: Vec<crate::snowflake::Snowflake> = {
                let entry = match resumption_unprocessed.get(address) {
                    Some(e) => e,
                    None => return Ok(()),
                };
                entry.value().iter().take(1024).map(|r| *r).collect()
            };

            if ids.is_empty() {
                return Ok(());
            }

            // Re-fetch from storage
            let messages = match storage.unprocessed_messages_by_id(&ids).await {
                Ok(msgs) => msgs,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        entity_address = %address,
                        "resumption: failed to fetch messages, retrying"
                    );
                    tokio::time::sleep(retry_interval).await;
                    continue;
                }
            };

            if messages.is_empty() {
                // Messages may have been processed by another mechanism
                tokio::time::sleep(retry_interval).await;
                // Remove the IDs we tried — they're no longer unprocessed
                if let Some(entry) = resumption_unprocessed.get(address) {
                    for id in &ids {
                        entry.value().remove(id);
                    }
                }
                continue;
            }

            // Try to deliver each message
            for envelope in messages {
                let request_id = envelope.request_id;
                let entity_type = &envelope.address.entity_type;

                let manager = match entity_managers.get(entity_type) {
                    Some(m) => m,
                    None => {
                        if let Some(entry) = resumption_unprocessed.get(address) {
                            entry.value().remove(&request_id);
                        }
                        continue;
                    }
                };

                // Retry loop for this specific message
                let mut attempts: u32 = 0;
                loop {
                    if cancel.is_cancelled() {
                        return Ok(());
                    }

                    let msg = IncomingMessage::Envelope {
                        envelope: envelope.clone(),
                    };
                    match manager.send_local(msg).await {
                        Ok(()) => {
                            if let Some(entry) = resumption_unprocessed.get(address) {
                                entry.value().remove(&request_id);
                            }
                            break;
                        }
                        Err(ClusterError::MailboxFull { .. }) => {
                            attempts += 1;
                            if max_retries > 0 && attempts >= max_retries {
                                tracing::error!(
                                    request_id = %request_id,
                                    entity_address = %address,
                                    attempts = attempts,
                                    max_retries = max_retries,
                                    "resumption: MailboxFull retry limit exhausted, dead-lettering message"
                                );
                                // Save a failure reply to storage so the message is marked processed
                                let failure_reply =
                                    crate::reply::Reply::WithExit(crate::reply::ReplyWithExit {
                                        request_id,
                                        id: dead_letter_reply_id(request_id),
                                        exit: crate::reply::ExitResult::Failure(
                                            "resumption: MailboxFull retry limit exhausted"
                                                .to_string(),
                                        ),
                                    });
                                if let Err(e) = storage.save_reply(&failure_reply).await {
                                    tracing::warn!(
                                        error = %e,
                                        request_id = %request_id,
                                        "resumption: failed to save dead-letter reply"
                                    );
                                }
                                if let Some(entry) = resumption_unprocessed.get(address) {
                                    entry.value().remove(&request_id);
                                }
                                break;
                            }
                            tokio::time::sleep(retry_interval).await;
                        }
                        Err(ClusterError::EntityNotAssignedToRunner { .. }) => {
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                request_id = %request_id,
                                entity_address = %address,
                                "resumption: delivery failed, giving up on message"
                            );
                            if let Some(entry) = resumption_unprocessed.get(address) {
                                entry.value().remove(&request_id);
                            }
                            break;
                        }
                    }
                }
            }

            // Acquire storage_read_lock before checking remaining. This ensures
            // the poll loop can't add new MailboxFull entries between our check
            // and our return. Without this lock, the sequence:
            //   1. resumption checks remaining == 0
            //   2. poll_storage adds new entry for same entity
            //   3. resumption exits — new entry is orphaned
            // is possible. The lock serializes step 1 with step 2.
            let _lock = storage_read_lock
                .acquire()
                .await
                .expect("semaphore not closed");
            let remaining = resumption_unprocessed
                .get(address)
                .map(|e| e.value().len())
                .unwrap_or(0);
            if remaining == 0 {
                return Ok(());
            }
            drop(_lock);
        }
    }

    /// Background loop that periodically checks runner health and updates status.
    ///
    /// Registered as a cluster singleton (`"cluster/RunnerHealth"`) in `start()`,
    /// so only one runner in the cluster performs health checks at a time.
    /// The singleton is managed by `sync_singletons()` which spawns/cancels it
    /// based on shard ownership. It:
    /// - Iterates all runners with a concurrency limit of 10
    /// - Calls `runner_health.is_alive()` for each
    /// - Updates `runner_storage.set_runner_health()`
    /// - Never marks the last healthy runner as unhealthy (prevents deadlock)
    /// - If zero healthy runners: force-marks self as healthy
    #[instrument(skip(self, cancel))]
    async fn runner_health_loop(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), ClusterError> {
        let runner_storage = match &self.runner_storage {
            Some(s) => Arc::clone(s),
            None => return Ok(()),
        };
        let runner_health = match &self.runner_health {
            Some(h) => Arc::clone(h),
            None => return Ok(()),
        };

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(self.config.runner_poll_interval) => {},
            }

            // Check cancellation after waking up
            if cancel.is_cancelled() {
                break;
            }

            if let Err(e) = self
                .check_runner_health(&runner_storage, &runner_health, &cancel)
                .await
            {
                tracing::warn!(error = %e, "runner health check failed");
            }
        }
        Ok(())
    }

    /// Perform one round of runner health checks.
    async fn check_runner_health(
        &self,
        runner_storage: &Arc<dyn RunnerStorage>,
        runner_health: &Arc<dyn RunnerHealth>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<(), ClusterError> {
        use futures::stream::StreamExt;

        // Check cancellation early
        if cancel.is_cancelled() {
            return Ok(());
        }

        let runners = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = runner_storage.get_runners() => result?,
        };
        if runners.is_empty() {
            return Ok(());
        }

        // Check health with concurrency limit of 10 (with cancellation support)
        let cancel_clone = cancel.clone();
        let checks: Vec<_> = runners
            .iter()
            .map(|runner| {
                let addr = runner.address.clone();
                let health = Arc::clone(runner_health);
                let cancel = cancel_clone.clone();
                async move {
                    // Check cancellation before each health check
                    if cancel.is_cancelled() {
                        return None;
                    }
                    let alive = match health.is_alive(&addr).await {
                        Ok(alive) => alive,
                        Err(e) => {
                            tracing::warn!(runner = %addr, error = %e, "health check error");
                            false
                        }
                    };
                    Some((addr, alive))
                }
            })
            .collect();
        let health_results: Vec<_> = futures::stream::iter(checks)
            .buffer_unordered(10)
            .filter_map(|x| async { x })
            .collect()
            .await;

        // Check cancellation after health checks
        if cancel.is_cancelled() {
            return Ok(());
        }

        // Apply health updates with the "last healthy runner" guard.
        // Track runners marked unhealthy in this iteration so each successive
        // decision reflects prior decisions in the same loop.
        let mut marked_unhealthy_this_cycle: Vec<&RunnerAddress> = Vec::new();
        for (addr, alive) in &health_results {
            // Check cancellation before each update
            if cancel.is_cancelled() {
                return Ok(());
            }
            let runner = runners.iter().find(|r| &r.address == addr);
            let was_healthy = runner.map(|r| r.healthy).unwrap_or(false);

            if was_healthy && !alive {
                // About to mark a healthy runner as unhealthy.
                // Count how many would remain healthy after this change,
                // accounting for runners already marked unhealthy in this cycle.
                let healthy_after: usize = health_results
                    .iter()
                    .filter(|(a, is_alive)| {
                        if a == addr || marked_unhealthy_this_cycle.contains(&a) {
                            // This runner would be (or was already) marked unhealthy
                            false
                        } else {
                            // Use the new health result
                            *is_alive
                        }
                    })
                    .count();

                if healthy_after == 0 {
                    tracing::warn!(
                        runner = %addr,
                        "skipping health update: would leave zero healthy runners"
                    );
                    continue;
                }
            }

            if was_healthy != *alive {
                if let Err(e) = runner_storage.set_runner_health(addr, *alive).await {
                    tracing::warn!(runner = %addr, error = %e, "failed to update runner health");
                } else {
                    tracing::info!(runner = %addr, alive = %alive, "updated runner health");
                    if !alive {
                        marked_unhealthy_this_cycle.push(addr);
                    }
                }
            }
        }

        // If zero runners are healthy, force-mark self as healthy
        let any_healthy = health_results.iter().any(|(_, alive)| *alive);
        if !any_healthy {
            tracing::warn!("no healthy runners detected, force-marking self as healthy");
            if let Err(e) = runner_storage
                .set_runner_health(&self.config.runner_address, true)
                .await
            {
                tracing::warn!(error = %e, "failed to force-mark self as healthy");
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_singleton_task(
        &self,
        name: String,
        cancel: tokio_util::sync::CancellationToken,
        factory: SingletonFactory,
        running_flag: Arc<std::sync::atomic::AtomicBool>,
        failure_count: Arc<std::sync::atomic::AtomicU32>,
        failure_time: Arc<std::sync::atomic::AtomicU64>,
        managed: Arc<std::sync::atomic::AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        running_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        // Reset managed flag for this spawn cycle
        managed.store(false, std::sync::atomic::Ordering::Release);
        let managed_clone = managed.clone();
        tokio::spawn(async move {
            use futures::FutureExt;
            use std::sync::atomic::Ordering;

            // Create context with cancellation token for graceful shutdown.
            // The managed flag is shared with SingletonEntry so sync_singletons
            // can check if the singleton opted in to manage cancellation.
            let ctx = SingletonContext::new(cancel.clone(), managed_clone);
            let managed = ctx.managed.clone();

            // Spawn the factory in a separate task so we can choose to abort or await it
            let mut factory_handle = tokio::spawn(
                std::panic::AssertUnwindSafe(async move { (factory)(ctx).await }).catch_unwind(),
            );

            // Result type: Option<Result<Result<(), ClusterError>, Box<dyn Any + Send>>>
            // None = force-cancelled, Some(...) = completed or panicked
            let result: Option<Result<Result<(), ClusterError>, Box<dyn std::any::Any + Send>>> = tokio::select! {
                _ = cancel.cancelled() => {
                    // Cancellation triggered - check if singleton is managing it
                    if managed.load(Ordering::Acquire) {
                        // Singleton opted in - wait for it to finish gracefully
                        tracing::debug!(name = %name, "singleton managing cancellation, waiting for graceful shutdown");
                        match factory_handle.await {
                            Ok(result) => Some(result),
                            Err(join_err) => {
                                tracing::error!(name = %name, "singleton task join error: {}", join_err);
                                None
                            }
                        }
                    } else {
                        // Singleton didn't opt in - force cancel it
                        tracing::debug!(name = %name, "singleton not managing cancellation, force-cancelling");
                        factory_handle.abort();
                        None // Force-cancelled, treat as success
                    }
                }
                join_result = &mut factory_handle => {
                    // Factory completed before cancellation
                    match join_result {
                        Ok(result) => Some(result),
                        Err(join_err) => {
                            tracing::error!(name = %name, "singleton task join error: {}", join_err);
                            None
                        }
                    }
                }
            };

            // Handle the result
            let mut failed = false;
            match result {
                Some(Ok(Ok(()))) | None => {
                    tracing::debug!(name = %name, "singleton completed normally");
                    failure_count.store(0, Ordering::Release);
                }
                Some(Ok(Err(e))) => {
                    tracing::error!(name = %name, error = %e, "singleton failed");
                    failed = true;
                }
                Some(Err(_panic)) => {
                    tracing::error!(name = %name, "singleton panicked");
                    failed = true;
                }
            }

            if failed {
                failure_count.fetch_add(1, Ordering::Release);
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock before Unix epoch")
                    .as_millis() as u64;
                failure_time.store(now_ms, Ordering::Release);
            }
            running_flag.store(false, Ordering::SeqCst);
        })
    }

    /// Synchronize singleton execution based on current shard ownership.
    ///
    /// Each singleton is assigned to a shard by hashing its name.
    /// Only the runner owning that shard should run the singleton.
    ///
    /// This method snapshots singleton state first (without holding DashMap locks),
    /// then performs async shard ownership checks, and finally re-acquires per-entry
    /// mutable access to apply state changes. This avoids holding DashMap shard locks
    /// across `.await` points which would cause contention with concurrent
    /// `register_singleton`, `shutdown`, or other `sync_singletons` calls.
    async fn sync_singletons(&self) {
        // Check cancellation early
        if self.cancel.is_cancelled() {
            return;
        }

        // Serialize with register_singleton to prevent races.
        // Matches the TS `withSingletonLock` (Sharding.ts).
        let _singleton_guard = self.singleton_lock.lock().await;

        // Phase 1: Snapshot singleton state without holding mutable locks.
        struct SingletonSnapshot {
            name: String,
            shard_id: ShardId,
            is_running: bool,
            is_cancelled: bool,
            consecutive_failures: u32,
            last_failure_ms: u64,
        }

        let snapshots: Vec<SingletonSnapshot> = self
            .singletons
            .iter()
            .map(|entry| {
                let name = entry.key().clone();
                let singleton = entry.value();
                // Compute shard ID directly from singleton name and shard group,
                // bypassing entity manager lookup. This matches the TS source
                // (Sharding.ts:1189) which uses getShardId(makeEntityId(name), shardGroup).
                let shard = crate::hash::shard_for_entity(&name, self.config.shards_per_group);
                let shard_id = ShardId::new(&singleton.shard_group, shard);
                let is_running = singleton.running.load(std::sync::atomic::Ordering::SeqCst);
                let is_cancelled = singleton.cancel.is_cancelled();
                let consecutive_failures = singleton
                    .consecutive_failures
                    .load(std::sync::atomic::Ordering::Acquire);
                let last_failure_ms = singleton
                    .last_failure_ms
                    .load(std::sync::atomic::Ordering::Acquire);
                SingletonSnapshot {
                    name,
                    shard_id,
                    is_running,
                    is_cancelled,
                    consecutive_failures,
                    last_failure_ms,
                }
            })
            .collect();
        // DashMap read locks are now fully released.

        // Phase 2: Check shard ownership (async) and apply state changes per-entry.
        for snap in &snapshots {
            let should_run = self.has_shard_async(&snap.shard_id).await;
            let is_cancelling = snap.is_cancelled && snap.is_running;

            if should_run && !snap.is_running && !is_cancelling {
                // Apply exponential backoff for failed singletons.
                if snap.consecutive_failures > 0 && snap.last_failure_ms > 0 {
                    let backoff_base = self.config.singleton_crash_backoff_base;
                    let exponent = std::cmp::min(snap.consecutive_failures.saturating_sub(1), 10);
                    let backoff = backoff_base.saturating_mul(1u32 << exponent);
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("system clock before Unix epoch")
                        .as_millis() as u64;
                    let elapsed_ms = now_ms.saturating_sub(snap.last_failure_ms);
                    if elapsed_ms < backoff.as_millis() as u64 {
                        tracing::debug!(
                            name = %snap.name,
                            consecutive_failures = snap.consecutive_failures,
                            backoff_ms = backoff.as_millis() as u64,
                            elapsed_ms,
                            "singleton respawn delayed by crash backoff"
                        );
                        continue;
                    }
                }

                // Re-acquire mutable access and re-verify state hasn't changed.
                if let Some(mut entry) = self.singletons.get_mut(&snap.name) {
                    let singleton = entry.value_mut();
                    let current_running =
                        singleton.running.load(std::sync::atomic::Ordering::SeqCst);
                    let current_cancelling = singleton.cancel.is_cancelled() && current_running;
                    if current_running || current_cancelling {
                        // State changed between snapshot and now — skip.
                        continue;
                    }
                    // Should run — clone the reusable factory and spawn.
                    // Create a fresh CancellationToken for this spawn cycle.
                    let new_cancel = tokio_util::sync::CancellationToken::new();
                    singleton.cancel = new_cancel.clone();
                    let factory = Arc::clone(&singleton.factory);
                    let name_clone = snap.name.clone();
                    let running_flag = Arc::clone(&singleton.running);
                    let failure_count = Arc::clone(&singleton.consecutive_failures);
                    let failure_time = Arc::clone(&singleton.last_failure_ms);
                    let managed_flag = Arc::clone(&singleton.managed);
                    let handle = self.spawn_singleton_task(
                        name_clone,
                        new_cancel,
                        factory,
                        running_flag,
                        failure_count,
                        failure_time,
                        managed_flag,
                    );
                    singleton.handle = Some(handle);
                    tracing::info!(name = %snap.name, "started singleton (shard acquired)");
                }
            } else if should_run && is_cancelling {
                tracing::debug!(name = %snap.name, "singleton still shutting down, will respawn on next sync");
            } else if !should_run && snap.is_running && !is_cancelling {
                // Re-acquire and re-verify before cancelling.
                // If the singleton is managing cancellation, we need to await its handle
                // to ensure it finishes gracefully before another runner starts a new instance.
                let (should_cancel, is_managed, handle) =
                    if let Some(mut entry) = self.singletons.get_mut(&snap.name) {
                        let singleton = entry.value_mut();
                        let current_running =
                            singleton.running.load(std::sync::atomic::Ordering::SeqCst);
                        if current_running && !singleton.cancel.is_cancelled() {
                            singleton.cancel.cancel();
                            let is_managed =
                                singleton.managed.load(std::sync::atomic::Ordering::Acquire);
                            let handle = if is_managed {
                                singleton.handle.take()
                            } else {
                                None
                            };
                            (true, is_managed, handle)
                        } else {
                            (false, false, None)
                        }
                    } else {
                        (false, false, None)
                    };

                if should_cancel {
                    if is_managed {
                        if let Some(handle) = handle {
                            tracing::info!(name = %snap.name, "waiting for managed singleton to shut down gracefully (shard no longer owned)");
                            // Await the handle - the singleton will observe cancellation and return
                            let _ = handle.await;
                            tracing::info!(name = %snap.name, "managed singleton shut down gracefully");
                        }
                    } else {
                        tracing::info!(name = %snap.name, "cancelled singleton (shard no longer owned)");
                    }
                }
            }
        }

        self.metrics.singletons.set(
            self.singletons
                .iter()
                .filter(|e| e.value().running.load(std::sync::atomic::Ordering::SeqCst))
                .count() as i64,
        );
    }

    /// Poll storage for a reply to a persisted message.
    ///
    /// This is used as a fallback when the gRPC streaming connection to the
    /// remote node fails. The message was already saved to storage, so another
    /// node may process it and save the reply there.
    ///
    /// Polls at `storage_poll_interval` until an exit reply is found.
    async fn reply_from_storage(
        &self,
        request_id: Snowflake,
    ) -> Result<ReplyReceiver, ClusterError> {
        let storage =
            self.message_storage
                .as_ref()
                .ok_or_else(|| ClusterError::PersistenceError {
                    reason: "no message storage configured for reply polling".into(),
                    source: None,
                })?;

        let poll_interval = self.config.storage_poll_interval;
        let (tx, rx) = mpsc::channel(16);

        let storage = Arc::clone(storage);
        let cancel = self.cancel.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!(request_id = request_id.0, "reply_from_storage: cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(poll_interval) => {}
                }

                match storage.replies_for(request_id).await {
                    Ok(replies) => {
                        for reply in replies {
                            let is_exit = matches!(reply, Reply::WithExit(_));
                            if tx.send(reply).await.is_err() {
                                // Receiver dropped
                                return;
                            }
                            if is_exit {
                                tracing::debug!(
                                    request_id = request_id.0,
                                    "reply_from_storage: delivered exit reply"
                                );
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            request_id = request_id.0,
                            error = %e,
                            "reply_from_storage: failed to poll replies"
                        );
                    }
                }
            }
        });

        Ok(rx)
    }
}

#[async_trait]
impl Sharding for ShardingImpl {
    fn get_shard_id(&self, entity_type: &EntityType, entity_id: &EntityId) -> ShardId {
        let shard = shard_for_entity(entity_id.as_ref(), self.config.shards_per_group);
        // Resolve shard group from the registered entity's shard_group_for() method,
        // falling back to the first configured shard group or "default".
        let group = self
            .entity_managers
            .get(entity_type)
            .map(|manager| manager.entity().shard_group_for(entity_id).to_string())
            .unwrap_or_else(|| {
                if let Some(group) = self.config.shard_groups.first() {
                    group.clone()
                } else {
                    tracing::warn!(
                        entity_type = %entity_type,
                        "no entity manager registered and shard_groups config is empty, \
                         falling back to hardcoded \"default\" shard group"
                    );
                    "default".to_string()
                }
            });
        ShardId::new(&group, shard)
    }

    fn has_shard_id(&self, shard_id: &ShardId) -> bool {
        self.has_shard_sync(shard_id)
    }

    fn snowflake(&self) -> &SnowflakeGenerator {
        &self.snowflake
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    #[instrument(skip(self, entity), fields(entity_type))]
    async fn register_entity(&self, entity: Arc<dyn Entity>) -> Result<(), ClusterError> {
        let entity_type = entity.entity_type();
        tracing::Span::current().record("entity_type", tracing::field::display(&entity_type));
        let max_idle = entity
            .max_idle_time()
            .unwrap_or(self.config.entity_max_idle_time);

        // Get Arc<dyn Sharding> from self_ref for entity inter-entity communication.
        let sharding: Option<Arc<dyn Sharding>> = self
            .self_ref
            .get()
            .and_then(|weak| weak.upgrade())
            .map(|arc| arc as Arc<dyn Sharding>);

        let manager = Arc::new(EntityManager::with_sharding(
            Arc::clone(&entity),
            Arc::clone(&self.config),
            self.config.runner_address.clone(),
            Arc::clone(&self.snowflake),
            self.message_storage.clone(),
            self.state_storage.clone(),
            self.workflow_engine.clone(),
            sharding,
        ));

        self.reaper.register(Arc::clone(&manager), max_idle).await;

        // If an entity type was already registered, interrupt its active entities
        // before replacing the manager to avoid orphaned entity instances.
        if let Some((_key, old_manager)) = self.entity_managers.remove(&entity_type) {
            tracing::warn!(%entity_type, "replacing existing entity registration, interrupting active entities");
            // Interrupt all shards for the old manager
            let owned_shards = self.owned_shards.read().await;
            for shard_id in owned_shards.iter() {
                old_manager.interrupt_shard(shard_id).await;
            }
        }

        // Acquire storage_read_lock before inserting the entity manager to ensure
        // no storage poll is running concurrently. This matches the TS behavior
        // where `registerEntity` acquires `withStorageReadLock` (Sharding.ts:1296-1302)
        // to prevent races where messages are half-dispatched during registration.
        let _storage_lock = self
            .storage_read_lock
            .acquire()
            .await
            .expect("semaphore not closed");

        self.entity_managers.insert(entity_type.clone(), manager);

        drop(_storage_lock);

        // Clean up any first-seen tracking for this entity type now that it's registered.
        self.unregistered_first_seen
            .retain(|(et, _), _| et != &entity_type);

        // Wake any route_local calls waiting for this entity type.
        self.entity_registration_notify.notify_waiters();

        let _ = self
            .event_tx
            .send(ShardingRegistrationEvent::EntityRegistered {
                entity_type: entity_type.clone(),
            });

        self.metrics.entities.set(self.entity_managers.len() as i64);

        tracing::info!(%entity_type, "registered entity type");
        Ok(())
    }

    #[instrument(skip(self, run))]
    async fn register_singleton(
        &self,
        name: &str,
        shard_group: Option<&str>,
        run: Arc<
            dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>> + Send + Sync,
        >,
    ) -> Result<(), ClusterError> {
        // Serialize with sync_singletons to prevent races.
        // Matches the TS `withSingletonLock` (Sharding.ts).
        let _singleton_guard = self.singleton_lock.lock().await;

        // Cancel any previously registered singleton with the same name
        // to avoid orphaned tasks that can't be tracked or stopped.
        // We must abort the old JoinHandle in addition to cancelling the token,
        // because the factory may ignore the cancellation token.
        if let Some((_, mut old)) = self.singletons.remove(name) {
            old.cancel.cancel();
            if let Some(h) = old.handle.take() {
                h.abort();
            }
            tracing::info!(name, "cancelled previously registered singleton");
        }

        let cancel = tokio_util::sync::CancellationToken::new();

        // Compute shard ID directly from singleton name, bypassing entity manager lookup.
        // This matches the TS source (Sharding.ts:1189): options?.shardGroup ?? "default".
        let shard_group = shard_group.unwrap_or("default").to_string();
        let shard = crate::hash::shard_for_entity(name, self.config.shards_per_group);
        let shard_id = ShardId::new(&shard_group, shard);

        // In single-node mode (no runner_storage), always spawn immediately.
        // In multi-runner mode, only spawn if the computed shard is owned.
        let should_run = if self.runner_storage.is_none() {
            true
        } else {
            self.has_shard_async(&shard_id).await
        };

        let running = Arc::new(std::sync::atomic::AtomicBool::new(should_run));
        let consecutive_failures = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let last_failure_ms = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let managed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handle: Option<tokio::task::JoinHandle<()>> = None;
        if should_run {
            // Shard is owned — spawn now.
            let cancel_clone = cancel.clone();
            let name_owned = name.to_string();
            let factory = Arc::clone(&run);
            let running_flag = Arc::clone(&running);
            let failure_count = Arc::clone(&consecutive_failures);
            let failure_time = Arc::clone(&last_failure_ms);
            let managed_flag = Arc::clone(&managed);
            handle = Some(self.spawn_singleton_task(
                name_owned,
                cancel_clone,
                factory,
                running_flag,
                failure_count,
                failure_time,
                managed_flag,
            ));
        }

        self.singletons.insert(
            name.to_string(),
            SingletonEntry {
                cancel,
                running,
                factory: run,
                handle,
                shard_group,
                consecutive_failures,
                last_failure_ms,
                managed,
            },
        );

        let _ = self
            .event_tx
            .send(ShardingRegistrationEvent::SingletonRegistered {
                name: name.to_string(),
            });

        self.metrics.singletons.set(
            self.singletons
                .iter()
                .filter(|e| e.value().running.load(std::sync::atomic::Ordering::SeqCst))
                .count() as i64,
        );

        tracing::info!(name, "registered singleton");
        Ok(())
    }

    fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient {
        EntityClient::new(self, entity_type)
    }

    #[instrument(skip(self, envelope), fields(
        entity_type = %envelope.address.entity_type,
        entity_id = %envelope.address.entity_id,
        shard_id = %envelope.address.shard_id,
        request_id = %envelope.request_id,
        tag = %envelope.tag,
    ))]
    async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
        if self.is_shutdown() {
            return Err(ClusterError::ShuttingDown);
        }

        let shard_id = envelope.address.shard_id.clone();

        // For persisted messages, save to storage before routing.
        // If duplicate, return the existing reply immediately.
        if envelope.persisted {
            let storage = match &self.message_storage {
                Some(storage) => storage,
                None => {
                    return Err(ClusterError::PersistenceError {
                        reason: "persisted messages require message storage".to_string(),
                        source: None,
                    })
                }
            };
            match storage.save_request(&envelope).await? {
                SaveResult::Success => {}
                SaveResult::Duplicate { existing_reply } => {
                    // Return the existing reply if available, otherwise register a handler.
                    let (tx, rx) = mpsc::channel(16);
                    if let Some(reply) = existing_reply {
                        let _ = tx.send(reply).await;
                        return Ok(rx);
                    }
                    storage.register_reply_handler(envelope.request_id, tx);
                    self.storage_poll_notify.notify_one();
                    return Ok(rx);
                }
            }
        }

        let max_retries = self.config.send_retry_count;
        let retry_interval = self.config.send_retry_interval;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                tokio::time::sleep(retry_interval).await;
                if self.is_shutdown() {
                    return Err(ClusterError::ShuttingDown);
                }
            }

            let is_last = attempt == max_retries;

            if self.has_shard_async(&shard_id).await {
                if envelope.persisted {
                    // Persisted messages are dispatched via the storage poll path.
                    let (tx, rx) = mpsc::channel(16);
                    if let Some(ref storage) = self.message_storage {
                        storage.register_reply_handler(envelope.request_id, tx);
                    }
                    self.storage_poll_notify.notify_one();
                    return Ok(rx);
                }

                // Local delivery
                let (tx, rx) = mpsc::channel(16);
                match self.route_local(envelope.clone(), Some(tx)).await {
                    Ok(()) => return Ok(rx),
                    Err(e) if Self::is_retryable(&e) && !is_last => {
                        tracing::debug!(attempt, error = %e, "send: retryable error, will retry");
                        last_err = Some(e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else if let Some(owner) = self.get_shard_owner_async(&shard_id).await {
                // Remote delivery via the Runners transport.
                // Use send() to get streaming replies from the remote node.
                match self.runners.send(&owner, envelope.clone()).await {
                    Ok(rx) => return Ok(rx),
                    Err(e) if envelope.persisted => {
                        // For persisted messages, fall back to polling storage for replies.
                        // The message was already saved to storage, so another node may
                        // process it and save the reply there.
                        tracing::debug!(
                            attempt,
                            error = %e,
                            "send: remote send failed for persisted message, falling back to storage polling"
                        );
                        return self.reply_from_storage(envelope.request_id).await;
                    }
                    Err(e) if Self::is_retryable(&e) && !is_last => {
                        tracing::debug!(attempt, error = %e, "send: retryable error on remote, will retry");
                        last_err = Some(e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                // No runner assigned for this shard
                if !is_last {
                    tracing::debug!(attempt, "send: shard not assigned, will retry");
                    last_err = Some(ClusterError::EntityNotAssignedToRunner {
                        entity_type: envelope.address.entity_type.clone(),
                        entity_id: envelope.address.entity_id.clone(),
                    });
                    continue;
                }
                return Err(ClusterError::EntityNotAssignedToRunner {
                    entity_type: envelope.address.entity_type,
                    entity_id: envelope.address.entity_id,
                });
            }
        }

        Err(
            last_err.unwrap_or_else(|| ClusterError::EntityNotAssignedToRunner {
                entity_type: envelope.address.entity_type,
                entity_id: envelope.address.entity_id,
            }),
        )
    }

    #[instrument(skip(self, envelope), fields(
        entity_type = %envelope.address.entity_type,
        entity_id = %envelope.address.entity_id,
        shard_id = %envelope.address.shard_id,
        tag = %envelope.tag,
    ))]
    async fn notify(&self, envelope: EnvelopeRequest) -> Result<(), ClusterError> {
        if self.is_shutdown() {
            return Err(ClusterError::ShuttingDown);
        }

        let shard_id = envelope.address.shard_id.clone();

        // For persisted messages, save to storage before routing.
        if envelope.persisted {
            let storage = match &self.message_storage {
                Some(storage) => storage,
                None => {
                    return Err(ClusterError::PersistenceError {
                        reason: "persisted messages require message storage".to_string(),
                        source: None,
                    })
                }
            };
            match storage.save_envelope(&envelope).await? {
                SaveResult::Success => {}
                SaveResult::Duplicate { .. } => {
                    // Already saved, nothing more to do for fire-and-forget.
                    return Ok(());
                }
            }
        }

        let max_retries = self.config.send_retry_count;
        let retry_interval = self.config.send_retry_interval;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                tokio::time::sleep(retry_interval).await;
                if self.is_shutdown() {
                    return Err(ClusterError::ShuttingDown);
                }
            }

            let is_last = attempt == max_retries;

            if self.has_shard_async(&shard_id).await {
                if envelope.persisted {
                    // Persisted notifications are delivered via storage polling.
                    self.storage_poll_notify.notify_one();
                    return Ok(());
                }

                match self.route_local(envelope.clone(), None).await {
                    Ok(()) => return Ok(()),
                    Err(e) if Self::is_retryable(&e) && !is_last => {
                        tracing::debug!(attempt, error = %e, "notify: retryable error, will retry");
                        last_err = Some(e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else if let Some(owner) = self.get_shard_owner_async(&shard_id).await {
                match self
                    .runners
                    .notify(&owner, Envelope::Request(envelope.clone()))
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(e) if Self::is_retryable(&e) && !is_last => {
                        tracing::debug!(attempt, error = %e, "notify: retryable error on remote, will retry");
                        last_err = Some(e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                if !is_last {
                    tracing::debug!(attempt, "notify: shard not assigned, will retry");
                    last_err = Some(ClusterError::EntityNotAssignedToRunner {
                        entity_type: envelope.address.entity_type.clone(),
                        entity_id: envelope.address.entity_id.clone(),
                    });
                    continue;
                }
                return Err(ClusterError::EntityNotAssignedToRunner {
                    entity_type: envelope.address.entity_type,
                    entity_id: envelope.address.entity_id,
                });
            }
        }

        Err(
            last_err.unwrap_or_else(|| ClusterError::EntityNotAssignedToRunner {
                entity_type: envelope.address.entity_type,
                entity_id: envelope.address.entity_id,
            }),
        )
    }

    #[instrument(skip(self, ack), fields(request_id = %ack.request_id, sequence = ack.sequence))]
    async fn ack_chunk(&self, ack: AckChunk) -> Result<(), ClusterError> {
        if self.is_shutdown() {
            return Err(ClusterError::ShuttingDown);
        }

        let storage = match &self.message_storage {
            Some(storage) => storage,
            None => {
                return Err(ClusterError::PersistenceError {
                    reason: "ack requires message storage".to_string(),
                    source: None,
                })
            }
        };

        storage.ack_chunk(&ack).await
    }

    #[instrument(skip(self, interrupt), fields(
        entity_type = %interrupt.address.entity_type,
        entity_id = %interrupt.address.entity_id,
        shard_id = %interrupt.address.shard_id,
        request_id = %interrupt.request_id,
    ))]
    async fn interrupt(&self, interrupt: Interrupt) -> Result<(), ClusterError> {
        if self.is_shutdown() {
            return Err(ClusterError::ShuttingDown);
        }

        let shard_id = interrupt.address.shard_id.clone();
        let entity_type = interrupt.address.entity_type.clone();
        let entity_id = interrupt.address.entity_id.clone();

        if self.has_shard_async(&shard_id).await {
            self.handle_interrupt_local(&interrupt).await;
            return Ok(());
        }

        if let Some(owner) = self.get_shard_owner_async(&shard_id).await {
            return self
                .runners
                .notify(&owner, Envelope::Interrupt(interrupt))
                .await;
        }

        Err(ClusterError::EntityNotAssignedToRunner {
            entity_type,
            entity_id,
        })
    }

    async fn poll_storage(&self) -> Result<(), ClusterError> {
        self.poll_storage_inner().await
    }

    fn active_entity_count(&self) -> usize {
        self.entity_managers
            .iter()
            .map(|entry| entry.value().active_count())
            .sum()
    }

    async fn registration_events(
        &self,
    ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
        use tokio_stream::StreamExt;
        let rx = self.event_tx.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
        Box::pin(
            stream.filter_map(|r: Result<ShardingRegistrationEvent, _>| match r {
                Ok(event) => Some(event),
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(count)) => {
                    tracing::warn!(
                        lagged_count = count,
                        "registration_events subscriber lagged, {} events dropped",
                        count
                    );
                    None
                }
            }),
        )
    }

    #[instrument(skip(self))]
    async fn shutdown(&self) -> Result<(), ClusterError> {
        self.shutdown.store(true, Ordering::Release);
        self.cancel.cancel();

        // Cancel all singletons and collect their JoinHandles for awaiting.
        let mut singleton_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for mut entry in self.singletons.iter_mut() {
            entry.value().cancel.cancel();
            if let Some(h) = entry.value_mut().handle.take() {
                singleton_handles.push(h);
            }
        }

        // Transition all entity managers to "closing" state before interrupting.
        // In closing state, new Request messages are rejected but Envelope messages
        // (fire-and-forget, ack, interrupt) are still accepted, allowing in-flight
        // operations to complete. Matches TS Sharding.ts:1286 (alive → closing → closed).
        for entry in self.entity_managers.iter() {
            entry.value().set_closing();
        }

        // Interrupt all entities on all owned shards
        let owned = self.owned_shards.read().await.clone();
        for shard_id in &owned {
            for entry in self.entity_managers.iter() {
                entry.value().interrupt_shard(shard_id).await;
            }
        }

        // Collect resumption task handles.
        let resumption_handles: Vec<_> = self
            .resumption_handles
            .iter()
            .map(|r| r.key().clone())
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|k| self.resumption_handles.remove(&k).map(|(_, h)| h))
            .collect();

        // Wait for background tasks to stop FIRST, before releasing shard locks.
        // This ensures no concurrent etcd operations from rebalance/refresh loops.
        let handles: Vec<_> = {
            let mut tasks = self.background_tasks.lock().await;
            let mut all = tasks.drain(..).collect::<Vec<_>>();
            all.extend(singleton_handles);
            all.extend(resumption_handles);
            all
        };
        let total = handles.len();
        for (i, handle) in handles.into_iter().enumerate() {
            match tokio::time::timeout(self.config.entity_termination_timeout, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_error)) => {
                    if join_error.is_panic() {
                        let panic_payload = join_error.into_panic();
                        let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        tracing::error!(
                            task_index = i,
                            task_count = total,
                            panic_message = %msg,
                            "background task panicked during shutdown"
                        );
                    } else {
                        tracing::warn!(
                            task_index = i,
                            task_count = total,
                            error = %join_error,
                            "background task failed during shutdown"
                        );
                    }
                }
                Err(_timeout) => {
                    tracing::warn!(
                        task_index = i,
                        task_count = total,
                        timeout_secs = self.config.entity_termination_timeout.as_secs_f64(),
                        "background task did not complete within shutdown timeout"
                    );
                }
            }
        }

        // Release all shard locks in storage AFTER background tasks have stopped.
        // This prevents concurrent etcd operations from rebalance/refresh loops.
        if let Some(ref runner_storage) = self.runner_storage {
            if let Err(e) = runner_storage
                .release_all(&self.config.runner_address)
                .await
            {
                tracing::warn!(error = %e, "failed to release all shard locks during shutdown");
            }
        }

        // Clear owned shards
        self.owned_shards.write().await.clear();
        self.metrics.shards.set(0);

        tracing::info!("sharding shut down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{Entity, EntityContext, EntityHandler};
    use crate::reply::{ExitResult, Reply};
    use crate::snowflake::Snowflake;
    use crate::storage::noop_runners::NoopRunners;
    use crate::types::{EntityAddress, EntityId};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct EchoEntity;

    #[async_trait]
    impl Entity for EchoEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Echo")
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(EchoHandler))
        }
    }

    struct EchoHandler;

    #[async_trait]
    impl EntityHandler for EchoHandler {
        async fn handle_request(
            &self,
            _tag: &str,
            payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            Ok(payload.to_vec())
        }
    }

    #[derive(Default, Clone)]
    struct RecordingWorkflowEngine {
        resolved: Arc<Mutex<Vec<(String, String, String)>>>,
    }

    #[async_trait]
    impl WorkflowEngine for RecordingWorkflowEngine {
        async fn sleep(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
            _name: &str,
            _duration: std::time::Duration,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn await_deferred(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
            _name: &str,
        ) -> Result<Vec<u8>, ClusterError> {
            Ok(Vec::new())
        }

        async fn resolve_deferred(
            &self,
            workflow_name: &str,
            execution_id: &str,
            name: &str,
            _value: Vec<u8>,
        ) -> Result<(), ClusterError> {
            self.resolved.lock().unwrap().push((
                workflow_name.to_string(),
                execution_id.to_string(),
                name.to_string(),
            ));
            Ok(())
        }

        async fn on_interrupt(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
        ) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    fn make_sharding() -> Arc<ShardingImpl> {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        ShardingImpl::new(config, runners, None, None, None, metrics).unwrap()
    }

    #[tokio::test]
    async fn interrupt_resolves_workflow_signal_locally() {
        let resolved = Arc::new(Mutex::new(Vec::new()));
        let engine = Arc::new(RecordingWorkflowEngine {
            resolved: Arc::clone(&resolved),
        });
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let sharding = ShardingImpl::new_with_engines(
            config,
            runners,
            None,
            None,
            None,
            None,
            Some(engine),
            metrics,
        )
        .unwrap();
        sharding.acquire_all_shards().await;

        let address = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Echo"),
            entity_id: EntityId::new("e-1"),
        };
        let interrupt = Interrupt {
            request_id: Snowflake(1),
            address,
        };

        sharding.interrupt(interrupt).await.unwrap();

        let resolved = resolved.lock().unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0, "Echo");
        assert_eq!(resolved[0].1, "e-1");
        assert_eq!(resolved[0].2, INTERRUPT_SIGNAL);
    }

    struct CustomGroupEntity;

    #[async_trait]
    impl Entity for CustomGroupEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("CustomGroup")
        }

        fn shard_group(&self) -> &str {
            "premium"
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(EchoHandler))
        }
    }

    /// Entity with per-ID shard group resolution.
    struct PerIdGroupEntity;

    #[async_trait]
    impl Entity for PerIdGroupEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("PerIdGroup")
        }

        fn shard_group_for(&self, entity_id: &EntityId) -> &str {
            if entity_id.as_ref().starts_with("vip-") {
                "premium"
            } else {
                "default"
            }
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(EchoHandler))
        }
    }

    #[tokio::test]
    async fn get_shard_id_is_deterministic() {
        let s = make_sharding();
        let et = EntityType::new("Test");
        let eid = EntityId::new("abc");
        let s1 = s.get_shard_id(&et, &eid);
        let s2 = s.get_shard_id(&et, &eid);
        assert_eq!(s1, s2);
    }

    #[tokio::test]
    async fn get_shard_id_in_range() {
        let s = make_sharding();
        let et = EntityType::new("Test");
        for i in 0..100 {
            let eid = EntityId::new(format!("id-{i}"));
            let shard = s.get_shard_id(&et, &eid);
            assert!(shard.id >= 0 && shard.id < 10);
            assert_eq!(shard.group, "default");
        }
    }

    #[tokio::test]
    async fn has_shard_id_after_acquire() {
        let s = make_sharding();
        let shard = ShardId::new("default", 0);
        assert!(!s.has_shard_id(&shard));

        s.acquire_all_shards().await;
        assert!(s.has_shard_id(&shard));
    }

    #[tokio::test]
    async fn register_entity_and_send_locally() {
        let s = make_sharding();
        s.acquire_all_shards().await;

        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-1");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);
        let payload = rmp_serde::to_vec(&42i32).unwrap();

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard,
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: payload.clone(),
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let mut rx = s.send(envelope).await.unwrap();
        let reply = rx.recv().await.unwrap();
        match reply {
            Reply::WithExit(r) => match r.exit {
                ExitResult::Success(bytes) => assert_eq!(bytes, payload),
                ExitResult::Failure(msg) => panic!("unexpected failure: {msg}"),
            },
            Reply::Chunk(_) => panic!("unexpected chunk"),
        }
    }

    #[tokio::test]
    async fn notify_locally() {
        let s = make_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-1");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard,
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "ping".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        s.notify(envelope).await.unwrap();
        // Give time for processing
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(s.active_entity_count() > 0);
    }

    #[tokio::test]
    async fn send_to_unowned_shard_fails() {
        let s = make_sharding();
        // Don't acquire shards
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Echo"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "echo".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let result = s.send(envelope).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn send_to_unregistered_entity_fails() {
        let s = make_sharding();
        s.acquire_all_shards().await;
        // Don't register any entity

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("NonExistent"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "x".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let result = s.send(envelope).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn make_client_works() {
        let s = make_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let client = Arc::clone(&s).make_client(EntityType::new("Echo"));
        assert_eq!(client.entity_type(), &EntityType::new("Echo"));
    }

    #[tokio::test]
    async fn shutdown_sets_flag() {
        let s = make_sharding();
        assert!(!s.is_shutdown());
        s.shutdown().await.unwrap();
        assert!(s.is_shutdown());
    }

    #[tokio::test]
    async fn active_entity_count_reflects_spawned() {
        let s = make_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        assert_eq!(s.active_entity_count(), 0);

        // Send a message to spawn an entity
        let eid = EntityId::new("e-1");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);
        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard,
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: vec![1],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let mut rx = s.send(envelope).await.unwrap();
        rx.recv().await.unwrap();

        assert!(s.active_entity_count() > 0);
    }

    #[tokio::test]
    async fn registration_events_emitted() {
        use tokio_stream::StreamExt;

        let s = make_sharding();
        let mut events = s.registration_events().await;

        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_millis(100), events.next())
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            event,
            ShardingRegistrationEvent::EntityRegistered { .. }
        ));
    }

    #[tokio::test]
    async fn singleton_registration() {
        let s = make_sharding();
        let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
        let tx = Arc::new(std::sync::Mutex::new(Some(tx)));

        s.register_singleton(
            "test-singleton",
            None,
            Arc::new(move |_ctx| {
                let tx = tx.clone();
                Box::pin(async move {
                    if let Some(tx) = tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

        // The singleton should run
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut rx)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_singletons() {
        let s = make_sharding();
        let (tx, mut rx) = tokio::sync::oneshot::channel::<&'static str>();
        let tx = Arc::new(std::sync::Mutex::new(Some(tx)));

        s.register_singleton(
            "long-singleton",
            None,
            Arc::new(move |_ctx| {
                let tx = tx.clone();
                Box::pin(async move {
                    // This would run forever, but shutdown should cancel it
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                    if let Some(tx) = tx.lock().unwrap().take() {
                        let _ = tx.send("completed");
                    }
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        s.shutdown().await.unwrap();

        // The singleton should have been cancelled, so rx should be dropped
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn get_shard_id_uses_entity_shard_group() {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string(), "premium".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();

        // Before registration, falls back to first config group
        let eid = EntityId::new("abc");
        let shard = s.get_shard_id(&EntityType::new("CustomGroup"), &eid);
        assert_eq!(shard.group, "default");

        // After registration, uses entity's shard_group_for()
        s.register_entity(Arc::new(CustomGroupEntity))
            .await
            .unwrap();
        let shard = s.get_shard_id(&EntityType::new("CustomGroup"), &eid);
        assert_eq!(shard.group, "premium");
    }

    #[tokio::test]
    async fn get_shard_id_per_entity_id_group_resolution() {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string(), "premium".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();

        s.register_entity(Arc::new(PerIdGroupEntity)).await.unwrap();

        let regular = EntityId::new("user-123");
        let vip = EntityId::new("vip-456");

        let shard_regular = s.get_shard_id(&EntityType::new("PerIdGroup"), &regular);
        let shard_vip = s.get_shard_id(&EntityType::new("PerIdGroup"), &vip);

        assert_eq!(shard_regular.group, "default");
        assert_eq!(shard_vip.group, "premium");
    }

    #[tokio::test]
    async fn persisted_send_saves_to_storage() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-1");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);
        let payload = rmp_serde::to_vec(&42i32).unwrap();

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard.clone(),
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: payload.clone(),
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let request_id = envelope.request_id;
        let mut rx = s.send(envelope).await.unwrap();
        let reply = rx.recv().await.unwrap();
        match &reply {
            Reply::WithExit(r) => match &r.exit {
                crate::reply::ExitResult::Success(bytes) => assert_eq!(bytes, &payload),
                crate::reply::ExitResult::Failure(msg) => panic!("unexpected failure: {msg}"),
            },
            Reply::Chunk(_) => panic!("unexpected chunk"),
        }

        // After processing, the reply should also be saved to storage
        let replies = storage.replies_for(request_id).await.unwrap();
        assert_eq!(replies.len(), 1);
    }

    #[tokio::test]
    async fn persisted_send_duplicate_returns_existing_reply() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-dup");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard,
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: vec![1, 2, 3],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        // First send
        let mut rx1 = s.send(envelope.clone()).await.unwrap();
        let _reply1 = rx1.recv().await.unwrap();

        // Second send (duplicate) — should return immediately with the stored reply.
        let mut rx2 = s.send(envelope).await.unwrap();
        let reply2 = tokio::time::timeout(std::time::Duration::from_millis(100), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(reply2, Reply::WithExit(_)));
    }

    #[tokio::test]
    async fn persisted_send_duplicate_registers_reply_handler() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();

        let request_id = Snowflake(5000);
        let envelope = EnvelopeRequest {
            request_id,
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Echo"),
                entity_id: EntityId::new("e-dup-no-reply"),
            },
            tag: "echo".into(),
            payload: vec![1, 2, 3],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        match storage.save_request(&envelope).await.unwrap() {
            SaveResult::Success => {}
            other => panic!("unexpected save result: {other:?}"),
        }

        let mut rx = s.send(envelope).await.unwrap();

        let reply = Reply::WithExit(ReplyWithExit {
            request_id,
            id: Snowflake(6000),
            exit: ExitResult::Success(vec![9, 9, 9]),
        });
        storage.save_reply(&reply).await.unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(received, Reply::WithExit(_)));
    }

    #[tokio::test]
    async fn persisted_notify_saves_to_storage() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-notify");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard.clone(),
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "ping".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        s.notify(envelope.clone()).await.unwrap();

        // Duplicate notify should be silently ignored
        s.notify(envelope).await.unwrap();
    }

    #[tokio::test]
    async fn persisted_send_without_storage_returns_error() {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-no-storage");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard.clone(),
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: vec![1],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let err = s.send(envelope).await.unwrap_err();
        match err {
            ClusterError::PersistenceError { reason, .. } => {
                assert!(reason.contains("message storage"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn persisted_notify_without_storage_returns_error() {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-no-storage-notify");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard.clone(),
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "ping".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let err = s.notify(envelope).await.unwrap_err();
        match err {
            ClusterError::PersistenceError { reason, .. } => {
                assert!(reason.contains("message storage"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_persisted_send_does_not_save_to_storage() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-non-persist");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard.clone(),
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: vec![1],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let request_id = envelope.request_id;
        let mut rx = s.send(envelope).await.unwrap();
        let _reply = rx.recv().await.unwrap();

        // Non-persisted messages should NOT be saved to storage
        let replies = storage.replies_for(request_id).await.unwrap();
        assert!(replies.is_empty());
    }

    #[tokio::test]
    async fn poll_storage_dispatches_unprocessed_messages() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let eid = EntityId::new("e-poll");
        let shard = s.get_shard_id(&EntityType::new("Echo"), &eid);

        // Save a persisted envelope directly to storage (simulating a message
        // written by another runner or a previous incarnation).
        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: shard.clone(),
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: vec![7, 8, 9],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        storage.save_envelope(&envelope).await.unwrap();

        // Before poll_storage, no entity instances should exist
        assert_eq!(s.active_entity_count(), 0);

        // Poll storage should pick up the unprocessed message and route it
        s.poll_storage().await.unwrap();

        // Give the entity manager time to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The entity should have been spawned
        assert!(s.active_entity_count() > 0);
    }

    #[tokio::test]
    async fn poll_storage_no_op_without_storage() {
        let s = make_sharding();
        s.acquire_all_shards().await;
        // No storage configured — should return Ok without error
        s.poll_storage().await.unwrap();
    }

    #[tokio::test]
    async fn poll_storage_skips_unregistered_entity_types() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        // Do NOT register any entity

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Unknown"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "x".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        storage.save_envelope(&envelope).await.unwrap();

        // Should not error — just skip the unregistered entity type
        s.poll_storage().await.unwrap();
        assert_eq!(s.active_entity_count(), 0);
    }

    #[tokio::test]
    async fn singleton_deferred_spawn_on_shard_acquisition() {
        use crate::storage::memory_runner::MemoryRunnerStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            Some(runner_storage as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();

        // Register singleton BEFORE acquiring shards — factory should be stored
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tx = Arc::new(std::sync::Mutex::new(Some(tx)));
        s.register_singleton(
            "test-deferred",
            None,
            Arc::new(move |_ctx| {
                let tx = tx.clone();
                Box::pin(async move {
                    if let Some(tx) = tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

        // Singleton should NOT be running yet (no shards owned)
        assert!(!s
            .singletons
            .get("test-deferred")
            .unwrap()
            .running
            .load(std::sync::atomic::Ordering::SeqCst));

        // Acquire all shards, then sync singletons
        s.acquire_all_shards().await;
        s.sync_singletons().await;

        // Now the singleton should be running
        let result = tokio::time::timeout(std::time::Duration::from_millis(200), rx).await;
        assert!(
            result.is_ok(),
            "singleton should have been spawned after shard acquisition"
        );
    }

    #[tokio::test]
    async fn singleton_cancelled_when_shard_lost() {
        use crate::storage::memory_runner::MemoryRunnerStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            Some(runner_storage as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;

        // Register singleton with shards owned — should spawn immediately
        let (tx, _rx) = tokio::sync::oneshot::channel::<()>();
        let tx = Arc::new(std::sync::Mutex::new(Some(tx)));
        s.register_singleton(
            "test-cancel",
            None,
            Arc::new(move |_ctx| {
                let tx = tx.clone();
                Box::pin(async move {
                    // Long-running singleton
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                    if let Some(tx) = tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

        assert!(s
            .singletons
            .get("test-cancel")
            .unwrap()
            .running
            .load(std::sync::atomic::Ordering::SeqCst));

        // Clear owned shards (simulate shard loss) and sync
        s.owned_shards.write().await.clear();
        s.sync_singletons().await;

        // Singleton should be cancelled (token cancelled). The running flag will be
        // set to false by the spawned task after it observes cancellation.
        assert!(s
            .singletons
            .get("test-cancel")
            .unwrap()
            .cancel
            .is_cancelled());
        // Give the spawned task a moment to observe cancellation and set running=false.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!s
            .singletons
            .get("test-cancel")
            .unwrap()
            .running
            .load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn singleton_running_flag_reset_on_factory_panic() {
        use crate::storage::memory_runner::MemoryRunnerStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            Some(runner_storage as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;

        // Register a singleton whose factory panics
        s.register_singleton(
            "panicking-singleton",
            None,
            Arc::new(|_ctx| {
                Box::pin(async {
                    panic!("intentional test panic");
                })
            }),
        )
        .await
        .unwrap();

        // Give the spawned task time to panic and recover
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // The running flag should be reset to false despite the panic
        assert!(
            !s.singletons
                .get("panicking-singleton")
                .unwrap()
                .running
                .load(std::sync::atomic::Ordering::SeqCst),
            "running flag should be false after factory panic"
        );

        // Verify the singleton can be re-spawned on next sync
        // (use a non-panicking factory this time by re-registering)
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed_clone = Arc::clone(&completed);
        s.register_singleton(
            "panicking-singleton",
            None,
            Arc::new(move |_ctx| {
                let completed = completed_clone.clone();
                Box::pin(async move {
                    completed.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            completed.load(std::sync::atomic::Ordering::SeqCst),
            "singleton should have been re-spawned after panic recovery"
        );
    }

    #[tokio::test]
    async fn singleton_normal_completion_stays_alive_until_cancelled() {
        use crate::storage::memory_runner::MemoryRunnerStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            Some(runner_storage as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;

        // Register a singleton that manages cancellation and waits for it
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started_clone = Arc::clone(&started);
        s.register_singleton(
            "completing-singleton",
            None,
            Arc::new(move |ctx| {
                let started = started_clone.clone();
                Box::pin(async move {
                    // Opt-in to manage cancellation
                    let cancel = ctx.cancellation();
                    started.store(true, std::sync::atomic::Ordering::SeqCst);
                    // Wait for cancellation
                    cancel.cancelled().await;
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

        // Give the spawned task time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The singleton should have started
        assert!(
            started.load(std::sync::atomic::Ordering::SeqCst),
            "singleton should have started"
        );

        // The running flag should still be true (waiting for cancellation)
        assert!(
            s.singletons
                .get("completing-singleton")
                .unwrap()
                .running
                .load(std::sync::atomic::Ordering::SeqCst),
            "running flag should still be true while singleton manages cancellation"
        );

        // Cancel the singleton
        s.singletons
            .get("completing-singleton")
            .unwrap()
            .cancel
            .cancel();

        // Give it time to process cancellation gracefully
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Now running should be false
        assert!(
            !s.singletons
                .get("completing-singleton")
                .unwrap()
                .running
                .load(std::sync::atomic::Ordering::SeqCst),
            "running flag should be false after graceful shutdown"
        );
    }

    #[tokio::test]
    async fn singleton_custom_shard_group_uses_specified_group() {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string(), "custom".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            None,
            Arc::new(ClusterMetrics::unregistered()),
        )
        .unwrap();
        s.acquire_all_shards().await;

        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = Arc::clone(&executed);

        // Register with a custom shard group
        s.register_singleton(
            "custom-group-singleton",
            Some("custom"),
            Arc::new(move |_ctx| {
                let e = executed_clone.clone();
                Box::pin(async move {
                    e.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Singleton should have executed (all shards owned in single-node mode)
        assert!(
            executed.load(std::sync::atomic::Ordering::SeqCst),
            "singleton with custom shard group should have executed"
        );

        // Verify the shard_group is stored correctly
        let entry = s.singletons.get("custom-group-singleton").unwrap();
        assert_eq!(entry.shard_group, "custom");
    }

    #[tokio::test]
    async fn metrics_updated_on_registration() {
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            None,
            Arc::clone(&metrics),
        )
        .unwrap();

        s.acquire_all_shards().await;
        assert_eq!(metrics.shards.get(), 10);

        s.register_entity(Arc::new(EchoEntity)).await.unwrap();
        assert_eq!(metrics.entities.get(), 1);
    }

    #[tokio::test]
    async fn start_registers_runner_and_sets_machine_id() {
        use crate::runner_storage::RunnerStorage;
        use crate::storage::memory_runner::MemoryRunnerStorage;

        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let address = RunnerAddress::new("10.0.0.1", 9000);
        let config = Arc::new(ShardingConfig {
            runner_address: address.clone(),
            runner_weight: 3,
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();

        s.start().await.unwrap();

        let runners = runner_storage.get_runners().await.unwrap();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].address, address);
        assert_eq!(runners[0].weight, 3);

        let machine_id = runner_storage
            .register(&Runner::new(address.clone(), 3))
            .await
            .unwrap();
        let snowflake = s.snowflake().next_async().await.unwrap();
        assert_eq!(snowflake.parts().machine_id, machine_id);

        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn runner_health_check_updates_status() {
        use crate::runner::Runner;
        use crate::runner_health::RunnerHealth;
        use crate::runner_storage::RunnerStorage;
        use crate::storage::memory_runner::MemoryRunnerStorage;
        use crate::storage::noop_health::NoopRunnerHealth;

        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let runner_health: Arc<dyn RunnerHealth> = Arc::new(NoopRunnerHealth);

        let addr1 = RunnerAddress::new("10.0.0.1", 9000);
        let addr2 = RunnerAddress::new("10.0.0.2", 9000);

        runner_storage
            .register(&Runner::new(addr1.clone(), 1))
            .await
            .unwrap();
        runner_storage
            .register(&Runner::new(addr2.clone(), 1))
            .await
            .unwrap();

        let config = Arc::new(ShardingConfig {
            runner_address: addr1.clone(),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            Some(runner_health),
            None,
            metrics,
        )
        .unwrap();

        // Perform a health check round — NoopRunnerHealth reports all alive
        let cancel = tokio_util::sync::CancellationToken::new();
        s.check_runner_health(
            &(runner_storage.clone() as Arc<dyn RunnerStorage>),
            &(Arc::new(NoopRunnerHealth) as Arc<dyn RunnerHealth>),
            &cancel,
        )
        .await
        .unwrap();

        // Both runners should still be healthy (NoopRunnerHealth returns true)
        let runners = runner_storage.get_runners().await.unwrap();
        assert!(runners.iter().all(|r| r.healthy));
    }

    #[tokio::test]
    async fn runner_health_check_marks_dead_runner() {
        use crate::runner::Runner;
        use crate::runner_health::RunnerHealth;
        use crate::runner_storage::RunnerStorage;
        use crate::storage::memory_runner::MemoryRunnerStorage;

        /// Health check that reports a specific address as dead.
        struct SelectiveHealth {
            dead_addr: RunnerAddress,
        }

        #[async_trait]
        impl RunnerHealth for SelectiveHealth {
            async fn is_alive(&self, address: &RunnerAddress) -> Result<bool, ClusterError> {
                Ok(address != &self.dead_addr)
            }
        }

        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let addr1 = RunnerAddress::new("10.0.0.1", 9000);
        let addr2 = RunnerAddress::new("10.0.0.2", 9000);

        runner_storage
            .register(&Runner::new(addr1.clone(), 1))
            .await
            .unwrap();
        runner_storage
            .register(&Runner::new(addr2.clone(), 1))
            .await
            .unwrap();

        let config = Arc::new(ShardingConfig {
            runner_address: addr1.clone(),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let dead_health: Arc<dyn RunnerHealth> = Arc::new(SelectiveHealth {
            dead_addr: addr2.clone(),
        });
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            Some(dead_health.clone()),
            None,
            metrics,
        )
        .unwrap();

        let cancel = tokio_util::sync::CancellationToken::new();
        s.check_runner_health(
            &(runner_storage.clone() as Arc<dyn RunnerStorage>),
            &dead_health,
            &cancel,
        )
        .await
        .unwrap();

        let runners = runner_storage.get_runners().await.unwrap();
        let r1 = runners.iter().find(|r| r.address == addr1).unwrap();
        let r2 = runners.iter().find(|r| r.address == addr2).unwrap();
        assert!(r1.healthy);
        assert!(!r2.healthy);
    }

    #[tokio::test]
    async fn runner_health_check_guards_last_healthy() {
        use crate::runner::Runner;
        use crate::runner_health::RunnerHealth;
        use crate::runner_storage::RunnerStorage;
        use crate::storage::memory_runner::MemoryRunnerStorage;

        /// Health check that reports all runners as dead.
        struct AllDeadHealth;

        #[async_trait]
        impl RunnerHealth for AllDeadHealth {
            async fn is_alive(&self, _address: &RunnerAddress) -> Result<bool, ClusterError> {
                Ok(false)
            }
        }

        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let addr1 = RunnerAddress::new("10.0.0.1", 9000);

        runner_storage
            .register(&Runner::new(addr1.clone(), 1))
            .await
            .unwrap();

        let config = Arc::new(ShardingConfig {
            runner_address: addr1.clone(),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let health: Arc<dyn RunnerHealth> = Arc::new(AllDeadHealth);
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            Some(health.clone()),
            None,
            metrics,
        )
        .unwrap();

        let cancel = tokio_util::sync::CancellationToken::new();
        s.check_runner_health(
            &(runner_storage.clone() as Arc<dyn RunnerStorage>),
            &health,
            &cancel,
        )
        .await
        .unwrap();

        // The runner should still be healthy because the guard prevents
        // marking the last healthy runner as unhealthy, AND the force-mark
        // self-as-healthy kicks in when zero runners are healthy.
        let runners = runner_storage.get_runners().await.unwrap();
        assert!(runners.iter().any(|r| r.healthy));
    }

    #[tokio::test]
    async fn lock_refresh_failure_releases_shard_after_max_consecutive_failures() {
        use crate::storage::memory_runner::MemoryRunnerStorage;
        use std::time::Duration;

        let runner_storage = Arc::new(MemoryRunnerStorage::new());
        let config = Arc::new(ShardingConfig {
            runner_lock_refresh_interval: Duration::from_millis(10),
            shard_lock_refresh_max_failures: 2,
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();

        // Manually add a shard to owned_shards.
        let shard = ShardId {
            group: "default".to_string(),
            id: 1,
        };
        {
            let mut owned = s.owned_shards.write().await;
            owned.insert(shard.clone());
        }
        assert!(s.has_shard_id(&shard));

        // Acquire the lock so MemoryRunnerStorage considers it held by this runner.
        runner_storage
            .acquire(&shard, &s.config.runner_address)
            .await
            .unwrap();

        // Release the lock from storage to make refresh return false (lock lost).
        runner_storage
            .release(&shard, &s.config.runner_address)
            .await
            .unwrap();

        // Start the refresh loop in the background.
        let s_clone = Arc::clone(&s);
        let handle = tokio::spawn(async move {
            s_clone.lock_refresh_loop().await;
        });

        // Wait for the refresh loop to detect the lost lock.
        tokio::time::sleep(Duration::from_millis(100)).await;
        s.cancel.cancel();
        let _ = handle.await;

        // Shard should have been removed from owned_shards.
        assert!(!s.has_shard_id(&shard));
    }

    #[tokio::test]
    async fn route_local_waits_for_late_entity_registration() {
        use std::time::Duration;
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_registration_timeout: Duration::from_secs(2),
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();
        s.acquire_all_shards().await;

        // Build an envelope targeting an "Echo" entity (not yet registered).
        let entity_id = EntityId::new("test-1");
        let shard_id = s.get_shard_id(&EntityType::new("Echo"), &entity_id);
        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id,
                entity_type: EntityType::new("Echo"),
                entity_id,
            },
            tag: "ping".to_string(),
            payload: vec![1, 2, 3],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let (reply_tx, mut reply_rx) = mpsc::channel(1);

        // Spawn route_local — it should wait for entity registration.
        let s_clone = Arc::clone(&s);
        let envelope_clone = envelope.clone();
        let route_handle =
            tokio::spawn(async move { s_clone.route_local(envelope_clone, Some(reply_tx)).await });

        // Give the route a moment to enter the wait state.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!route_handle.is_finished(), "route_local should be waiting");

        // Register the entity — should unblock route_local.
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        // route_local should succeed.
        let result = tokio::time::timeout(Duration::from_secs(1), route_handle)
            .await
            .expect("route_local should complete")
            .expect("task should not panic");
        assert!(result.is_ok(), "route_local should succeed: {result:?}");

        // Should receive a reply from the echo handler.
        let reply = tokio::time::timeout(Duration::from_secs(1), reply_rx.recv())
            .await
            .expect("should receive reply")
            .expect("reply channel should not be closed");
        match reply {
            Reply::WithExit(r) => match r.exit {
                ExitResult::Success(data) => assert_eq!(data, vec![1, 2, 3]),
                ExitResult::Failure(e) => panic!("unexpected failure: {e}"),
            },
            _ => panic!("expected WithExit reply"),
        }
    }

    #[tokio::test]
    async fn route_local_times_out_for_unregistered_entity() {
        use std::time::Duration;
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_registration_timeout: Duration::from_millis(100),
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();
        s.acquire_all_shards().await;

        let entity_id = EntityId::new("test-1");
        let shard_id = s.get_shard_id(&EntityType::new("Unknown"), &entity_id);
        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next_async().await.unwrap(),
            address: crate::types::EntityAddress {
                shard_id,
                entity_type: EntityType::new("Unknown"),
                entity_id,
            },
            tag: "ping".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let result = s.route_local(envelope, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match &err {
            ClusterError::MalformedMessage { reason, .. } => {
                assert!(
                    reason.contains("waited"),
                    "error should mention wait: {reason}"
                );
            }
            _ => panic!("expected MalformedMessage, got: {err}"),
        }
    }

    #[tokio::test]
    async fn mailbox_full_resumption_retries_delivery() {
        use crate::storage::memory_message::MemoryMessageStorage;
        use std::sync::atomic::{AtomicU32, Ordering};

        // Create a tiny-mailbox entity that will fill up quickly.
        let handled = Arc::new(AtomicU32::new(0));
        let handled_clone = Arc::clone(&handled);

        struct SlowEntity {
            handled: Arc<AtomicU32>,
        }

        struct SlowHandler {
            handled: Arc<AtomicU32>,
        }

        #[async_trait]
        impl Entity for SlowEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Slow")
            }
            fn mailbox_capacity(&self) -> Option<usize> {
                Some(1) // Tiny mailbox — will fill up fast
            }
            async fn spawn(
                &self,
                _ctx: crate::entity::EntityContext,
            ) -> Result<Box<dyn crate::entity::EntityHandler>, ClusterError> {
                Ok(Box::new(SlowHandler {
                    handled: Arc::clone(&self.handled),
                }))
            }
        }

        #[async_trait]
        impl crate::entity::EntityHandler for SlowHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                // Simulate slow processing so the mailbox fills up
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                self.handled.fetch_add(1, Ordering::SeqCst);
                Ok(vec![])
            }
        }

        let storage = Arc::new(MemoryMessageStorage::new());
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_mailbox_capacity: 1,
            send_retry_interval: std::time::Duration::from_millis(20),
            storage_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        });

        let s = ShardingImpl::new(
            config,
            Arc::new(crate::storage::noop_runners::NoopRunners),
            None,
            None,
            Some(storage.clone() as Arc<dyn MessageStorage>),
            Arc::new(ClusterMetrics::new(&prometheus::Registry::new()).expect("valid metrics")),
        )
        .unwrap();

        s.acquire_all_shards().await;

        let entity = SlowEntity {
            handled: handled_clone,
        };
        s.register_entity(Arc::new(entity)).await.unwrap();

        // Save multiple messages to storage for the same entity.
        let shard = ShardId::new("default", 0);
        let eid = EntityId::new("e1");
        for i in 0..5 {
            let envelope = EnvelopeRequest {
                request_id: crate::snowflake::Snowflake(1000 + i),
                address: crate::types::EntityAddress {
                    shard_id: shard.clone(),
                    entity_type: EntityType::new("Slow"),
                    entity_id: eid.clone(),
                },
                tag: "work".into(),
                payload: vec![],
                headers: HashMap::new(),
                span_id: None,
                trace_id: None,
                sampled: None,
                persisted: false,
                uninterruptible: Default::default(),
                deliver_at: None,
            };
            storage.save_request(&envelope).await.unwrap();
        }

        // Poll storage — some will get MailboxFull and trigger resumption
        s.poll_storage_inner().await.unwrap();

        // Wait for resumption tasks to deliver all messages
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // All 5 messages should eventually be handled
        let count = handled.load(Ordering::SeqCst);
        assert!(
            count >= 3,
            "expected at least 3 messages handled (got {count}), \
             resumption should retry MailboxFull messages"
        );
    }

    #[tokio::test]
    async fn resumption_max_retries_exhaustion_dead_letters_message() {
        use crate::storage::memory_message::MemoryMessageStorage;
        // Create a permanently-full-mailbox entity: spawn blocks forever so the
        // entity's process_mailbox task never runs and the channel stays full.
        struct BlockingSpawnEntity;
        struct NeverHandler;

        #[async_trait]
        impl Entity for BlockingSpawnEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Blocking")
            }
            fn mailbox_capacity(&self) -> Option<usize> {
                Some(1)
            }
            async fn spawn(
                &self,
                _ctx: crate::entity::EntityContext,
            ) -> Result<Box<dyn crate::entity::EntityHandler>, ClusterError> {
                Ok(Box::new(NeverHandler))
            }
        }

        #[async_trait]
        impl crate::entity::EntityHandler for NeverHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                // Block forever so the mailbox never drains
                futures::future::pending::<()>().await;
                Ok(vec![])
            }
        }

        let storage = Arc::new(MemoryMessageStorage::new());
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_mailbox_capacity: 1,
            send_retry_interval: std::time::Duration::from_millis(5),
            storage_poll_interval: std::time::Duration::from_millis(50),
            storage_resumption_max_retries: 3,
            ..Default::default()
        });

        let s = ShardingImpl::new(
            config,
            Arc::new(crate::storage::noop_runners::NoopRunners),
            None,
            None,
            Some(storage.clone() as Arc<dyn MessageStorage>),
            Arc::new(ClusterMetrics::new(&prometheus::Registry::new()).expect("valid metrics")),
        )
        .unwrap();

        s.acquire_all_shards().await;
        s.register_entity(Arc::new(BlockingSpawnEntity))
            .await
            .unwrap();

        let shard = ShardId::new("default", 0);
        let eid = EntityId::new("e1");

        // Save many messages to guarantee at least one MailboxFull.
        // With capacity=1, even if the runtime schedules process_mailbox between
        // send_local calls (consuming one message), enough messages ensure overflow.
        for i in 0..10 {
            let envelope = EnvelopeRequest {
                request_id: crate::snowflake::Snowflake(9000 + i),
                address: crate::types::EntityAddress {
                    shard_id: shard.clone(),
                    entity_type: EntityType::new("Blocking"),
                    entity_id: eid.clone(),
                },
                tag: "work".into(),
                payload: vec![],
                headers: HashMap::new(),
                span_id: None,
                trace_id: None,
                sampled: None,
                persisted: false,
                uninterruptible: Default::default(),
                deliver_at: None,
            };
            storage.save_request(&envelope).await.unwrap();
        }

        // Poll storage — first two messages accepted, third hits MailboxFull → resumption
        s.poll_storage_inner().await.unwrap();

        // Find which message(s) got MailboxFull — check the resumption_unprocessed map
        // Wait a bit for resumption to exhaust retries
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // At least one message should have been dead-lettered. Check all 10.
        let mut found_dead_letter = false;
        for i in 0..10 {
            let target_id = crate::snowflake::Snowflake(9000 + i);
            let replies = storage.replies_for(target_id).await.unwrap();
            if replies.iter().any(|r| {
                matches!(
                    r,
                    crate::reply::Reply::WithExit(crate::reply::ReplyWithExit {
                        exit: crate::reply::ExitResult::Failure(reason),
                        ..
                    }) if reason.contains("retry limit exhausted")
                )
            }) {
                found_dead_letter = true;
                break;
            }
        }

        assert!(
            found_dead_letter,
            "expected at least one dead-lettered message after resumption retry exhaustion"
        );
    }

    #[tokio::test]
    async fn singleton_crash_backoff_delays_respawn() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            // Use a short backoff base for testing
            singleton_crash_backoff_base: std::time::Duration::from_millis(500),
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();
        s.acquire_all_shards().await;

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        // Register a singleton that always fails
        s.register_singleton(
            "failing-singleton",
            None,
            Arc::new(move |_ctx| {
                let cc = call_count_clone.clone();
                Box::pin(async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Err(ClusterError::PersistenceError {
                        reason: "test failure".into(),
                        source: None,
                    })
                })
            }),
        )
        .await
        .unwrap();

        // Wait for the first run to complete (it will fail)
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(call_count.load(Ordering::SeqCst), 1, "should have run once");

        // The singleton should not be running now (it failed)
        let entry = s.singletons.get("failing-singleton").unwrap();
        assert!(!entry.running.load(Ordering::SeqCst));
        assert_eq!(
            entry.consecutive_failures.load(Ordering::Acquire),
            1,
            "should have 1 consecutive failure"
        );
        assert!(
            entry.last_failure_ms.load(Ordering::Acquire) > 0,
            "should have recorded failure time"
        );
        drop(entry);

        // Call sync_singletons immediately — should NOT respawn due to backoff (500ms base)
        s.sync_singletons().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "should not have respawned yet due to backoff"
        );

        // Wait for backoff to expire (500ms base * 2^0 = 500ms)
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Now sync_singletons should respawn
        s.sync_singletons().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "should have respawned after backoff expired"
        );

        // After 2nd failure, backoff is 500ms * 2^1 = 1000ms — sync should NOT respawn
        s.sync_singletons().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "should not have respawned — 2nd backoff not expired"
        );
    }

    #[tokio::test]
    async fn send_retries_on_entity_not_assigned() {
        // Create a sharding instance with no shards owned and send_retry_count = 2.
        // send() should retry and eventually return EntityNotAssignedToRunner.
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            send_retry_count: 2,
            send_retry_interval: std::time::Duration::from_millis(10),
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();
        // Don't acquire any shards — all sends should fail

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next().unwrap(),
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new("1"),
            },
            tag: "test".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: crate::schema::Uninterruptible::No,
            deliver_at: None,
        };

        let start = std::time::Instant::now();
        let result = s.send(envelope).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(
            matches!(
                result.unwrap_err(),
                ClusterError::EntityNotAssignedToRunner { .. }
            ),
            "expected EntityNotAssignedToRunner"
        );
        // Should have taken at least 2 retry intervals (2 * 10ms = 20ms)
        assert!(
            elapsed >= std::time::Duration::from_millis(15),
            "expected retries to take at least 15ms, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn send_retry_count_zero_no_retries() {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            send_retry_count: 0,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(config, runners, None, None, None, metrics).unwrap();

        let envelope = EnvelopeRequest {
            request_id: s.snowflake().next().unwrap(),
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new("1"),
            },
            tag: "test".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: crate::schema::Uninterruptible::No,
            deliver_at: None,
        };

        let start = std::time::Instant::now();
        let result = s.send(envelope).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        // With 0 retries, should fail almost immediately
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "expected no retries, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn poll_storage_saves_failure_reply_for_unregistered_entity_after_timeout() {
        use crate::storage::memory_message::MemoryMessageStorage;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_registration_timeout: std::time::Duration::from_millis(50),
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let s = ShardingImpl::new(
            config,
            runners,
            None,
            None,
            Some(storage.clone() as Arc<dyn crate::message_storage::MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;

        let request_id = s.snowflake().next_async().await.unwrap();
        let envelope = EnvelopeRequest {
            request_id,
            address: crate::types::EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("NeverRegistered"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "x".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        storage.save_envelope(&envelope).await.unwrap();

        // First poll — within timeout, message is skipped but tracked
        s.poll_storage().await.unwrap();
        let replies = storage.replies_for(request_id).await.unwrap();
        assert!(replies.is_empty(), "no failure reply yet before timeout");

        // Wait for the timeout to elapse
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        // Second poll — timeout exceeded, failure reply should be saved
        s.poll_storage().await.unwrap();
        let replies = storage.replies_for(request_id).await.unwrap();
        assert_eq!(
            replies.len(),
            1,
            "failure reply should be saved after timeout"
        );
        match &replies[0] {
            crate::reply::Reply::WithExit(r) => {
                assert_eq!(r.request_id, request_id);
                match &r.exit {
                    crate::reply::ExitResult::Failure(msg) => {
                        assert!(
                            msg.contains("NeverRegistered"),
                            "failure message should mention entity type: {msg}"
                        );
                    }
                    _ => panic!("expected Failure exit"),
                }
            }
            _ => panic!("expected WithExit reply"),
        }

        // Verify tracking is cleaned up
        assert!(
            s.unregistered_first_seen.is_empty(),
            "first-seen tracking should be cleaned up after failure reply"
        );
    }

    // -------------------------------------------------------------------------
    // Detachment Tests
    // -------------------------------------------------------------------------

    /// Mock runner storage that can be configured to fail on specific operations.
    mod failing_storage {
        use super::*;
        use crate::runner::Runner;
        use crate::types::MachineId;
        use futures::Stream;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicBool, Ordering};

        pub struct FailingRunnerStorage {
            inner: crate::storage::memory_runner::MemoryRunnerStorage,
            pub fail_get_runners: AtomicBool,
            pub fail_refresh_batch: AtomicBool,
            pub fail_acquire_batch: AtomicBool,
        }

        impl FailingRunnerStorage {
            pub fn new() -> Self {
                Self {
                    inner: crate::storage::memory_runner::MemoryRunnerStorage::new(),
                    fail_get_runners: AtomicBool::new(false),
                    fail_refresh_batch: AtomicBool::new(false),
                    fail_acquire_batch: AtomicBool::new(false),
                }
            }
        }

        #[async_trait]
        impl RunnerStorage for FailingRunnerStorage {
            async fn register(&self, runner: &Runner) -> Result<MachineId, ClusterError> {
                self.inner.register(runner).await
            }

            async fn unregister(&self, address: &RunnerAddress) -> Result<(), ClusterError> {
                self.inner.unregister(address).await
            }

            async fn get_runners(&self) -> Result<Vec<Runner>, ClusterError> {
                if self.fail_get_runners.load(Ordering::SeqCst) {
                    return Err(ClusterError::PersistenceError {
                        reason: "simulated get_runners failure".into(),
                        source: None,
                    });
                }
                self.inner.get_runners().await
            }

            async fn set_runner_health(
                &self,
                address: &RunnerAddress,
                healthy: bool,
            ) -> Result<(), ClusterError> {
                self.inner.set_runner_health(address, healthy).await
            }

            async fn acquire(
                &self,
                shard_id: &ShardId,
                runner: &RunnerAddress,
            ) -> Result<bool, ClusterError> {
                self.inner.acquire(shard_id, runner).await
            }

            async fn acquire_batch(
                &self,
                shard_ids: &[ShardId],
                runner: &RunnerAddress,
            ) -> Result<crate::runner_storage::BatchAcquireResult, ClusterError> {
                if self.fail_acquire_batch.load(Ordering::SeqCst) {
                    return Err(ClusterError::PersistenceError {
                        reason: "simulated acquire_batch failure".into(),
                        source: None,
                    });
                }
                self.inner.acquire_batch(shard_ids, runner).await
            }

            async fn refresh(
                &self,
                shard_id: &ShardId,
                runner: &RunnerAddress,
            ) -> Result<bool, ClusterError> {
                self.inner.refresh(shard_id, runner).await
            }

            async fn refresh_batch(
                &self,
                shard_ids: &[ShardId],
                runner: &RunnerAddress,
            ) -> Result<crate::runner_storage::BatchRefreshResult, ClusterError> {
                if self.fail_refresh_batch.load(Ordering::SeqCst) {
                    return Err(ClusterError::PersistenceError {
                        reason: "simulated refresh_batch failure".into(),
                        source: None,
                    });
                }
                self.inner.refresh_batch(shard_ids, runner).await
            }

            async fn release(
                &self,
                shard_id: &ShardId,
                runner: &RunnerAddress,
            ) -> Result<(), ClusterError> {
                self.inner.release(shard_id, runner).await
            }

            async fn release_all(&self, runner: &RunnerAddress) -> Result<(), ClusterError> {
                self.inner.release_all(runner).await
            }

            async fn watch_runners(
                &self,
            ) -> Result<Pin<Box<dyn Stream<Item = Vec<Runner>> + Send>>, ClusterError> {
                self.inner.watch_runners().await
            }
        }
    }

    #[tokio::test]
    async fn detach_on_get_runners_error() {
        use failing_storage::FailingRunnerStorage;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        let runner_storage = Arc::new(FailingRunnerStorage::new());
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            detachment_enabled: true,
            detachment_recover_window: Duration::from_millis(100),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();

        // Acquire some shards first
        s.acquire_all_shards().await;
        assert!(!s.owned_shards.read().await.is_empty());
        assert!(!s.is_detached());

        // Configure storage to fail on get_runners
        runner_storage
            .fail_get_runners
            .store(true, Ordering::SeqCst);

        // Trigger rebalance_shards which calls get_runners
        let result = s
            .rebalance_shards(&(runner_storage.clone() as Arc<dyn RunnerStorage>))
            .await;

        // Should return Ok(false) but trigger detachment
        assert!(result.is_ok());
        assert!(
            s.is_detached(),
            "should be detached after get_runners error"
        );

        // Owned shards should be cleared
        assert!(
            s.owned_shards.read().await.is_empty(),
            "owned_shards should be cleared on detachment"
        );

        // Metrics should reflect detachment
        assert_eq!(s.metrics.sharding_detached.get(), 1);
    }

    #[tokio::test]
    async fn detach_on_acquire_batch_error() {
        use failing_storage::FailingRunnerStorage;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        let runner_storage = Arc::new(FailingRunnerStorage::new());
        let address = RunnerAddress::new("10.0.0.1", 9000);
        let config = Arc::new(ShardingConfig {
            runner_address: address.clone(),
            shard_groups: vec!["default".to_string()],
            shards_per_group: 3,
            detachment_enabled: true,
            detachment_recover_window: Duration::from_millis(100),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();

        // Register this runner so it gets assigned shards
        runner_storage
            .register(&Runner::new(address.clone(), 1))
            .await
            .unwrap();

        assert!(!s.is_detached());

        // Configure storage to fail on acquire_batch
        runner_storage
            .fail_acquire_batch
            .store(true, Ordering::SeqCst);

        // Trigger rebalance_shards which will try to acquire shards
        let result = s
            .rebalance_shards(&(runner_storage.clone() as Arc<dyn RunnerStorage>))
            .await;

        // Should return Ok but trigger detachment
        assert!(result.is_ok());
        assert!(
            s.is_detached(),
            "should be detached after acquire_batch error"
        );
    }

    #[tokio::test]
    async fn detach_on_refresh_batch_error() {
        use failing_storage::FailingRunnerStorage;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        let runner_storage = Arc::new(FailingRunnerStorage::new());
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            runner_lock_refresh_interval: Duration::from_millis(10),
            detachment_enabled: true,
            detachment_recover_window: Duration::from_millis(100),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();

        // Acquire some shards
        s.acquire_all_shards().await;
        assert!(!s.owned_shards.read().await.is_empty());
        assert!(!s.is_detached());

        // Configure storage to fail on refresh_batch
        runner_storage
            .fail_refresh_batch
            .store(true, Ordering::SeqCst);

        // Start the lock refresh loop
        let s_clone = Arc::clone(&s);
        let handle = tokio::spawn(async move {
            s_clone.lock_refresh_loop().await;
        });

        // Wait for the refresh loop to trigger detachment
        tokio::time::sleep(Duration::from_millis(50)).await;
        s.cancel.cancel();
        let _ = handle.await;

        assert!(
            s.is_detached(),
            "should be detached after refresh_batch error"
        );
        assert!(
            s.owned_shards.read().await.is_empty(),
            "owned_shards should be cleared on detachment"
        );
    }

    #[tokio::test]
    async fn detach_on_keepalive_failure_streak() {
        use std::time::Duration;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            detachment_enabled: true,
            detachment_recover_window: Duration::from_millis(100),
            keepalive_failure_threshold: 3,
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            None,
            metrics.clone(),
        )
        .unwrap();

        // Acquire some shards
        s.acquire_all_shards().await;
        assert!(!s.owned_shards.read().await.is_empty());
        assert!(!s.is_detached());

        // Create a lease health channel to simulate keep-alive updates
        let (health_tx, health_rx) = tokio::sync::broadcast::channel::<LeaseHealth>(16);

        // Start the lease health loop
        let s_clone = Arc::clone(&s);
        let handle = tokio::spawn(async move {
            s_clone.lease_health_loop(health_rx).await;
        });

        // Send healthy signals first
        health_tx
            .send(LeaseHealth {
                healthy: true,
                failure_streak: 0,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!s.is_detached());

        // Send failures below threshold
        health_tx
            .send(LeaseHealth {
                healthy: false,
                failure_streak: 2,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!s.is_detached(), "should not detach below threshold");

        // Send failure at threshold
        health_tx
            .send(LeaseHealth {
                healthy: false,
                failure_streak: 3,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(
            s.is_detached(),
            "should be detached after failure streak exceeds threshold"
        );
        assert!(
            s.owned_shards.read().await.is_empty(),
            "owned_shards should be cleared on detachment"
        );

        // Verify metrics
        assert_eq!(metrics.lease_keepalive_failure_streak.get(), 3);

        s.cancel.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn detached_loops_skip_work() {
        use failing_storage::FailingRunnerStorage;
        use std::time::Duration;

        let runner_storage = Arc::new(FailingRunnerStorage::new());
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            shard_rebalance_retry_interval: Duration::from_millis(10),
            runner_lock_refresh_interval: Duration::from_millis(10),
            detachment_enabled: true,
            detachment_recover_window: Duration::from_millis(500),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            Some(runner_storage.clone() as Arc<dyn RunnerStorage>),
            None,
            None,
            metrics,
        )
        .unwrap();

        // Manually detach
        s.detach(DetachmentReason::Manual).await;
        assert!(s.is_detached());
        assert!(s.owned_shards.read().await.is_empty());

        // Start background loops
        let s_clone = Arc::clone(&s);
        let acq_handle = tokio::spawn(async move {
            s_clone.shard_acquisition_loop().await;
        });

        let s_clone = Arc::clone(&s);
        let refresh_handle = tokio::spawn(async move {
            s_clone.lock_refresh_loop().await;
        });

        // Let loops run a bit
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should still be detached (loops should skip work)
        assert!(
            s.is_detached(),
            "should remain detached while loops are skipping"
        );
        assert!(
            s.owned_shards.read().await.is_empty(),
            "owned_shards should remain empty while detached"
        );

        s.cancel.cancel();
        let _ = acq_handle.await;
        let _ = refresh_handle.await;
    }

    #[tokio::test]
    async fn reattach_after_recovery_window() {
        use std::time::Duration;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            detachment_enabled: true,
            detachment_recover_window: Duration::from_millis(50),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            None,
            metrics.clone(),
        )
        .unwrap();

        // Detach
        s.detach(DetachmentReason::Manual).await;
        assert!(s.is_detached());
        assert_eq!(metrics.sharding_detached.get(), 1);

        // First healthy signal starts recovery window
        s.signal_healthy();
        assert!(
            s.is_detached(),
            "should still be detached immediately after first healthy signal"
        );

        // Wait for recovery window to elapse
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Another healthy signal should trigger re-attachment
        s.signal_healthy();
        assert!(
            !s.is_detached(),
            "should be re-attached after recovery window"
        );
        assert_eq!(metrics.sharding_detached.get(), 0);
    }

    #[tokio::test]
    async fn unhealthy_signal_resets_recovery_window() {
        use std::time::Duration;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            detachment_enabled: true,
            detachment_recover_window: Duration::from_millis(100),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s =
            ShardingImpl::new(config, Arc::new(NoopRunners), None, None, None, metrics).unwrap();

        // Detach
        s.detach(DetachmentReason::Manual).await;
        assert!(s.is_detached());

        // Start recovery
        s.signal_healthy();

        // Wait partway through recovery window
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Signal unhealthy - resets recovery
        s.signal_unhealthy();

        // Wait a bit more (would have exceeded window if not reset)
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Should still be detached because recovery was reset
        s.signal_healthy();
        assert!(
            s.is_detached(),
            "should still be detached after recovery reset"
        );

        // Now wait for full recovery window
        tokio::time::sleep(Duration::from_millis(110)).await;
        s.signal_healthy();
        assert!(
            !s.is_detached(),
            "should be re-attached after full recovery window"
        );
    }

    #[tokio::test]
    async fn detachment_disabled_ignores_detach_calls() {
        use std::time::Duration;

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            detachment_enabled: false, // Disabled!
            detachment_recover_window: Duration::from_millis(50),
            ..Default::default()
        });
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            None,
            metrics.clone(),
        )
        .unwrap();

        // Acquire shards
        s.acquire_all_shards().await;
        let shard_count = s.owned_shards.read().await.len();
        assert!(shard_count > 0);

        // Try to detach
        s.detach(DetachmentReason::Manual).await;

        // Should NOT be detached (feature disabled)
        assert!(
            !s.is_detached(),
            "should not detach when detachment_enabled=false"
        );
        assert_eq!(
            s.owned_shards.read().await.len(),
            shard_count,
            "owned_shards should not be cleared when detachment is disabled"
        );
        assert_eq!(
            metrics.sharding_detached.get(),
            0,
            "detachment metric should not change when disabled"
        );
    }
}
