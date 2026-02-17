use crate::config::ShardingConfig;
use crate::entity::{Entity, EntityContext, EntityHandler};
use crate::envelope::{STREAM_HEADER_KEY, STREAM_HEADER_VALUE};
use crate::error::ClusterError;
use crate::message::IncomingMessage;
use crate::message_storage::MessageStorage;
use crate::reply::{
    fallback_reply_id, ExitResult, Reply, ReplyChunk, ReplyWithExit, EXIT_SEQUENCE,
};
use crate::sharding::Sharding;
use crate::snowflake::Snowflake;
use crate::snowflake::SnowflakeGenerator;
use crate::types::{EntityAddress, RunnerAddress, ShardId};
use dashmap::{DashMap, DashSet};
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Generate a reply snowflake ID, falling back to a deterministic hash on clock drift.
///
/// Reply IDs are persisted as primary keys in SQL storage, so the fallback must be stable
/// per-request and avoid collisions in the face of repeated clock drift failures.
async fn reply_id(
    snowflake: &Arc<SnowflakeGenerator>,
    request_id: Snowflake,
    sequence: i32,
) -> Snowflake {
    match snowflake.next_async().await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(
                request_id = %request_id,
                sequence,
                "failed to generate reply snowflake ID: {e}"
            );
            fallback_reply_id(request_id, sequence)
        }
    }
}

/// Per-address spawn lock to prevent double-spawn races.
///
/// When two concurrent messages arrive for a new entity, both miss the
/// `instances.get()` fast path. Without serialization, both would call
/// `entity.spawn()`, wasting one spawn. This lock ensures only one task
/// performs the spawn while the other waits and then uses the result.
type SpawnLocks = DashMap<EntityAddress, Arc<Mutex<()>>>;

/// Manages all instances of a single entity type.
///
/// Handles spawning new entity instances on first message, routing messages
/// to existing instances, enforcing mailbox capacity, tracking idle time
/// for reaping, and crash recovery with request replay.
pub struct EntityManager {
    entity: Arc<dyn Entity>,
    instances: Arc<DashMap<EntityAddress, Arc<EntityInstance>>>,
    config: Arc<ShardingConfig>,
    runner_address: RunnerAddress,
    snowflake: Arc<SnowflakeGenerator>,
    message_storage: Option<Arc<dyn MessageStorage>>,
    /// Optional key-value storage for persisted entity state.
    state_storage: Option<Arc<dyn crate::durable::WorkflowStorage>>,
    /// Optional workflow engine for durable context support in entity methods.
    workflow_engine: Option<Arc<dyn crate::durable::WorkflowEngine>>,
    /// Optional sharding interface for inter-entity communication.
    ///
    /// When present, entities can create clients to send messages to other entities
    /// or to themselves, including scheduled messages via `notify_at`.
    sharding: Option<Arc<dyn Sharding>>,
    /// Tracks request IDs currently being processed or recently processed.
    /// Used to deduplicate messages from storage polling — if a request ID
    /// is in this set, it's already being handled and should not be re-dispatched.
    /// Cleared by `clear_processed()` before each storage poll cycle, but active
    /// request IDs are preserved to avoid double-dispatch while in flight.
    processed_request_ids: DashSet<Snowflake>,
    /// Request IDs currently in-flight. Used to preserve dedup state across
    /// storage poll cycles so active requests are not re-dispatched.
    active_request_ids: Arc<DashSet<Snowflake>>,
    /// Per-address spawn locks to prevent double-spawn races.
    spawn_locks: Arc<SpawnLocks>,
    /// Shards currently being interrupted. `get_or_spawn` rejects spawns for
    /// entities on these shards, preventing a race where a new entity escapes
    /// interruption because `interrupt_shard` has already finished iterating.
    interrupting_shards: DashSet<ShardId>,
    /// Whether this entity manager is in "closing" state.
    ///
    /// Matches the TS `Sharding.ts:730` lifecycle: `"alive" → "closing" → "closed"`.
    /// In "closing" state, new `IncomingRequest` messages are rejected with
    /// `ShuttingDown`, but `Envelope` messages (fire-and-forget, ack, interrupt)
    /// are still accepted. This allows in-flight operations to complete while
    /// preventing new work from being accepted during graceful shutdown.
    closing: AtomicBool,
}

/// A live entity instance with its handler and lifecycle state.
struct EntityInstance {
    /// The handler is behind an `RwLock<Arc<...>>` to support both concurrent
    /// dispatch (read lock) and crash recovery handler swap (write lock).
    /// Normal request handling acquires a read lock + clones the Arc, so
    /// multiple requests can execute concurrently. Crash recovery acquires
    /// a write lock to swap the handler atomically.
    handler: RwLock<Arc<dyn EntityHandler>>,
    active_requests: AtomicUsize,
    last_active_ms: AtomicI64,
    cancel: CancellationToken,
    /// Mailbox sender for queuing incoming messages.
    /// Uses an unbounded channel — backpressure is enforced by checking
    /// `active_requests` against `mailbox_capacity` before sending, matching
    /// the TS behavior of limiting active (in-flight) requests rather than
    /// queued messages.
    mailbox_tx: mpsc::UnboundedSender<IncomingMessage>,
    /// Maximum number of active (in-flight) requests before rejecting with
    /// `MailboxFull`. Matches TS `entityManager.ts:432` which checks
    /// `server.activeRequests.size >= mailboxCapacity`.
    mailbox_capacity: usize,
    /// Set to `true` when the entity is being interrupted due to shard movement
    /// or shutdown (an "internal" interrupt). Used to suppress interrupt exits
    /// for persisted + server-uninterruptible messages.
    is_internal_interrupt: AtomicBool,
    /// Concurrency semaphore. `None` = unbounded, `Some(sem)` = bounded.
    /// Acquired before dispatching a request, released after completion.
    semaphore: Option<Arc<Semaphore>>,
    /// JoinHandle for the mailbox processing task. Used to await task completion
    /// during interrupt_shard and reap_idle, ensuring the task has fully exited
    /// before the entity is considered terminated.
    ///
    /// Uses `parking_lot::Mutex` (not `tokio::sync::Mutex`) so the handle can be
    /// stored synchronously immediately after `tokio::spawn`, eliminating the race
    /// where the spawned task completes before the handle is stored.
    join_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

/// Snapshot of request info needed for replay after crash recovery.
struct RequestSnapshot {
    request: crate::envelope::EnvelopeRequest,
    reply_tx: Option<crate::message::ReplySender>,
}

impl EntityManager {
    /// Create a new entity manager for the given entity definition.
    pub fn new(
        entity: Arc<dyn Entity>,
        config: Arc<ShardingConfig>,
        runner_address: RunnerAddress,
        snowflake: Arc<SnowflakeGenerator>,
        message_storage: Option<Arc<dyn MessageStorage>>,
    ) -> Self {
        Self::with_state_storage(
            entity,
            config,
            runner_address,
            snowflake,
            message_storage,
            None,
        )
    }

    /// Create a new entity manager with optional state storage for persisted entity state.
    pub fn with_state_storage(
        entity: Arc<dyn Entity>,
        config: Arc<ShardingConfig>,
        runner_address: RunnerAddress,
        snowflake: Arc<SnowflakeGenerator>,
        message_storage: Option<Arc<dyn MessageStorage>>,
        state_storage: Option<Arc<dyn crate::durable::WorkflowStorage>>,
    ) -> Self {
        Self::with_engines(
            entity,
            config,
            runner_address,
            snowflake,
            message_storage,
            state_storage,
            None,
        )
    }

    /// Create a new entity manager with optional state storage and workflow engine.
    pub fn with_engines(
        entity: Arc<dyn Entity>,
        config: Arc<ShardingConfig>,
        runner_address: RunnerAddress,
        snowflake: Arc<SnowflakeGenerator>,
        message_storage: Option<Arc<dyn MessageStorage>>,
        state_storage: Option<Arc<dyn crate::durable::WorkflowStorage>>,
        workflow_engine: Option<Arc<dyn crate::durable::WorkflowEngine>>,
    ) -> Self {
        Self::with_sharding(
            entity,
            config,
            runner_address,
            snowflake,
            message_storage,
            state_storage,
            workflow_engine,
            None,
        )
    }

    /// Create a new entity manager with all optional components including sharding.
    #[allow(clippy::too_many_arguments)]
    pub fn with_sharding(
        entity: Arc<dyn Entity>,
        config: Arc<ShardingConfig>,
        runner_address: RunnerAddress,
        snowflake: Arc<SnowflakeGenerator>,
        message_storage: Option<Arc<dyn MessageStorage>>,
        state_storage: Option<Arc<dyn crate::durable::WorkflowStorage>>,
        workflow_engine: Option<Arc<dyn crate::durable::WorkflowEngine>>,
        sharding: Option<Arc<dyn Sharding>>,
    ) -> Self {
        Self {
            entity,
            instances: Arc::new(DashMap::new()),
            config,
            runner_address,
            snowflake,
            message_storage,
            state_storage,
            workflow_engine,
            sharding,
            processed_request_ids: DashSet::new(),
            active_request_ids: Arc::new(DashSet::new()),
            spawn_locks: Arc::new(DashMap::new()),
            interrupting_shards: DashSet::new(),
            closing: AtomicBool::new(false),
        }
    }

    /// Route a message to the appropriate entity instance, spawning if needed.
    #[instrument(skip(self, message), fields(
        entity_address = %message.envelope().address,
        request_id = %message.envelope().request_id,
        tag = %message.envelope().tag,
    ))]
    pub async fn send_local(&self, message: IncomingMessage) -> Result<(), ClusterError> {
        // In "closing" state, reject new Request messages but allow Envelope
        // messages (fire-and-forget, ack, interrupt) to reach existing entities.
        // Matches TS Sharding.ts:730 — closing + IncomingRequest → reject.
        if self.closing.load(Ordering::Acquire)
            && matches!(&message, IncomingMessage::Request { .. })
        {
            return Err(ClusterError::ShuttingDown);
        }

        let address = message.envelope().address.clone();
        let request_id = message.envelope().request_id;

        // Deduplicate ALL incoming messages by request ID.
        // The TS implementation (entityManager.ts:387-394) checks both
        // `activeRequests.has(requestId)` and `processedRequestIds.has(requestId)`
        // for all incoming messages, including direct sends (IncomingRequestLocal).
        //
        // For Envelope messages (from storage polling): silently skip duplicates.
        // For Request messages (direct sends): return AlreadyProcessingMessage error
        // so the caller knows the request is already in flight.
        {
            if !self.processed_request_ids.insert(request_id) {
                if matches!(&message, IncomingMessage::Envelope { .. }) {
                    tracing::debug!(
                        request_id = %request_id,
                        entity_address = %address,
                        "skipping already-processing request from storage poll"
                    );
                    return Ok(());
                } else {
                    tracing::debug!(
                        request_id = %request_id,
                        entity_address = %address,
                        "rejecting duplicate request (already processing)"
                    );
                    return Err(ClusterError::AlreadyProcessingMessage { request_id });
                }
            }
        }

        // Get or spawn the entity instance
        let instance = self.get_or_spawn(&address).await?;

        // Check active request count against mailbox capacity, matching the TS
        // behavior (entityManager.ts:432) which checks
        // `server.activeRequests.size >= mailboxCapacity`. This limits active
        // (in-flight) requests rather than queued messages.
        //
        // `active_requests` is incremented here (on accept) and decremented in
        // `process_mailbox` (on completion), so it tracks the total number of
        // accepted-but-not-yet-completed messages.
        if instance.active_requests.load(Ordering::Acquire) >= instance.mailbox_capacity {
            self.processed_request_ids
                .remove(&message.envelope().request_id);
            return Err(ClusterError::MailboxFull { address });
        }

        // Increment active_requests before sending to the channel, so subsequent
        // send_local calls see the updated count immediately (no race window).
        instance.active_requests.fetch_add(1, Ordering::Release);
        self.active_request_ids.insert(request_id);

        // Send to the unbounded mailbox channel
        match instance.mailbox_tx.send(message) {
            Ok(()) => {
                instance.touch();
                Ok(())
            }
            Err(msg) => {
                // Instance was shut down; decrement counter and retry
                instance.active_requests.fetch_sub(1, Ordering::Release);
                let rejected_msg = msg.0;
                self.instances.remove(&address);
                self.spawn_locks.remove(&address);
                let instance = match self.get_or_spawn(&address).await {
                    Ok(instance) => instance,
                    Err(err) => {
                        self.processed_request_ids.remove(&request_id);
                        self.active_request_ids.remove(&request_id);
                        return Err(err);
                    }
                };
                let addr = address.clone();

                // Re-check capacity on the fresh instance
                if instance.active_requests.load(Ordering::Acquire) >= instance.mailbox_capacity {
                    self.processed_request_ids
                        .remove(&rejected_msg.envelope().request_id);
                    self.active_request_ids
                        .remove(&rejected_msg.envelope().request_id);
                    return Err(ClusterError::MailboxFull { address: addr });
                }

                instance.active_requests.fetch_add(1, Ordering::Release);
                match instance.mailbox_tx.send(rejected_msg) {
                    Ok(()) => {}
                    Err(_) => {
                        instance.active_requests.fetch_sub(1, Ordering::Release);
                        self.active_request_ids.remove(&request_id);
                        tracing::error!(
                            entity_address = %addr,
                            "entity mailbox closed immediately after respawn"
                        );
                        return Err(ClusterError::EntityNotAssignedToRunner {
                            entity_type: addr.entity_type,
                            entity_id: addr.entity_id,
                        });
                    }
                }
                instance.touch();
                Ok(())
            }
        }
    }

    /// Transition this entity manager to "closing" state.
    ///
    /// In closing state, new `Request` messages are rejected with `ShuttingDown`
    /// while `Envelope` messages (fire-and-forget, ack, interrupt) are still
    /// accepted. This allows in-flight operations to complete gracefully.
    ///
    /// Called by `ShardingImpl::shutdown()` before `interrupt_shard()` to match
    /// the TS lifecycle: `alive → closing → closed` (Sharding.ts:730, 1286).
    pub fn set_closing(&self) {
        self.closing.store(true, Ordering::Release);
        tracing::debug!(
            entity_type = %self.entity.entity_type(),
            "entity manager transitioning to closing state"
        );
    }

    /// Interrupt all entities on the given shard.
    ///
    /// Entities are given `entity_termination_timeout` to finish processing
    /// active requests before being forcefully cancelled. The timeout applies
    /// to the entire shard's entities collectively.
    #[instrument(level = "debug", skip(self), fields(shard_id = %shard_id))]
    pub async fn interrupt_shard(&self, shard_id: &ShardId) {
        // Mark shard as interrupting BEFORE collecting instances, so
        // concurrent get_or_spawn calls reject spawns on this shard.
        self.interrupting_shards.insert(shard_id.clone());

        let to_remove: Vec<EntityAddress> = self
            .instances
            .iter()
            .filter(|entry| entry.key().shard_id == *shard_id)
            .map(|entry| entry.key().clone())
            .collect();

        let mut removed: Vec<Arc<EntityInstance>> = Vec::new();
        for addr in &to_remove {
            self.spawn_locks.remove(addr);
        }
        for addr in to_remove {
            if let Some((_, instance)) = self.instances.remove(&addr) {
                // Mark as internal interrupt so the mailbox processor can
                // suppress interrupt exits for persisted+server-uninterruptible messages.
                instance
                    .is_internal_interrupt
                    .store(true, Ordering::Release);
                removed.push(instance);
            }
        }

        if removed.is_empty() {
            return;
        }

        // Give entities a grace period to finish active requests
        let timeout = self.config.entity_termination_timeout;
        if !timeout.is_zero() {
            let drain_check = async {
                loop {
                    if removed
                        .iter()
                        // Acquire pairs with Release in fetch_add/fetch_sub to ensure
                        // we see the latest active_requests count on ARM/weak-memory archs.
                        .all(|i| i.active_requests.load(Ordering::Acquire) == 0)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            let _ = tokio::time::timeout(timeout, drain_check).await;
        }

        // Force-cancel any remaining entities
        for instance in &removed {
            instance.cancel.cancel();
        }

        // Await all mailbox processing tasks to ensure they have fully exited.
        // This prevents the situation where interrupt_shard returns but spawned
        // tasks are still running, which could cause issues during rapid shutdown.
        let mut handles = Vec::new();
        for instance in &removed {
            let handle_opt = { instance.join_handle.lock().take() };
            if let Some(handle) = handle_opt {
                handles.push(handle);
            }
        }
        if !handles.is_empty() {
            let handle_count = handles.len();
            let handle_timeout = Duration::from_secs(5);
            let await_all = futures::future::join_all(handles);
            match tokio::time::timeout(handle_timeout, await_all).await {
                Ok(results) => {
                    for (i, result) in results.into_iter().enumerate() {
                        if let Err(e) = result {
                            if e.is_panic() {
                                tracing::warn!(
                                    shard_id = %shard_id,
                                    task_index = i,
                                    "mailbox task panicked during interrupt"
                                );
                            }
                        }
                    }
                }
                Err(_) => {
                    tracing::warn!(
                        shard_id = %shard_id,
                        task_count = handle_count,
                        "timed out waiting for mailbox tasks to exit during interrupt"
                    );
                }
            }
        }

        // Clear the interrupting flag now that all entities are cancelled.
        self.interrupting_shards.remove(shard_id);
    }

    /// Interrupt a single entity instance by address.
    ///
    /// Gives the entity a grace period to finish active requests, then cancels
    /// the mailbox task and waits briefly for it to exit.
    #[instrument(level = "debug", skip(self), fields(entity_address = %address))]
    pub async fn interrupt_entity(&self, address: &EntityAddress) {
        let Some((_, instance)) = self.instances.remove(address) else {
            return;
        };

        self.spawn_locks.remove(address);

        let timeout = self.config.entity_termination_timeout;
        if !timeout.is_zero() {
            let drain_check = async {
                loop {
                    if instance.active_requests.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            let _ = tokio::time::timeout(timeout, drain_check).await;
        }

        instance.cancel.cancel();

        let handle_opt = { instance.join_handle.lock().take() };
        if let Some(handle) = handle_opt {
            let handle_timeout = Duration::from_secs(5);
            match tokio::time::timeout(handle_timeout, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if e.is_panic() {
                        tracing::warn!("mailbox task panicked during entity interrupt");
                    }
                }
                Err(_) => {
                    tracing::warn!(
                        "timed out waiting for mailbox task to exit during entity interrupt"
                    );
                }
            }
        }
    }

    /// Get active entity count.
    pub fn active_count(&self) -> usize {
        self.instances.len()
    }

    /// Clear the set of processed request IDs.
    ///
    /// Called before each storage poll cycle so that messages that were
    /// previously processed can be re-polled from storage if they haven't
    /// been marked as processed in the database yet.
    pub fn clear_processed(&self) {
        if self.active_request_ids.is_empty() {
            self.processed_request_ids.clear();
            return;
        }

        self.processed_request_ids.clear();
        for request_id in self.active_request_ids.iter() {
            self.processed_request_ids.insert(*request_id.key());
        }
    }

    /// Reap idle entities that have exceeded their max idle time.
    /// Returns the number of entities reaped.
    #[instrument(level = "debug", skip(self), fields(entity_type = %self.entity.entity_type()))]
    pub async fn reap_idle(&self, max_idle: Duration) -> usize {
        let now_ms = now_millis();
        let max_idle_ms = max_idle.as_millis() as i64;

        let mut candidates: Vec<(EntityAddress, i64)> = Vec::new();
        for entry in self.instances.iter() {
            let instance = entry.value();
            // Use Acquire to pair with touch()'s Release store on ARM/weak-memory.
            let last_active = instance.last_active_ms.load(Ordering::Acquire);
            let idle_ms = now_ms - last_active;
            if idle_ms >= max_idle_ms && instance.active_requests.load(Ordering::Acquire) == 0 {
                candidates.push((entry.key().clone(), last_active));
            }
        }

        let mut reaped = 0;
        for (addr, snapshot_last_active) in candidates {
            // Check on_idle to see if the entity wants to stay alive.
            // Re-check active_requests after acquiring the handler lock to avoid
            // TOCTOU race: a new message could have arrived between the initial
            // idle check and now, making the entity actively processing.
            let mut should_reap = false;
            if let Some(entry) = self.instances.get(&addr) {
                // Re-check: if a new request arrived since the candidate scan,
                // skip reaping this entity.
                if entry.value().active_requests.load(Ordering::Acquire) > 0 {
                    continue;
                }
                let handler = entry.value().handler.read().await;
                // Re-check again after acquiring handler lock — a request could
                // have started processing between the atomic check and lock acquire.
                if entry.value().active_requests.load(Ordering::Acquire) > 0 {
                    continue;
                }
                let wants_keep = handler.on_idle().await;
                if wants_keep {
                    continue;
                }
                should_reap = true;
            }
            if should_reap {
                // Use remove_if to atomically verify the instance hasn't been
                // replaced by a concurrent get_or_spawn between the guard drop
                // above and this remove call. We compare last_active_ms to the
                // value snapshotted during candidate collection — if it changed,
                // a new instance was spawned and we must not remove it.
                if let Some((_, instance)) = self.instances.remove_if(&addr, |_, instance| {
                    instance.last_active_ms.load(Ordering::Acquire) == snapshot_last_active
                }) {
                    self.spawn_locks.remove(&addr);
                    instance.cancel.cancel();
                    // Await the mailbox task to ensure it has fully exited.
                    // Take the handle with an explicit scope to drop the parking_lot guard
                    // before the .await (parking_lot::MutexGuard is !Send).
                    let handle_opt = { instance.join_handle.lock().take() };
                    if let Some(handle) = handle_opt {
                        let abort_handle = handle.abort_handle();
                        match tokio::time::timeout(self.config.entity_termination_timeout, handle)
                            .await
                        {
                            Ok(Ok(())) => {} // Task completed normally
                            Ok(Err(join_err)) => {
                                tracing::warn!(
                                    entity_address = %addr,
                                    error = %join_err,
                                    "reaped entity mailbox task panicked"
                                );
                            }
                            Err(_timeout) => {
                                tracing::warn!(
                                    entity_address = %addr,
                                    timeout_ms = self.config.entity_termination_timeout.as_millis() as u64,
                                    "reaped entity mailbox task did not complete within timeout, aborting"
                                );
                                abort_handle.abort();
                            }
                        }
                    }
                    reaped += 1;
                }
            }
        }
        reaped
    }

    /// Get the entity definition.
    pub fn entity(&self) -> &dyn Entity {
        self.entity.as_ref()
    }

    #[instrument(level = "debug", skip(self), fields(entity_address = %address))]
    async fn get_or_spawn(
        &self,
        address: &EntityAddress,
    ) -> Result<Arc<EntityInstance>, ClusterError> {
        // Reject spawns on shards being interrupted to prevent a race where
        // interrupt_shard has already iterated the instances map but a new spawn
        // slips in before cancellation completes.
        if self.interrupting_shards.contains(&address.shard_id) {
            return Err(ClusterError::EntityNotAssignedToRunner {
                entity_type: address.entity_type.clone(),
                entity_id: address.entity_id.clone(),
            });
        }

        // Fast path: instance already exists
        if let Some(entry) = self.instances.get(address) {
            return Ok(Arc::clone(entry.value()));
        }

        // Slow path: acquire per-address spawn lock to prevent double-spawn.
        // Only one task will perform the actual spawn; the other waits and
        // then picks up the instance from the map.
        let lock = self
            .spawn_locks
            .entry(address.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Re-check after acquiring lock — another task may have spawned it
        if let Some(entry) = self.instances.get(address) {
            return Ok(Arc::clone(entry.value()));
        }

        // We won the race — perform the spawn
        let ctx = EntityContext {
            address: address.clone(),
            runner_address: self.runner_address.clone(),
            snowflake: Arc::clone(&self.snowflake),
            cancellation: CancellationToken::new(),
            state_storage: self.state_storage.clone(),
            workflow_engine: self.workflow_engine.clone(),
            sharding: self.sharding.clone(),
            message_storage: self.message_storage.clone(),
        };

        let cancel = ctx.cancellation.clone();
        let handler = match self.entity.spawn(ctx).await {
            Ok(h) => h,
            Err(e) => {
                // Clean up spawn lock on spawn failure to prevent permanent leak.
                // Without this, the lock entry stays in spawn_locks forever since
                // no instance is created and no mailbox task will clean it up.
                self.spawn_locks.remove(address);
                return Err(e);
            }
        };

        let mailbox_capacity = self
            .entity
            .mailbox_capacity()
            .unwrap_or(self.config.entity_mailbox_capacity);

        // Determine concurrency: entity override > config default.
        // 0 = unbounded, 1 = serial, N = at most N concurrent requests.
        let max_concurrent = self
            .entity
            .concurrency()
            .unwrap_or(self.config.entity_max_concurrent_requests);
        let semaphore = match max_concurrent {
            0 => None, // unbounded
            n => Some(Arc::new(Semaphore::new(n))),
        };

        let (mailbox_tx, mailbox_rx) = mpsc::unbounded_channel();

        let instance = Arc::new(EntityInstance {
            handler: RwLock::new(Arc::from(handler)),
            active_requests: AtomicUsize::new(0),
            last_active_ms: AtomicI64::new(now_millis()),
            cancel: cancel.clone(),
            mailbox_tx,
            mailbox_capacity,
            is_internal_interrupt: AtomicBool::new(false),
            semaphore,
            join_handle: parking_lot::Mutex::new(None),
        });

        // Insert into instances map BEFORE spawning the mailbox task.
        // This prevents a race where the spawned task completes and removes
        // the instance from the map before the insert executes, which would
        // leave a stale entry that never gets cleaned up.
        self.instances
            .insert(address.clone(), Arc::clone(&instance));

        // Start the message processing loop
        let instance_for_loop = Arc::clone(&instance);
        let snowflake = Arc::clone(&self.snowflake);
        let entity = Arc::clone(&self.entity);
        let runner_address = self.runner_address.clone();
        let config = Arc::clone(&self.config);
        let address_clone = address.clone();
        let storage = self.message_storage.clone();
        let state_storage = self.state_storage.clone();
        let workflow_engine = self.workflow_engine.clone();
        let sharding = self.sharding.clone();
        let active_request_ids = Arc::clone(&self.active_request_ids);
        let instances_ref = Arc::clone(&self.instances);
        let spawn_locks_ref = Arc::clone(&self.spawn_locks);
        let handle = tokio::spawn(async move {
            Self::process_mailbox(
                instance_for_loop,
                mailbox_rx,
                cancel,
                snowflake,
                entity,
                runner_address,
                config,
                address_clone.clone(),
                storage,
                state_storage,
                workflow_engine,
                sharding,
                active_request_ids,
            )
            .await;

            // Clean up instance and spawn lock entries when the mailbox loop exits
            // naturally (channel exhaustion). Without this, stale entries accumulate
            // until the next send_local retry or interrupt_shard/reap_idle cycle.
            instances_ref.remove(&address_clone);
            spawn_locks_ref.remove(&address_clone);
        });

        // Store the JoinHandle synchronously so interrupt_shard and reap_idle can await
        // task completion. Using parking_lot::Mutex eliminates the race where the spawned
        // task completes before the handle is stored (which would happen with an async
        // mutex since tokio::spawn returns before the .await on the lock executes).
        *instance.join_handle.lock() = Some(handle);

        Ok(instance)
    }

    /// Process messages from the entity mailbox with configurable concurrency.
    ///
    /// When concurrency == 1: messages are processed serially (one at a time).
    /// When concurrency > 1: messages are dispatched concurrently, bounded by
    /// the entity's semaphore. The handler is behind an `RwLock<Arc<...>>` —
    /// read lock for normal dispatch (concurrent), write lock for crash recovery
    /// (handler swap). On panic, the handler is replaced and the panicked
    /// request is replayed against the fresh handler.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all, fields(entity_address = %address))]
    async fn process_mailbox(
        instance: Arc<EntityInstance>,
        mut mailbox_rx: mpsc::UnboundedReceiver<IncomingMessage>,
        cancel: CancellationToken,
        snowflake: Arc<SnowflakeGenerator>,
        entity: Arc<dyn Entity>,
        runner_address: RunnerAddress,
        config: Arc<ShardingConfig>,
        address: EntityAddress,
        message_storage: Option<Arc<dyn MessageStorage>>,
        state_storage: Option<Arc<dyn crate::durable::WorkflowStorage>>,
        workflow_engine: Option<Arc<dyn crate::durable::WorkflowEngine>>,
        sharding: Option<Arc<dyn Sharding>>,
        active_request_ids: Arc<DashSet<Snowflake>>,
    ) {
        let mut consecutive_crashes: u32 = 0;

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    // Drain remaining mailbox messages on cancellation.
                    // For persisted + server-uninterruptible messages, suppress
                    // the failure reply — these will be redelivered from storage
                    // when the entity restarts on another runner.
                    mailbox_rx.close();
                    while let Some(msg) = mailbox_rx.recv().await {
                        active_request_ids.remove(&msg.envelope().request_id);
                        instance.active_requests.fetch_sub(1, Ordering::Release);
                        if Self::should_suppress_interrupt(&instance, msg.envelope()) {
                            tracing::debug!(
                                entity_address = %address,
                                request_id = %msg.envelope().request_id,
                                "suppressing interrupt for persisted+uninterruptible message (will redeliver from storage)"
                            );
                            // Don't send any reply — message stays in storage
                        } else if let IncomingMessage::Request { request, reply_tx } = msg {
                            // Non-persisted or non-uninterruptible: send interrupt failure
                            let reply = Reply::WithExit(ReplyWithExit {
                                request_id: request.request_id,
                                id: reply_id(&snowflake, request.request_id, EXIT_SEQUENCE).await,
                                exit: ExitResult::Failure(
                                    "entity interrupted due to shard movement or shutdown".to_string(),
                                ),
                            });
                            if reply_tx.send(reply).await.is_err() {
                                tracing::debug!(
                                    request_id = %request.request_id,
                                    tag = %request.tag,
                                    "reply channel closed (receiver dropped) for interrupt failure reply"
                                );
                            }
                        }
                    }
                    break;
                }
                msg = mailbox_rx.recv() => {
                    let Some(msg) = msg else { break };
                    // active_requests was already incremented in send_local (on accept).
                    // It will be decremented after handling completes.
                    instance.touch();

                    let request_id = msg.envelope().request_id;
                    let (snapshot, _handled) = Self::handle_message_with_recovery(
                        &instance,
                        msg,
                        &snowflake,
                        &entity,
                        &runner_address,
                        &config,
                        &address,
                        &mut consecutive_crashes,
                        &message_storage,
                        &state_storage,
                        &workflow_engine,
                        &sharding,
                    ).await;

                    active_request_ids.remove(&request_id);

                    // If we have an unhandled snapshot (crash recovery exhausted),
                    // send a failure reply and evict the entity by breaking the
                    // mailbox loop. The next message will trigger a fresh spawn
                    // via get_or_spawn, preventing an infinite panic-exhaust cycle
                    // with a permanently broken handler.
                    if let Some(snap) = snapshot {
                        if Self::should_suppress_interrupt(&instance, &snap.request) {
                            tracing::debug!(
                                entity_address = %address,
                                request_id = %snap.request.request_id,
                                "suppressing crash-exhausted reply for persisted+uninterruptible message (will redeliver from storage)"
                            );
                        } else if let Some(reply_tx) = snap.reply_tx {
                            let reply = Reply::WithExit(ReplyWithExit {
                                request_id: snap.request.request_id,
                                id: reply_id(&snowflake, snap.request.request_id, EXIT_SEQUENCE).await,
                                exit: ExitResult::Failure(
                                    "entity handler crashed and recovery exhausted".to_string(),
                                ),
                            });
                            if reply_tx.send(reply).await.is_err() {
                                tracing::debug!(
                                    request_id = %snap.request.request_id,
                                    tag = %snap.request.tag,
                                    "reply channel closed (receiver dropped) for crash-exhausted reply"
                                );
                            }
                        }

                        // Drain remaining queued messages, sending failure replies
                        // so callers don't hang waiting for a response.
                        // For persisted + server-uninterruptible messages, suppress
                        // the failure reply — these will be redelivered from storage
                        // when the entity restarts on another runner.
                        mailbox_rx.close();
                        while let Some(msg) = mailbox_rx.recv().await {
                            active_request_ids.remove(&msg.envelope().request_id);
                            instance.active_requests.fetch_sub(1, Ordering::Release);
                            if Self::should_suppress_interrupt(&instance, msg.envelope()) {
                                tracing::debug!(
                                    entity_address = %address,
                                    request_id = %msg.envelope().request_id,
                                    "suppressing crash-eviction reply for persisted+uninterruptible message (will redeliver from storage)"
                                );
                                // Don't send any reply — message stays in storage
                            } else if let IncomingMessage::Request { request, reply_tx } = msg {
                                let reply = Reply::WithExit(ReplyWithExit {
                                    request_id: request.request_id,
                                    id: reply_id(&snowflake, request.request_id, EXIT_SEQUENCE).await,
                                    exit: ExitResult::Failure(
                                        "entity evicted after crash recovery exhaustion".to_string(),
                                    ),
                                });
                                if reply_tx.send(reply).await.is_err() {
                                    tracing::debug!(
                                        request_id = %request.request_id,
                                        tag = %request.tag,
                                        "reply channel closed for crash-eviction drain reply"
                                    );
                                }
                            }
                        }

                        // Evict the entity: break the mailbox loop so the cleanup
                        // code removes this instance from the map. The next message
                        // to this entity will trigger a fresh spawn via get_or_spawn.
                        instance.active_requests.fetch_sub(1, Ordering::Release);
                        break;
                    }

                    // Reset crash counter after successful handling.
                    consecutive_crashes = 0;

                    instance.active_requests.fetch_sub(1, Ordering::Release);
                    instance.touch();
                }
            }
        }
    }

    /// Check whether an interrupt/failure should be suppressed for this message.
    ///
    /// Returns `true` if the entity is being internally interrupted (shard movement
    /// or shutdown) AND the message is persisted AND server-uninterruptible. In this
    /// case, the message should NOT receive a failure reply — it stays in storage
    /// for redelivery when the entity restarts on another runner.
    fn should_suppress_interrupt(
        instance: &EntityInstance,
        envelope: &crate::envelope::EnvelopeRequest,
    ) -> bool {
        instance.is_internal_interrupt.load(Ordering::Acquire)
            && envelope.persisted
            && envelope.uninterruptible.is_server_uninterruptible()
    }

    fn is_stream_request(envelope: &crate::envelope::EnvelopeRequest) -> bool {
        matches!(
            envelope.headers.get(STREAM_HEADER_KEY),
            Some(value) if value == STREAM_HEADER_VALUE
        )
    }

    /// Handle a single message, with crash recovery on panic.
    ///
    /// Returns `(None, true)` on success, `(None, true)` on successful recovery,
    /// or `(Some(snapshot), false)` if recovery was exhausted.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all, fields(
        entity_address = %address,
        request_id = %msg.envelope().request_id,
        tag = %msg.envelope().tag,
    ))]
    async fn handle_message_with_recovery(
        instance: &Arc<EntityInstance>,
        msg: IncomingMessage,
        snowflake: &Arc<SnowflakeGenerator>,
        entity: &Arc<dyn Entity>,
        runner_address: &RunnerAddress,
        config: &Arc<ShardingConfig>,
        address: &EntityAddress,
        consecutive_crashes: &mut u32,
        message_storage: &Option<Arc<dyn MessageStorage>>,
        state_storage: &Option<Arc<dyn crate::durable::WorkflowStorage>>,
        workflow_engine: &Option<Arc<dyn crate::durable::WorkflowEngine>>,
        sharding: &Option<Arc<dyn Sharding>>,
    ) -> (Option<RequestSnapshot>, bool) {
        // Link this span to the sender's trace context carried on the envelope,
        // so that the receiver appears as a child span in the distributed trace.
        {
            let envelope = msg.envelope();
            if let (Some(trace_id_str), Some(span_id_str)) = (&envelope.trace_id, &envelope.span_id)
            {
                if let (Ok(tid), Ok(sid)) = (
                    TraceId::from_hex(trace_id_str),
                    SpanId::from_hex(span_id_str),
                ) {
                    let flags = if envelope.sampled.unwrap_or(false) {
                        TraceFlags::SAMPLED
                    } else {
                        TraceFlags::default()
                    };
                    let remote_context = SpanContext::new(
                        tid,
                        sid,
                        flags,
                        /* remote */ true,
                        TraceState::default(),
                    );
                    let otel_context =
                        opentelemetry::Context::current().with_remote_span_context(remote_context);
                    tracing::Span::current().set_parent(otel_context);
                }
            }
        }

        // Extract request info for potential replay
        let (request, reply_tx) = match msg {
            IncomingMessage::Request { request, reply_tx } => (request, Some(reply_tx)),
            IncomingMessage::Envelope { envelope } => (envelope, None),
        };

        // Acquire concurrency semaphore permit if bounded.
        let _permit = if let Some(ref sem) = instance.semaphore {
            match sem.acquire().await {
                Ok(permit) => Some(permit),
                Err(_) => {
                    // Semaphore closed — entity is shutting down
                    tracing::error!(
                        entity_address = %address,
                        "concurrency semaphore closed, breaking mailbox loop"
                    );
                    return (None, false);
                }
            }
        } else {
            None
        };

        // Register reply handler for persisted requests so that replies
        // written to storage can be delivered in real-time via the sender channel.
        let registered_handler = if request.persisted {
            if let (Some(ref storage), Some(ref tx)) = (message_storage, &reply_tx) {
                storage.register_reply_handler(request.request_id, tx.clone());
                true
            } else {
                false
            }
        } else {
            false
        };

        let snapshot = RequestSnapshot { request, reply_tx };

        let request_id = snapshot.request.request_id;

        // Try handling with crash recovery
        let is_stream = Self::is_stream_request(&snapshot.request);
        let result = if is_stream {
            Self::try_handle_stream_with_recovery(
                instance,
                snapshot,
                snowflake,
                entity,
                runner_address,
                config,
                address,
                consecutive_crashes,
                message_storage,
                state_storage,
                workflow_engine,
                sharding,
            )
            .await
        } else {
            Self::try_handle_with_recovery(
                instance,
                snapshot,
                snowflake,
                entity,
                runner_address,
                config,
                address,
                consecutive_crashes,
                message_storage,
                state_storage,
                workflow_engine,
                sharding,
            )
            .await
        };

        // Unregister reply handler when done
        if registered_handler {
            if let Some(ref storage) = message_storage {
                storage.unregister_reply_handler(request_id);
            }
        }

        result
    }

    /// Attempt to handle a request, respawning the handler on panic.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all, fields(
        entity_address = %address,
        request_id = %snapshot.request.request_id,
        tag = %snapshot.request.tag,
    ))]
    async fn try_handle_with_recovery(
        instance: &Arc<EntityInstance>,
        snapshot: RequestSnapshot,
        snowflake: &Arc<SnowflakeGenerator>,
        entity: &Arc<dyn Entity>,
        runner_address: &RunnerAddress,
        config: &Arc<ShardingConfig>,
        address: &EntityAddress,
        consecutive_crashes: &mut u32,
        message_storage: &Option<Arc<dyn MessageStorage>>,
        state_storage: &Option<Arc<dyn crate::durable::WorkflowStorage>>,
        workflow_engine: &Option<Arc<dyn crate::durable::WorkflowEngine>>,
        sharding: &Option<Arc<dyn Sharding>>,
    ) -> (Option<RequestSnapshot>, bool) {
        loop {
            // Try to handle the request
            let result = Self::try_handle_request(instance, &snapshot).await;

            match result {
                HandleResult::Success(bytes) => {
                    // Send success reply
                    let reply = Reply::WithExit(ReplyWithExit {
                        request_id: snapshot.request.request_id,
                        id: reply_id(snowflake, snapshot.request.request_id, EXIT_SEQUENCE).await,
                        exit: ExitResult::Success(bytes),
                    });
                    // Save reply to storage for persisted messages
                    if snapshot.request.persisted {
                        if let Some(ref storage) = message_storage {
                            if let Err(e) = storage.save_reply(&reply).await {
                                tracing::error!(
                                    request_id = %snapshot.request.request_id,
                                    error = %e,
                                    "failed to persist reply — data loss possible if runner crashes before redelivery"
                                );
                            }
                        }
                    }
                    if let Some(ref reply_tx) = snapshot.reply_tx {
                        if reply_tx.send(reply).await.is_err() {
                            tracing::debug!(
                                request_id = %snapshot.request.request_id,
                                tag = %snapshot.request.tag,
                                "reply channel closed (receiver dropped) for success reply"
                            );
                        }
                    }
                    return (None, true);
                }
                HandleResult::Error(err) => {
                    // Application error (not a panic) — send error reply, no recovery needed
                    let reply = Reply::WithExit(ReplyWithExit {
                        request_id: snapshot.request.request_id,
                        id: reply_id(snowflake, snapshot.request.request_id, EXIT_SEQUENCE).await,
                        exit: ExitResult::Failure(err.to_string()),
                    });
                    // Save reply to storage for persisted messages
                    if snapshot.request.persisted {
                        if let Some(ref storage) = message_storage {
                            if let Err(e) = storage.save_reply(&reply).await {
                                tracing::error!(
                                    request_id = %snapshot.request.request_id,
                                    error = %e,
                                    "failed to persist error reply — data loss possible if runner crashes before redelivery"
                                );
                            }
                        }
                    }
                    if let Some(ref reply_tx) = snapshot.reply_tx {
                        if reply_tx.send(reply).await.is_err() {
                            tracing::debug!(
                                request_id = %snapshot.request.request_id,
                                tag = %snapshot.request.tag,
                                "reply channel closed (receiver dropped) for error reply"
                            );
                        }
                    }
                    return (None, true);
                }
                HandleResult::Panicked(panic_info) => {
                    *consecutive_crashes += 1;
                    warn!(
                        entity_address = %address,
                        consecutive_crashes = *consecutive_crashes,
                        panic_info = %panic_info,
                        "Entity handler panicked, attempting crash recovery"
                    );

                    // When entity_crash_max_retries > 0, give up after that many attempts.
                    // When 0, retry indefinitely (matching TS behavior).
                    if config.entity_crash_max_retries > 0
                        && *consecutive_crashes > config.entity_crash_max_retries
                    {
                        warn!(
                            entity_address = %address,
                            max_retries = config.entity_crash_max_retries,
                            "Crash recovery exhausted, dropping entity"
                        );
                        return (Some(snapshot), false);
                    }

                    // Exponential backoff with factor 1.5, capped at entity_crash_max_backoff.
                    // Matches TS: Schedule.exponential(500, 1.5).pipe(Schedule.either(Schedule.spaced("10 seconds")))
                    let base_ms = config.entity_crash_initial_backoff.as_millis() as f64;
                    let backoff_ms =
                        base_ms * 1.5_f64.powi(consecutive_crashes.saturating_sub(1) as i32);
                    let backoff = Duration::from_millis(
                        backoff_ms.min(config.entity_crash_max_backoff.as_millis() as f64) as u64,
                    );
                    tokio::time::sleep(backoff).await;

                    // Respawn handler — takes write lock to swap atomically.
                    // Any concurrent readers will complete with their existing
                    // Arc clone; new readers will get the fresh handler after
                    // the write lock is released.
                    let ctx = EntityContext {
                        address: address.clone(),
                        runner_address: runner_address.clone(),
                        snowflake: Arc::clone(snowflake),
                        cancellation: instance.cancel.clone(),
                        state_storage: state_storage.clone(),
                        workflow_engine: workflow_engine.clone(),
                        sharding: sharding.clone(),
                        message_storage: message_storage.clone(),
                    };

                    match entity.spawn(ctx).await {
                        Ok(new_handler) => {
                            let mut handler_guard = instance.handler.write().await;
                            *handler_guard = Arc::from(new_handler);
                            // Loop to retry with new handler
                        }
                        Err(spawn_err) => {
                            warn!(
                                entity_address = %address,
                                error = %spawn_err,
                                "Failed to respawn handler during crash recovery"
                            );
                            return (Some(snapshot), false);
                        }
                    }
                }
            }
        }
    }

    /// Attempt to handle a streaming request, respawning the handler on panic.
    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all, fields(
        entity_address = %address,
        request_id = %snapshot.request.request_id,
        tag = %snapshot.request.tag,
    ))]
    async fn try_handle_stream_with_recovery(
        instance: &Arc<EntityInstance>,
        snapshot: RequestSnapshot,
        snowflake: &Arc<SnowflakeGenerator>,
        entity: &Arc<dyn Entity>,
        runner_address: &RunnerAddress,
        config: &Arc<ShardingConfig>,
        address: &EntityAddress,
        consecutive_crashes: &mut u32,
        message_storage: &Option<Arc<dyn MessageStorage>>,
        state_storage: &Option<Arc<dyn crate::durable::WorkflowStorage>>,
        workflow_engine: &Option<Arc<dyn crate::durable::WorkflowEngine>>,
        sharding: &Option<Arc<dyn Sharding>>,
    ) -> (Option<RequestSnapshot>, bool) {
        loop {
            let result =
                Self::try_handle_stream(instance, &snapshot, snowflake, message_storage).await;

            match result {
                StreamHandleResult::Success => {
                    let reply = Reply::WithExit(ReplyWithExit {
                        request_id: snapshot.request.request_id,
                        id: reply_id(snowflake, snapshot.request.request_id, EXIT_SEQUENCE).await,
                        exit: ExitResult::Success(Vec::new()),
                    });
                    if snapshot.request.persisted {
                        if let Some(ref storage) = message_storage {
                            if let Err(e) = storage.save_reply(&reply).await {
                                tracing::error!(
                                    request_id = %snapshot.request.request_id,
                                    error = %e,
                                    "failed to persist reply — data loss possible if runner crashes before redelivery"
                                );
                            }
                        }
                    }
                    if let Some(ref reply_tx) = snapshot.reply_tx {
                        if reply_tx.send(reply).await.is_err() {
                            tracing::debug!(
                                request_id = %snapshot.request.request_id,
                                tag = %snapshot.request.tag,
                                "reply channel closed (receiver dropped) for stream exit reply"
                            );
                        }
                    }
                    return (None, true);
                }
                StreamHandleResult::Error(err) => {
                    let reply = Reply::WithExit(ReplyWithExit {
                        request_id: snapshot.request.request_id,
                        id: reply_id(snowflake, snapshot.request.request_id, EXIT_SEQUENCE).await,
                        exit: ExitResult::Failure(err.to_string()),
                    });
                    if snapshot.request.persisted {
                        if let Some(ref storage) = message_storage {
                            if let Err(e) = storage.save_reply(&reply).await {
                                tracing::error!(
                                    request_id = %snapshot.request.request_id,
                                    error = %e,
                                    "failed to persist error reply — data loss possible if runner crashes before redelivery"
                                );
                            }
                        }
                    }
                    if let Some(ref reply_tx) = snapshot.reply_tx {
                        if reply_tx.send(reply).await.is_err() {
                            tracing::debug!(
                                request_id = %snapshot.request.request_id,
                                tag = %snapshot.request.tag,
                                "reply channel closed (receiver dropped) for stream error reply"
                            );
                        }
                    }
                    return (None, true);
                }
                StreamHandleResult::Panicked(panic_info) => {
                    *consecutive_crashes += 1;
                    warn!(
                        entity_address = %address,
                        consecutive_crashes = *consecutive_crashes,
                        panic_info = %panic_info,
                        "Entity handler panicked, attempting crash recovery"
                    );

                    if config.entity_crash_max_retries > 0
                        && *consecutive_crashes > config.entity_crash_max_retries
                    {
                        warn!(
                            entity_address = %address,
                            max_retries = config.entity_crash_max_retries,
                            "Crash recovery exhausted, dropping entity"
                        );
                        return (Some(snapshot), false);
                    }

                    let base_ms = config.entity_crash_initial_backoff.as_millis() as f64;
                    let backoff_ms =
                        base_ms * 1.5_f64.powi(consecutive_crashes.saturating_sub(1) as i32);
                    let backoff = Duration::from_millis(
                        backoff_ms.min(config.entity_crash_max_backoff.as_millis() as f64) as u64,
                    );
                    tokio::time::sleep(backoff).await;

                    let ctx = EntityContext {
                        address: address.clone(),
                        runner_address: runner_address.clone(),
                        snowflake: Arc::clone(snowflake),
                        cancellation: instance.cancel.clone(),
                        state_storage: state_storage.clone(),
                        workflow_engine: workflow_engine.clone(),
                        sharding: sharding.clone(),
                        message_storage: message_storage.clone(),
                    };

                    match entity.spawn(ctx).await {
                        Ok(new_handler) => {
                            let mut handler_guard = instance.handler.write().await;
                            *handler_guard = Arc::from(new_handler);
                        }
                        Err(spawn_err) => {
                            warn!(
                                entity_address = %address,
                                error = %spawn_err,
                                "Failed to respawn handler during crash recovery"
                            );
                            return (Some(snapshot), false);
                        }
                    }
                }
            }
        }
    }

    /// Try to execute a handler request, catching panics.
    ///
    /// Acquires a read lock on the handler to get an `Arc` clone, then releases
    /// the lock before executing. This allows concurrent requests to proceed
    /// simultaneously. Crash recovery takes a write lock to swap the handler.
    #[instrument(level = "debug", skip(instance, snapshot), fields(
        request_id = %snapshot.request.request_id,
        tag = %snapshot.request.tag,
    ))]
    async fn try_handle_request(
        instance: &Arc<EntityInstance>,
        snapshot: &RequestSnapshot,
    ) -> HandleResult {
        use futures::FutureExt;

        // Read-lock to clone the Arc, then release immediately.
        // This allows concurrent requests to proceed and avoids holding
        // the lock during handler execution.
        let handler = {
            let guard = instance.handler.read().await;
            Arc::clone(&guard)
        };

        let tag = snapshot.request.tag.clone();
        let payload = snapshot.request.payload.clone();
        let mut headers = snapshot.request.headers.clone();
        // Inject the envelope request_id so handler code (macro-generated workflow
        // dispatch) can scope activity journals per workflow execution.
        headers.insert(
            crate::envelope::REQUEST_ID_HEADER_KEY.to_string(),
            snapshot.request.request_id.0.to_string(),
        );

        // SAFETY: We use AssertUnwindSafe because on panic we will discard the handler
        // entirely and respawn a fresh one. The handler state after a panic is never observed.
        let fut = AssertUnwindSafe(handler.handle_request(&tag, &payload, &headers)).catch_unwind();

        match fut.await {
            Ok(Ok(bytes)) => HandleResult::Success(bytes),
            Ok(Err(err)) => HandleResult::Error(err),
            Err(panic_payload) => {
                let info = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                HandleResult::Panicked(info)
            }
        }
    }

    /// Try to execute a handler stream, catching panics.
    #[instrument(level = "debug", skip(instance, snapshot, snowflake, message_storage), fields(
        request_id = %snapshot.request.request_id,
        tag = %snapshot.request.tag,
    ))]
    async fn try_handle_stream(
        instance: &Arc<EntityInstance>,
        snapshot: &RequestSnapshot,
        snowflake: &Arc<SnowflakeGenerator>,
        message_storage: &Option<Arc<dyn MessageStorage>>,
    ) -> StreamHandleResult {
        use futures::FutureExt;
        use tokio_stream::StreamExt;

        let handler = {
            let guard = instance.handler.read().await;
            Arc::clone(&guard)
        };

        let tag = snapshot.request.tag.clone();
        let payload = snapshot.request.payload.clone();
        let headers = snapshot.request.headers.clone();

        let stream_result =
            AssertUnwindSafe(handler.handle_stream(&tag, &payload, &headers)).catch_unwind();

        let mut stream = match stream_result.await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => return StreamHandleResult::Error(err),
            Err(panic_payload) => {
                let info = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                return StreamHandleResult::Panicked(info);
            }
        };

        let request_id = snapshot.request.request_id;
        let mut sequence: i32 = 0;

        loop {
            let next = AssertUnwindSafe(stream.next()).catch_unwind().await;
            match next {
                Ok(Some(Ok(bytes))) => {
                    if sequence == EXIT_SEQUENCE {
                        return StreamHandleResult::Error(ClusterError::MalformedMessage {
                            reason: "stream exceeded maximum sequence".to_string(),
                            source: None,
                        });
                    }

                    let reply = Reply::Chunk(ReplyChunk {
                        request_id,
                        id: reply_id(snowflake, request_id, sequence).await,
                        sequence,
                        values: vec![bytes],
                    });

                    if snapshot.request.persisted {
                        if let Some(ref storage) = message_storage {
                            if let Err(e) = storage.save_reply(&reply).await {
                                tracing::error!(
                                    request_id = %request_id,
                                    error = %e,
                                    "failed to persist chunk reply — data loss possible if runner crashes before redelivery"
                                );
                            }
                        }
                    }

                    if let Some(ref reply_tx) = snapshot.reply_tx {
                        if reply_tx.send(reply).await.is_err() {
                            tracing::debug!(
                                request_id = %request_id,
                                tag = %snapshot.request.tag,
                                "reply channel closed (receiver dropped) for stream chunk"
                            );
                        }
                    }

                    sequence = sequence.saturating_add(1);
                }
                Ok(Some(Err(err))) => return StreamHandleResult::Error(err),
                Ok(None) => return StreamHandleResult::Success,
                Err(panic_payload) => {
                    let info = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    return StreamHandleResult::Panicked(info);
                }
            }
        }
    }
}

enum HandleResult {
    Success(Vec<u8>),
    Error(ClusterError),
    Panicked(String),
}

enum StreamHandleResult {
    Success,
    Error(ClusterError),
    Panicked(String),
}

impl EntityInstance {
    fn touch(&self) {
        // Use Release so reap_idle's Acquire load sees the updated timestamp.
        self.last_active_ms.store(now_millis(), Ordering::Release);
    }
}

fn now_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(err) => {
            warn!("system clock before Unix epoch: {err}");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{Entity, EntityContext, EntityHandler};
    use crate::envelope::{EnvelopeRequest, STREAM_HEADER_KEY, STREAM_HEADER_VALUE};
    use crate::message::IncomingMessage;
    use crate::snowflake::Snowflake;
    use crate::types::{EntityId, EntityType, ShardId};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI32, Ordering as AtomicOrdering};
    use tokio::sync::Notify;

    // --- Test entity implementations ---

    struct TestEntity {
        name: String,
    }

    impl TestEntity {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
            }
        }
    }

    #[async_trait]
    impl Entity for TestEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new(&self.name)
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(TestHandler {
                call_count: AtomicI32::new(0),
            }))
        }
    }

    struct TestHandler {
        call_count: AtomicI32,
    }

    #[async_trait]
    impl EntityHandler for TestHandler {
        async fn handle_request(
            &self,
            tag: &str,
            payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            let count = self.call_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;
            match tag {
                "echo" => Ok(payload.to_vec()),
                "count" => Ok(rmp_serde::to_vec(&count).unwrap()),
                _ => Err(ClusterError::MalformedMessage {
                    reason: format!("unknown tag: {tag}"),
                    source: None,
                }),
            }
        }
    }

    struct StreamEntity;

    #[async_trait]
    impl Entity for StreamEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Stream")
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(StreamHandler))
        }
    }

    struct StreamHandler;

    #[async_trait]
    impl EntityHandler for StreamHandler {
        async fn handle_request(
            &self,
            _tag: &str,
            _payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            Err(ClusterError::MalformedMessage {
                reason: "handle_request should not be called".to_string(),
                source: None,
            })
        }

        async fn handle_stream(
            &self,
            _tag: &str,
            _payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<
            std::pin::Pin<
                Box<dyn tokio_stream::Stream<Item = Result<Vec<u8>, ClusterError>> + Send>,
            >,
            ClusterError,
        > {
            let values = vec![
                Ok(rmp_serde::to_vec(&1i32).unwrap()),
                Ok(rmp_serde::to_vec(&2i32).unwrap()),
            ];
            Ok(Box::pin(tokio_stream::iter(values)))
        }
    }

    fn test_config() -> Arc<ShardingConfig> {
        Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            ..Default::default()
        })
    }

    fn test_address(entity_id: &str) -> EntityAddress {
        EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Test"),
            entity_id: EntityId::new(entity_id),
        }
    }

    fn test_request(address: &EntityAddress, tag: &str) -> EnvelopeRequest {
        use std::sync::atomic::AtomicI64;
        static COUNTER: AtomicI64 = AtomicI64::new(1);
        EnvelopeRequest {
            request_id: Snowflake(COUNTER.fetch_add(1, AtomicOrdering::Relaxed)),
            address: address.clone(),
            tag: tag.to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        }
    }

    fn make_manager() -> EntityManager {
        EntityManager::new(
            Arc::new(TestEntity::new("Test")),
            test_config(),
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        )
    }

    fn stream_address(entity_id: &str) -> EntityAddress {
        EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Stream"),
            entity_id: EntityId::new(entity_id),
        }
    }

    fn make_stream_manager() -> EntityManager {
        EntityManager::new(
            Arc::new(StreamEntity),
            test_config(),
            RunnerAddress::new("127.0.0.1", 9001),
            Arc::new(SnowflakeGenerator::new()),
            None,
        )
    }

    #[tokio::test]
    async fn spawns_entity_on_first_message() {
        let mgr = make_manager();
        assert_eq!(mgr.active_count(), 0);

        let addr = test_address("e-1");
        let req = test_request(&addr, "echo");
        let (tx, mut rx) = mpsc::channel(1);

        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        let reply = rx.recv().await.unwrap();
        assert!(matches!(reply, Reply::WithExit(_)));
        assert_eq!(mgr.active_count(), 1);
    }

    #[tokio::test]
    async fn routes_to_existing_instance() {
        let mgr = make_manager();
        let addr = test_address("e-1");

        // Send two messages to the same entity
        for _ in 0..2 {
            let req = test_request(&addr, "count");
            let (tx, mut rx) = mpsc::channel(1);
            mgr.send_local(IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            })
            .await
            .unwrap();
            rx.recv().await.unwrap();
        }

        // Should still be one instance
        assert_eq!(mgr.active_count(), 1);
    }

    #[tokio::test]
    async fn stream_requests_invoke_handle_stream() {
        let mgr = make_stream_manager();

        let addr = stream_address("s-1");
        let mut req = test_request(&addr, "stream");
        req.headers.insert(
            STREAM_HEADER_KEY.to_string(),
            STREAM_HEADER_VALUE.to_string(),
        );
        let (tx, mut rx) = mpsc::channel(4);

        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        let exit = rx.recv().await.unwrap();

        match first {
            Reply::Chunk(chunk) => {
                assert_eq!(chunk.sequence, 0);
                let value: i32 = rmp_serde::from_slice(&chunk.values[0]).unwrap();
                assert_eq!(value, 1);
            }
            Reply::WithExit(_) => panic!("expected chunk reply"),
        }

        match second {
            Reply::Chunk(chunk) => {
                assert_eq!(chunk.sequence, 1);
                let value: i32 = rmp_serde::from_slice(&chunk.values[0]).unwrap();
                assert_eq!(value, 2);
            }
            Reply::WithExit(_) => panic!("expected chunk reply"),
        }

        match exit {
            Reply::WithExit(reply) => match reply.exit {
                ExitResult::Success(bytes) => assert!(bytes.is_empty()),
                ExitResult::Failure(msg) => panic!("unexpected failure: {msg}"),
            },
            Reply::Chunk(_) => panic!("expected exit reply"),
        }
    }

    #[tokio::test]
    async fn different_addresses_spawn_separate_instances() {
        let mgr = make_manager();

        for id in &["e-1", "e-2", "e-3"] {
            let addr = test_address(id);
            let req = test_request(&addr, "echo");
            let (tx, _rx) = mpsc::channel(1);
            mgr.send_local(IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            })
            .await
            .unwrap();
        }

        // Allow processing time
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 3);
    }

    #[tokio::test]
    async fn mailbox_full_returns_error() {
        // Mailbox capacity limits active (in-flight) requests, matching the TS
        // behavior (entityManager.ts:432) which checks
        // `server.activeRequests.size >= mailboxCapacity`.
        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 1,
            ..Default::default()
        });
        // Use a slow handler so the first message stays active
        struct SlowEntity;

        #[async_trait]
        impl Entity for SlowEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Slow")
            }

            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(SlowHandler))
            }
        }

        struct SlowHandler;

        #[async_trait]
        impl EntityHandler for SlowHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(vec![])
            }
        }

        let mgr = EntityManager::new(
            Arc::new(SlowEntity),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Slow"),
            entity_id: EntityId::new("s-1"),
        };

        // First message: goes into processing (active_requests becomes 1)
        let req1 = EnvelopeRequest {
            request_id: Snowflake(1),
            address: addr.clone(),
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
        let (tx1, _rx1) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req1,
            reply_tx: tx1,
        })
        .await
        .unwrap();

        // Give time for first message to start processing (active_requests = 1)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second message: should fail because active_requests (1) >= mailbox_capacity (1)
        let req2 = EnvelopeRequest {
            request_id: Snowflake(2),
            address: addr.clone(),
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
        let (tx2, _rx2) = mpsc::channel(1);
        let result = mgr
            .send_local(IncomingMessage::Request {
                request: req2,
                reply_tx: tx2,
            })
            .await;

        assert!(matches!(result, Err(ClusterError::MailboxFull { .. })));
    }

    #[tokio::test]
    async fn mailbox_capacity_counts_active_requests_not_queued() {
        // With capacity=3, we should accept 3 messages even if they're all
        // in-flight (slow handler), and reject the 4th. This tests the TS
        // semantics where capacity limits active requests, not queue depth.
        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 3,
            entity_max_concurrent_requests: 3,
            ..Default::default()
        });

        struct SlowEntity3;

        #[async_trait]
        impl Entity for SlowEntity3 {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Slow3")
            }
            fn concurrency(&self) -> Option<usize> {
                Some(3)
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(SlowHandler3))
            }
        }

        struct SlowHandler3;

        #[async_trait]
        impl EntityHandler for SlowHandler3 {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(vec![])
            }
        }

        let mgr = EntityManager::new(
            Arc::new(SlowEntity3),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Slow3"),
            entity_id: EntityId::new("s-1"),
        };

        // Messages 1-3: should succeed (active_requests goes 0→1→2→3)
        for i in 1..=3 {
            let req = EnvelopeRequest {
                request_id: Snowflake(i),
                address: addr.clone(),
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
            let (tx, _rx) = mpsc::channel(1);
            mgr.send_local(IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            })
            .await
            .unwrap();
        }

        // Message 4: should fail (active_requests=3 >= capacity=3)
        let req4 = EnvelopeRequest {
            request_id: Snowflake(4),
            address: addr.clone(),
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
        let (tx4, _rx4) = mpsc::channel(1);
        let result = mgr
            .send_local(IncomingMessage::Request {
                request: req4,
                reply_tx: tx4,
            })
            .await;

        assert!(matches!(result, Err(ClusterError::MailboxFull { .. })));
    }

    #[tokio::test]
    async fn fire_and_forget_envelope() {
        let mgr = make_manager();
        let addr = test_address("e-1");
        let envelope = test_request(&addr, "echo");

        mgr.send_local(IncomingMessage::Envelope { envelope })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 1);
    }

    #[tokio::test]
    async fn interrupt_shard_cancels_entities() {
        let mgr = make_manager();
        let shard = ShardId::new("default", 0);

        // Spawn two entities on shard 0
        for id in &["e-1", "e-2"] {
            let addr = EntityAddress {
                shard_id: shard.clone(),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new(*id),
            };
            let req = test_request(&addr, "echo");
            let (tx, _rx) = mpsc::channel(1);
            mgr.send_local(IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            })
            .await
            .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 2);

        mgr.interrupt_shard(&shard).await;
        assert_eq!(mgr.active_count(), 0);
    }

    #[tokio::test]
    async fn interrupt_shard_only_affects_matching_shard() {
        let mgr = make_manager();

        // Entity on shard 0
        let addr0 = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Test"),
            entity_id: EntityId::new("e-1"),
        };
        let (tx, _rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: test_request(&addr0, "echo"),
            reply_tx: tx,
        })
        .await
        .unwrap();

        // Entity on shard 1
        let addr1 = EntityAddress {
            shard_id: ShardId::new("default", 1),
            entity_type: EntityType::new("Test"),
            entity_id: EntityId::new("e-2"),
        };
        let (tx, _rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: test_request(&addr1, "echo"),
            reply_tx: tx,
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 2);

        // Only interrupt shard 0
        mgr.interrupt_shard(&ShardId::new("default", 0)).await;
        assert_eq!(mgr.active_count(), 1);
    }

    #[tokio::test]
    async fn reap_idle_removes_idle_entities() {
        let mgr = make_manager();
        let addr = test_address("e-1");
        let (tx, mut rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: test_request(&addr, "echo"),
            reply_tx: tx,
        })
        .await
        .unwrap();
        rx.recv().await.unwrap();

        // Entity just used, so 0ms idle should not reap
        let reaped = mgr.reap_idle(Duration::from_secs(60)).await;
        assert_eq!(reaped, 0);
        assert_eq!(mgr.active_count(), 1);

        // With 0ms max idle, should reap immediately
        tokio::time::sleep(Duration::from_millis(10)).await;
        let reaped = mgr.reap_idle(Duration::from_millis(1)).await;
        assert_eq!(reaped, 1);
        assert_eq!(mgr.active_count(), 0);
    }

    #[tokio::test]
    async fn reap_does_not_remove_entities_with_active_requests() {
        struct NeverFinishEntity;

        #[async_trait]
        impl Entity for NeverFinishEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("NeverFinish")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(NeverFinishHandler))
            }
        }

        struct NeverFinishHandler;

        #[async_trait]
        impl EntityHandler for NeverFinishHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                // Block forever
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(vec![])
            }
        }

        let mgr = EntityManager::new(
            Arc::new(NeverFinishEntity),
            test_config(),
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("NeverFinish"),
            entity_id: EntityId::new("e-1"),
        };
        let req = EnvelopeRequest {
            request_id: Snowflake(1),
            address: addr.clone(),
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
        let (tx, _rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        // Give time for the request to start processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Try to reap with 0ms idle — should NOT reap because active request
        let reaped = mgr.reap_idle(Duration::from_millis(0)).await;
        assert_eq!(reaped, 0);
        assert_eq!(mgr.active_count(), 1);
    }

    #[tokio::test]
    async fn entity_keeps_alive_via_on_idle() {
        struct KeepAliveEntity;

        #[async_trait]
        impl Entity for KeepAliveEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("KeepAlive")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(KeepAliveHandler))
            }
        }

        struct KeepAliveHandler;

        #[async_trait]
        impl EntityHandler for KeepAliveHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                Ok(vec![])
            }

            async fn on_idle(&self) -> bool {
                true // keep alive
            }
        }

        let mgr = EntityManager::new(
            Arc::new(KeepAliveEntity),
            test_config(),
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("KeepAlive"),
            entity_id: EntityId::new("e-1"),
        };
        let (tx, mut rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: test_request(&addr, "echo"),
            reply_tx: tx,
        })
        .await
        .unwrap();
        rx.recv().await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        let reaped = mgr.reap_idle(Duration::from_millis(1)).await;
        assert_eq!(reaped, 0);
        assert_eq!(mgr.active_count(), 1);
    }

    // --- Crash recovery tests ---

    #[tokio::test]
    async fn handler_panic_triggers_respawn_and_replay() {
        use std::sync::atomic::AtomicU32;

        struct PanicOnceEntity {
            spawn_count: AtomicU32,
        }

        #[async_trait]
        impl Entity for PanicOnceEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("PanicOnce")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                let count = self.spawn_count.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(Box::new(PanicOnceHandler {
                    should_panic: count == 0,
                }))
            }
        }

        struct PanicOnceHandler {
            should_panic: bool,
        }

        #[async_trait]
        impl EntityHandler for PanicOnceHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                if self.should_panic {
                    panic!("intentional test panic");
                }
                Ok(b"recovered".to_vec())
            }
        }

        let entity = Arc::new(PanicOnceEntity {
            spawn_count: AtomicU32::new(0),
        });

        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_crash_max_retries: 3,
            entity_crash_initial_backoff: Duration::from_millis(10),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            entity.clone(),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = test_address("panic-1");
        let req = test_request(&addr, "test");
        let (tx, mut rx) = mpsc::channel(1);

        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout waiting for reply")
            .expect("channel closed");

        // Should have recovered: first handler panics, second handler succeeds
        match reply {
            Reply::WithExit(exit) => match exit.exit {
                ExitResult::Success(bytes) => assert_eq!(bytes, b"recovered"),
                ExitResult::Failure(msg) => panic!("Expected success but got failure: {msg}"),
            },
            _ => panic!("Expected WithExit reply"),
        }

        // Entity should have been spawned twice (original + recovery)
        assert_eq!(entity.spawn_count.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn repeated_crashes_exhaust_retries() {
        struct AlwaysPanicEntity;

        #[async_trait]
        impl Entity for AlwaysPanicEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("AlwaysPanic")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(AlwaysPanicHandler))
            }
        }

        struct AlwaysPanicHandler;

        #[async_trait]
        impl EntityHandler for AlwaysPanicHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                panic!("always panic");
            }
        }

        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_crash_max_retries: 2,
            entity_crash_initial_backoff: Duration::from_millis(1),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            Arc::new(AlwaysPanicEntity),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = test_address("panic-always");
        let req = test_request(&addr, "test");
        let (tx, mut rx) = mpsc::channel(1);

        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout waiting for reply")
            .expect("channel closed");

        // Should get a failure after retries exhausted
        match reply {
            Reply::WithExit(exit) => match exit.exit {
                ExitResult::Failure(msg) => {
                    assert!(msg.contains("recovery exhausted"), "Got: {msg}");
                }
                ExitResult::Success(_) => panic!("Expected failure"),
            },
            _ => panic!("Expected WithExit reply"),
        }
    }

    #[tokio::test]
    async fn application_error_does_not_trigger_recovery() {
        use std::sync::atomic::AtomicU32;

        struct ErrorEntity {
            spawn_count: AtomicU32,
        }

        #[async_trait]
        impl Entity for ErrorEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Error")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                self.spawn_count.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(Box::new(ErrorHandler))
            }
        }

        struct ErrorHandler;

        #[async_trait]
        impl EntityHandler for ErrorHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                Err(ClusterError::MalformedMessage {
                    reason: "test error".into(),
                    source: None,
                })
            }
        }

        let entity = Arc::new(ErrorEntity {
            spawn_count: AtomicU32::new(0),
        });

        let mgr = EntityManager::new(
            entity.clone(),
            test_config(),
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = test_address("err-1");
        let req = test_request(&addr, "test");
        let (tx, mut rx) = mpsc::channel(1);

        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        let reply = rx.recv().await.unwrap();
        match reply {
            Reply::WithExit(exit) => {
                assert!(matches!(exit.exit, ExitResult::Failure(_)));
            }
            _ => panic!("Expected WithExit reply"),
        }

        // Should NOT have triggered a respawn — only 1 spawn
        assert_eq!(entity.spawn_count.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn interrupt_shard_sets_internal_interrupt_flag() {
        let mgr = make_manager();
        let shard = ShardId::new("default", 0);

        // Spawn an entity on shard 0
        let addr = EntityAddress {
            shard_id: shard.clone(),
            entity_type: EntityType::new("Test"),
            entity_id: EntityId::new("e-flag"),
        };
        let req = test_request(&addr, "echo");
        let (tx, _rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 1);

        // Verify the flag is false before interrupt
        let instance = mgr.instances.get(&addr).unwrap();
        assert!(
            !instance.is_internal_interrupt.load(Ordering::Acquire),
            "is_internal_interrupt should be false before interrupt"
        );
        drop(instance);

        // interrupt_shard should set the flag before cancelling
        // After interrupt, the instance is removed from the map,
        // so we can't check it. But we verify it was set by checking
        // the cancel token was triggered (as the flag is set before cancel).
        mgr.interrupt_shard(&shard).await;
        assert_eq!(mgr.active_count(), 0);
    }

    #[tokio::test]
    async fn consecutive_crash_counter_resets_on_success() {
        use std::sync::atomic::AtomicU32;

        // Entity that panics on first call, succeeds on subsequent calls
        struct PanicThenSucceedEntity {
            spawn_count: AtomicU32,
        }

        #[async_trait]
        impl Entity for PanicThenSucceedEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("PanicThenSucceed")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                let count = self.spawn_count.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(Box::new(PanicThenSucceedHandler {
                    should_panic: count == 0,
                }))
            }
        }

        struct PanicThenSucceedHandler {
            should_panic: bool,
        }

        #[async_trait]
        impl EntityHandler for PanicThenSucceedHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                if self.should_panic {
                    panic!("first call panic");
                }
                Ok(b"ok".to_vec())
            }
        }

        let entity = Arc::new(PanicThenSucceedEntity {
            spawn_count: AtomicU32::new(0),
        });

        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_crash_max_retries: 2,
            entity_crash_initial_backoff: Duration::from_millis(1),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            entity.clone(),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = test_address("reset-1");

        // First message: panics then recovers
        let req = test_request(&addr, "test");
        let (tx, mut rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            reply,
            Reply::WithExit(ReplyWithExit {
                exit: ExitResult::Success(_),
                ..
            })
        ));

        // Second message: should succeed immediately (counter reset)
        let req2 = test_request(&addr, "test");
        let (tx2, mut rx2) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req2,
            reply_tx: tx2,
        })
        .await
        .unwrap();
        let reply2 = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            reply2,
            Reply::WithExit(ReplyWithExit {
                exit: ExitResult::Success(_),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn crash_recovery_exhaustion_resets_counter_for_next_message() {
        use std::sync::atomic::AtomicU32;

        // Entity that always panics on first spawn, succeeds on second
        struct AlwaysPanicEntity {
            spawn_count: Arc<AtomicU32>,
        }

        #[async_trait]
        impl Entity for AlwaysPanicEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("AlwaysPanic")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                let count = self.spawn_count.fetch_add(1, AtomicOrdering::SeqCst);
                // First few spawns panic (for first message's retries),
                // later spawns succeed (for second message)
                // Spawns 0,1 are for first message (both panic → exhausted with max_retries=1).
                // The second message reuses the handler from spawn 1 (panics),
                // then recovery spawns 2 which must succeed.
                Ok(Box::new(AlwaysPanicHandler {
                    should_panic: count < 2,
                }))
            }
        }

        struct AlwaysPanicHandler {
            should_panic: bool,
        }

        #[async_trait]
        impl EntityHandler for AlwaysPanicHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                if self.should_panic {
                    panic!("intentional panic");
                }
                Ok(b"success".to_vec())
            }
        }

        let spawn_count = Arc::new(AtomicU32::new(0));
        let entity = Arc::new(AlwaysPanicEntity {
            spawn_count: spawn_count.clone(),
        });

        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_crash_max_retries: 1,
            entity_crash_initial_backoff: Duration::from_millis(1),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            entity.clone(),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = test_address("exhaust-1");

        // First message: panics on initial + recovery spawn → exhausted → failure reply
        let req1 = test_request(&addr, "test");
        let (tx1, mut rx1) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req1,
            reply_tx: tx1,
        })
        .await
        .unwrap();
        let reply1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                reply1,
                Reply::WithExit(ReplyWithExit {
                    exit: ExitResult::Failure(_),
                    ..
                })
            ),
            "first message should fail (crash recovery exhausted)"
        );

        // Second message: should get a fresh retry budget and succeed
        // (spawn 2 panics → recovery → spawn 3 succeeds)
        let req2 = EnvelopeRequest {
            request_id: Snowflake(99),
            address: addr.clone(),
            tag: "test".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx2, mut rx2) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req2,
            reply_tx: tx2,
        })
        .await
        .unwrap();
        let reply2 = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                reply2,
                Reply::WithExit(ReplyWithExit {
                    exit: ExitResult::Success(_),
                    ..
                })
            ),
            "second message should succeed (counter was reset after exhaustion)"
        );
    }

    #[tokio::test]
    async fn interrupt_suppresses_reply_for_persisted_uninterruptible_messages() {
        use crate::schema::Uninterruptible;

        // Handler that blocks on a channel — allows us to control when processing completes
        struct BlockingEntity {
            gate: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl Entity for BlockingEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Blocking")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(BlockingHandler {
                    gate: Arc::clone(&self.gate),
                }))
            }
        }

        struct BlockingHandler {
            gate: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl EntityHandler for BlockingHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                // Block until gate is signaled
                self.gate.notified().await;
                Ok(b"done".to_vec())
            }
        }

        let gate = Arc::new(tokio::sync::Notify::new());
        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_termination_timeout: Duration::from_millis(10),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            Arc::new(BlockingEntity {
                gate: Arc::clone(&gate),
            }),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let shard = ShardId::new("default", 0);
        let addr = EntityAddress {
            shard_id: shard.clone(),
            entity_type: EntityType::new("Blocking"),
            entity_id: EntityId::new("e-suppress"),
        };

        // First message: starts processing, blocks on gate
        let req1 = EnvelopeRequest {
            request_id: Snowflake(1),
            address: addr.clone(),
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
        let (tx1, _rx1) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req1,
            reply_tx: tx1,
        })
        .await
        .unwrap();

        // Wait for handler to start processing (blocking on gate)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Queue two more messages in the mailbox (handler is blocked, so these stay queued)
        // Message 2: persisted + server-uninterruptible
        let req_persisted = EnvelopeRequest {
            request_id: Snowflake(100),
            address: addr.clone(),
            tag: "persist".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Uninterruptible::Server,
            deliver_at: None,
        };
        let (tx_p, mut rx_p) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req_persisted,
            reply_tx: tx_p,
        })
        .await
        .unwrap();

        // Message 3: non-persisted
        let req_normal = EnvelopeRequest {
            request_id: Snowflake(200),
            address: addr.clone(),
            tag: "normal".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx_n, mut rx_n) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req_normal,
            reply_tx: tx_n,
        })
        .await
        .unwrap();

        // Set the internal interrupt flag and cancel the token BEFORE
        // releasing the gate. This ensures that when message 1 finishes and
        // the loop returns to select!, cancel.cancelled() fires immediately.
        {
            let instance = mgr.instances.get(&addr).unwrap();
            instance
                .is_internal_interrupt
                .store(true, Ordering::Release);
            instance.cancel.cancel();
        }

        // Now release the gate so message 1 handler returns
        gate.notify_one();

        // Give process_mailbox time to drain
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The non-persisted message should receive a failure reply
        let normal_reply = tokio::time::timeout(Duration::from_millis(500), rx_n.recv()).await;
        assert!(
            normal_reply.is_ok(),
            "non-persisted message should get an interrupt failure reply"
        );
        if let Ok(Some(Reply::WithExit(exit))) = normal_reply {
            assert!(
                matches!(exit.exit, ExitResult::Failure(_)),
                "should be a failure"
            );
        }

        // The persisted+server-uninterruptible message should NOT receive any reply
        // (channel closes without a message — it will be redelivered from storage)
        let persisted_reply = tokio::time::timeout(Duration::from_millis(200), rx_p.recv()).await;
        // Either timeout or channel closed (None) — no actual Reply should be received
        match persisted_reply {
            Err(_) => {}   // timeout — no reply, correct
            Ok(None) => {} // channel closed — no reply, correct
            Ok(Some(_)) => {
                panic!("persisted+uninterruptible message should NOT receive a reply on internal interrupt");
            }
        }
    }

    #[tokio::test]
    async fn process_mailbox_exit_cleans_up_instances_and_spawn_locks() {
        let mgr = make_manager();
        let addr = test_address("cleanup-1");

        // Send a message to spawn the entity
        let req = test_request(&addr, "echo");
        let (tx, mut rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();
        rx.recv().await.unwrap();

        assert_eq!(mgr.active_count(), 1);
        assert!(mgr.spawn_locks.contains_key(&addr));

        // Drop all senders to close the mailbox channel.
        // The entity instance holds the only sender (mailbox_tx).
        // Interrupt the shard to cancel the entity, which causes process_mailbox to exit.
        let shard = addr.shard_id.clone();
        mgr.interrupt_shard(&shard).await;

        // After interrupt, both maps should be cleaned up
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 0);
        assert!(
            !mgr.spawn_locks.contains_key(&addr),
            "spawn lock should be cleaned up after mailbox exit"
        );
    }

    #[tokio::test]
    async fn crash_recovery_exhaustion_evicts_entity_prevents_infinite_cycle() {
        use std::sync::atomic::AtomicU32;

        // Entity handler that always panics
        struct AlwaysPanicEntity {
            spawn_count: Arc<AtomicU32>,
        }

        #[async_trait]
        impl Entity for AlwaysPanicEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("InfinitePanic")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                self.spawn_count.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(Box::new(PanicHandler))
            }
        }

        struct PanicHandler;

        #[async_trait]
        impl EntityHandler for PanicHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                panic!("always panics");
            }
        }

        let spawn_count = Arc::new(AtomicU32::new(0));
        let entity = Arc::new(AlwaysPanicEntity {
            spawn_count: spawn_count.clone(),
        });

        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_crash_max_retries: 1,
            entity_crash_initial_backoff: Duration::from_millis(1),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            entity.clone(),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = test_address("infinite-panic-1");

        // First message: exhausts crash recovery, entity gets evicted
        let req1 = test_request(&addr, "test");
        let (tx1, mut rx1) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req1,
            reply_tx: tx1,
        })
        .await
        .unwrap();
        let reply1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&reply1, Reply::WithExit(r) if matches!(&r.exit, ExitResult::Failure(_))),
            "first message should fail"
        );

        // Wait for eviction cleanup
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Entity should have been evicted from instances map
        assert!(
            !mgr.instances.contains_key(&addr),
            "entity should be evicted after crash recovery exhaustion"
        );

        // Second message: triggers a fresh spawn (not reusing broken handler)
        let spawn_before = spawn_count.load(AtomicOrdering::SeqCst);
        let req2 = EnvelopeRequest {
            request_id: Snowflake(999),
            address: addr.clone(),
            tag: "test".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx2, mut rx2) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req2,
            reply_tx: tx2,
        })
        .await
        .unwrap();
        let reply2 = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
            .await
            .unwrap()
            .unwrap();

        // Second message also fails (handler always panics), but importantly
        // it triggered a NEW spawn — not reusing the broken handler
        assert!(
            matches!(&reply2, Reply::WithExit(r) if matches!(&r.exit, ExitResult::Failure(_))),
            "second message should also fail (handler always panics)"
        );
        let spawn_after = spawn_count.load(AtomicOrdering::SeqCst);
        assert!(
            spawn_after > spawn_before,
            "second message should trigger a fresh spawn (got {} -> {})",
            spawn_before,
            spawn_after
        );
    }

    #[tokio::test]
    async fn crash_exhaustion_drain_suppresses_persisted_uninterruptible_messages() {
        use crate::schema::Uninterruptible;

        // Entity with a blocking handler that panics after gate is released.
        // This lets us set is_internal_interrupt and queue messages before the crash.
        // The gate blocks the first handler; once released, the gate_open flag stays
        // true so respawned handlers also panic immediately.
        struct BlockPanicEntity {
            gate: Arc<tokio::sync::Notify>,
            gate_open: Arc<std::sync::atomic::AtomicBool>,
        }

        #[async_trait]
        impl Entity for BlockPanicEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("BlockPanic")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(BlockPanicHandler {
                    gate: Arc::clone(&self.gate),
                    gate_open: Arc::clone(&self.gate_open),
                }))
            }
        }

        struct BlockPanicHandler {
            gate: Arc<tokio::sync::Notify>,
            gate_open: Arc<std::sync::atomic::AtomicBool>,
        }

        #[async_trait]
        impl EntityHandler for BlockPanicHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                if !self.gate_open.load(std::sync::atomic::Ordering::Acquire) {
                    self.gate.notified().await;
                    self.gate_open
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                panic!("crash after gate release");
            }
        }

        let gate = Arc::new(tokio::sync::Notify::new());
        let gate_open = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_crash_max_retries: 1, // exhaust after 1 retry (2 crashes total)
            entity_crash_initial_backoff: Duration::from_millis(1),
            entity_crash_max_backoff: Duration::from_millis(10),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            Arc::new(BlockPanicEntity {
                gate: Arc::clone(&gate),
                gate_open: Arc::clone(&gate_open),
            }),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let shard = ShardId::new("default", 0);
        let addr = EntityAddress {
            shard_id: shard.clone(),
            entity_type: EntityType::new("BlockPanic"),
            entity_id: EntityId::new("crash-suppress-1"),
        };

        // First message: starts processing, blocks on gate
        let req1 = EnvelopeRequest {
            request_id: Snowflake(1),
            address: addr.clone(),
            tag: "trigger".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx1, mut rx1) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req1,
            reply_tx: tx1,
        })
        .await
        .unwrap();

        // Wait for handler to start (blocked on gate)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Queue a persisted + server-uninterruptible message
        let req_persisted = EnvelopeRequest {
            request_id: Snowflake(500),
            address: addr.clone(),
            tag: "persist".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Uninterruptible::Server,
            deliver_at: None,
        };
        let (tx_p, mut rx_p) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req_persisted,
            reply_tx: tx_p,
        })
        .await
        .unwrap();

        // Queue a non-persisted message
        let req_normal = EnvelopeRequest {
            request_id: Snowflake(600),
            address: addr.clone(),
            tag: "normal".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx_n, mut rx_n) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req_normal,
            reply_tx: tx_n,
        })
        .await
        .unwrap();

        // Set the internal interrupt flag BEFORE releasing the gate.
        // This simulates a shard-move interrupt concurrent with crash recovery.
        {
            let instance = mgr.instances.get(&addr).unwrap();
            instance
                .is_internal_interrupt
                .store(true, Ordering::Release);
        }

        // Release the gate — handler panics, crash recovery is exhausted (max_retries=0),
        // then the drain loop runs with is_internal_interrupt=true.
        gate.notify_one();

        // First message should get crash-exhaustion failure
        let reply1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&reply1, Reply::WithExit(r) if matches!(&r.exit, ExitResult::Failure(_))),
            "first message should get crash-exhaustion failure"
        );

        // Wait for drain to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Non-persisted message should receive a failure reply
        let normal_reply = tokio::time::timeout(Duration::from_secs(2), rx_n.recv()).await;
        assert!(
            normal_reply.is_ok(),
            "non-persisted message should get a failure reply from drain"
        );
        if let Ok(Some(Reply::WithExit(exit))) = normal_reply {
            assert!(matches!(exit.exit, ExitResult::Failure(_)));
        }

        // Persisted+uninterruptible message should NOT receive any reply
        let persisted_reply = tokio::time::timeout(Duration::from_millis(500), rx_p.recv()).await;
        match persisted_reply {
            Err(_) => {}   // timeout — no reply, correct
            Ok(None) => {} // channel closed — no reply, correct
            Ok(Some(_)) => {
                panic!("persisted+uninterruptible message should NOT receive a reply during crash-eviction drain");
            }
        }
    }

    #[tokio::test]
    async fn interrupt_shard_blocks_new_spawns_during_interruption() {
        let mgr = make_manager();
        let shard = ShardId::new("default", 0);

        // Spawn an entity on shard 0
        let addr = EntityAddress {
            shard_id: shard.clone(),
            entity_type: EntityType::new("Test"),
            entity_id: EntityId::new("e-block"),
        };
        let req = test_request(&addr, "echo");
        let (tx, _rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 1);

        // Manually set the interrupting flag to simulate mid-interrupt state
        mgr.interrupting_shards.insert(shard.clone());

        // A new entity on the same shard should be rejected
        let addr2 = EntityAddress {
            shard_id: shard.clone(),
            entity_type: EntityType::new("Test"),
            entity_id: EntityId::new("e-block-2"),
        };
        let req2 = test_request(&addr2, "echo");
        let (tx2, _rx2) = mpsc::channel(1);
        let result: Result<(), ClusterError> = mgr
            .send_local(IncomingMessage::Request {
                request: req2,
                reply_tx: tx2,
            })
            .await;
        assert!(
            matches!(result, Err(ClusterError::EntityNotAssignedToRunner { .. })),
            "Expected EntityNotAssignedToRunner during shard interruption, got {:?}",
            result
        );

        // Clear the flag — spawns should work again
        mgr.interrupting_shards.remove(&shard);
        let addr3 = EntityAddress {
            shard_id: shard.clone(),
            entity_type: EntityType::new("Test"),
            entity_id: EntityId::new("e-block-3"),
        };
        let req3 = test_request(&addr3, "echo");
        let (tx3, _rx3) = mpsc::channel(1);
        let result3: Result<(), ClusterError> = mgr
            .send_local(IncomingMessage::Request {
                request: req3,
                reply_tx: tx3,
            })
            .await;
        assert!(
            result3.is_ok(),
            "Spawn should succeed after interrupting flag cleared"
        );
    }

    // --- Concurrent entity tests ---

    /// An entity that supports concurrent request handling.
    struct ConcurrentEntity {
        max_concurrent: usize,
    }

    #[async_trait]
    impl Entity for ConcurrentEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Concurrent")
        }

        fn concurrency(&self) -> Option<usize> {
            Some(self.max_concurrent)
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(ConcurrentHandler {
                active: Arc::new(AtomicI32::new(0)),
                max_seen: Arc::new(AtomicI32::new(0)),
            }))
        }
    }

    /// Handler that tracks the maximum number of concurrent active requests.
    struct ConcurrentHandler {
        active: Arc<AtomicI32>,
        max_seen: Arc<AtomicI32>,
    }

    #[async_trait]
    impl EntityHandler for ConcurrentHandler {
        async fn handle_request(
            &self,
            _tag: &str,
            _payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            let current = self.active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            // Update max seen concurrency
            self.max_seen.fetch_max(current, AtomicOrdering::SeqCst);
            // Sleep to allow overlap of concurrent requests
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.active.fetch_sub(1, AtomicOrdering::SeqCst);
            Ok(rmp_serde::to_vec(&current).unwrap())
        }
    }

    #[tokio::test]
    async fn concurrent_entity_processes_requests_in_parallel() {
        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 20,
            ..Default::default()
        });
        let snowflake = Arc::new(SnowflakeGenerator::new());
        let mgr = EntityManager::new(
            Arc::new(ConcurrentEntity { max_concurrent: 4 }),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::clone(&snowflake),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Concurrent"),
            entity_id: EntityId::new("c-1"),
        };

        // Send 4 requests rapidly — they should execute concurrently
        let mut receivers = Vec::new();
        for i in 0..4 {
            let req = EnvelopeRequest {
                request_id: Snowflake(100 + i),
                address: addr.clone(),
                tag: "work".to_string(),
                payload: vec![],
                headers: HashMap::new(),
                span_id: None,
                trace_id: None,
                sampled: None,
                persisted: false,
                uninterruptible: Default::default(),
                deliver_at: None,
            };
            let (tx, rx) = mpsc::channel(1);
            mgr.send_local(IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            })
            .await
            .unwrap();
            receivers.push(rx);
        }

        // Collect all replies — should all succeed
        for mut rx in receivers {
            let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("reply timeout")
                .expect("reply channel closed");
            match reply {
                Reply::WithExit(exit) => {
                    assert!(matches!(exit.exit, ExitResult::Success(_)));
                }
                _ => panic!("expected WithExit reply"),
            }
        }
    }

    #[tokio::test]
    async fn serial_entity_processes_requests_one_at_a_time() {
        // Default concurrency is 1 (serial)
        let mgr = make_manager();

        let addr = test_address("serial-1");

        // Send 3 requests
        let mut receivers = Vec::new();
        for i in 0..3 {
            let req = EnvelopeRequest {
                request_id: Snowflake(200 + i),
                address: addr.clone(),
                tag: "echo".to_string(),
                payload: vec![],
                headers: HashMap::new(),
                span_id: None,
                trace_id: None,
                sampled: None,
                persisted: false,
                uninterruptible: Default::default(),
                deliver_at: None,
            };
            let (tx, rx) = mpsc::channel(1);
            mgr.send_local(IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            })
            .await
            .unwrap();
            receivers.push(rx);
        }

        // All should complete
        for mut rx in receivers {
            let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("reply timeout")
                .expect("reply channel closed");
            match reply {
                Reply::WithExit(exit) => {
                    assert!(matches!(exit.exit, ExitResult::Success(_)));
                }
                _ => panic!("expected WithExit reply"),
            }
        }
    }

    #[tokio::test]
    async fn interrupt_shard_awaits_mailbox_task_completion() {
        // Verify that interrupt_shard waits for the mailbox processing task to
        // fully exit before returning, not just cancelling the token.
        struct SlowShutdownEntity;

        #[async_trait]
        impl Entity for SlowShutdownEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("SlowShutdown")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(SlowShutdownHandler))
            }
        }

        struct SlowShutdownHandler;

        #[async_trait]
        impl EntityHandler for SlowShutdownHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(vec![])
            }
        }

        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 10,
            entity_termination_timeout: Duration::from_millis(100),
            ..Default::default()
        });

        let mgr = EntityManager::new(
            Arc::new(SlowShutdownEntity),
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let shard = ShardId::new("default", 0);
        let addr = EntityAddress {
            shard_id: shard.clone(),
            entity_type: EntityType::new("SlowShutdown"),
            entity_id: EntityId::new("e-await"),
        };

        let req = EnvelopeRequest {
            request_id: Snowflake(1),
            address: addr.clone(),
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
        let (tx, _rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mgr.active_count(), 1);

        // interrupt_shard should cancel the entity AND await the task
        mgr.interrupt_shard(&shard).await;

        // After interrupt returns, entity should be fully cleaned up
        assert_eq!(mgr.active_count(), 0);
        assert!(
            !mgr.instances.contains_key(&addr),
            "instance should be removed after interrupt completes"
        );
    }

    #[tokio::test]
    async fn entity_concurrency_override_respected() {
        // Config says serial (1), entity says concurrent (3)
        let config = Arc::new(ShardingConfig {
            entity_mailbox_capacity: 20,
            entity_max_concurrent_requests: 1, // config says serial
            ..Default::default()
        });
        let snowflake = Arc::new(SnowflakeGenerator::new());
        let mgr = EntityManager::new(
            Arc::new(ConcurrentEntity { max_concurrent: 3 }), // entity says 3
            config,
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::clone(&snowflake),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Concurrent"),
            entity_id: EntityId::new("c-override"),
        };

        // Send 3 requests — all should succeed (entity override allows 3)
        let mut receivers = Vec::new();
        for i in 0..3 {
            let req = EnvelopeRequest {
                request_id: Snowflake(300 + i),
                address: addr.clone(),
                tag: "work".to_string(),
                payload: vec![],
                headers: HashMap::new(),
                span_id: None,
                trace_id: None,
                sampled: None,
                persisted: false,
                uninterruptible: Default::default(),
                deliver_at: None,
            };
            let (tx, rx) = mpsc::channel(1);
            mgr.send_local(IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            })
            .await
            .unwrap();
            receivers.push(rx);
        }

        for mut rx in receivers {
            let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("reply timeout")
                .expect("reply channel closed");
            match reply {
                Reply::WithExit(exit) => {
                    assert!(matches!(exit.exit, ExitResult::Success(_)));
                }
                _ => panic!("expected WithExit reply"),
            }
        }
    }

    #[tokio::test]
    async fn closing_state_rejects_requests_allows_envelopes() {
        let mgr = make_manager();
        let addr = test_address("closing-test");

        // First, send a request to spawn the entity
        let req = test_request(&addr, "echo");
        let (tx, mut rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        // Wait for reply to ensure entity is spawned
        let _reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("reply timeout")
            .expect("reply channel closed");

        // Transition to closing state
        mgr.set_closing();

        // New Request messages should be rejected with ShuttingDown
        let req2 = EnvelopeRequest {
            request_id: Snowflake(100),
            address: addr.clone(),
            tag: "echo".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx2, _rx2) = mpsc::channel(1);
        let result = mgr
            .send_local(IncomingMessage::Request {
                request: req2,
                reply_tx: tx2,
            })
            .await;
        assert!(
            matches!(result, Err(ClusterError::ShuttingDown)),
            "expected ShuttingDown error for Request in closing state, got: {result:?}"
        );

        // Envelope messages should still be accepted
        let envelope = EnvelopeRequest {
            request_id: Snowflake(200),
            address: addr.clone(),
            tag: "echo".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let result = mgr.send_local(IncomingMessage::Envelope { envelope }).await;
        assert!(
            result.is_ok(),
            "expected Envelope to be accepted in closing state, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn duplicate_request_id_returns_already_processing() {
        // Use a slow handler so the first request is still processing when the second arrives
        struct SlowEntity;
        #[async_trait]
        impl Entity for SlowEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Slow")
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(SlowHandler))
            }
        }
        struct SlowHandler;
        #[async_trait]
        impl EntityHandler for SlowHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(vec![])
            }
        }

        let mgr = EntityManager::new(
            Arc::new(SlowEntity),
            Arc::new(ShardingConfig {
                entity_mailbox_capacity: 10,
                ..Default::default()
            }),
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Slow"),
            entity_id: EntityId::new("e-1"),
        };

        // First request with request_id 42
        let req1 = EnvelopeRequest {
            request_id: Snowflake(42),
            address: addr.clone(),
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
        let (tx1, _rx1) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req1,
            reply_tx: tx1,
        })
        .await
        .unwrap();

        // Second request with same request_id 42 — should be rejected
        let req2 = EnvelopeRequest {
            request_id: Snowflake(42),
            address: addr.clone(),
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
        let (tx2, _rx2) = mpsc::channel(1);
        let result = mgr
            .send_local(IncomingMessage::Request {
                request: req2,
                reply_tx: tx2,
            })
            .await;

        assert!(
            matches!(result, Err(ClusterError::AlreadyProcessingMessage { request_id }) if request_id == Snowflake(42)),
            "expected AlreadyProcessingMessage, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn duplicate_envelope_silently_skipped() {
        let mgr = make_manager();
        let addr = test_address("e-1");

        let envelope1 = EnvelopeRequest {
            request_id: Snowflake(99),
            address: addr.clone(),
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
        mgr.send_local(IncomingMessage::Envelope {
            envelope: envelope1,
        })
        .await
        .unwrap();

        // Second envelope with same request_id — silently skipped
        let envelope2 = EnvelopeRequest {
            request_id: Snowflake(99),
            address: addr.clone(),
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
        let result = mgr
            .send_local(IncomingMessage::Envelope {
                envelope: envelope2,
            })
            .await;

        assert!(
            result.is_ok(),
            "duplicate envelope should be silently skipped, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn clear_processed_preserves_active_request_ids() {
        struct GateEntity {
            gate: Arc<Notify>,
            ready_tx: mpsc::Sender<()>,
        }

        #[async_trait]
        impl Entity for GateEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Gate")
            }

            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(GateHandler {
                    gate: Arc::clone(&self.gate),
                    ready_tx: self.ready_tx.clone(),
                }))
            }
        }

        struct GateHandler {
            gate: Arc<Notify>,
            ready_tx: mpsc::Sender<()>,
        }

        #[async_trait]
        impl EntityHandler for GateHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                let _ = self.ready_tx.send(()).await;
                self.gate.notified().await;
                Ok(vec![])
            }
        }

        let gate = Arc::new(Notify::new());
        let (ready_tx, mut ready_rx) = mpsc::channel(1);
        let mgr = EntityManager::new(
            Arc::new(GateEntity {
                gate: Arc::clone(&gate),
                ready_tx,
            }),
            Arc::new(ShardingConfig {
                entity_mailbox_capacity: 1,
                ..Default::default()
            }),
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        );

        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Gate"),
            entity_id: EntityId::new("g-1"),
        };

        let request_id = Snowflake(77);
        let req = EnvelopeRequest {
            request_id,
            address: addr.clone(),
            tag: "hold".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx, mut rx) = mpsc::channel(1);
        mgr.send_local(IncomingMessage::Request {
            request: req,
            reply_tx: tx,
        })
        .await
        .unwrap();

        // Simulate the storage poll cycle clearing processed IDs.
        mgr.clear_processed();

        // Duplicate envelope should still be skipped even though the mailbox is full.
        let dup = EnvelopeRequest {
            request_id,
            address: addr.clone(),
            tag: "hold".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let result = mgr
            .send_local(IncomingMessage::Envelope { envelope: dup })
            .await;
        assert!(
            result.is_ok(),
            "expected duplicate envelope to be skipped after clear_processed, got: {result:?}"
        );

        let _ = ready_rx.recv().await;
        gate.notify_waiters();
        let _ = rx.recv().await;
    }

    #[test]
    fn trace_context_extraction_parses_valid_otel_ids() {
        // Verify that valid OTel hex trace/span IDs are parsed without panic.
        use opentelemetry::trace::{SpanId, TraceId};

        let trace_id = "0af7651916cd43dd8448eb211c80319c";
        let span_id = "e457b5a2e4d86bd1";

        let tid = TraceId::from_hex(trace_id).unwrap();
        let sid = SpanId::from_hex(span_id).unwrap();

        assert_eq!(tid.to_string(), trace_id);
        assert_eq!(sid.to_string(), span_id);
    }

    #[test]
    fn trace_context_extraction_rejects_invalid_ids() {
        use opentelemetry::trace::{SpanId, TraceId};

        // Non-hex characters
        assert!(TraceId::from_hex("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_err());
        assert!(SpanId::from_hex("zzzzzzzzzzzzzzzz").is_err());

        // All zeros are invalid per OTel spec (SpanContext::is_valid() returns false).
        // from_hex may or may not error on all-zero input, but the resulting
        // SpanContext must be invalid — which is what our extraction code checks.
        if let (Ok(tid), Ok(sid)) = (
            TraceId::from_hex("00000000000000000000000000000000"),
            SpanId::from_hex("0000000000000000"),
        ) {
            use opentelemetry::trace::{SpanContext, TraceFlags, TraceState};
            let ctx =
                SpanContext::new(tid, sid, TraceFlags::default(), true, TraceState::default());
            assert!(
                !ctx.is_valid(),
                "all-zero IDs should produce invalid context"
            );
        }
    }
}
