//! Internal durable helpers for entity workflows.
//!
//! This module exposes `DurableContext` so entity methods annotated with
//! `#[workflow]` can call `sleep`, `await_deferred`, `resolve_deferred`, and `on_interrupt`.
//!
//! It also provides `MemoryWorkflowEngine` and `MemoryWorkflowStorage` for testing
//! entities that use durable workflows.

use crate::entity_client::persisted_request_id;
use crate::envelope::EnvelopeRequest;
use crate::error::ClusterError;
use crate::message_storage::{MessageStorage, SaveResult};
use crate::reply::ExitResult;
use crate::types::{EntityAddress, EntityId, EntityType, ShardId};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Deferred key name used for interrupt signals.
pub const INTERRUPT_SIGNAL: &str = "Workflow/InterruptSignal";

// ── WorkflowScope ──────────────────────────────────────────────────────
// Task-local that carries the current workflow execution's request ID.
// Set by macro-generated dispatch code for `#[workflow]` methods so that
// activity journal keys are scoped per workflow execution.

tokio::task_local! {
    static WORKFLOW_REQUEST_ID: i64;
    static WORKFLOW_JOURNAL_KEYS: std::cell::RefCell<Vec<String>>;
}

/// Scope that carries the current workflow execution's request ID.
///
/// Used by the macro-generated dispatch code to make activity journal keys
/// unique per workflow execution instead of globally per entity.
pub struct WorkflowScope;

impl WorkflowScope {
    /// Execute `f` with the given workflow request ID in scope.
    ///
    /// Also sets up a journal key collector so that activity journal keys
    /// written during this workflow execution can be marked as completed
    /// when the workflow finishes. Returns `(result, journal_keys)`.
    pub async fn run<F, Fut, T>(request_id: i64, f: F) -> (T, Vec<String>)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        WORKFLOW_REQUEST_ID
            .scope(
                request_id,
                WORKFLOW_JOURNAL_KEYS.scope(std::cell::RefCell::new(Vec::new()), async {
                    let result = f().await;
                    let keys =
                        WORKFLOW_JOURNAL_KEYS.with(|keys| keys.borrow_mut().drain(..).collect());
                    (result, keys)
                }),
            )
            .await
    }

    /// Get the current workflow request ID, if inside a `WorkflowScope`.
    pub fn current() -> Option<i64> {
        WORKFLOW_REQUEST_ID.try_with(|id| *id).ok()
    }

    /// Register a journal key written during this workflow execution.
    ///
    /// Called by `DurableContext` when a journal entry is written so that
    /// all keys can be marked as completed when the workflow finishes.
    pub fn register_journal_key(key: String) {
        let _ = WORKFLOW_JOURNAL_KEYS.try_with(|keys| {
            keys.borrow_mut().push(key);
        });
    }
}

/// Persistent key-value storage for durable state.
///
/// Used by entity macros to persist state across restarts.
#[async_trait]
pub trait WorkflowStorage: Send + Sync {
    /// Load a value by key.
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>, ClusterError>;

    /// Save a value by key.
    async fn save(&self, key: &str, value: &[u8]) -> Result<(), ClusterError>;

    /// Delete a value by key.
    async fn delete(&self, key: &str) -> Result<(), ClusterError>;

    /// List all keys with the given prefix.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, ClusterError>;

    /// Mark a key as completed (sets `completed_at` timestamp).
    async fn mark_completed(&self, key: &str) -> Result<(), ClusterError>;

    /// Delete all entries where `completed_at` is older than the given duration.
    async fn cleanup(&self, older_than: Duration) -> Result<u64, ClusterError>;

    /// Begin a new transaction.
    ///
    /// Returns a transaction handle that can be used to batch operations.
    /// The transaction is committed when `commit()` is called, or rolled back
    /// when dropped without committing.
    ///
    /// Default implementation returns a no-op transaction that commits immediately.
    async fn begin_transaction(&self) -> Result<Box<dyn StorageTransaction>, ClusterError> {
        Ok(Box::new(NoopTransaction {
            storage: self.as_arc(),
        }))
    }

    /// Get self as an Arc for use in transactions.
    ///
    /// This is used by the default `begin_transaction` implementation.
    /// Implementations that provide real transactions can return a dummy value.
    fn as_arc(&self) -> Arc<dyn WorkflowStorage> {
        panic!("WorkflowStorage::as_arc() must be implemented for default begin_transaction()")
    }

    /// Get the underlying SQL connection pool, if this is a SQL-backed storage.
    ///
    /// Returns `Some(&PgPool)` for `SqlWorkflowStorage`, `None` for others.
    /// Used by the framework to open transactions for activity execution
    /// and to provide `self.db` in activity views.
    fn sql_pool(&self) -> Option<&sqlx::PgPool> {
        None
    }
}

/// A transaction for batching storage operations.
///
/// Operations performed on a transaction are not visible until `commit()` is called.
/// If the transaction is dropped without calling `commit()`, all operations are rolled back.
#[async_trait]
pub trait StorageTransaction: Send + Sync {
    /// Save a value by key within the transaction.
    async fn save(&mut self, key: &str, value: &[u8]) -> Result<(), ClusterError>;

    /// Delete a value by key within the transaction.
    async fn delete(&mut self, key: &str) -> Result<(), ClusterError>;

    /// Commit the transaction, making all operations permanent.
    async fn commit(self: Box<Self>) -> Result<(), ClusterError>;

    /// Rollback the transaction, discarding all operations.
    async fn rollback(self: Box<Self>) -> Result<(), ClusterError>;

    /// Returns self as `Any` for downcasting to concrete transaction types.
    ///
    /// This enables activities to access the underlying database transaction
    /// (e.g., `sqlx::Transaction<Postgres>`) for executing arbitrary SQL
    /// within the same transaction as state changes.
    ///
    /// # Example
    ///
    /// ```text
    /// if let Some(sql_tx) = tx.as_any_mut().downcast_mut::<SqlTransaction>() {
    ///     sql_tx.execute(sqlx::query("INSERT INTO ...")).await?;
    /// }
    /// ```
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// A no-op transaction that commits operations immediately.
///
/// Used as the default implementation for storage backends that don't support transactions.
struct NoopTransaction {
    storage: Arc<dyn WorkflowStorage>,
}

#[async_trait]
impl StorageTransaction for NoopTransaction {
    async fn save(&mut self, key: &str, value: &[u8]) -> Result<(), ClusterError> {
        self.storage.save(key, value).await
    }

    async fn delete(&mut self, key: &str) -> Result<(), ClusterError> {
        self.storage.delete(key).await
    }

    async fn commit(self: Box<Self>) -> Result<(), ClusterError> {
        // No-op, operations were already applied
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), ClusterError> {
        // No-op, can't rollback immediate operations
        // This is a limitation of the no-op transaction
        Ok(())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// In-memory workflow storage for testing.
///
/// This storage keeps all data in memory and is not durable across restarts.
/// Use [`SqlWorkflowStorage`](crate::storage::sql_workflow::SqlWorkflowStorage) for
/// production persistence (requires the `sql` feature).
///
/// The storage uses `Arc` internally so clones share the same underlying data.
/// This is important for transactions to work correctly.
#[derive(Clone)]
pub struct MemoryWorkflowStorage {
    entries: Arc<DashMap<String, Vec<u8>>>,
    completed_at: Arc<DashMap<String, std::time::Instant>>,
}

impl MemoryWorkflowStorage {
    /// Create a new in-memory workflow storage.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            completed_at: Arc::new(DashMap::new()),
        }
    }
}

impl Default for MemoryWorkflowStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WorkflowStorage for MemoryWorkflowStorage {
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>, ClusterError> {
        Ok(self.entries.get(key).map(|v| v.value().clone()))
    }

    async fn save(&self, key: &str, value: &[u8]) -> Result<(), ClusterError> {
        self.entries.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), ClusterError> {
        self.entries.remove(key);
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, ClusterError> {
        Ok(self
            .entries
            .iter()
            .filter(|e| e.key().starts_with(prefix))
            .map(|e| e.key().clone())
            .collect())
    }

    async fn mark_completed(&self, key: &str) -> Result<(), ClusterError> {
        self.completed_at
            .insert(key.to_string(), std::time::Instant::now());
        Ok(())
    }

    async fn cleanup(&self, older_than: Duration) -> Result<u64, ClusterError> {
        let cutoff = std::time::Instant::now() - older_than;
        let mut deleted = 0u64;
        let expired_keys: Vec<String> = self
            .completed_at
            .iter()
            .filter(|e| *e.value() < cutoff)
            .map(|e| e.key().clone())
            .collect();
        for key in expired_keys {
            self.entries.remove(&key);
            self.completed_at.remove(&key);
            deleted += 1;
        }
        Ok(deleted)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn StorageTransaction>, ClusterError> {
        Ok(Box::new(MemoryTransaction {
            storage: Arc::new(self.clone()),
            pending_saves: Vec::new(),
            pending_deletes: Vec::new(),
        }))
    }

    fn as_arc(&self) -> Arc<dyn WorkflowStorage> {
        Arc::new(self.clone())
    }
}

/// A transaction for `MemoryWorkflowStorage`.
///
/// Buffers all operations and applies them atomically on commit.
struct MemoryTransaction {
    storage: Arc<MemoryWorkflowStorage>,
    pending_saves: Vec<(String, Vec<u8>)>,
    pending_deletes: Vec<String>,
}

#[async_trait]
impl StorageTransaction for MemoryTransaction {
    async fn save(&mut self, key: &str, value: &[u8]) -> Result<(), ClusterError> {
        // Remove any pending delete for this key
        self.pending_deletes.retain(|k| k != key);
        // Add or update the pending save
        if let Some(pos) = self.pending_saves.iter().position(|(k, _)| k == key) {
            self.pending_saves[pos].1 = value.to_vec();
        } else {
            self.pending_saves.push((key.to_string(), value.to_vec()));
        }
        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<(), ClusterError> {
        // Remove any pending save for this key
        self.pending_saves.retain(|(k, _)| k != key);
        // Add to pending deletes if not already there
        if !self.pending_deletes.contains(&key.to_string()) {
            self.pending_deletes.push(key.to_string());
        }
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<(), ClusterError> {
        // Apply all pending deletes
        for key in &self.pending_deletes {
            self.storage.entries.remove(key);
        }
        // Apply all pending saves
        for (key, value) in self.pending_saves {
            self.storage.entries.insert(key, value);
        }
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), ClusterError> {
        // Just drop the pending operations
        Ok(())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// A typed, compile-time key for deferred signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeferredKey<T> {
    pub name: &'static str,
    _marker: PhantomData<T>,
}

impl<T> DeferredKey<T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _marker: PhantomData,
        }
    }
}

/// A key-like value that can name a deferred signal.
pub trait DeferredKeyLike<T> {
    fn name(&self) -> &str;
}

impl<T> DeferredKeyLike<T> for DeferredKey<T> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<T> DeferredKeyLike<T> for &DeferredKey<T> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<T> DeferredKeyLike<T> for &str {
    fn name(&self) -> &str {
        self
    }
}

impl<T> DeferredKeyLike<T> for String {
    fn name(&self) -> &str {
        self.as_str()
    }
}

impl<T> DeferredKeyLike<T> for &String {
    fn name(&self) -> &str {
        self.as_str()
    }
}

/// Context for durable operations within entity handler methods.
///
/// `DurableContext` provides durable capabilities (`sleep`, `await_deferred`, `resolve_deferred`)
/// that can be used inside entity methods marked with `#[workflow]`.
///
/// When `message_storage` is present, also provides `run()` for activity journaling:
/// activity results are cached in `MessageStorage` so that on crash-recovery replay
/// the cached result is returned instead of re-executing the activity body.
pub struct DurableContext {
    engine: Arc<dyn WorkflowEngine>,
    workflow_name: String,
    execution_id: String,
    /// Optional message storage for activity journal duplicate detection.
    message_storage: Option<Arc<dyn MessageStorage>>,
    /// Optional workflow storage for loading journal results.
    /// Journal results are stored here (not in MessageStorage) so they can be
    /// committed atomically with state changes in the ActivityScope transaction.
    workflow_storage: Option<Arc<dyn WorkflowStorage>>,
    /// Entity type for building deterministic journal keys.
    entity_type: EntityType,
    /// Entity ID for building deterministic journal keys.
    entity_id: EntityId,
}

impl DurableContext {
    /// Create a new `DurableContext` for use within an entity handler.
    pub fn new(
        engine: Arc<dyn WorkflowEngine>,
        workflow_name: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Self {
        let workflow_name = workflow_name.into();
        let execution_id = execution_id.into();
        Self {
            engine,
            entity_type: EntityType::new(&workflow_name),
            entity_id: EntityId::new(&execution_id),
            workflow_name,
            execution_id,
            message_storage: None,
            workflow_storage: None,
        }
    }

    /// Create a new `DurableContext` with message storage for activity journaling.
    ///
    /// The `message_storage` is used for duplicate detection (save_request).
    /// The `workflow_storage` is used for loading cached journal results.
    /// Journal results are written to WorkflowStorage via `ActivityScope::buffer_write`
    /// to ensure atomicity with state changes.
    pub fn with_journal_storage(
        engine: Arc<dyn WorkflowEngine>,
        workflow_name: impl Into<String>,
        execution_id: impl Into<String>,
        message_storage: Arc<dyn MessageStorage>,
        workflow_storage: Arc<dyn WorkflowStorage>,
    ) -> Self {
        let workflow_name = workflow_name.into();
        let execution_id = execution_id.into();
        Self {
            engine,
            entity_type: EntityType::new(&workflow_name),
            entity_id: EntityId::new(&execution_id),
            workflow_name,
            execution_id,
            message_storage: Some(message_storage),
            workflow_storage: Some(workflow_storage),
        }
    }

    /// Durable sleep that survives restarts.
    pub async fn sleep(&self, name: &str, duration: Duration) -> Result<(), ClusterError> {
        self.engine
            .sleep(&self.workflow_name, &self.execution_id, name, duration)
            .await
    }

    /// Wait for an external signal to resolve a typed value.
    pub async fn await_deferred<T, K>(&self, key: K) -> Result<T, ClusterError>
    where
        T: Serialize + DeserializeOwned,
        K: DeferredKeyLike<T>,
    {
        let name = key.name().to_string();
        let bytes = self
            .engine
            .await_deferred(&self.workflow_name, &self.execution_id, &name)
            .await?;
        rmp_serde::from_slice(&bytes).map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to deserialize deferred '{name}': {e}"),
            source: Some(Box::new(e)),
        })
    }

    /// Resolve a deferred value, resuming any entity method waiting on it.
    pub async fn resolve_deferred<T, K>(&self, key: K, value: &T) -> Result<(), ClusterError>
    where
        T: Serialize,
        K: DeferredKeyLike<T>,
    {
        let name = key.name().to_string();
        let bytes = rmp_serde::to_vec(value).map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to serialize deferred value: {e}"),
            source: Some(Box::new(e)),
        })?;
        self.engine
            .resolve_deferred(&self.workflow_name, &self.execution_id, &name, bytes)
            .await
    }

    /// Wait for an interrupt signal.
    pub async fn on_interrupt(&self) -> Result<(), ClusterError> {
        self.engine
            .on_interrupt(&self.workflow_name, &self.execution_id)
            .await
    }

    /// Check the journal for a cached activity result.
    ///
    /// This performs the duplicate-detection check against `MessageStorage` and
    /// looks up the cached result in `WorkflowStorage`.
    ///
    /// Returns `Ok(Some(T))` if a cached result exists (replay hit),
    /// `Ok(None)` if this is a first execution or re-execution after crash,
    /// or if no `MessageStorage` is configured (backward-compatible mode).
    pub async fn check_journal<T: DeserializeOwned>(
        &self,
        name: &str,
        key_bytes: &[u8],
    ) -> Result<Option<T>, ClusterError> {
        let msg_storage = match &self.message_storage {
            Some(s) => s,
            None => return Ok(None), // No journal — caller should execute directly
        };

        let journal_tag = format!("__journal/{name}");
        let request_id =
            persisted_request_id(&self.entity_type, &self.entity_id, &journal_tag, key_bytes);

        // Build an envelope for duplicate detection
        let envelope = EnvelopeRequest {
            request_id,
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: self.entity_type.clone(),
                entity_id: self.entity_id.clone(),
            },
            tag: journal_tag,
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        match msg_storage.save_request(&envelope).await? {
            SaveResult::Duplicate { .. } => {
                // Duplicate request — check WorkflowStorage for the cached result.
                // The result lives in WorkflowStorage (not MessageStorage) because
                // it is written atomically with state changes in the ActivityScope
                // transaction.
                if let Some(wf_storage) = &self.workflow_storage {
                    let storage_key = Self::journal_storage_key(
                        name,
                        key_bytes,
                        &self.entity_type,
                        &self.entity_id,
                    );
                    if let Some(bytes) = wf_storage.load(&storage_key).await? {
                        // Register key for completion even on replay hits
                        WorkflowScope::register_journal_key(storage_key);
                        let result: T = Self::deserialize_journal_result(&bytes)?;
                        return Ok(Some(result));
                    }
                }
                // Duplicate request but no stored result — crash happened after
                // save_request but before the ActivityScope committed. Re-execute.
                Ok(None)
            }
            SaveResult::Success => {
                // First execution
                Ok(None)
            }
        }
    }

    /// Compute the WorkflowStorage key for a journal entry.
    ///
    /// Journal results are stored in WorkflowStorage (same transaction as state)
    /// under a deterministic key derived from the activity identity.
    pub fn journal_storage_key(
        name: &str,
        key_bytes: &[u8],
        entity_type: &EntityType,
        entity_id: &EntityId,
    ) -> String {
        let journal_tag = format!("__journal/{name}");
        let request_id = persisted_request_id(entity_type, entity_id, &journal_tag, key_bytes);
        format!("__journal/{}", request_id.0)
    }

    /// Serialize an activity result for journal storage.
    ///
    /// The result is serialized as msgpack bytes suitable for storage in
    /// WorkflowStorage. On success, the value is serialized directly.
    /// On error, the error message is stored as an `ExitResult::Failure`.
    pub fn serialize_journal_result<T: Serialize>(
        result: &Result<T, ClusterError>,
    ) -> Result<Vec<u8>, ClusterError> {
        let exit = match result {
            Ok(value) => {
                let bytes =
                    rmp_serde::to_vec(value).map_err(|e| ClusterError::PersistenceError {
                        reason: format!("failed to serialize journal result: {e}"),
                        source: Some(Box::new(e)),
                    })?;
                ExitResult::Success(bytes)
            }
            Err(e) => ExitResult::Failure(e.to_string()),
        };
        rmp_serde::to_vec(&exit).map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to serialize journal exit: {e}"),
            source: Some(Box::new(e)),
        })
    }

    /// Deserialize a journal result from WorkflowStorage bytes.
    pub fn deserialize_journal_result<T: DeserializeOwned>(
        bytes: &[u8],
    ) -> Result<T, ClusterError> {
        let exit: ExitResult =
            rmp_serde::from_slice(bytes).map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to deserialize journal exit: {e}"),
                source: Some(Box::new(e)),
            })?;
        match exit {
            ExitResult::Success(data) => {
                rmp_serde::from_slice(&data).map_err(|e| ClusterError::PersistenceError {
                    reason: format!("failed to deserialize cached journal result: {e}"),
                    source: Some(Box::new(e)),
                })
            }
            ExitResult::Failure(msg) => Err(ClusterError::PersistenceError {
                reason: format!("cached journal result was a failure: {msg}"),
                source: None,
            }),
        }
    }

    /// Check if journaling is enabled (message storage is configured).
    pub fn has_journal(&self) -> bool {
        self.message_storage.is_some()
    }

    /// Get the entity type for journal key computation.
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Get the entity ID for journal key computation.
    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    /// Execute a closure with journaled result caching.
    ///
    /// On first execution, runs the closure and persists the serialized result
    /// atomically with the activity's state changes (via `ActivityScope::buffer_write`).
    /// On replay (crash recovery), returns the cached result without re-executing.
    ///
    /// **Note:** This method is used by unit tests. The macro-generated code uses
    /// `check_journal()` + `ActivityScope::buffer_write()` directly to ensure the
    /// journal write is part of the same transaction as state persistence.
    ///
    /// If no `MessageStorage` is configured, the closure is executed directly
    /// without journaling (backward-compatible fallback).
    pub async fn run<T, F, Fut>(
        &self,
        name: &str,
        key_bytes: &[u8],
        f: F,
    ) -> Result<T, ClusterError>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ClusterError>>,
    {
        // Check for cached result
        if let Some(cached) = self.check_journal::<T>(name, key_bytes).await? {
            return Ok(cached);
        }

        if self.message_storage.is_none() {
            // No journal — execute directly (backward-compatible)
            return f().await;
        }

        // First execution (or re-execution after crash) — run the closure
        let result = f().await;

        // Buffer the journal write into the active ActivityScope transaction (if any).
        // This ensures the journal entry is committed atomically with state changes.
        let storage_key =
            Self::journal_storage_key(name, key_bytes, &self.entity_type, &self.entity_id);
        let journal_bytes = Self::serialize_journal_result(&result)?;

        // Register this journal key so it can be marked completed when the workflow finishes
        WorkflowScope::register_journal_key(storage_key.clone());

        if crate::state_guard::ActivityScope::is_active() {
            // Inside an ActivityScope — buffer into the same transaction
            crate::state_guard::ActivityScope::buffer_write(storage_key, journal_bytes);
        } else if let Some(wf_storage) = &self.workflow_storage {
            // No ActivityScope but have storage — write directly to WorkflowStorage.
            // This is NOT atomic with state, but is acceptable for test scenarios
            // and activities that don't mutate state.
            wf_storage.save(&storage_key, &journal_bytes).await?;
        }

        result
    }
}

/// Compute the backoff delay for an activity retry attempt.
///
/// Used by macro-generated retry loops for `#[activity(retries = N, backoff = "...")]`.
///
/// - `"exponential"`: `min(base_secs * 2^attempt, 60)` seconds (capped at 60s)
/// - `"constant"`: `base_secs` seconds (default 1s)
///
/// Returns the delay as a [`Duration`].
pub fn compute_retry_backoff(attempt: u32, backoff_strategy: &str, base_secs: u64) -> Duration {
    match backoff_strategy {
        "constant" => Duration::from_secs(base_secs),
        // Default to exponential
        _ => {
            let power = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
            let delay_secs = base_secs.saturating_mul(power);
            Duration::from_secs(delay_secs.min(60))
        }
    }
}

/// Minimal engine interface required by `DurableContext`.
#[async_trait]
pub trait WorkflowEngine: Send + Sync {
    /// Durable sleep that blocks until the timer fires.
    async fn sleep(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
        duration: Duration,
    ) -> Result<(), ClusterError>;

    /// Wait for a deferred signal and return its serialized value.
    async fn await_deferred(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Vec<u8>, ClusterError>;

    /// Resolve a deferred signal with a serialized value.
    async fn resolve_deferred(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
        value: Vec<u8>,
    ) -> Result<(), ClusterError>;

    /// Wait for an interrupt signal.
    async fn on_interrupt(
        &self,
        workflow_name: &str,
        execution_id: &str,
    ) -> Result<(), ClusterError>;
}

/// In-memory workflow engine for testing.
///
/// This engine provides an in-memory implementation of the [`WorkflowEngine`] trait
/// suitable for use in tests and examples. It does not persist state across restarts.
///
/// # Example
///
/// ```text
/// use std::sync::Arc;
/// use cruster::testing::TestCluster;
///
/// let cluster = TestCluster::with_workflow_support().await;
/// // Register entities with #[workflow] methods and test them
/// ```
#[derive(Default)]
pub struct MemoryWorkflowEngine {
    /// Storage for deferred values: (workflow_name, execution_id, name) -> serialized value
    deferred: DashMap<(String, String, String), Vec<u8>>,
    /// Notifiers for awaiting deferred values
    notifiers: DashMap<(String, String, String), Arc<Notify>>,
}

impl MemoryWorkflowEngine {
    /// Create a new in-memory workflow engine.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl WorkflowEngine for MemoryWorkflowEngine {
    async fn sleep(
        &self,
        _workflow_name: &str,
        _execution_id: &str,
        _name: &str,
        duration: Duration,
    ) -> Result<(), ClusterError> {
        // Simple tokio sleep - not durable, but sufficient for testing
        tokio::time::sleep(duration).await;
        Ok(())
    }

    async fn await_deferred(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Vec<u8>, ClusterError> {
        let key = (
            workflow_name.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );

        // Check if value already exists
        if let Some(value) = self.deferred.get(&key) {
            return Ok(value.clone());
        }

        // Get or create notifier
        let notify = self
            .notifiers
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();

        // Re-check after inserting notifier (avoid race)
        if let Some(value) = self.deferred.get(&key) {
            return Ok(value.clone());
        }

        // Wait for notification
        notify.notified().await;

        // Return the value
        self.deferred
            .get(&key)
            .map(|v| v.clone())
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: format!(
                    "deferred value not found after notification: {}/{}/{}",
                    workflow_name, execution_id, name
                ),
                source: None,
            })
    }

    async fn resolve_deferred(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
        value: Vec<u8>,
    ) -> Result<(), ClusterError> {
        let key = (
            workflow_name.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );

        // Store the value
        self.deferred.insert(key.clone(), value);

        // Notify any waiters
        if let Some(notify) = self.notifiers.get(&key) {
            notify.notify_waiters();
        }

        Ok(())
    }

    async fn on_interrupt(
        &self,
        workflow_name: &str,
        execution_id: &str,
    ) -> Result<(), ClusterError> {
        // Wait for the special interrupt signal
        let _ = self
            .await_deferred(workflow_name, execution_id, INTERRUPT_SIGNAL)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory_message::MemoryMessageStorage;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Build a DurableContext with message and workflow storage for testing.
    fn test_ctx(
        msg_storage: Arc<dyn MessageStorage>,
        wf_storage: Arc<dyn WorkflowStorage>,
    ) -> DurableContext {
        let engine = Arc::new(MemoryWorkflowEngine::new());
        DurableContext::with_journal_storage(engine, "TestEntity", "e-1", msg_storage, wf_storage)
    }

    /// Build a DurableContext *without* message storage (backward-compatible mode).
    fn test_ctx_no_storage() -> DurableContext {
        let engine = Arc::new(MemoryWorkflowEngine::new());
        DurableContext::new(engine, "TestEntity", "e-1")
    }

    #[tokio::test]
    async fn run_caches_result_on_first_execution() {
        let msg = Arc::new(MemoryMessageStorage::new());
        let wf: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx(msg.clone(), wf.clone());

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: i32 = ctx
            .run("my_activity", b"key1", || async move {
                cc.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            })
            .await
            .unwrap();

        assert_eq!(result, 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_returns_cached_on_replay() {
        let msg = Arc::new(MemoryMessageStorage::new());
        let wf: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());

        // First execution — caches the result
        {
            let ctx = test_ctx(msg.clone(), wf.clone());
            let result: i32 = ctx
                .run("my_activity", b"key1", || async { Ok(42) })
                .await
                .unwrap();
            assert_eq!(result, 42);
        }

        // Second execution (simulates replay) — should return cached result
        {
            let ctx = test_ctx(msg.clone(), wf.clone());
            let call_count = Arc::new(AtomicU32::new(0));
            let cc = call_count.clone();

            let result: i32 = ctx
                .run("my_activity", b"key1", || async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(99) // Would return 99 if actually executed
                })
                .await
                .unwrap();

            assert_eq!(result, 42, "should return cached result, not re-execute");
            assert_eq!(
                call_count.load(Ordering::SeqCst),
                0,
                "closure should not have been called"
            );
        }
    }

    #[tokio::test]
    async fn run_different_keys_execute_independently() {
        let msg = Arc::new(MemoryMessageStorage::new());
        let wf: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx(msg.clone(), wf.clone());

        let a: i32 = ctx
            .run("activity_a", b"k1", || async { Ok(1) })
            .await
            .unwrap();
        let b: i32 = ctx
            .run("activity_b", b"k2", || async { Ok(2) })
            .await
            .unwrap();

        assert_eq!(a, 1);
        assert_eq!(b, 2);

        // Replay — both should return cached values
        let ctx2 = test_ctx(msg.clone(), wf.clone());
        let a2: i32 = ctx2
            .run("activity_a", b"k1", || async { Ok(99) })
            .await
            .unwrap();
        let b2: i32 = ctx2
            .run("activity_b", b"k2", || async { Ok(99) })
            .await
            .unwrap();

        assert_eq!(a2, 1, "activity_a should return cached value");
        assert_eq!(b2, 2, "activity_b should return cached value");
    }

    #[tokio::test]
    async fn run_same_name_different_args_execute_independently() {
        let msg = Arc::new(MemoryMessageStorage::new());
        let wf: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx(msg.clone(), wf.clone());

        // Same activity name but different key bytes (different arguments)
        let a: i32 = ctx
            .run("do_work", b"arg-1", || async { Ok(10) })
            .await
            .unwrap();
        let b: i32 = ctx
            .run("do_work", b"arg-2", || async { Ok(20) })
            .await
            .unwrap();

        assert_eq!(a, 10);
        assert_eq!(b, 20);

        // Replay — each should return its own cached value
        let ctx2 = test_ctx(msg.clone(), wf.clone());
        let a2: i32 = ctx2
            .run("do_work", b"arg-1", || async { Ok(99) })
            .await
            .unwrap();
        let b2: i32 = ctx2
            .run("do_work", b"arg-2", || async { Ok(99) })
            .await
            .unwrap();

        assert_eq!(a2, 10, "arg-1 should return its cached value");
        assert_eq!(b2, 20, "arg-2 should return its cached value");
    }

    #[tokio::test]
    async fn run_without_storage_executes_directly() {
        let ctx = test_ctx_no_storage();

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: i32 = ctx
            .run("my_activity", b"key1", || async move {
                cc.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            })
            .await
            .unwrap();

        assert_eq!(result, 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_without_storage_always_re_executes() {
        let ctx = test_ctx_no_storage();

        let call_count = Arc::new(AtomicU32::new(0));

        for _ in 0..3 {
            let cc = call_count.clone();
            let _: i32 = ctx
                .run("my_activity", b"key1", || async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                })
                .await
                .unwrap();
        }

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "without storage, every call should execute"
        );
    }

    #[tokio::test]
    async fn run_caches_error_result() {
        let msg = Arc::new(MemoryMessageStorage::new());
        let wf: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());

        // First execution — fails
        {
            let ctx = test_ctx(msg.clone(), wf.clone());
            let result: Result<i32, ClusterError> = ctx
                .run("failing_activity", b"key1", || async {
                    Err(ClusterError::PersistenceError {
                        reason: "activity failed".into(),
                        source: None,
                    })
                })
                .await;
            assert!(result.is_err());
        }

        // Replay — should return the cached failure
        {
            let ctx = test_ctx(msg.clone(), wf.clone());
            let call_count = Arc::new(AtomicU32::new(0));
            let cc = call_count.clone();

            let result: Result<i32, ClusterError> = ctx
                .run("failing_activity", b"key1", || async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(99) // Would succeed if actually executed
                })
                .await;

            assert!(result.is_err(), "should return cached failure");
            assert_eq!(
                call_count.load(Ordering::SeqCst),
                0,
                "closure should not have been called"
            );
        }
    }
}
