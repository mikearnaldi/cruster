//! Internal durable helpers for entity workflows.
//!
//! This module exposes `DurableContext` so entity methods annotated with
//! `#[workflow]` can call `sleep`, `await_deferred`, `resolve_deferred`, and `on_interrupt`.
//!
//! It also provides `MemoryWorkflowEngine` and `MemoryWorkflowStorage` for testing
//! entities that use durable workflows.

use crate::error::ClusterError;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Deferred key name used for interrupt signals.
pub const INTERRUPT_SIGNAL: &str = "Workflow/InterruptSignal";

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
pub struct DurableContext {
    engine: Arc<dyn WorkflowEngine>,
    workflow_name: String,
    execution_id: String,
}

impl DurableContext {
    /// Create a new `DurableContext` for use within an entity handler.
    pub fn new(
        engine: Arc<dyn WorkflowEngine>,
        workflow_name: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Self {
        Self {
            engine,
            workflow_name: workflow_name.into(),
            execution_id: execution_id.into(),
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
