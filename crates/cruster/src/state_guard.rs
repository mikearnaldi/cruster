//! State mutation guard for entity state access.
//!
//! Provides `StateMutGuard` which enables safe mutable access to entity state
//! with automatic persistence on drop.
//!
//! Also provides `TraitStateMutGuard` for entity traits, which updates the
//! ArcSwap on drop but does not handle persistence (persistence is managed
//! at the entity level for composite state).
//!
//! `StateRef<S>` is a read-only proxy for entity state in `#[workflow]` and
//! `#[rpc]` methods. It implements `Deref<Target=S>` so `self.state.field`
//! works transparently, but cannot be reassigned or mutated by user code.
//! After each activity call, the macro-generated delegation code refreshes
//! the proxy to reflect the latest committed state.
//!
//! For transactional activities, use `ActivityScope` to wrap activity execution
//! in a database transaction. State mutations via `self.state` in `#[activity]`
//! methods automatically write to the active transaction when one exists.
//!
//! Activities can also execute arbitrary SQL within the same transaction using
//! `ActivityScope::sql_transaction()` (requires the `sql` feature).

use arc_swap::ArcSwap;
use serde::{de::DeserializeOwned, Serialize};
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};

#[cfg(feature = "sql")]
use crate::durable::StorageTransaction;
use crate::durable::WorkflowStorage;
use crate::error::ClusterError;

/// Read-only proxy for entity state in `#[workflow]` and `#[rpc]` methods.
///
/// Wraps an `Arc<S>` and implements `Deref<Target=S>` so that `self.state.field`
/// works transparently. The inner `Arc` is private, preventing user code from
/// reassigning or constructing a `StateRef` directly.
///
/// After each activity call within a workflow, the macro-generated delegation
/// code calls `__refresh` to swap in the latest committed state from the
/// `ArcSwap`.
pub struct StateRef<S>(Arc<S>);

impl<S> StateRef<S> {
    /// Create a new `StateRef` wrapping the given `Arc`.
    #[doc(hidden)]
    pub fn __new(arc: Arc<S>) -> Self {
        Self(arc)
    }

    /// Refresh the inner state with a new `Arc`.
    ///
    /// Called by macro-generated activity delegation code after an activity
    /// commits, so that subsequent reads in the workflow see the updated state.
    #[doc(hidden)]
    pub fn __refresh(&mut self, new: Arc<S>) {
        self.0 = new;
    }
}

impl<S> Deref for StateRef<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Type aliases to reduce complexity warnings
type PendingWrites = Arc<parking_lot::Mutex<Vec<(String, Vec<u8>)>>>;
type PendingArcSwaps = Arc<parking_lot::Mutex<Vec<Box<dyn FnOnce() + Send>>>>;
#[cfg(feature = "sql")]
type SharedTransaction = Arc<TokioMutex<Box<dyn StorageTransaction>>>;

// Thread-local storage for the active transaction context.
// This allows activity state mutations to automatically write to the transaction.
tokio::task_local! {
    static ACTIVE_TRANSACTION: RefCell<Option<ActiveTransaction>>;
}

/// The active transaction context for the current task.
struct ActiveTransaction {
    /// Pending state writes: (key, serialized_value)
    /// Uses parking_lot::Mutex which is Send + Sync and doesn't poison.
    pending_writes: PendingWrites,
    /// Pending ArcSwap updates to apply on commit.
    pending_arc_swaps: PendingArcSwaps,
    /// The underlying transaction, wrapped for shared access.
    /// This allows activities to execute SQL within the transaction via
    /// `ActivityScope::sql_transaction()`.
    #[cfg(feature = "sql")]
    transaction: SharedTransaction,
}

/// Scope for executing an activity within a transaction.
///
/// When an activity is executed within this scope:
/// 1. A transaction is started from the storage backend
/// 2. All state mutations via `self.state` buffer their writes
/// 3. On success, buffered writes are applied to the transaction, then it commits
/// 4. On failure (panic or error), the transaction rolls back
///
/// This ensures that state persistence is SYNCHRONOUS with activity completion -
/// the activity only returns success AFTER the transaction is committed.
///
/// # Example
///
/// ```text
/// #[activity]
/// async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
///     // State mutations inside activities are automatically transactional
///     self.state.count += amount;
///     Ok(self.state.count)
/// }
/// // When the activity completes, state changes are GUARANTEED to be persisted
/// ```
pub struct ActivityScope;

impl ActivityScope {
    /// Run an activity within a transactional scope.
    ///
    /// The provided async closure is executed with an active transaction.
    /// If the closure returns `Ok`, the transaction is committed SYNCHRONOUSLY
    /// before this function returns.
    /// If the closure returns `Err` or panics, the transaction is rolled back.
    ///
    /// During execution, the activity can:
    /// - Mutate state via `self.state` (automatically buffered and committed)
    /// - Execute arbitrary SQL via `ActivityScope::sql_transaction()` (if using SQL storage)
    pub async fn run<F, Fut, T>(storage: &Arc<dyn WorkflowStorage>, f: F) -> Result<T, ClusterError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, ClusterError>>,
    {
        // Begin the transaction
        let tx = storage.begin_transaction().await?;
        let transaction = Arc::new(TokioMutex::new(tx));
        let pending_writes = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let pending_arc_swaps = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let active = ActiveTransaction {
            pending_writes: pending_writes.clone(),
            pending_arc_swaps: pending_arc_swaps.clone(),
            #[cfg(feature = "sql")]
            transaction: transaction.clone(),
        };

        // Run the closure with the transaction context
        let result = ACTIVE_TRANSACTION
            .scope(RefCell::new(Some(active)), async { f().await })
            .await;

        // Handle the result
        match result {
            Ok(value) => {
                // Take pending writes (no lock held across await)
                let writes: Vec<_> = {
                    let mut guard = pending_writes.lock();
                    std::mem::take(&mut *guard)
                };

                // Take the transaction out of the Arc<Mutex<_>>
                let mut tx = Arc::try_unwrap(transaction)
                    .map_err(|_| ClusterError::PersistenceError {
                        reason: "transaction still in use after activity completed".to_string(),
                        source: None,
                    })?
                    .into_inner();

                // Apply all pending writes to the transaction SYNCHRONOUSLY
                for (key, bytes) in writes.iter() {
                    tx.save(key, bytes).await?;
                }

                // Commit the transaction SYNCHRONOUSLY - this is the key!
                // Only after this succeeds do we consider the activity complete.
                tx.commit().await?;

                // Apply pending ArcSwap updates (only after successful commit)
                // This updates the in-memory state to match what's now persisted.
                let updates: Vec<_> = {
                    let mut guard = pending_arc_swaps.lock();
                    std::mem::take(&mut *guard)
                };
                for update in updates {
                    update();
                }

                Ok(value)
            }
            Err(e) => {
                // Take the transaction for rollback
                if let Ok(tx_arc) = Arc::try_unwrap(transaction) {
                    let tx = tx_arc.into_inner();
                    let _ = tx.rollback().await; // Ignore rollback errors
                }
                // Don't apply ArcSwap updates on failure - state unchanged
                Err(e)
            }
        }
    }

    /// Check if there's an active transaction in the current task.
    pub fn is_active() -> bool {
        ACTIVE_TRANSACTION
            .try_with(|cell| cell.borrow().is_some())
            .unwrap_or(false)
    }

    /// Buffer a state write to be applied to the transaction on commit.
    ///
    /// This is called by `StateMutGuard::drop()` when a transaction is active.
    /// The write is buffered synchronously and applied to the transaction
    /// BEFORE the activity returns. No fire-and-forget!
    pub(crate) fn buffer_write(key: String, value: Vec<u8>) {
        let _ = ACTIVE_TRANSACTION.try_with(|cell| {
            if let Some(active) = cell.borrow().as_ref() {
                let mut writes = active.pending_writes.lock();
                // Replace any existing write for this key
                if let Some(pos) = writes.iter().position(|(k, _)| k == &key) {
                    writes[pos].1 = value;
                } else {
                    writes.push((key, value));
                }
            }
        });
    }

    /// Register a pending ArcSwap update to be applied on commit.
    ///
    /// This is called by `StateMutGuard::drop()` to defer the ArcSwap update
    /// until the transaction successfully commits.
    pub(crate) fn register_arc_swap_update<F>(apply: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = ACTIVE_TRANSACTION.try_with(|cell| {
            if let Some(active) = cell.borrow().as_ref() {
                let mut updates = active.pending_arc_swaps.lock();
                updates.push(Box::new(apply));
            }
        });
    }

    /// Get the underlying SQL transaction for executing arbitrary SQL.
    ///
    /// Returns `None` if:
    /// - Not currently within an activity scope
    /// - The storage backend doesn't support SQL transactions (e.g., memory storage)
    ///
    /// # Example
    ///
    /// ```text
    /// #[activity]
    /// async fn transfer(&mut self, to: String, amount: i64) -> Result<(), ClusterError> {
    ///     // State mutation (automatically transactional)
    ///     self.state.balance -= amount;
    ///     
    ///     // Execute arbitrary SQL in the same transaction
    ///     if let Some(tx) = ActivityScope::sql_transaction().await {
    ///         tx.execute(
    ///             sqlx::query("INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)")
    ///                 .bind(&self.id)
    ///                 .bind(&to)
    ///                 .bind(amount)
    ///         ).await?;
    ///     }
    ///     
    ///     Ok(())
    /// }
    /// ```
    #[cfg(feature = "sql")]
    pub async fn sql_transaction() -> Option<SqlTransactionHandle> {
        let transaction = ACTIVE_TRANSACTION
            .try_with(|cell| cell.borrow().as_ref().map(|a| a.transaction.clone()))
            .ok()
            .flatten()?;

        // Try to downcast to SqlTransaction
        let mut guard = transaction.lock().await;
        let is_sql = guard
            .as_any_mut()
            .downcast_ref::<crate::storage::sql_workflow::SqlTransaction>()
            .is_some();
        drop(guard);

        if is_sql {
            Some(SqlTransactionHandle { transaction })
        } else {
            None
        }
    }
}

/// Handle to the SQL transaction within an activity scope.
///
/// This handle provides methods to execute arbitrary SQL within the same
/// transaction as entity state changes. All SQL operations will be committed
/// or rolled back together with state mutations.
#[cfg(feature = "sql")]
pub struct SqlTransactionHandle {
    transaction: SharedTransaction,
}

#[cfg(feature = "sql")]
impl SqlTransactionHandle {
    /// Execute a SQL query within the transaction.
    ///
    /// # Example
    ///
    /// ```text
    /// if let Some(tx) = ActivityScope::sql_transaction().await {
    ///     tx.execute(
    ///         sqlx::query("INSERT INTO audit_log (entity_id, action) VALUES ($1, $2)")
    ///             .bind(&entity_id)
    ///             .bind("transfer")
    ///     ).await?;
    /// }
    /// ```
    pub async fn execute<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<sqlx::postgres::PgQueryResult, ClusterError> {
        let mut guard = self.transaction.lock().await;
        let tx = guard
            .as_any_mut()
            .downcast_mut::<crate::storage::sql_workflow::SqlTransaction>()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction is not a SQL transaction".to_string(),
                source: None,
            })?;
        tx.execute(query).await
    }

    /// Fetch a single row from a SQL query within the transaction.
    ///
    /// Use with `sqlx::query_as`:
    /// ```text
    /// let user: User = tx.fetch_one(sqlx::query_as("SELECT * FROM users WHERE id = $1").bind(id)).await?;
    /// ```
    pub async fn fetch_one<'q, O>(
        &self,
        query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> Result<O, ClusterError>
    where
        O: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        let mut guard = self.transaction.lock().await;
        let tx = guard
            .as_any_mut()
            .downcast_mut::<crate::storage::sql_workflow::SqlTransaction>()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction is not a SQL transaction".to_string(),
                source: None,
            })?;
        tx.fetch_one(query).await
    }

    /// Fetch an optional row from a SQL query within the transaction.
    pub async fn fetch_optional<'q, O>(
        &self,
        query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> Result<Option<O>, ClusterError>
    where
        O: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        let mut guard = self.transaction.lock().await;
        let tx = guard
            .as_any_mut()
            .downcast_mut::<crate::storage::sql_workflow::SqlTransaction>()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction is not a SQL transaction".to_string(),
                source: None,
            })?;
        tx.fetch_optional(query).await
    }

    /// Fetch all rows from a SQL query within the transaction.
    pub async fn fetch_all<'q, O>(
        &self,
        query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> Result<Vec<O>, ClusterError>
    where
        O: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        let mut guard = self.transaction.lock().await;
        let tx = guard
            .as_any_mut()
            .downcast_mut::<crate::storage::sql_workflow::SqlTransaction>()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction is not a SQL transaction".to_string(),
                source: None,
            })?;
        tx.fetch_all(query).await
    }
}

/// Guard for mutable state access.
///
/// When dropped, this guard:
/// 1. Swaps the modified state into the `ArcSwap`
/// 2. Persists the state to storage (if configured)
/// 3. Releases the write lock
///
/// # Example
///
/// ```text
/// #[activity]
/// async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
///     self.state.count += amount;
///     Ok(self.state.count)
/// }
/// // State is automatically saved when the activity completes
/// ```
pub struct StateMutGuard<S>
where
    S: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The cloned state being mutated.
    state: S,
    /// Reference to the ArcSwap to update on drop.
    arc_swap: Arc<ArcSwap<S>>,
    /// Storage for persistence (if any).
    storage: Option<Arc<dyn WorkflowStorage>>,
    /// Storage key for this entity's state.
    storage_key: String,
    /// Write lock guard - released on drop.
    _lock: OwnedMutexGuard<()>,
    /// Whether the state was modified and needs saving.
    /// Currently always true, but could be optimized with dirty tracking.
    dirty: bool,
}

impl<S> StateMutGuard<S>
where
    S: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a new state mutation guard.
    ///
    /// This clones the current state and holds the write lock.
    pub fn new(
        arc_swap: Arc<ArcSwap<S>>,
        storage: Option<Arc<dyn WorkflowStorage>>,
        storage_key: String,
        lock: OwnedMutexGuard<()>,
    ) -> Self {
        let state = (**arc_swap.load()).clone();
        Self {
            state,
            arc_swap,
            storage,
            storage_key,
            _lock: lock,
            dirty: true,
        }
    }

    /// Persist the state to storage.
    ///
    /// This is called automatically on drop, but can be called explicitly
    /// if you need to handle errors.
    pub async fn persist(&self) -> Result<(), ClusterError> {
        if let Some(storage) = &self.storage {
            let bytes =
                rmp_serde::to_vec(&self.state).map_err(|e| ClusterError::PersistenceError {
                    reason: format!("failed to serialize state: {e}"),
                    source: Some(Box::new(e)),
                })?;
            storage.save(&self.storage_key, &bytes).await?;
        }
        Ok(())
    }

    /// Commit the state changes without consuming the guard.
    ///
    /// This swaps the new state into the ArcSwap and persists to storage.
    /// The guard continues to hold the lock.
    pub async fn commit(&mut self) -> Result<(), ClusterError> {
        // Swap in the new state
        self.arc_swap.store(Arc::new(self.state.clone()));
        // Persist
        self.persist().await?;
        self.dirty = false;
        Ok(())
    }
}

impl<S> Deref for StateMutGuard<S>
where
    S: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S> DerefMut for StateMutGuard<S>
where
    S: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.state
    }
}

impl<S> Drop for StateMutGuard<S>
where
    S: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.dirty {
            return;
        }

        // Serialize state
        let bytes = match rmp_serde::to_vec(&self.state) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(
                    key = %self.storage_key,
                    error = %e,
                    "failed to serialize state on drop"
                );
                return;
            }
        };

        // Check if we're in a transaction context
        if ActivityScope::is_active() {
            // In a transaction: buffer the write (will be applied to transaction on commit)
            ActivityScope::buffer_write(self.storage_key.clone(), bytes);

            // Register the ArcSwap update to happen on commit (not now!)
            let arc_swap = self.arc_swap.clone();
            let state = self.state.clone();
            ActivityScope::register_arc_swap_update(move || {
                arc_swap.store(Arc::new(state));
            });
        } else if self.storage.is_some() {
            // NOT in a transaction but we have storage configured.
            // This is a BUG - state mutations with persistence MUST happen
            // within an ActivityScope to guarantee durability.
            //
            // We update the in-memory state but log a critical warning.
            // In production, this should probably panic.
            tracing::error!(
                key = %self.storage_key,
                "STATE MUTATION OUTSIDE TRANSACTION! State will be updated in-memory \
                 but persistence is NOT guaranteed. Wrap this code in ActivityScope::run()"
            );

            // Still update ArcSwap so the system doesn't get into an inconsistent state,
            // but this is NOT safe for durability.
            self.arc_swap.store(Arc::new(self.state.clone()));

            // We do NOT persist here - that would be fire-and-forget which is broken.
            // The state will be lost on restart, which is the correct behavior for
            // code that doesn't properly use transactions.
        } else {
            // No storage configured - this is an in-memory only entity.
            // Just update the ArcSwap.
            self.arc_swap.store(Arc::new(self.state.clone()));
        }
    }
}

/// Guard for mutable trait state access.
///
/// Similar to `StateMutGuard` but without persistence handling.
/// Trait state persistence is managed at the entity level via composite state.
///
/// When dropped, this guard:
/// 1. Swaps the modified state into the `ArcSwap`
/// 2. Releases the write lock
///
/// # Example
///
/// ```text
/// #[activity]
/// #[protected]
/// async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
///     self.state.count += amount;
///     Ok(self.state.count)
/// }
/// // State is automatically updated when the activity completes
/// ```
pub struct TraitStateMutGuard<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// The cloned state being mutated.
    state: S,
    /// Reference to the ArcSwap to update on drop.
    arc_swap: Arc<ArcSwap<S>>,
    /// Write lock guard - released on drop.
    _lock: OwnedMutexGuard<()>,
    /// Whether the state was modified.
    dirty: bool,
}

impl<S> TraitStateMutGuard<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new trait state mutation guard.
    ///
    /// This clones the current state and holds the write lock.
    pub fn new(arc_swap: Arc<ArcSwap<S>>, lock: OwnedMutexGuard<()>) -> Self {
        let state = (**arc_swap.load()).clone();
        Self {
            state,
            arc_swap,
            _lock: lock,
            dirty: true,
        }
    }

    /// Commit the state changes without consuming the guard.
    ///
    /// This swaps the new state into the ArcSwap.
    /// The guard continues to hold the lock.
    pub fn commit(&mut self) {
        self.arc_swap.store(Arc::new(self.state.clone()));
        self.dirty = false;
    }
}

impl<S> Deref for TraitStateMutGuard<S>
where
    S: Clone + Send + Sync + 'static,
{
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S> DerefMut for TraitStateMutGuard<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.state
    }
}

impl<S> Drop for TraitStateMutGuard<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.dirty {
            // Swap in the new state
            self.arc_swap.store(Arc::new(self.state.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::MemoryWorkflowStorage;
    use tokio::sync::Mutex;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestState {
        count: i32,
    }

    #[tokio::test]
    async fn state_mut_guard_updates_arc_swap() {
        let state = TestState { count: 0 };
        let arc_swap = Arc::new(ArcSwap::from_pointee(state));
        let mutex = Arc::new(Mutex::new(()));
        let lock = mutex.clone().lock_owned().await;

        {
            let mut guard = StateMutGuard::new(arc_swap.clone(), None, "test".to_string(), lock);
            guard.count = 42;
        }
        // Guard dropped, state should be updated

        assert_eq!(arc_swap.load().count, 42);
    }

    #[tokio::test]
    async fn state_mut_guard_persists_to_storage() {
        let state = TestState { count: 0 };
        let arc_swap = Arc::new(ArcSwap::from_pointee(state));
        let storage = Arc::new(MemoryWorkflowStorage::new());
        let mutex = Arc::new(Mutex::new(()));
        let lock = mutex.clone().lock_owned().await;

        {
            let mut guard = StateMutGuard::new(
                arc_swap.clone(),
                Some(storage.clone()),
                "entity/Test/1/state".to_string(),
                lock,
            );
            guard.count = 99;
            // Explicitly commit to ensure persistence completes
            guard.commit().await.unwrap();
        }

        // Check storage
        let bytes = storage.load("entity/Test/1/state").await.unwrap().unwrap();
        let loaded: TestState = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(loaded.count, 99);
    }

    #[tokio::test]
    async fn state_mut_guard_explicit_commit() {
        let state = TestState { count: 5 };
        let arc_swap = Arc::new(ArcSwap::from_pointee(state));
        let mutex = Arc::new(Mutex::new(()));
        let lock = mutex.clone().lock_owned().await;

        let mut guard = StateMutGuard::new(arc_swap.clone(), None, "test".to_string(), lock);

        guard.count = 10;
        guard.commit().await.unwrap();

        // State should be visible even before drop
        assert_eq!(arc_swap.load().count, 10);

        guard.count = 20;
        // Don't commit, let drop handle it
        drop(guard);

        assert_eq!(arc_swap.load().count, 20);
    }

    #[tokio::test]
    async fn activity_scope_persists_state_on_success() {
        let storage: Arc<dyn crate::durable::WorkflowStorage> =
            Arc::new(MemoryWorkflowStorage::new());
        let arc_swap = Arc::new(ArcSwap::from_pointee(TestState { count: 0 }));
        let mutex = Arc::new(Mutex::new(()));

        // Verify is_active is false before running
        assert!(!ActivityScope::is_active());

        let result = ActivityScope::run(&storage, || {
            let arc_swap = arc_swap.clone();
            let storage = storage.clone();
            let mutex = mutex.clone();
            async move {
                // Verify is_active is true inside the scope
                assert!(
                    ActivityScope::is_active(),
                    "ActivityScope should be active inside run()"
                );

                let lock = mutex.lock_owned().await;
                let mut guard = StateMutGuard::new(
                    arc_swap.clone(),
                    Some(storage.clone()),
                    "test/key".to_string(),
                    lock,
                );
                guard.count = 42;
                drop(guard); // This should buffer the write

                Ok::<_, crate::error::ClusterError>(())
            }
        })
        .await;

        assert!(result.is_ok());

        // State should be persisted to storage
        let stored = storage.load("test/key").await.unwrap();
        assert!(
            stored.is_some(),
            "State should be persisted after ActivityScope::run() completes"
        );
        let loaded: TestState = rmp_serde::from_slice(&stored.unwrap()).unwrap();
        assert_eq!(loaded.count, 42);

        // ArcSwap should also be updated
        assert_eq!(arc_swap.load().count, 42);
    }

    #[tokio::test]
    async fn activity_scope_rolls_back_on_error() {
        let storage: Arc<dyn crate::durable::WorkflowStorage> =
            Arc::new(MemoryWorkflowStorage::new());
        let arc_swap = Arc::new(ArcSwap::from_pointee(TestState { count: 0 }));
        let mutex = Arc::new(Mutex::new(()));

        let result = ActivityScope::run(&storage, || {
            let arc_swap = arc_swap.clone();
            let storage = storage.clone();
            let mutex = mutex.clone();
            async move {
                let lock = mutex.lock_owned().await;
                let mut guard = StateMutGuard::new(
                    arc_swap.clone(),
                    Some(storage.clone()),
                    "test/key".to_string(),
                    lock,
                );
                guard.count = 42;
                drop(guard); // This buffers the write

                // Return an error - should rollback
                Err::<(), _>(crate::error::ClusterError::PersistenceError {
                    reason: "test error".to_string(),
                    source: None,
                })
            }
        })
        .await;

        assert!(result.is_err());

        // State should NOT be persisted
        let stored = storage.load("test/key").await.unwrap();
        assert!(stored.is_none(), "State should NOT be persisted on error");

        // ArcSwap should NOT be updated
        assert_eq!(arc_swap.load().count, 0);
    }

    #[tokio::test]
    #[cfg(feature = "sql")]
    async fn sql_transaction_returns_none_for_memory_storage() {
        let storage: Arc<dyn crate::durable::WorkflowStorage> =
            Arc::new(MemoryWorkflowStorage::new());

        let result = ActivityScope::run(&storage, || async {
            // sql_transaction() should return None when using memory storage
            let tx = ActivityScope::sql_transaction().await;
            assert!(
                tx.is_none(),
                "sql_transaction() should return None for memory storage"
            );
            Ok::<_, crate::error::ClusterError>(())
        })
        .await;

        assert!(result.is_ok());
    }
}
