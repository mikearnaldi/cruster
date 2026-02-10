//! Activity scope for transactional activity execution.
//!
//! Provides `ActivityScope` to wrap activity execution in a database transaction.
//! State mutations and journal writes are buffered and committed atomically.
//!
//! Activities can also execute arbitrary SQL within the same transaction using
//! `ActivityScope::sql_transaction()`.

use std::cell::RefCell;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

use crate::durable::StorageTransaction;
use crate::durable::WorkflowStorage;
use crate::error::ClusterError;

// Type aliases to reduce complexity warnings
type PendingWrites = Arc<parking_lot::Mutex<Vec<(String, Vec<u8>)>>>;
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
    /// The underlying transaction, wrapped for shared access.
    /// This allows activities to execute SQL within the transaction via
    /// `ActivityScope::sql_transaction()`.
    transaction: SharedTransaction,
}

/// Scope for executing an activity within a transaction.
///
/// When an activity is executed within this scope:
/// 1. A transaction is started from the storage backend
/// 2. All writes are buffered via `buffer_write`
/// 3. On success, buffered writes are applied to the transaction, then it commits
/// 4. On failure (panic or error), the transaction rolls back
///
/// This ensures that persistence is SYNCHRONOUS with activity completion -
/// the activity only returns success AFTER the transaction is committed.
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
    /// - Buffer writes via `ActivityScope::buffer_write()` (committed atomically)
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

        let active = ActiveTransaction {
            pending_writes: pending_writes.clone(),
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

                Ok(value)
            }
            Err(e) => {
                // Take the transaction for rollback
                if let Ok(tx_arc) = Arc::try_unwrap(transaction) {
                    let tx = tx_arc.into_inner();
                    let _ = tx.rollback().await; // Ignore rollback errors
                }
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

    /// Buffer a write to be applied to the transaction on commit.
    ///
    /// This is called by `DurableContext` to buffer journal writes
    /// so that activity results are persisted atomically.
    ///
    /// The write is buffered synchronously and applied to the transaction
    /// BEFORE the activity returns. No fire-and-forget!
    pub fn buffer_write(key: String, value: Vec<u8>) {
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

    /// Get the activity's SQL transaction handle.
    ///
    /// This is a convenience method that calls `sql_transaction()` and panics
    /// if no SQL transaction is available. Use this in `#[activity]` methods
    /// when you know the storage backend supports SQL transactions.
    ///
    /// # Panics
    ///
    /// Panics if called outside of an activity scope or if the storage
    /// backend does not support SQL transactions (e.g., memory storage).
    ///
    /// # Note
    ///
    /// Prefer using `self.tx()` in activity methods instead. `ActivityScope::db()`
    /// is the legacy API retained for backward compatibility.
    pub async fn db() -> SqlTransactionHandle {
        Self::sql_transaction()
            .await
            .expect("db() requires an active SQL transaction; are you inside an #[activity] with SQL storage?")
    }

    /// Get the underlying SQL transaction for executing arbitrary SQL.
    ///
    /// Returns `None` if:
    /// - Not currently within an activity scope
    /// - The storage backend doesn't support SQL transactions (e.g., memory storage)
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
/// transaction as journal writes. All SQL operations will be committed
/// or rolled back together.
pub struct SqlTransactionHandle {
    transaction: SharedTransaction,
}

impl SqlTransactionHandle {
    /// Execute a SQL query within the transaction.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::MemoryWorkflowStorage;

    #[tokio::test]
    async fn activity_scope_commits_writes_on_success() {
        let storage: Arc<dyn crate::durable::WorkflowStorage> =
            Arc::new(MemoryWorkflowStorage::new());

        // Verify is_active is false before running
        assert!(!ActivityScope::is_active());

        let result = ActivityScope::run(&storage, || {
            async move {
                // Verify is_active is true inside the scope
                assert!(
                    ActivityScope::is_active(),
                    "ActivityScope should be active inside run()"
                );

                ActivityScope::buffer_write("test/key".to_string(), b"hello".to_vec());

                Ok::<_, crate::error::ClusterError>(())
            }
        })
        .await;

        assert!(result.is_ok());

        // Write should be persisted to storage
        let stored = storage.load("test/key").await.unwrap();
        assert!(
            stored.is_some(),
            "Write should be persisted after ActivityScope::run() completes"
        );
        assert_eq!(stored.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn activity_scope_rolls_back_on_error() {
        let storage: Arc<dyn crate::durable::WorkflowStorage> =
            Arc::new(MemoryWorkflowStorage::new());

        let result = ActivityScope::run(&storage, || {
            async move {
                ActivityScope::buffer_write("test/key".to_string(), b"hello".to_vec());

                // Return an error - should rollback
                Err::<(), _>(crate::error::ClusterError::PersistenceError {
                    reason: "test error".to_string(),
                    source: None,
                })
            }
        })
        .await;

        assert!(result.is_err());

        // Write should NOT be persisted
        let stored = storage.load("test/key").await.unwrap();
        assert!(stored.is_none(), "Write should NOT be persisted on error");
    }

    #[tokio::test]
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

    #[tokio::test]
    #[should_panic(expected = "db() requires an active SQL transaction")]
    async fn db_panics_for_memory_storage() {
        let storage: Arc<dyn crate::durable::WorkflowStorage> =
            Arc::new(MemoryWorkflowStorage::new());

        let _ = ActivityScope::run(&storage, || async {
            // db() should panic when using memory storage (no SQL transaction)
            let _db = ActivityScope::db().await;
            Ok::<_, crate::error::ClusterError>(())
        })
        .await;
    }
}
