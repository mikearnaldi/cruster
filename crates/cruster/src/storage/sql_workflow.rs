//! SQL-backed workflow storage using PostgreSQL via sqlx.
//!
//! Persists workflow journal entries (idempotency cache) so that
//! workflow execution results and deferred values survive entity
//! restarts and runner crashes.
//!
//! This module is only available when the `sql` feature is enabled.

use async_trait::async_trait;
use chrono::Utc;
use sqlx::postgres::{PgPool, PgQueryResult, PgRow, Postgres};
use sqlx::{Row, Transaction};
use std::any::Any;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::durable::{StorageTransaction, WorkflowStorage};
use crate::error::ClusterError;

/// Compute the exclusive upper bound for a prefix range query.
///
/// Increments the last byte of the prefix string. If the last byte is 0xFF,
/// walks backwards to find a byte that can be incremented. Returns `None` if
/// the prefix is empty or consists entirely of 0xFF bytes (meaning "match
/// everything from prefix onward").
fn increment_last_byte(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(&last) = bytes.last() {
        if last < 0xFF {
            *bytes.last_mut().unwrap() = last + 1;
            // If incrementing the last byte produces valid UTF-8, use it directly.
            // Otherwise, truncate to the last valid UTF-8 boundary and retry.
            // This avoids producing invalid UTF-8 strings that PostgreSQL TEXT
            // columns would reject, or replacement characters from lossy conversion
            // that would produce incorrect range bounds.
            match String::from_utf8(bytes.clone()) {
                Ok(s) => return Some(s),
                Err(_) => {
                    // The increment broke a multi-byte UTF-8 sequence.
                    // Pop back to the start of the broken character and retry.
                    bytes.pop();
                    continue;
                }
            }
        }
        bytes.pop();
    }
    None
}

/// PostgreSQL-backed workflow storage.
pub struct SqlWorkflowStorage {
    pool: PgPool,
}

impl SqlWorkflowStorage {
    /// Create a new SQL workflow storage with the given connection pool.
    ///
    /// **Important:** You must call [`migrate()`](Self::migrate) before using this storage,
    /// or ensure that [`SqlMessageStorage::migrate()`](super::sql_message::SqlMessageStorage::migrate)
    /// has already been called (it runs all cluster migrations including the workflow journal table).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Run database migrations to create the `cluster_workflow_journal` table.
    ///
    /// This runs all cluster migrations (shared with `SqlMessageStorage`).
    /// It is safe to call multiple times — migrations are idempotent.
    pub async fn migrate(&self) -> Result<(), ClusterError> {
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("workflow storage migration failed: {e}"),
                source: Some(Box::new(e)),
            })
    }
}

#[async_trait]
impl WorkflowStorage for SqlWorkflowStorage {
    async fn load(&self, key: &str) -> Result<Option<Vec<u8>>, ClusterError> {
        let row = sqlx::query("SELECT value FROM cluster_workflow_journal WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("workflow storage load failed: {e}"),
                source: Some(Box::new(e)),
            })?;

        match row {
            Some(r) => {
                let value: Vec<u8> =
                    r.try_get("value")
                        .map_err(|e| ClusterError::PersistenceError {
                            reason: format!(
                                "workflow storage load: failed to read 'value' column: {e}"
                            ),
                            source: Some(Box::new(e)),
                        })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn save(&self, key: &str, value: &[u8]) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO cluster_workflow_journal (key, value, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow storage save failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), ClusterError> {
        sqlx::query("DELETE FROM cluster_workflow_journal WHERE key = $1")
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("workflow storage delete failed: {e}"),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }

    async fn mark_completed(&self, key: &str) -> Result<(), ClusterError> {
        sqlx::query("UPDATE cluster_workflow_journal SET completed_at = NOW() WHERE key = $1")
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("workflow storage mark_completed failed: {e}"),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    async fn cleanup(&self, older_than: std::time::Duration) -> Result<u64, ClusterError> {
        let cutoff =
            Utc::now() - chrono::Duration::from_std(older_than).unwrap_or(chrono::TimeDelta::MAX);
        let result = sqlx::query(
            "DELETE FROM cluster_workflow_journal WHERE completed_at IS NOT NULL AND completed_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow storage cleanup failed: {e}"),
            source: Some(Box::new(e)),
        })?;
        Ok(result.rows_affected())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, ClusterError> {
        // Use a range query instead of LIKE for reliable btree index usage.
        // LIKE with bind parameters and ESCAPE may not use text_pattern_ops
        // indexes on non-C locale databases. Range queries always use btree.
        // COLLATE "C" forces byte-order comparison, ensuring correct results
        // for non-ASCII keys on databases with non-C default collation.
        let range_end = increment_last_byte(prefix);
        let rows = match &range_end {
            Some(end) => {
                sqlx::query(
                    "SELECT key FROM cluster_workflow_journal WHERE key COLLATE \"C\" >= $1 AND key COLLATE \"C\" < $2 ORDER BY key ASC",
                )
                .bind(prefix)
                .bind(end)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                // Prefix is empty or all 0xFF bytes — match everything from prefix onward
                sqlx::query(
                    "SELECT key FROM cluster_workflow_journal WHERE key COLLATE \"C\" >= $1 ORDER BY key ASC",
                )
                .bind(prefix)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow storage list_keys failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        rows.iter()
            .map(|r| {
                r.try_get("key")
                    .map_err(|e| ClusterError::PersistenceError {
                        reason: format!(
                            "workflow storage list_keys: failed to read 'key' column: {e}"
                        ),
                        source: Some(Box::new(e)),
                    })
            })
            .collect()
    }

    async fn begin_transaction(&self) -> Result<Box<dyn StorageTransaction>, ClusterError> {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to begin transaction: {e}"),
                source: Some(Box::new(e)),
            })?;

        Ok(Box::new(SqlTransaction {
            tx: Arc::new(Mutex::new(Some(tx))),
        }))
    }

    fn as_arc(&self) -> Arc<dyn WorkflowStorage> {
        // This is only called by the default begin_transaction impl,
        // which we override above. So this should never be called.
        panic!("SqlWorkflowStorage::as_arc() should not be called")
    }

    fn sql_pool(&self) -> Option<&PgPool> {
        Some(&self.pool)
    }
}

/// A PostgreSQL transaction for workflow storage.
///
/// All operations are performed within the transaction and only become
/// visible when `commit()` is called. If the transaction is dropped
/// without committing, all operations are rolled back.
pub struct SqlTransaction {
    tx: Arc<Mutex<Option<Transaction<'static, Postgres>>>>,
}

#[async_trait]
impl StorageTransaction for SqlTransaction {
    async fn save(&mut self, key: &str, value: &[u8]) -> Result<(), ClusterError> {
        let mut guard = self.tx.lock().await;
        let tx = guard
            .as_mut()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction already committed or rolled back".to_string(),
                source: None,
            })?;

        sqlx::query(
            "INSERT INTO cluster_workflow_journal (key, value, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()",
        )
        .bind(key)
        .bind(value)
        .execute(&mut **tx)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("transaction save failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<(), ClusterError> {
        let mut guard = self.tx.lock().await;
        let tx = guard
            .as_mut()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction already committed or rolled back".to_string(),
                source: None,
            })?;

        sqlx::query("DELETE FROM cluster_workflow_journal WHERE key = $1")
            .bind(key)
            .execute(&mut **tx)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("transaction delete failed: {e}"),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<(), ClusterError> {
        let mut guard = self.tx.lock().await;
        let tx = guard.take().ok_or_else(|| ClusterError::PersistenceError {
            reason: "transaction already committed or rolled back".to_string(),
            source: None,
        })?;

        tx.commit()
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("transaction commit failed: {e}"),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), ClusterError> {
        let mut guard = self.tx.lock().await;
        let tx = guard.take().ok_or_else(|| ClusterError::PersistenceError {
            reason: "transaction already committed or rolled back".to_string(),
            source: None,
        })?;

        tx.rollback()
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("transaction rollback failed: {e}"),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl SqlTransaction {
    /// Execute a raw SQL query within this transaction.
    ///
    /// This allows activities to run arbitrary SQL statements that will be
    /// committed or rolled back together with entity state changes.
    ///
    /// # Example
    ///
    /// ```text
    /// #[activity]
    /// async fn transfer(&mut self, to: String, amount: i64) -> Result<(), ClusterError> {
    ///     self.state.balance -= amount;
    ///     
    ///     if let Some(tx) = ActivityScope::sql_transaction().await {
    ///         tx.execute(
    ///             sqlx::query("INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)")
    ///                 .bind(&self.id)
    ///                 .bind(&to)
    ///                 .bind(amount)
    ///         ).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn execute<'q>(
        &self,
        query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<PgQueryResult, ClusterError> {
        let mut guard = self.tx.lock().await;
        let tx = guard
            .as_mut()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction already committed or rolled back".to_string(),
                source: None,
            })?;

        query
            .execute(&mut **tx)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("SQL execute failed: {e}"),
                source: Some(Box::new(e)),
            })
    }

    /// Fetch a single row from a SQL query within this transaction.
    pub async fn fetch_one<'q, O>(
        &self,
        query: sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments>,
    ) -> Result<O, ClusterError>
    where
        O: Send + Unpin + for<'r> sqlx::FromRow<'r, PgRow>,
    {
        let mut guard = self.tx.lock().await;
        let tx = guard
            .as_mut()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction already committed or rolled back".to_string(),
                source: None,
            })?;

        query
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("SQL fetch_one failed: {e}"),
                source: Some(Box::new(e)),
            })
    }

    /// Fetch an optional row from a SQL query within this transaction.
    pub async fn fetch_optional<'q, O>(
        &self,
        query: sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments>,
    ) -> Result<Option<O>, ClusterError>
    where
        O: Send + Unpin + for<'r> sqlx::FromRow<'r, PgRow>,
    {
        let mut guard = self.tx.lock().await;
        let tx = guard
            .as_mut()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction already committed or rolled back".to_string(),
                source: None,
            })?;

        query
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("SQL fetch_optional failed: {e}"),
                source: Some(Box::new(e)),
            })
    }

    /// Fetch all rows from a SQL query within this transaction.
    pub async fn fetch_all<'q, O>(
        &self,
        query: sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments>,
    ) -> Result<Vec<O>, ClusterError>
    where
        O: Send + Unpin + for<'r> sqlx::FromRow<'r, PgRow>,
    {
        let mut guard = self.tx.lock().await;
        let tx = guard
            .as_mut()
            .ok_or_else(|| ClusterError::PersistenceError {
                reason: "transaction already committed or rolled back".to_string(),
                source: None,
            })?;

        query
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("SQL fetch_all failed: {e}"),
                source: Some(Box::new(e)),
            })
    }
}

/// Write a journal entry into an open SQL transaction.
///
/// This is used by the macro-generated activity dispatch code to write
/// the journal entry atomically with activity SQL writes (`self.tx`).
/// Both the journal entry and any user SQL execute in the same transaction.
pub async fn save_journal_entry(
    conn: &mut sqlx::PgConnection,
    key: &str,
    value: &[u8],
) -> Result<(), ClusterError> {
    sqlx::query(
        "INSERT INTO cluster_workflow_journal (key, value, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()",
    )
    .bind(key)
    .bind(value)
    .execute(conn)
    .await
    .map_err(|e| ClusterError::PersistenceError {
        reason: format!("journal entry save failed: {e}"),
        source: Some(Box::new(e)),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_workflow_storage_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SqlWorkflowStorage>();
    }

    #[test]
    fn increment_last_byte_normal() {
        assert_eq!(super::increment_last_byte("abc"), Some("abd".to_string()));
    }

    #[test]
    fn increment_last_byte_trailing_high_byte() {
        // 'z' (0x7A) + 1 = '{' (0x7B)
        assert_eq!(super::increment_last_byte("az"), Some("a{".to_string()));
    }

    #[test]
    fn increment_last_byte_empty() {
        assert_eq!(super::increment_last_byte(""), None);
    }

    #[test]
    fn increment_last_byte_prefix_query_semantics() {
        // Verify that "result/" -> "result0" covers all keys starting with "result/"
        let result = super::increment_last_byte("result/");
        assert_eq!(result, Some("result0".to_string())); // '/' + 1 = '0'
    }
}
