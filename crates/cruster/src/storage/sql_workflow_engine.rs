//! SQL-backed workflow engine using PostgreSQL via sqlx.
//!
//! Provides durable `sleep()` and `await_deferred()`/`resolve_deferred()` operations
//! that survive entity restarts and runner crashes.
//!
//! This module is only available when the `sql` feature is enabled.
//!
//! # Architecture
//!
//! The engine uses two tables:
//! - `cluster_workflow_timers`: Stores scheduled wake-up times for `sleep()` operations
//! - `cluster_workflow_deferred`: Stores key-value pairs for deferred signal operations
//!
//! ## Timer Flow (sleep)
//!
//! 1. Workflow calls `sleep(name, duration)`
//! 2. Engine checks if timer already exists and has fired (idempotency)
//! 3. If not, creates timer record with `fire_at = now + duration`
//! 4. Polls database until timer is due
//! 5. Marks timer as fired and returns
//!
//! ## Deferred Value Flow (await_deferred/resolve_deferred)
//!
//! 1. Workflow calls `await_deferred(key)`
//! 2. Engine checks if value already resolved (idempotency)
//! 3. If not, creates pending record and polls until resolved
//! 4. External caller invokes `resolve_deferred(key, value)`
//! 5. Engine updates record with value and sets resolved=true
//! 6. Waiting workflow sees resolved value and returns

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sqlx::postgres::PgPool;
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::durable::{WorkflowEngine, INTERRUPT_SIGNAL};
use crate::error::ClusterError;

/// Poll interval for checking timer/deferred status in the database.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// PostgreSQL-backed workflow engine.
///
/// Provides durable implementations of `sleep()`, `await_deferred()`, and
/// `resolve_deferred()` that persist to the database and survive restarts.
///
/// # Example
///
/// ```text
/// use sqlx::postgres::PgPool;
/// use cruster::storage::sql_workflow_engine::SqlWorkflowEngine;
///
/// let pool = PgPool::connect("postgres://...").await?;
/// let engine = SqlWorkflowEngine::new(pool);
/// engine.migrate().await?;
/// ```
pub struct SqlWorkflowEngine {
    pool: PgPool,
    /// Poll interval for database checks
    poll_interval: Duration,
    /// In-memory notifiers for immediate wake-up on resolve_deferred
    /// Key: (workflow_name, execution_id, deferred_name)
    deferred_notifiers: DashMap<(String, String, String), Arc<Notify>>,
    /// In-memory notifiers for timers (for testing with short sleeps)
    timer_notifiers: DashMap<(String, String, String), Arc<Notify>>,
}

impl SqlWorkflowEngine {
    /// Create a new SQL workflow engine with the given connection pool.
    ///
    /// **Important:** You must call [`migrate()`](Self::migrate) before using this engine,
    /// or ensure that migrations have already been run.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            poll_interval: DEFAULT_POLL_INTERVAL,
            deferred_notifiers: DashMap::new(),
            timer_notifiers: DashMap::new(),
        }
    }

    /// Create a new SQL workflow engine with a custom poll interval.
    ///
    /// The poll interval controls how often the engine checks the database
    /// for timer/deferred state changes. Lower values provide faster response
    /// but increase database load.
    pub fn with_poll_interval(pool: PgPool, poll_interval: Duration) -> Self {
        Self {
            pool,
            poll_interval,
            deferred_notifiers: DashMap::new(),
            timer_notifiers: DashMap::new(),
        }
    }

    /// Run database migrations to create the workflow engine tables.
    ///
    /// This runs all cluster migrations (shared with other SQL storage types).
    /// It is safe to call multiple times - migrations are idempotent.
    pub async fn migrate(&self) -> Result<(), ClusterError> {
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("workflow engine migration failed: {e}"),
                source: Some(Box::new(e)),
            })
    }

    /// Get or create a notifier for a deferred key.
    fn get_deferred_notifier(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
    ) -> Arc<Notify> {
        let key = (
            workflow_name.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );
        self.deferred_notifiers
            .entry(key)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Get or create a notifier for a timer.
    fn get_timer_notifier(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
    ) -> Arc<Notify> {
        let key = (
            workflow_name.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );
        self.timer_notifiers
            .entry(key)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Clean up old completed entries from the database.
    ///
    /// Deletes timers that have fired and deferred values that have been resolved
    /// older than the specified duration.
    pub async fn cleanup(&self, older_than: Duration) -> Result<u64, ClusterError> {
        let cutoff =
            Utc::now() - chrono::Duration::from_std(older_than).unwrap_or(chrono::TimeDelta::MAX);

        // Clean up fired timers
        let timers_deleted = sqlx::query(
            "DELETE FROM cluster_workflow_timers WHERE fired = TRUE AND created_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow engine timer cleanup failed: {e}"),
            source: Some(Box::new(e)),
        })?
        .rows_affected();

        // Clean up resolved deferred values
        let deferred_deleted = sqlx::query(
            "DELETE FROM cluster_workflow_deferred WHERE resolved = TRUE AND resolved_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow engine deferred cleanup failed: {e}"),
            source: Some(Box::new(e)),
        })?
        .rows_affected();

        Ok(timers_deleted + deferred_deleted)
    }
}

#[async_trait]
impl WorkflowEngine for SqlWorkflowEngine {
    async fn sleep(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
        duration: Duration,
    ) -> Result<(), ClusterError> {
        let fire_at =
            Utc::now() + chrono::Duration::from_std(duration).unwrap_or(chrono::TimeDelta::MAX);

        // Check if timer already exists
        let existing: Option<(bool, DateTime<Utc>)> = sqlx::query(
            "SELECT fired, fire_at FROM cluster_workflow_timers 
             WHERE workflow_name = $1 AND execution_id = $2 AND timer_name = $3",
        )
        .bind(workflow_name)
        .bind(execution_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow engine sleep check failed: {e}"),
            source: Some(Box::new(e)),
        })?
        .map(|row| {
            let fired: bool = row.get("fired");
            let fire_at: DateTime<Utc> = row.get("fire_at");
            (fired, fire_at)
        });

        match existing {
            Some((true, _)) => {
                // Timer already fired - idempotent return
                return Ok(());
            }
            Some((false, existing_fire_at)) => {
                // Timer exists but hasn't fired - wait for it
                // Use the existing fire_at time, not the new one
                self.wait_for_timer(workflow_name, execution_id, name, existing_fire_at)
                    .await?;
            }
            None => {
                // Create new timer
                sqlx::query(
                    "INSERT INTO cluster_workflow_timers (workflow_name, execution_id, timer_name, fire_at)
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(workflow_name)
                .bind(execution_id)
                .bind(name)
                .bind(fire_at)
                .execute(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("workflow engine sleep create failed: {e}"),
                    source: Some(Box::new(e)),
                })?;

                // Wait for timer
                self.wait_for_timer(workflow_name, execution_id, name, fire_at)
                    .await?;
            }
        }

        Ok(())
    }

    async fn await_deferred(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Vec<u8>, ClusterError> {
        // Check if already resolved
        let existing: Option<(bool, Option<Vec<u8>>)> = sqlx::query(
            "SELECT resolved, value FROM cluster_workflow_deferred 
             WHERE workflow_name = $1 AND execution_id = $2 AND deferred_name = $3",
        )
        .bind(workflow_name)
        .bind(execution_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow engine await_deferred check failed: {e}"),
            source: Some(Box::new(e)),
        })?
        .map(|row| {
            let resolved: bool = row.get("resolved");
            let value: Option<Vec<u8>> = row.get("value");
            (resolved, value)
        });

        match existing {
            Some((true, Some(value))) => {
                // Already resolved - idempotent return
                return Ok(value);
            }
            Some((true, None)) => {
                // Resolved but no value - shouldn't happen but handle gracefully
                return Err(ClusterError::PersistenceError {
                    reason: format!(
                        "deferred value resolved but missing: {}/{}/{}",
                        workflow_name, execution_id, name
                    ),
                    source: None,
                });
            }
            Some((false, _)) => {
                // Record exists but not resolved - wait for it
            }
            None => {
                // Create pending record
                sqlx::query(
                    "INSERT INTO cluster_workflow_deferred (workflow_name, execution_id, deferred_name, resolved)
                     VALUES ($1, $2, $3, FALSE)
                     ON CONFLICT (workflow_name, execution_id, deferred_name) DO NOTHING",
                )
                .bind(workflow_name)
                .bind(execution_id)
                .bind(name)
                .execute(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("workflow engine await_deferred create failed: {e}"),
                    source: Some(Box::new(e)),
                })?;
            }
        }

        // Poll/wait for resolution
        let notifier = self.get_deferred_notifier(workflow_name, execution_id, name);

        loop {
            // Check if resolved
            let row: Option<(bool, Option<Vec<u8>>)> = sqlx::query(
                "SELECT resolved, value FROM cluster_workflow_deferred 
                 WHERE workflow_name = $1 AND execution_id = $2 AND deferred_name = $3",
            )
            .bind(workflow_name)
            .bind(execution_id)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("workflow engine await_deferred poll failed: {e}"),
                source: Some(Box::new(e)),
            })?
            .map(|r| {
                let resolved: bool = r.get("resolved");
                let value: Option<Vec<u8>> = r.get("value");
                (resolved, value)
            });

            match row {
                Some((true, Some(value))) => return Ok(value),
                Some((true, None)) => {
                    return Err(ClusterError::PersistenceError {
                        reason: format!(
                            "deferred value resolved but missing: {}/{}/{}",
                            workflow_name, execution_id, name
                        ),
                        source: None,
                    });
                }
                _ => {
                    // Wait for notification or poll timeout
                    tokio::select! {
                        _ = notifier.notified() => {
                            // Got notification, re-check immediately
                        }
                        _ = tokio::time::sleep(self.poll_interval) => {
                            // Poll timeout, re-check
                        }
                    }
                }
            }
        }
    }

    async fn resolve_deferred(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
        value: Vec<u8>,
    ) -> Result<(), ClusterError> {
        // Upsert the resolved value
        sqlx::query(
            "INSERT INTO cluster_workflow_deferred (workflow_name, execution_id, deferred_name, value, resolved, resolved_at)
             VALUES ($1, $2, $3, $4, TRUE, NOW())
             ON CONFLICT (workflow_name, execution_id, deferred_name) 
             DO UPDATE SET value = $4, resolved = TRUE, resolved_at = NOW()",
        )
        .bind(workflow_name)
        .bind(execution_id)
        .bind(name)
        .bind(&value)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("workflow engine resolve_deferred failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        // Notify any in-memory waiters
        let key = (
            workflow_name.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );
        if let Some(notifier) = self.deferred_notifiers.get(&key) {
            notifier.notify_waiters();
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

impl SqlWorkflowEngine {
    /// Wait for a timer to fire, polling the database.
    async fn wait_for_timer(
        &self,
        workflow_name: &str,
        execution_id: &str,
        name: &str,
        fire_at: DateTime<Utc>,
    ) -> Result<(), ClusterError> {
        let notifier = self.get_timer_notifier(workflow_name, execution_id, name);

        loop {
            let now = Utc::now();

            if now >= fire_at {
                // Timer is due - try to mark as fired (atomic check-and-set)
                let result = sqlx::query(
                    "UPDATE cluster_workflow_timers 
                     SET fired = TRUE 
                     WHERE workflow_name = $1 AND execution_id = $2 AND timer_name = $3 AND fired = FALSE",
                )
                .bind(workflow_name)
                .bind(execution_id)
                .bind(name)
                .execute(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("workflow engine timer fire failed: {e}"),
                    source: Some(Box::new(e)),
                })?;

                if result.rows_affected() > 0 {
                    // We successfully fired the timer
                    return Ok(());
                }

                // Someone else fired it - check if it's actually fired
                let fired: bool = sqlx::query(
                    "SELECT fired FROM cluster_workflow_timers 
                     WHERE workflow_name = $1 AND execution_id = $2 AND timer_name = $3",
                )
                .bind(workflow_name)
                .bind(execution_id)
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("workflow engine timer check failed: {e}"),
                    source: Some(Box::new(e)),
                })?
                .map(|r| r.get("fired"))
                .unwrap_or(true); // If deleted, treat as fired

                if fired {
                    return Ok(());
                }
                // Rare race condition - retry
                continue;
            }

            // Calculate how long to wait
            let remaining = (fire_at - now).to_std().unwrap_or(Duration::ZERO);
            let wait_time = remaining.min(self.poll_interval);

            // Wait for notification or timeout
            tokio::select! {
                _ = notifier.notified() => {
                    // Got notification (e.g., for testing), re-check immediately
                }
                _ = tokio::time::sleep(wait_time) => {
                    // Timeout, re-check
                }
            }
        }
    }

    /// Notify a timer (for testing - allows immediate wake-up).
    #[cfg(test)]
    pub fn notify_timer(&self, workflow_name: &str, execution_id: &str, name: &str) {
        let key = (
            workflow_name.to_string(),
            execution_id.to_string(),
            name.to_string(),
        );
        if let Some(notifier) = self.timer_notifiers.get(&key) {
            notifier.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_workflow_engine_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SqlWorkflowEngine>();
    }
}
