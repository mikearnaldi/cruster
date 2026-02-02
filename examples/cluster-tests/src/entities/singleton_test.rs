//! SingletonTest - A cluster singleton that writes state to PostgreSQL.
//!
//! This demonstrates the singleton pattern: a background task that runs on
//! exactly one node in the cluster. The singleton periodically writes to
//! PostgreSQL, and HTTP endpoints read from PostgreSQL to observe its state.
//!
//! ## What it tests:
//! - Singleton runs on exactly one node
//! - Singleton increments a counter in PostgreSQL
//! - Any node can read the singleton's state via PostgreSQL
//! - If the singleton node dies, another node takes over

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::sharding::Sharding;
use cruster::singleton::{register_singleton, SingletonContext};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

/// The name used for the singleton registration.
pub const SINGLETON_NAME: &str = "cluster-tests/singleton-test";

/// Table name for singleton state.
const TABLE_NAME: &str = "singleton_test_state";

/// State stored in PostgreSQL by the singleton.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SingletonState {
    /// The runner ID currently hosting the singleton.
    pub runner_id: String,
    /// Monotonically increasing tick counter.
    pub tick_count: i64,
    /// When the singleton last wrote to the database.
    pub last_tick_at: DateTime<Utc>,
    /// When this runner became the singleton leader.
    pub became_leader_at: DateTime<Utc>,
    /// When this singleton gracefully shut down (None if still running or force-killed).
    pub graceful_shutdown_at: Option<DateTime<Utc>>,
}

/// Manager for the singleton test.
///
/// Holds the database pool for reading singleton state.
/// The actual singleton runs as a background task on one node.
pub struct SingletonManager {
    pool: PgPool,
}

impl SingletonManager {
    /// Create a new singleton manager with the given database pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Ensure the singleton state table exists.
    pub async fn init_schema(&self) -> Result<(), ClusterError> {
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY DEFAULT 1,
                runner_id TEXT NOT NULL,
                tick_count BIGINT NOT NULL DEFAULT 0,
                last_tick_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                became_leader_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                graceful_shutdown_at TIMESTAMPTZ,
                CONSTRAINT singleton_single_row CHECK (id = 1)
            )
            "#
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to create singleton table: {e}"),
            source: Some(Box::new(e)),
        })?;

        // Add graceful_shutdown_at column if it doesn't exist (migration for existing tables)
        sqlx::query(&format!(
            r#"
            ALTER TABLE {TABLE_NAME} 
            ADD COLUMN IF NOT EXISTS graceful_shutdown_at TIMESTAMPTZ
            "#
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to add graceful_shutdown_at column: {e}"),
            source: Some(Box::new(e)),
        })?;

        Ok(())
    }

    /// Register the singleton with the cluster.
    ///
    /// The singleton task will run on exactly one node and periodically
    /// update its state in PostgreSQL. It manages its own cancellation to
    /// demonstrate graceful shutdown when the shard moves to another node.
    pub async fn register(&self, sharding: Arc<dyn Sharding>) -> Result<(), ClusterError> {
        let pool = self.pool.clone();

        register_singleton(sharding.as_ref(), SINGLETON_NAME, move |ctx: SingletonContext| {
            let pool = pool.clone();

            async move {
                // Opt-in to manage cancellation for graceful shutdown
                let cancel = ctx.cancellation();

                let runner_id = std::env::var("RUNNER_ADDRESS")
                    .unwrap_or_else(|_| format!("runner-{}", std::process::id()));

                tracing::info!(
                    runner = %runner_id,
                    "SingletonTest singleton started - this runner is now the leader"
                );

                // Initialize or take over leadership (clear any previous graceful_shutdown_at)
                let now = Utc::now();
                sqlx::query(&format!(
                    r#"
                    INSERT INTO {TABLE_NAME} (id, runner_id, tick_count, last_tick_at, became_leader_at, graceful_shutdown_at)
                    VALUES (1, $1, 0, $2, $2, NULL)
                    ON CONFLICT (id) DO UPDATE SET
                        runner_id = $1,
                        became_leader_at = $2,
                        last_tick_at = $2,
                        graceful_shutdown_at = NULL
                    "#
                ))
                .bind(&runner_id)
                .bind(now)
                .execute(&pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("singleton failed to initialize: {e}"),
                    source: Some(Box::new(e)),
                })?;

                // Main loop: increment tick counter every second, exit on cancellation
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            // Graceful shutdown: write marker to database
                            tracing::info!(
                                runner = %runner_id,
                                "SingletonTest singleton shutting down gracefully"
                            );
                            let now = Utc::now();
                            if let Err(e) = sqlx::query(&format!(
                                r#"
                                UPDATE {TABLE_NAME}
                                SET graceful_shutdown_at = $1
                                WHERE id = 1
                                "#
                            ))
                            .bind(now)
                            .execute(&pool)
                            .await
                            {
                                tracing::error!(error = %e, "failed to write graceful shutdown marker");
                            } else {
                                tracing::info!(
                                    runner = %runner_id,
                                    "SingletonTest singleton wrote graceful shutdown marker"
                                );
                            }
                            break;
                        }
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                            let now = Utc::now();
                            // Use UPSERT to handle case where row was deleted by reset
                            if let Err(e) = sqlx::query(&format!(
                                r#"
                                INSERT INTO {TABLE_NAME} (id, runner_id, tick_count, last_tick_at, became_leader_at, graceful_shutdown_at)
                                VALUES (1, $1, 1, $2, $2, NULL)
                                ON CONFLICT (id) DO UPDATE SET
                                    tick_count = {TABLE_NAME}.tick_count + 1,
                                    last_tick_at = $2
                                "#
                            ))
                            .bind(&runner_id)
                            .bind(now)
                            .execute(&pool)
                            .await
                            {
                                tracing::warn!(error = %e, "singleton failed to update tick");
                            } else {
                                tracing::trace!("singleton tick");
                            }
                        }
                    }
                }

                Ok(())
            }
        })
        .await
    }

    /// Get the current singleton state from PostgreSQL.
    ///
    /// Returns None if the singleton hasn't started yet.
    pub async fn get_state(&self) -> Result<Option<SingletonState>, ClusterError> {
        let row = sqlx::query_as::<_, (String, i64, DateTime<Utc>, DateTime<Utc>, Option<DateTime<Utc>>)>(&format!(
            r#"
            SELECT runner_id, tick_count, last_tick_at, became_leader_at, graceful_shutdown_at
            FROM {TABLE_NAME}
            WHERE id = 1
            "#
        ))
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to read singleton state: {e}"),
            source: Some(Box::new(e)),
        })?;

        Ok(row.map(
            |(runner_id, tick_count, last_tick_at, became_leader_at, graceful_shutdown_at)| SingletonState {
                runner_id,
                tick_count,
                last_tick_at,
                became_leader_at,
                graceful_shutdown_at,
            },
        ))
    }

    /// Get the current tick count.
    ///
    /// Returns 0 if the singleton hasn't started yet.
    pub async fn get_tick_count(&self) -> Result<i64, ClusterError> {
        Ok(self.get_state().await?.map(|s| s.tick_count).unwrap_or(0))
    }

    /// Get the current runner ID hosting the singleton.
    ///
    /// Returns empty string if the singleton hasn't started yet.
    pub async fn get_current_runner(&self) -> Result<String, ClusterError> {
        Ok(self
            .get_state()
            .await?
            .map(|s| s.runner_id)
            .unwrap_or_default())
    }

    /// Reset the singleton state (for testing).
    pub async fn reset(&self) -> Result<(), ClusterError> {
        sqlx::query(&format!("DELETE FROM {TABLE_NAME} WHERE id = 1"))
            .execute(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to reset singleton state: {e}"),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleton_state_serialization() {
        let state = SingletonState {
            runner_id: "node1:9000".to_string(),
            tick_count: 42,
            last_tick_at: Utc::now(),
            became_leader_at: Utc::now(),
            graceful_shutdown_at: None,
        };

        let json = serde_json::to_string(&state).unwrap();
        let parsed: SingletonState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.runner_id, "node1:9000");
        assert_eq!(parsed.tick_count, 42);
        assert!(parsed.graceful_shutdown_at.is_none());
    }
}
