//! TimerTest - tests durable timer execution and scheduling.
//!
//! Migrated from old stateful entity API to new pure-RPC entity + standalone workflow.
//!
//! ## Architecture
//! - `TimerTest` entity: pure-RPC for reads and simple mutations
//!   - `get_timer_fires(entity_id)` — read fired timers from PG
//!   - `get_pending_timers(entity_id)` — read pending timers from PG
//!   - `cancel_timer(entity_id, timer_id)` — mark timer cancelled in PG
//!   - `clear_fires(entity_id, request_id)` — clear fire history in PG
//! - `ScheduleTimerWorkflow`: standalone workflow with durable sleep
//!   - Adds pending timer, sleeps for delay, checks cancellation, records fire
//!
//! All timer state is stored in PostgreSQL tables:
//! - `timer_test_fires` — history of fired timers
//! - `timer_test_pending` — currently pending/cancelled timers

use chrono::{DateTime, Utc};
use cruster::__internal::ActivityScope;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;

/// Record of a timer that has fired.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerFire {
    /// Unique identifier for the timer.
    pub timer_id: String,
    /// When the timer was scheduled.
    pub scheduled_at: DateTime<Utc>,
    /// When the timer actually fired.
    pub fired_at: DateTime<Utc>,
}

/// Record of a pending timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingTimer {
    /// Unique identifier for the timer.
    pub timer_id: String,
    /// When the timer was scheduled.
    pub scheduled_at: DateTime<Utc>,
    /// Delay in milliseconds.
    pub delay_ms: i64,
    /// Whether the timer was cancelled.
    pub cancelled: bool,
}

// ============================================================================
// TimerTest entity - pure-RPC for reads and simple mutations
// ============================================================================

/// TimerTest entity for testing timer/scheduling functionality.
///
/// Uses the new stateless entity API — state is stored directly in PostgreSQL
/// via the `timer_test_fires` and `timer_test_pending` tables.
///
/// ## RPCs
/// - `get_timer_fires(entity_id)` - Get fired timer history
/// - `get_pending_timers(entity_id)` - Get pending timers
/// - `cancel_timer(entity_id, timer_id)` - Cancel a pending timer
/// - `clear_fires(entity_id, request_id)` - Clear fire history
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct TimerTest {
    /// Database pool for direct state queries.
    pub pool: PgPool,
}

/// Request to get timer fires for an entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetTimerFiresRequest {
    /// Entity ID to query.
    pub entity_id: String,
}

/// Request to get pending timers for an entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetPendingTimersRequest {
    /// Entity ID to query.
    pub entity_id: String,
}

/// Request to cancel a timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelTimerRequest {
    /// Entity ID that owns the timer.
    pub entity_id: String,
    /// Timer ID to cancel.
    pub timer_id: String,
}

/// Request to clear timer fires (includes unique ID for deduplication).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClearFiresRequest {
    /// Entity ID to clear fires for.
    pub entity_id: String,
    /// Unique request ID to ensure each clear is a separate execution.
    pub request_id: String,
}

#[entity_impl]
impl TimerTest {
    /// Get the list of all fired timers.
    #[rpc]
    pub async fn get_timer_fires(
        &self,
        request: GetTimerFiresRequest,
    ) -> Result<Vec<TimerFire>, ClusterError> {
        let rows: Vec<(String, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT timer_id, scheduled_at, fired_at FROM timer_test_fires
             WHERE entity_id = $1
             ORDER BY fired_at",
        )
        .bind(&request.entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_timer_fires failed: {e}"),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(|(timer_id, scheduled_at, fired_at)| TimerFire {
                timer_id,
                scheduled_at,
                fired_at,
            })
            .collect())
    }

    /// Get the list of pending timers.
    #[rpc]
    pub async fn get_pending_timers(
        &self,
        request: GetPendingTimersRequest,
    ) -> Result<Vec<PendingTimer>, ClusterError> {
        let rows: Vec<(String, DateTime<Utc>, i64, bool)> = sqlx::query_as(
            "SELECT timer_id, scheduled_at, delay_ms, cancelled FROM timer_test_pending
             WHERE entity_id = $1
             ORDER BY scheduled_at",
        )
        .bind(&request.entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_pending_timers failed: {e}"),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(
                |(timer_id, scheduled_at, delay_ms, cancelled)| PendingTimer {
                    timer_id,
                    scheduled_at,
                    delay_ms,
                    cancelled,
                },
            )
            .collect())
    }

    /// Cancel a pending timer.
    ///
    /// Returns true if the timer was found and cancelled, false if not found
    /// or already cancelled/fired.
    #[rpc(persisted)]
    pub async fn cancel_timer(&self, request: CancelTimerRequest) -> Result<bool, ClusterError> {
        let result = sqlx::query(
            "UPDATE timer_test_pending SET cancelled = true
             WHERE entity_id = $1 AND timer_id = $2 AND cancelled = false",
        )
        .bind(&request.entity_id)
        .bind(&request.timer_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("cancel_timer failed: {e}"),
            source: None,
        })?;

        Ok(result.rows_affected() > 0)
    }

    /// Clear the timer fire history.
    #[rpc(persisted)]
    pub async fn clear_fires(&self, request: ClearFiresRequest) -> Result<(), ClusterError> {
        sqlx::query("DELETE FROM timer_test_fires WHERE entity_id = $1")
            .bind(&request.entity_id)
            .execute(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("clear_fires failed: {e}"),
                source: None,
            })?;
        Ok(())
    }
}

// ============================================================================
// ScheduleTimerWorkflow - standalone workflow with durable sleep
// ============================================================================

/// Request to schedule a timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduleTimerRequest {
    /// Entity ID that owns the timer.
    pub entity_id: String,
    /// Unique timer identifier.
    pub timer_id: String,
    /// Delay in milliseconds.
    pub delay_ms: u64,
}

/// Standalone workflow for scheduling a timer with durable sleep.
///
/// Creates a pending timer record, sleeps for the delay duration,
/// then checks if the timer was cancelled. If not cancelled, records
/// the timer fire and removes the pending record.
///
/// Activities use `ActivityScope::db()` for transactional DB writes — no `pool` field needed.
#[workflow]
#[derive(Clone)]
pub struct ScheduleTimerWorkflow;

#[workflow_impl(key = |req: &ScheduleTimerRequest| format!("{}/{}", req.entity_id, req.timer_id), hash = false)]
impl ScheduleTimerWorkflow {
    async fn execute(&self, request: ScheduleTimerRequest) -> Result<(), ClusterError> {
        let entity_id = request.entity_id.clone();
        let timer_id = request.timer_id.clone();
        let delay_ms = request.delay_ms;
        let scheduled_at = Utc::now();

        // Record the pending timer
        self.add_pending_timer(
            entity_id.clone(),
            timer_id.clone(),
            scheduled_at,
            delay_ms as i64,
        )
        .await?;

        // Durable sleep — workflow suspends, resumes after delay
        self.sleep(Duration::from_millis(delay_ms)).await?;

        // After sleep completes, check if timer was cancelled
        let was_cancelled = self
            .check_cancelled(entity_id.clone(), timer_id.clone())
            .await?;

        if !was_cancelled {
            // Record the fire and remove from pending
            let fired_at = Utc::now();
            self.record_timer_fire(entity_id, timer_id, scheduled_at, fired_at)
                .await?;
        } else {
            // Remove cancelled timer from pending
            self.remove_pending_timer(entity_id, timer_id).await?;
        }

        Ok(())
    }

    #[activity]
    async fn add_pending_timer(
        &self,
        entity_id: String,
        timer_id: String,
        scheduled_at: DateTime<Utc>,
        delay_ms: i64,
    ) -> Result<(), ClusterError> {
        let db = ActivityScope::db().await;
        db.execute(
            sqlx::query(
                "INSERT INTO timer_test_pending (entity_id, timer_id, scheduled_at, delay_ms, cancelled)
                 VALUES ($1, $2, $3, $4, false)
                 ON CONFLICT (entity_id, timer_id) DO NOTHING",
            )
            .bind(&entity_id)
            .bind(&timer_id)
            .bind(scheduled_at)
            .bind(delay_ms),
        )
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("add_pending_timer failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn check_cancelled(
        &self,
        entity_id: String,
        timer_id: String,
    ) -> Result<bool, ClusterError> {
        let db = ActivityScope::db().await;
        let row: Option<(bool,)> = db
            .fetch_optional(
                sqlx::query_as(
                    "SELECT cancelled FROM timer_test_pending
                     WHERE entity_id = $1 AND timer_id = $2",
                )
                .bind(&entity_id)
                .bind(&timer_id),
            )
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("check_cancelled failed: {e}"),
                source: None,
            })?;

        Ok(row.is_some_and(|(cancelled,)| cancelled))
    }

    #[activity]
    async fn record_timer_fire(
        &self,
        entity_id: String,
        timer_id: String,
        scheduled_at: DateTime<Utc>,
        fired_at: DateTime<Utc>,
    ) -> Result<(), ClusterError> {
        let db = ActivityScope::db().await;

        // Insert fire record
        db.execute(
            sqlx::query(
                "INSERT INTO timer_test_fires (entity_id, timer_id, scheduled_at, fired_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (entity_id, timer_id) DO NOTHING",
            )
            .bind(&entity_id)
            .bind(&timer_id)
            .bind(scheduled_at)
            .bind(fired_at),
        )
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("record_timer_fire failed: {e}"),
            source: None,
        })?;

        // Remove from pending
        db.execute(
            sqlx::query("DELETE FROM timer_test_pending WHERE entity_id = $1 AND timer_id = $2")
                .bind(&entity_id)
                .bind(&timer_id),
        )
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("record_timer_fire (cleanup) failed: {e}"),
            source: None,
        })?;

        Ok(())
    }

    #[activity]
    async fn remove_pending_timer(
        &self,
        entity_id: String,
        timer_id: String,
    ) -> Result<(), ClusterError> {
        let db = ActivityScope::db().await;
        db.execute(
            sqlx::query("DELETE FROM timer_test_pending WHERE entity_id = $1 AND timer_id = $2")
                .bind(&entity_id)
                .bind(&timer_id),
        )
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("remove_pending_timer failed: {e}"),
            source: None,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer_fire_serialization() {
        let fire = TimerFire {
            timer_id: "timer-1".to_string(),
            scheduled_at: Utc::now(),
            fired_at: Utc::now(),
        };

        let json = serde_json::to_string(&fire).unwrap();
        let parsed: TimerFire = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.timer_id, "timer-1");
    }

    #[test]
    fn test_pending_timer_serialization() {
        let pending = PendingTimer {
            timer_id: "timer-1".to_string(),
            scheduled_at: Utc::now(),
            delay_ms: 5000,
            cancelled: false,
        };

        let json = serde_json::to_string(&pending).unwrap();
        let parsed: PendingTimer = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.timer_id, "timer-1");
        assert_eq!(parsed.delay_ms, 5000);
        assert!(!parsed.cancelled);
    }

    #[test]
    fn test_schedule_timer_request_serialization() {
        let req = ScheduleTimerRequest {
            entity_id: "timer-entity-1".to_string(),
            timer_id: "t1".to_string(),
            delay_ms: 1000,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ScheduleTimerRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "timer-entity-1");
        assert_eq!(parsed.timer_id, "t1");
        assert_eq!(parsed.delay_ms, 1000);
    }

    #[test]
    fn test_cancel_timer_request_serialization() {
        let req = CancelTimerRequest {
            entity_id: "timer-entity-1".to_string(),
            timer_id: "t1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: CancelTimerRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "timer-entity-1");
        assert_eq!(parsed.timer_id, "t1");
    }

    #[test]
    fn test_clear_fires_request_serialization() {
        let req = ClearFiresRequest {
            entity_id: "timer-entity-1".to_string(),
            request_id: "clear-1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ClearFiresRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "timer-entity-1");
        assert_eq!(parsed.request_id, "clear-1");
    }
}
