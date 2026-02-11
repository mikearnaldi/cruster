//! Counter entity - pure-RPC entity for testing basic cluster operations.
//!
//! This entity provides basic counter operations to test:
//! - Persisted RPCs (at-least-once delivery)
//! - Read-only RPCs (best-effort delivery)
//! - State persistence via PostgreSQL (entity manages its own state)
//! - State survival after entity eviction and reload

use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Counter entity for testing basic cluster operations.
///
/// Uses the new stateless entity API — state is managed directly in PostgreSQL
/// via the `counter_values` table rather than framework-managed state.
///
/// ## RPCs
/// - `increment(amount)` - Add to counter, return new value (persisted)
/// - `decrement(amount)` - Subtract from counter, return new value (persisted)
/// - `get()` - Get current value (non-persisted, read-only)
/// - `reset()` - Reset to zero (persisted)
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct Counter {
    /// Database pool for direct state management.
    pub pool: PgPool,
}

/// Request to increment the counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IncrementRequest {
    /// Amount to increment by.
    pub amount: i64,
}

/// Request to decrement the counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecrementRequest {
    /// Amount to decrement by.
    pub amount: i64,
}

/// Request to get the counter value.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetCounterRequest {}

/// Request to reset the counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResetCounterRequest {}

#[entity_impl]
impl Counter {
    /// Increment the counter by the given amount and return the new value.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn increment(&self, request: IncrementRequest) -> Result<i64, ClusterError> {
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO counter_values (entity_id, value)
             VALUES ($1, $2)
             ON CONFLICT (entity_id)
             DO UPDATE SET value = counter_values.value + $2
             RETURNING value",
        )
        .bind(self.entity_id())
        .bind(request.amount)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("increment failed: {e}"),
            source: None,
        })?;
        Ok(row.0)
    }

    /// Decrement the counter by the given amount and return the new value.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn decrement(&self, request: DecrementRequest) -> Result<i64, ClusterError> {
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO counter_values (entity_id, value)
             VALUES ($1, -$2)
             ON CONFLICT (entity_id)
             DO UPDATE SET value = counter_values.value - $2
             RETURNING value",
        )
        .bind(self.entity_id())
        .bind(request.amount)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("decrement failed: {e}"),
            source: None,
        })?;
        Ok(row.0)
    }

    /// Get the current counter value.
    ///
    /// Uses `#[rpc]` (non-persisted) since this is a read-only operation.
    #[rpc]
    pub async fn get(&self, _request: GetCounterRequest) -> Result<i64, ClusterError> {
        let result: Option<(i64,)> =
            sqlx::query_as("SELECT value FROM counter_values WHERE entity_id = $1")
                .bind(self.entity_id())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("get failed: {e}"),
                    source: None,
                })?;
        Ok(result.map(|r| r.0).unwrap_or(0))
    }

    /// Reset the counter to zero.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn reset(&self, _request: ResetCounterRequest) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO counter_values (entity_id, value)
             VALUES ($1, 0)
             ON CONFLICT (entity_id)
             DO UPDATE SET value = 0",
        )
        .bind(self.entity_id())
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("reset failed: {e}"),
            source: None,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_request_serialization() {
        let req = IncrementRequest { amount: 42 };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: IncrementRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.amount, 42);
    }

    #[test]
    fn test_decrement_request_serialization() {
        let req = DecrementRequest { amount: 10 };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: DecrementRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.amount, 10);
    }

    #[test]
    fn test_get_request_serialization() {
        let req = GetCounterRequest {};
        let json = serde_json::to_string(&req).unwrap();
        let parsed: GetCounterRequest = serde_json::from_str(&json).unwrap();
        let _ = parsed; // empty struct, just verify round-trip
    }

    #[test]
    fn test_reset_request_serialization() {
        let req = ResetCounterRequest {};
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ResetCounterRequest = serde_json::from_str(&json).unwrap();
        let _ = parsed; // empty struct, just verify round-trip
    }
}
