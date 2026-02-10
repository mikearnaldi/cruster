//! StatelessCounter entity - pure-RPC entity using the new stateless API.
//!
//! This entity tests the new pure-RPC entity pattern where:
//! - No `#[state]`, no `#[workflow]`, no `#[activity]` are used
//! - State is managed directly via PostgreSQL
//! - All methods are `#[rpc]` or `#[rpc(persisted)]`
//!
//! The counter value lives in a `stateless_counter_values` table,
//! demonstrating that entities can manage their own persistence.
//!
//! Since pure-RPC entity methods only receive `&self` and request parameters
//! (no `EntityContext`), the entity ID is passed through request structs.

use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// StatelessCounter entity - a pure-RPC entity that manages state in PostgreSQL directly.
///
/// ## RPCs
/// - `increment(amount)` - Add to counter, return new value (persisted)
/// - `decrement(amount)` - Subtract from counter, return new value (persisted)
/// - `get(entity_id)` - Get current value (non-persisted, read-only)
/// - `reset()` - Reset to zero (persisted)
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct StatelessCounter {
    /// Database pool for direct state management.
    pub pool: PgPool,
}

/// Request to increment the stateless counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatelessIncrementRequest {
    /// Entity identifier (used as the DB key).
    pub entity_id: String,
    /// Amount to increment by.
    pub amount: i64,
}

/// Request to decrement the stateless counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatelessDecrementRequest {
    /// Entity identifier (used as the DB key).
    pub entity_id: String,
    /// Amount to decrement by.
    pub amount: i64,
}

/// Request to get the stateless counter value.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatelessGetRequest {
    /// Entity identifier (used as the DB key).
    pub entity_id: String,
}

/// Request to reset the stateless counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatelessResetRequest {
    /// Entity identifier (used as the DB key).
    pub entity_id: String,
}

#[entity_impl]
impl StatelessCounter {
    /// Increment the counter by the given amount and return the new value.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn increment(&self, request: StatelessIncrementRequest) -> Result<i64, ClusterError> {
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO stateless_counter_values (entity_id, value)
             VALUES ($1, $2)
             ON CONFLICT (entity_id)
             DO UPDATE SET value = stateless_counter_values.value + $2
             RETURNING value",
        )
        .bind(&request.entity_id)
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
    pub async fn decrement(&self, request: StatelessDecrementRequest) -> Result<i64, ClusterError> {
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO stateless_counter_values (entity_id, value)
             VALUES ($1, -$2)
             ON CONFLICT (entity_id)
             DO UPDATE SET value = stateless_counter_values.value - $2
             RETURNING value",
        )
        .bind(&request.entity_id)
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
    pub async fn get(&self, request: StatelessGetRequest) -> Result<i64, ClusterError> {
        let result: Option<(i64,)> =
            sqlx::query_as("SELECT value FROM stateless_counter_values WHERE entity_id = $1")
                .bind(&request.entity_id)
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
    pub async fn reset(&self, request: StatelessResetRequest) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO stateless_counter_values (entity_id, value)
             VALUES ($1, 0)
             ON CONFLICT (entity_id)
             DO UPDATE SET value = 0",
        )
        .bind(&request.entity_id)
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
        let req = StatelessIncrementRequest {
            entity_id: "counter-1".to_string(),
            amount: 42,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: StatelessIncrementRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "counter-1");
        assert_eq!(parsed.amount, 42);
    }

    #[test]
    fn test_decrement_request_serialization() {
        let req = StatelessDecrementRequest {
            entity_id: "counter-1".to_string(),
            amount: 10,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: StatelessDecrementRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "counter-1");
        assert_eq!(parsed.amount, 10);
    }

    #[test]
    fn test_get_request_serialization() {
        let req = StatelessGetRequest {
            entity_id: "counter-1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: StatelessGetRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "counter-1");
    }

    #[test]
    fn test_reset_request_serialization() {
        let req = StatelessResetRequest {
            entity_id: "counter-1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: StatelessResetRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "counter-1");
    }
}
