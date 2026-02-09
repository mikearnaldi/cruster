//! KVStore entity - pure-RPC entity for testing key-value operations.
//!
//! This entity provides key-value operations to test:
//! - CRUD operations via persisted and non-persisted RPCs
//! - State managed directly in PostgreSQL (`kv_store_entries` table)
//! - Large value handling
//! - Many keys performance

use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// KVStore entity for testing key-value operations.
///
/// Uses the new stateless entity API — state is managed directly in PostgreSQL
/// via the `kv_store_entries` table rather than framework-managed state.
///
/// ## RPCs
/// - `set(key, value)` - Set a key to a value (persisted)
/// - `get(key)` - Get value for a key (non-persisted, read-only)
/// - `delete(key)` - Delete a key (persisted)
/// - `list_keys()` - List all keys (non-persisted, read-only)
/// - `clear()` - Clear all data (persisted)
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct KVStore {
    /// Database pool for direct state management.
    pub pool: PgPool,
}

/// Request to set a key-value pair.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetRequest {
    /// Entity identifier (used as the DB namespace).
    pub entity_id: String,
    /// Key to set.
    pub key: String,
    /// Value to set.
    pub value: serde_json::Value,
}

/// Request to get a value by key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetRequest {
    /// Entity identifier (used as the DB namespace).
    pub entity_id: String,
    /// Key to get.
    pub key: String,
}

/// Request to delete a key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Entity identifier (used as the DB namespace).
    pub entity_id: String,
    /// Key to delete.
    pub key: String,
}

/// Request to list all keys.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListKeysRequest {
    /// Entity identifier (used as the DB namespace).
    pub entity_id: String,
}

/// Request to clear all data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClearRequest {
    /// Entity identifier (used as the DB namespace).
    pub entity_id: String,
}

#[entity_impl]
impl KVStore {
    /// Set a key to a value.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn set(&self, request: SetRequest) -> Result<(), ClusterError> {
        let value_bytes = serde_json::to_vec(&request.value).map_err(|e| {
            ClusterError::MalformedMessage {
                reason: format!("failed to serialize value: {e}"),
                source: None,
            }
        })?;
        sqlx::query(
            "INSERT INTO kv_store_entries (entity_id, key, value)
             VALUES ($1, $2, $3)
             ON CONFLICT (entity_id, key)
             DO UPDATE SET value = $3",
        )
        .bind(&request.entity_id)
        .bind(&request.key)
        .bind(&value_bytes)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("set failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    /// Get the value for a key.
    ///
    /// Uses `#[rpc]` (non-persisted) since this is a read-only operation.
    #[rpc]
    pub async fn get(
        &self,
        request: GetRequest,
    ) -> Result<Option<serde_json::Value>, ClusterError> {
        let result: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT value FROM kv_store_entries WHERE entity_id = $1 AND key = $2",
        )
        .bind(&request.entity_id)
        .bind(&request.key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get failed: {e}"),
            source: None,
        })?;
        match result {
            Some((bytes,)) => {
                let value: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(|e| {
                        ClusterError::MalformedMessage {
                            reason: format!("failed to deserialize value: {e}"),
                            source: None,
                        }
                    })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Delete a key and return whether it existed.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn delete(&self, request: DeleteRequest) -> Result<bool, ClusterError> {
        let result = sqlx::query(
            "DELETE FROM kv_store_entries WHERE entity_id = $1 AND key = $2",
        )
        .bind(&request.entity_id)
        .bind(&request.key)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("delete failed: {e}"),
            source: None,
        })?;
        Ok(result.rows_affected() > 0)
    }

    /// List all keys.
    ///
    /// Uses `#[rpc]` (non-persisted) since this is a read-only operation.
    #[rpc]
    pub async fn list_keys(&self, request: ListKeysRequest) -> Result<Vec<String>, ClusterError> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT key FROM kv_store_entries WHERE entity_id = $1 ORDER BY key")
                .bind(&request.entity_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("list_keys failed: {e}"),
                    source: None,
                })?;
        Ok(rows.into_iter().map(|(k,)| k).collect())
    }

    /// Clear all data.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn clear(&self, request: ClearRequest) -> Result<(), ClusterError> {
        sqlx::query("DELETE FROM kv_store_entries WHERE entity_id = $1")
            .bind(&request.entity_id)
            .execute(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("clear failed: {e}"),
                source: None,
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_request_serialization() {
        let req = SetRequest {
            entity_id: "kv-1".to_string(),
            key: "key1".to_string(),
            value: serde_json::json!("value1"),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: SetRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "kv-1");
        assert_eq!(parsed.key, "key1");
        assert_eq!(parsed.value, serde_json::json!("value1"));
    }

    #[test]
    fn test_get_request_serialization() {
        let req = GetRequest {
            entity_id: "kv-1".to_string(),
            key: "key1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: GetRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "kv-1");
        assert_eq!(parsed.key, "key1");
    }

    #[test]
    fn test_delete_request_serialization() {
        let req = DeleteRequest {
            entity_id: "kv-1".to_string(),
            key: "key1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: DeleteRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "kv-1");
        assert_eq!(parsed.key, "key1");
    }

    #[test]
    fn test_list_keys_request_serialization() {
        let req = ListKeysRequest {
            entity_id: "kv-1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ListKeysRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "kv-1");
    }

    #[test]
    fn test_clear_request_serialization() {
        let req = ClearRequest {
            entity_id: "kv-1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ClearRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "kv-1");
    }
}
