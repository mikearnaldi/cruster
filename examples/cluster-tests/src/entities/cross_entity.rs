//! CrossEntity - entity for testing entity-to-entity communication.
//!
//! Migrated from old stateful entity API to new pure-RPC entity.
//!
//! This entity tests:
//! - Entity can call another entity
//! - Circular calls handled (A -> B -> A)
//! - Cross-shard communication works
//!
//! All state is stored directly in PostgreSQL tables:
//! - `cross_entity_messages` — messages received from other entities
//! - `cross_entity_ping_counts` — ping-pong counter state

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// A message received from another entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// The entity ID that sent the message.
    pub from_entity: String,
    /// The content of the message.
    pub content: String,
    /// When the message was received.
    pub timestamp: DateTime<Utc>,
}

/// CrossEntity for testing entity-to-entity communication.
///
/// Uses the new stateless entity API — state is managed directly in PostgreSQL.
///
/// ## RPCs (persisted, writes)
/// - `receive(from, message)` - Receive a message from another entity
/// - `clear_messages()` - Clear all received messages
/// - `ping(count)` - Receive a ping and return count for pong
/// - `reset_ping_count()` - Reset the ping counter
///
/// ## RPCs (non-persisted, reads)
/// - `get_messages()` - Get all received messages
/// - `get_ping_count()` - Get the current ping count
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct CrossEntity {
    /// Database pool for direct state management.
    pub pool: PgPool,
}

/// Request to receive a message from another entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceiveRequest {
    /// Entity ID of the recipient (used as the DB key).
    pub entity_id: String,
    /// The entity that sent the message.
    pub from: String,
    /// The message content.
    pub message: String,
}

/// Request for ping operation in ping-pong.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PingRequest {
    /// Entity ID of the recipient (used as the DB key).
    pub entity_id: String,
    /// Current ping count.
    pub count: u32,
}

/// Request to clear messages.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClearMessagesRequest {
    /// Entity ID whose messages to clear.
    pub entity_id: String,
}

/// Request to reset ping count.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResetPingCountRequest {
    /// Entity ID whose ping count to reset.
    pub entity_id: String,
}

/// Request to get messages.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetMessagesRequest {
    /// Entity ID to get messages for.
    pub entity_id: String,
}

/// Request to get ping count.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetPingCountRequest {
    /// Entity ID to get ping count for.
    pub entity_id: String,
}

#[entity_impl]
impl CrossEntity {
    /// Receive a message from another entity.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn receive(&self, request: ReceiveRequest) -> Result<(), ClusterError> {
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO cross_entity_messages (entity_id, from_entity, content, timestamp)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&request.entity_id)
        .bind(&request.from)
        .bind(&request.message)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("receive failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    /// Get all received messages.
    ///
    /// Uses `#[rpc]` (non-persisted) since this is a read-only operation.
    #[rpc]
    pub async fn get_messages(
        &self,
        request: GetMessagesRequest,
    ) -> Result<Vec<Message>, ClusterError> {
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT from_entity, content, timestamp FROM cross_entity_messages
             WHERE entity_id = $1
             ORDER BY timestamp ASC",
        )
        .bind(&request.entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_messages failed: {e}"),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(|(from_entity, content, timestamp)| Message {
                from_entity,
                content,
                timestamp,
            })
            .collect())
    }

    /// Clear all received messages.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn clear_messages(&self, request: ClearMessagesRequest) -> Result<(), ClusterError> {
        sqlx::query("DELETE FROM cross_entity_messages WHERE entity_id = $1")
            .bind(&request.entity_id)
            .execute(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("clear_messages failed: {e}"),
                source: None,
            })?;
        Ok(())
    }

    /// Handle a ping and return the count for pong.
    ///
    /// This is used in the ping-pong sequence. The entity receives
    /// a ping with a count, stores it, and returns the count.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn ping(&self, request: PingRequest) -> Result<u32, ClusterError> {
        sqlx::query(
            "INSERT INTO cross_entity_ping_counts (entity_id, ping_count)
             VALUES ($1, $2)
             ON CONFLICT (entity_id)
             DO UPDATE SET ping_count = $2",
        )
        .bind(&request.entity_id)
        .bind(request.count as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("ping failed: {e}"),
            source: None,
        })?;
        Ok(request.count)
    }

    /// Get the current ping count.
    ///
    /// Uses `#[rpc]` (non-persisted) since this is a read-only operation.
    #[rpc]
    pub async fn get_ping_count(&self, request: GetPingCountRequest) -> Result<u32, ClusterError> {
        let result: Option<(i32,)> =
            sqlx::query_as("SELECT ping_count FROM cross_entity_ping_counts WHERE entity_id = $1")
                .bind(&request.entity_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("get_ping_count failed: {e}"),
                    source: None,
                })?;
        Ok(result.map(|r| r.0 as u32).unwrap_or(0))
    }

    /// Reset the ping count.
    ///
    /// Uses `#[rpc(persisted)]` for at-least-once delivery (writes).
    #[rpc(persisted)]
    pub async fn reset_ping_count(
        &self,
        request: ResetPingCountRequest,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO cross_entity_ping_counts (entity_id, ping_count)
             VALUES ($1, 0)
             ON CONFLICT (entity_id)
             DO UPDATE SET ping_count = 0",
        )
        .bind(&request.entity_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("reset_ping_count failed: {e}"),
            source: None,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let msg = Message {
            from_entity: "entity-1".to_string(),
            content: "hello".to_string(),
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let parsed: Message = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.from_entity, "entity-1");
        assert_eq!(parsed.content, "hello");
    }

    #[test]
    fn test_receive_request_serialization() {
        let req = ReceiveRequest {
            entity_id: "cross-1".to_string(),
            from: "sender".to_string(),
            message: "hello".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: ReceiveRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_id, "cross-1");
        assert_eq!(parsed.from, "sender");
        assert_eq!(parsed.message, "hello");
    }

    #[test]
    fn test_ping_request_serialization() {
        let req = PingRequest {
            entity_id: "cross-1".to_string(),
            count: 42,
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: PingRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_id, "cross-1");
        assert_eq!(parsed.count, 42);
    }

    #[test]
    fn test_clear_messages_request_serialization() {
        let req = ClearMessagesRequest {
            entity_id: "cross-1".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: ClearMessagesRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_id, "cross-1");
    }

    #[test]
    fn test_reset_ping_count_request_serialization() {
        let req = ResetPingCountRequest {
            entity_id: "cross-1".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: ResetPingCountRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_id, "cross-1");
    }

    #[test]
    fn test_get_messages_request_serialization() {
        let req = GetMessagesRequest {
            entity_id: "cross-1".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: GetMessagesRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_id, "cross-1");
    }

    #[test]
    fn test_get_ping_count_request_serialization() {
        let req = GetPingCountRequest {
            entity_id: "cross-1".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: GetPingCountRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_id, "cross-1");
    }
}
