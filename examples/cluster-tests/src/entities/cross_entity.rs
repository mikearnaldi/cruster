//! CrossEntity - entity for testing entity-to-entity communication.
//!
//! This entity tests:
//! - Entity can call another entity
//! - Circular calls handled (A -> B -> A)
//! - Cross-shard communication works

use chrono::{DateTime, Utc};
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

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

/// State for a CrossEntity entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CrossEntityState {
    /// Messages received from other entities.
    pub received_messages: Vec<Message>,
    /// Counter used for ping-pong tracking.
    pub ping_count: u32,
}

/// CrossEntity for testing entity-to-entity communication.
///
/// ## State (Persisted)
/// - received_messages: `Vec<Message>` - messages received from other entities
/// - ping_count: u32 - counter for ping-pong tracking
///
/// ## RPCs
/// - `receive(from, message)` - Receive a message from another entity
/// - `get_messages()` - Get all received messages
/// - `clear_messages()` - Clear all received messages
/// - `ping(count)` - Receive a ping and return count for pong
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct CrossEntity;

/// Request to receive a message from another entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceiveRequest {
    /// The entity that sent the message.
    pub from: String,
    /// The message content.
    pub message: String,
}

/// Request for ping operation in ping-pong.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PingRequest {
    /// Current ping count.
    pub count: u32,
}

#[entity_impl]
#[state(CrossEntityState)]
impl CrossEntity {
    fn init(&self, _ctx: &EntityContext) -> Result<CrossEntityState, ClusterError> {
        Ok(CrossEntityState::default())
    }

    #[activity]
    async fn do_receive(&mut self, from: String, message: String) -> Result<(), ClusterError> {
        self.state.received_messages.push(Message {
            from_entity: from,
            content: message,
            timestamp: Utc::now(),
        });
        Ok(())
    }

    #[activity]
    async fn do_clear_messages(&mut self) -> Result<(), ClusterError> {
        self.state.received_messages.clear();
        Ok(())
    }

    #[activity]
    async fn do_ping(&mut self, count: u32) -> Result<u32, ClusterError> {
        self.state.ping_count = count;
        Ok(count)
    }

    #[activity]
    async fn do_reset_ping_count(&mut self) -> Result<(), ClusterError> {
        self.state.ping_count = 0;
        Ok(())
    }

    /// Receive a message from another entity.
    #[workflow]
    pub async fn receive(&self, request: ReceiveRequest) -> Result<(), ClusterError> {
        self.do_receive(request.from, request.message).await
    }

    /// Get all received messages.
    #[rpc]
    pub async fn get_messages(&self) -> Result<Vec<Message>, ClusterError> {
        Ok(self.state.received_messages.clone())
    }

    /// Clear all received messages.
    #[workflow]
    pub async fn clear_messages(&self) -> Result<(), ClusterError> {
        self.do_clear_messages().await
    }

    /// Handle a ping and return the count for pong.
    ///
    /// This is used in the ping-pong sequence. The entity receives
    /// a ping with a count, increments its internal counter, and
    /// returns the count.
    #[workflow]
    pub async fn ping(&self, request: PingRequest) -> Result<u32, ClusterError> {
        self.do_ping(request.count).await
    }

    /// Get the current ping count.
    #[rpc]
    pub async fn get_ping_count(&self) -> Result<u32, ClusterError> {
        Ok(self.state.ping_count)
    }

    /// Reset the ping count.
    #[workflow]
    pub async fn reset_ping_count(&self) -> Result<(), ClusterError> {
        self.do_reset_ping_count().await
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
    fn test_cross_entity_state_default() {
        let state = CrossEntityState::default();
        assert!(state.received_messages.is_empty());
        assert_eq!(state.ping_count, 0);
    }

    #[test]
    fn test_cross_entity_state_serialization() {
        let mut state = CrossEntityState::default();
        state.received_messages.push(Message {
            from_entity: "e1".to_string(),
            content: "test".to_string(),
            timestamp: Utc::now(),
        });
        state.ping_count = 5;

        let json = serde_json::to_string(&state).unwrap();
        let parsed: CrossEntityState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.received_messages.len(), 1);
        assert_eq!(parsed.received_messages[0].from_entity, "e1");
        assert_eq!(parsed.ping_count, 5);
    }

    #[test]
    fn test_receive_request_serialization() {
        let req = ReceiveRequest {
            from: "sender".to_string(),
            message: "hello".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: ReceiveRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.from, "sender");
        assert_eq!(parsed.message, "hello");
    }

    #[test]
    fn test_ping_request_serialization() {
        let req = PingRequest { count: 42 };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: PingRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.count, 42);
    }
}
