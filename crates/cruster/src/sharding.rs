use crate::entity::Entity;
use crate::entity_client::EntityClient;
use crate::envelope::{AckChunk, EnvelopeRequest, Interrupt};
use crate::error::ClusterError;
use crate::message::ReplyReceiver;
use crate::reply::Reply;
use crate::singleton::SingletonContext;
use crate::snowflake::{Snowflake, SnowflakeGenerator};
use crate::types::{EntityId, EntityType, ShardId};
use async_trait::async_trait;
use futures::future::BoxFuture;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;

/// Events emitted when entities or singletons are registered.
#[derive(Debug, Clone)]
pub enum ShardingRegistrationEvent {
    EntityRegistered { entity_type: EntityType },
    SingletonRegistered { name: String },
}

/// Core orchestrator trait for the cluster sharding system.
///
/// Manages shard assignment, entity lifecycle, and message routing.
/// This is the central interface through which all cluster operations flow.
#[async_trait]
pub trait Sharding: Send + Sync {
    /// Get shard ID for an entity based on consistent hashing.
    fn get_shard_id(&self, entity_type: &EntityType, entity_id: &EntityId) -> ShardId;

    /// Check if this runner owns the given shard.
    ///
    /// Note: This is a synchronous method. The `ShardingImpl` implementation may
    /// return `false` during brief lock contention (e.g., during rebalance writes).
    /// For routing decisions, prefer the async `send()`/`notify()` methods which
    /// use an async lock internally.
    fn has_shard_id(&self, shard_id: &ShardId) -> bool;

    /// Get the snowflake ID generator.
    fn snowflake(&self) -> &SnowflakeGenerator;

    /// Whether this runner is shutting down.
    fn is_shutdown(&self) -> bool;

    /// Register an entity type with its definition and handler factory.
    async fn register_entity(&self, entity: Arc<dyn Entity>) -> Result<(), ClusterError>;

    /// Register a singleton that runs on exactly one runner in the cluster.
    ///
    /// The factory is reusable: it will be called each time the singleton needs
    /// to (re)start, e.g. after a shard round-trip during rebalancing.
    ///
    /// The factory receives a [`SingletonContext`] containing a cancellation token
    /// that is triggered when the singleton should shut down gracefully (e.g., when
    /// the shard moves to another runner or the runner is shutting down).
    ///
    /// `shard_group` specifies the shard group for ownership computation.
    /// Pass `None` to use the default group (`"default"`).
    async fn register_singleton(
        &self,
        name: &str,
        shard_group: Option<&str>,
        run: Arc<
            dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>> + Send + Sync,
        >,
    ) -> Result<(), ClusterError>;

    /// Create a client for an entity type.
    fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient;

    /// Send an envelope to the appropriate runner and await a reply.
    async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError>;

    /// Send a fire-and-forget notification.
    async fn notify(&self, envelope: EnvelopeRequest) -> Result<(), ClusterError>;

    /// Acknowledge a streamed reply chunk.
    async fn ack_chunk(&self, ack: AckChunk) -> Result<(), ClusterError>;

    /// Request interrupt of an entity.
    async fn interrupt(&self, interrupt: Interrupt) -> Result<(), ClusterError>;

    /// Force re-read from storage.
    async fn poll_storage(&self) -> Result<(), ClusterError>;

    /// Number of active entity instances across all entity types.
    fn active_entity_count(&self) -> usize;

    /// Subscribe to registration events.
    async fn registration_events(
        &self,
    ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>>;

    /// Query stored replies for a given request ID.
    ///
    /// Used by workflow clients to poll for the result of a previously-started
    /// workflow execution. Returns an empty vector if no storage is configured
    /// or no replies exist.
    async fn replies_for(&self, request_id: Snowflake) -> Result<Vec<Reply>, ClusterError> {
        let _ = request_id;
        Ok(vec![])
    }

    /// Graceful shutdown.
    async fn shutdown(&self) -> Result<(), ClusterError>;
}
