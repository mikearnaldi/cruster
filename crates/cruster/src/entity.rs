use crate::error::ClusterError;
use crate::sharding::Sharding;
use crate::snowflake::SnowflakeGenerator;
use crate::types::{EntityAddress, EntityId, EntityType, RunnerAddress};
use async_trait::async_trait;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::Stream;

/// Context provided to entity instances when they are spawned.
#[derive(Clone)]
pub struct EntityContext {
    /// The address of this entity instance.
    pub address: EntityAddress,
    /// The address of the runner hosting this entity.
    pub runner_address: RunnerAddress,
    /// Shared snowflake ID generator.
    pub snowflake: Arc<SnowflakeGenerator>,
    /// Cancellation token for this entity's lifetime.
    pub cancellation: tokio_util::sync::CancellationToken,
    /// Optional key-value storage for persisted entity state.
    ///
    /// When present, entity macros with `#[state(Type)]` will load
    /// state from this storage on spawn and save after every `#[activity]` handler
    /// call. The storage key is `"entity/{entity_type}/{entity_id}/state"`.
    pub state_storage: Option<Arc<dyn crate::__internal::WorkflowStorage>>,
    /// Optional workflow engine for durable context support.
    ///
    /// When present, entity methods with `&DurableContext` parameters can use
    /// durable sleep, await_deferred, resolve_deferred, and on_interrupt operations.
    /// The macro generates code to construct a `DurableContext` from this engine.
    pub workflow_engine: Option<Arc<dyn crate::__internal::WorkflowEngine>>,
    /// Optional sharding interface for inter-entity communication.
    ///
    /// When present, entities can create clients to send messages to other entities
    /// or to themselves, including scheduled messages via `notify_at`.
    pub sharding: Option<Arc<dyn Sharding>>,
    /// Optional message storage for activity journaling.
    ///
    /// When present, activities called from within `#[workflow]` methods are
    /// journaled: their results are cached in `MessageStorage` so that on
    /// crash-recovery replay the cached result is returned instead of
    /// re-executing the activity body.
    pub message_storage: Option<Arc<dyn crate::message_storage::MessageStorage>>,
}

/// Defines an entity type with its RPCs and behavior.
///
/// Users implement this trait to define an entity. Each entity type has a unique
/// name and a factory method (`spawn`) that creates handler instances for
/// individual entity IDs.
#[async_trait]
pub trait Entity: Send + Sync + 'static {
    /// Unique type name for this entity (e.g., "User", "Order").
    fn entity_type(&self) -> EntityType;

    /// Shard group this entity belongs to. Default: "default".
    fn shard_group(&self) -> &str {
        "default"
    }

    /// Resolve shard group from entity ID. Override for custom routing.
    fn shard_group_for(&self, _entity_id: &EntityId) -> &str {
        self.shard_group()
    }

    /// Maximum idle time before reaping. None = use config default.
    fn max_idle_time(&self) -> Option<Duration> {
        None
    }

    /// Mailbox capacity. None = use config default.
    fn mailbox_capacity(&self) -> Option<usize> {
        None
    }

    /// Maximum number of concurrent requests this entity can handle.
    /// `None` = use config default (`entity_max_concurrent_requests`).
    /// `Some(0)` = unbounded concurrency. `Some(1)` = serial (default behavior).
    /// `Some(n)` = at most `n` concurrent requests.
    ///
    /// When concurrency > 1, the handler must be safe for concurrent access
    /// (which is guaranteed by the `Send + Sync` bound on `EntityHandler`).
    /// Crash recovery under concurrency > 1 will replay ALL in-flight requests
    /// against the new handler.
    fn concurrency(&self) -> Option<usize> {
        None
    }

    /// Create a handler instance for the given entity address.
    /// The returned handler lives for the lifetime of the entity instance.
    async fn spawn(&self, ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError>;
}

/// Handles incoming RPCs for a specific entity instance.
///
/// Each entity instance has one handler that processes all incoming messages.
/// The handler is created by `Entity::spawn` and lives until the entity is
/// reaped (idle timeout) or the runner shuts down.
#[async_trait]
pub trait EntityHandler: Send + Sync {
    /// Handle an incoming request. Returns serialized response bytes.
    async fn handle_request(
        &self,
        tag: &str,
        payload: &[u8],
        headers: &HashMap<String, String>,
    ) -> Result<Vec<u8>, ClusterError>;

    /// Handle a streaming request. Returns a stream of serialized chunks.
    ///
    /// Default implementation wraps `handle_request` as a single-item stream.
    async fn handle_stream(
        &self,
        tag: &str,
        payload: &[u8],
        headers: &HashMap<String, String>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, ClusterError>> + Send>>, ClusterError>
    {
        let result = self.handle_request(tag, payload, headers).await?;
        Ok(Box::pin(tokio_stream::once(Ok(result))))
    }

    /// Called when the entity is about to be reaped (idle timeout).
    /// Return true to keep alive, false to allow reaping.
    async fn on_idle(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EntityId, EntityType, ShardId};

    /// A mock entity for testing that the trait compiles and works.
    struct CounterEntity;

    #[async_trait]
    impl Entity for CounterEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Counter")
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(CounterHandler))
        }
    }

    struct CounterHandler;

    #[async_trait]
    impl EntityHandler for CounterHandler {
        async fn handle_request(
            &self,
            tag: &str,
            _payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            match tag {
                "increment" => Ok(rmp_serde::to_vec(&1i32).unwrap()),
                _ => Err(ClusterError::MalformedMessage {
                    reason: format!("unknown tag: {tag}"),
                    source: None,
                }),
            }
        }
    }

    #[test]
    fn default_shard_group_returns_default() {
        let entity = CounterEntity;
        assert_eq!(entity.shard_group(), "default");
    }

    #[test]
    fn default_shard_group_for_delegates() {
        let entity = CounterEntity;
        let id = EntityId::new("some-id");
        assert_eq!(entity.shard_group_for(&id), "default");
    }

    #[test]
    fn default_max_idle_time_is_none() {
        let entity = CounterEntity;
        assert!(entity.max_idle_time().is_none());
    }

    #[test]
    fn default_mailbox_capacity_is_none() {
        let entity = CounterEntity;
        assert!(entity.mailbox_capacity().is_none());
    }

    /// Custom entity that overrides defaults.
    struct CustomEntity;

    #[async_trait]
    impl Entity for CustomEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Custom")
        }

        fn shard_group(&self) -> &str {
            "premium"
        }

        fn max_idle_time(&self) -> Option<Duration> {
            Some(Duration::from_secs(120))
        }

        fn mailbox_capacity(&self) -> Option<usize> {
            Some(50)
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(CounterHandler))
        }
    }

    #[test]
    fn custom_shard_group() {
        let entity = CustomEntity;
        assert_eq!(entity.shard_group(), "premium");
        assert_eq!(entity.shard_group_for(&EntityId::new("x")), "premium");
    }

    #[test]
    fn custom_max_idle_time() {
        let entity = CustomEntity;
        assert_eq!(entity.max_idle_time(), Some(Duration::from_secs(120)));
    }

    #[test]
    fn custom_mailbox_capacity() {
        let entity = CustomEntity;
        assert_eq!(entity.mailbox_capacity(), Some(50));
    }

    #[tokio::test]
    async fn spawn_and_handle_request() {
        let entity = CounterEntity;
        let ctx = EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Counter"),
                entity_id: EntityId::new("c-1"),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: None,
            workflow_engine: None,
            sharding: None,
            message_storage: None,
        };
        let handler = entity.spawn(ctx).await.unwrap();
        let result = handler
            .handle_request("increment", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn handle_unknown_tag_returns_error() {
        let entity = CounterEntity;
        let ctx = EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Counter"),
                entity_id: EntityId::new("c-1"),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: None,
            workflow_engine: None,
            sharding: None,
            message_storage: None,
        };
        let handler = entity.spawn(ctx).await.unwrap();
        let err = handler
            .handle_request("unknown", &[], &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ClusterError::MalformedMessage { .. }));
    }

    #[tokio::test]
    async fn default_handle_stream_wraps_request() {
        use tokio_stream::StreamExt;

        let entity = CounterEntity;
        let ctx = EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Counter"),
                entity_id: EntityId::new("c-1"),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: None,
            workflow_engine: None,
            sharding: None,
            message_storage: None,
        };
        let handler = entity.spawn(ctx).await.unwrap();
        let mut stream = handler
            .handle_stream("increment", &[], &HashMap::new())
            .await
            .unwrap();

        let first = stream.next().await.unwrap().unwrap();
        let value: i32 = rmp_serde::from_slice(&first).unwrap();
        assert_eq!(value, 1);

        // Stream should be exhausted
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn default_on_idle_returns_false() {
        let handler = CounterHandler;
        assert!(!handler.on_idle().await);
    }
}
