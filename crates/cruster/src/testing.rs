//! In-memory test cluster for unit and integration testing.
//!
//! Provides a single-node cluster with in-memory storage, noop runners,
//! and noop health checks. All shards are immediately owned by the local runner.

use std::sync::Arc;

use crate::config::ShardingConfig;
use crate::durable::{MemoryWorkflowEngine, MemoryWorkflowStorage, WorkflowEngine};
use crate::entity::Entity;
use crate::entity_client::EntityClient;
use crate::error::ClusterError;
use crate::metrics::ClusterMetrics;
use crate::sharding::Sharding;
use crate::sharding_impl::ShardingImpl;
use crate::storage::memory_message::MemoryMessageStorage;
use crate::storage::noop_runners::NoopRunners;

/// A single-node in-memory cluster for testing.
///
/// Wraps a [`ShardingImpl`] with in-memory storage and noop runners.
/// All configured shards are immediately owned, so entities can be
/// registered and messaged without any external dependencies.
///
/// # Example
///
/// ```text
/// let cluster = TestCluster::new().await;
/// let client = cluster.register(MyEntity).await.unwrap();
/// let response: String = client.send(&EntityId::new("e-1"), "greet", &"hello").await.unwrap();
/// cluster.shutdown().await.unwrap();
/// ```
pub struct TestCluster {
    sharding: Arc<ShardingImpl>,
    config: Arc<ShardingConfig>,
}

impl TestCluster {
    /// Create a single-node in-memory cluster with default configuration.
    pub async fn new() -> Self {
        Self::with_config(ShardingConfig::default()).await
    }

    /// Create a single-node in-memory cluster with custom configuration.
    pub async fn with_config(config: ShardingConfig) -> Self {
        let config = Arc::new(config);
        let runners = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let sharding = ShardingImpl::new(Arc::clone(&config), runners, None, None, None, metrics)
            .expect("TestCluster config should be valid");
        sharding.acquire_all_shards().await;

        Self { sharding, config }
    }

    /// Create a single-node in-memory cluster with message storage enabled.
    ///
    /// This is required for testing entities with `#[workflow]` (persisted) methods.
    pub async fn with_message_storage() -> Self {
        Self::with_message_storage_and_config(ShardingConfig::default()).await
    }

    /// Create a single-node in-memory cluster with message storage and custom configuration.
    ///
    /// This is required for testing entities with `#[workflow]` (persisted) methods.
    pub async fn with_message_storage_and_config(config: ShardingConfig) -> Self {
        let config = Arc::new(config);
        let runners = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let sharding = ShardingImpl::new(
            Arc::clone(&config),
            runners,
            None,
            None,
            Some(storage),
            metrics,
        )
        .expect("TestCluster config should be valid");
        sharding.acquire_all_shards().await;

        Self { sharding, config }
    }

    /// Create a single-node in-memory cluster with full workflow support.
    ///
    /// This sets up:
    /// - In-memory message storage for at-least-once delivery
    /// - In-memory workflow storage for persisting entity state
    /// - In-memory workflow engine for `DurableContext` operations (sleep, await_deferred, etc.)
    ///
    /// Use this method when testing entities that use `#[workflow]` methods with `DurableContext`.
    pub async fn with_workflow_support() -> Self {
        Self::with_workflow_support_and_config(ShardingConfig::default()).await
    }

    /// Create a single-node in-memory cluster with full workflow support and custom configuration.
    ///
    /// See [`with_workflow_support`](Self::with_workflow_support) for details.
    pub async fn with_workflow_support_and_config(config: ShardingConfig) -> Self {
        Self::with_workflow_support_and_engine(config, Arc::new(MemoryWorkflowEngine::new())).await
    }

    /// Create a single-node in-memory cluster with a custom workflow engine.
    ///
    /// This is useful when you need to control the workflow engine behavior in tests,
    /// for example to simulate failures or inspect deferred values.
    pub async fn with_workflow_support_and_engine(
        config: ShardingConfig,
        workflow_engine: Arc<dyn WorkflowEngine>,
    ) -> Self {
        let config = Arc::new(config);
        let runners = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let message_storage = Arc::new(MemoryMessageStorage::new());
        let state_storage = Arc::new(MemoryWorkflowStorage::new());
        let sharding = ShardingImpl::new_with_engines(
            Arc::clone(&config),
            runners,
            None, // runner_storage
            None, // runner_health
            Some(message_storage),
            Some(state_storage),
            Some(workflow_engine),
            metrics,
        )
        .expect("TestCluster config should be valid");
        sharding.acquire_all_shards().await;

        Self { sharding, config }
    }

    /// Register an entity and return a client for it.
    pub async fn register(
        &self,
        entity: impl Entity + 'static,
    ) -> Result<EntityClient, ClusterError> {
        let entity = Arc::new(entity);
        let entity_type = entity.entity_type();
        self.sharding.register_entity(entity).await?;
        Ok(Arc::clone(&self.sharding).make_client(entity_type))
    }

    /// Get a reference to the underlying [`Sharding`] implementation.
    pub fn sharding(&self) -> &Arc<ShardingImpl> {
        &self.sharding
    }

    /// Get the cluster configuration.
    pub fn config(&self) -> &Arc<ShardingConfig> {
        &self.config
    }

    /// Gracefully shut down the cluster.
    pub async fn shutdown(&self) -> Result<(), ClusterError> {
        self.sharding.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{Entity, EntityContext, EntityHandler};
    use crate::error::ClusterError;
    use crate::types::{EntityId, EntityType};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -- Test entity: Counter --

    struct CounterEntity;

    #[async_trait]
    impl Entity for CounterEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Counter")
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(CounterHandler {
                count: AtomicUsize::new(0),
            }))
        }
    }

    struct CounterHandler {
        count: AtomicUsize,
    }

    #[async_trait]
    impl EntityHandler for CounterHandler {
        async fn handle_request(
            &self,
            tag: &str,
            payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            match tag {
                "increment" => {
                    let amount: usize = rmp_serde::from_slice(payload).map_err(|e| {
                        ClusterError::MalformedMessage {
                            reason: format!("bad payload: {e}"),
                            source: Some(Box::new(e)),
                        }
                    })?;
                    let new_val = self.count.fetch_add(amount, Ordering::Relaxed) + amount;
                    rmp_serde::to_vec(&new_val).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("serialize error: {e}"),
                        source: Some(Box::new(e)),
                    })
                }
                "get" => {
                    let val = self.count.load(Ordering::Relaxed);
                    rmp_serde::to_vec(&val).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("serialize error: {e}"),
                        source: Some(Box::new(e)),
                    })
                }
                _ => Err(ClusterError::MalformedMessage {
                    reason: format!("unknown tag: {tag}"),
                    source: None,
                }),
            }
        }
    }

    // -- Test entity: Echo --

    struct EchoEntity;

    #[async_trait]
    impl Entity for EchoEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Echo")
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(EchoHandler))
        }
    }

    struct EchoHandler;

    #[async_trait]
    impl EntityHandler for EchoHandler {
        async fn handle_request(
            &self,
            _tag: &str,
            payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            Ok(payload.to_vec())
        }
    }

    #[tokio::test]
    async fn create_test_cluster() {
        let cluster = TestCluster::new().await;
        assert!(!cluster.sharding().is_shutdown());
        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn register_entity_and_send_request() {
        let cluster = TestCluster::new().await;
        let client = cluster.register(EchoEntity).await.unwrap();

        let entity_id = EntityId::new("e-1");
        let response: Vec<u8> = client
            .send(&entity_id, "echo", &vec![1u8, 2, 3])
            .await
            .unwrap();
        assert_eq!(response, vec![1, 2, 3]);

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn counter_entity_stateful() {
        let cluster = TestCluster::new().await;
        let client = cluster.register(CounterEntity).await.unwrap();

        let eid = EntityId::new("c-1");

        let val: usize = client.send(&eid, "increment", &1usize).await.unwrap();
        assert_eq!(val, 1);

        let val: usize = client.send(&eid, "increment", &5usize).await.unwrap();
        assert_eq!(val, 6);

        let val: usize = client.send(&eid, "get", &()).await.unwrap();
        assert_eq!(val, 6);

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn multiple_entities_independent() {
        let cluster = TestCluster::new().await;
        let client = cluster.register(CounterEntity).await.unwrap();

        let eid1 = EntityId::new("c-1");
        let eid2 = EntityId::new("c-2");

        let _: usize = client.send(&eid1, "increment", &10usize).await.unwrap();
        let _: usize = client.send(&eid2, "increment", &20usize).await.unwrap();

        let val1: usize = client.send(&eid1, "get", &()).await.unwrap();
        let val2: usize = client.send(&eid2, "get", &()).await.unwrap();

        assert_eq!(val1, 10);
        assert_eq!(val2, 20);

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn notify_does_not_error() {
        let cluster = TestCluster::new().await;
        let client = cluster.register(EchoEntity).await.unwrap();

        let eid = EntityId::new("e-1");
        client.notify(&eid, "ping", &()).await.unwrap();

        // Give time for processing
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(cluster.sharding().active_entity_count() > 0);

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn multiple_entity_types() {
        let cluster = TestCluster::new().await;
        let echo_client = cluster.register(EchoEntity).await.unwrap();
        let counter_client = cluster.register(CounterEntity).await.unwrap();

        let eid = EntityId::new("x-1");

        let echo_result: Vec<u8> = echo_client.send(&eid, "echo", &vec![42u8]).await.unwrap();
        assert_eq!(echo_result, vec![42]);

        let counter_result: usize = counter_client
            .send(&eid, "increment", &3usize)
            .await
            .unwrap();
        assert_eq!(counter_result, 3);

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn custom_config() {
        let config = ShardingConfig {
            shards_per_group: 5,
            ..Default::default()
        };
        let cluster = TestCluster::with_config(config).await;
        assert_eq!(cluster.config().shards_per_group, 5);
        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unknown_tag_returns_error() {
        let cluster = TestCluster::new().await;
        let client = cluster.register(CounterEntity).await.unwrap();

        let eid = EntityId::new("c-1");
        let result = client.send::<(), usize>(&eid, "nonexistent", &()).await;
        assert!(result.is_err());

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let cluster = TestCluster::new().await;
        cluster.shutdown().await.unwrap();
        cluster.shutdown().await.unwrap();
        assert!(cluster.sharding().is_shutdown());
    }
}
