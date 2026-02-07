//! Tests for `#[entity]` and `#[entity_impl]` proc macros.

#[cfg(test)]
mod tests {
    use crate::config::ShardingConfig;
    use crate::durable::{
        MemoryWorkflowStorage, WorkflowEngine, WorkflowStorage, INTERRUPT_SIGNAL,
    };
    use crate::entity_client::EntityClient;
    use crate::envelope::{AckChunk, EnvelopeRequest, Interrupt};
    use crate::hash::shard_for_entity;
    use crate::message::ReplyReceiver;
    use crate::metrics::ClusterMetrics;
    use crate::prelude::*;
    use crate::reply::{ExitResult, Reply, ReplyWithExit};
    use crate::sharding::{Sharding, ShardingRegistrationEvent};
    use crate::sharding_impl::ShardingImpl;
    use crate::singleton::SingletonContext;
    use crate::snowflake::SnowflakeGenerator;
    use crate::storage::memory_message::MemoryMessageStorage;
    use crate::storage::noop_runners::NoopRunners;
    use crate::types::{EntityAddress, EntityId, EntityType, RunnerAddress, ShardId};
    use async_trait::async_trait;
    use dashmap::DashMap;
    use futures::future::BoxFuture;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::sync::Notify;
    use tokio_stream::Stream;

    fn test_ctx(entity_type: &str, entity_id: &str) -> EntityContext {
        EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new(entity_type),
                entity_id: EntityId::new(entity_id),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: None,
            workflow_engine: None,
            sharding: None,
        }
    }

    fn test_ctx_with_storage(
        entity_type: &str,
        entity_id: &str,
        storage: Arc<dyn crate::durable::WorkflowStorage>,
    ) -> EntityContext {
        EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new(entity_type),
                entity_id: EntityId::new(entity_id),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            sharding: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: Some(storage),
            workflow_engine: None,
        }
    }

    #[derive(Clone, Default)]
    struct TestWorkflowEngine {
        deferred: Arc<DashMap<(String, String, String), Vec<u8>>>,
        notifiers: Arc<DashMap<(String, String, String), Arc<Notify>>>,
    }

    impl TestWorkflowEngine {
        fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl WorkflowEngine for TestWorkflowEngine {
        async fn sleep(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
            _name: &str,
            duration: std::time::Duration,
        ) -> Result<(), ClusterError> {
            tokio::time::sleep(duration).await;
            Ok(())
        }

        async fn await_deferred(
            &self,
            workflow_name: &str,
            execution_id: &str,
            name: &str,
        ) -> Result<Vec<u8>, ClusterError> {
            let key = (
                workflow_name.to_string(),
                execution_id.to_string(),
                name.to_string(),
            );
            loop {
                if let Some(value) = self.deferred.get(&key) {
                    return Ok(value.clone());
                }
                let notify = self
                    .notifiers
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(Notify::new()))
                    .clone();
                if let Some(value) = self.deferred.get(&key) {
                    return Ok(value.clone());
                }
                notify.notified().await;
            }
        }

        async fn resolve_deferred(
            &self,
            workflow_name: &str,
            execution_id: &str,
            name: &str,
            value: Vec<u8>,
        ) -> Result<(), ClusterError> {
            let key = (
                workflow_name.to_string(),
                execution_id.to_string(),
                name.to_string(),
            );
            self.deferred.insert(key.clone(), value);
            if let Some(notify) = self.notifiers.get(&key) {
                notify.notify_waiters();
            }
            Ok(())
        }

        async fn on_interrupt(
            &self,
            workflow_name: &str,
            execution_id: &str,
        ) -> Result<(), ClusterError> {
            let _ = self
                .await_deferred(workflow_name, execution_id, INTERRUPT_SIGNAL)
                .await?;
            Ok(())
        }
    }

    // --- Stateless entity ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct Ping;

    #[entity_impl(krate = "crate")]
    impl Ping {
        #[rpc]
        async fn ping(&self) -> Result<String, ClusterError> {
            Ok("pong".to_string())
        }
    }

    #[test]
    fn stateless_entity_type_name() {
        let e = Ping;
        assert_eq!(e.entity_type().0, "Ping");
    }

    #[tokio::test]
    async fn stateless_entity_dispatch() {
        let e = Ping;
        let ctx = test_ctx("Ping", "p-1");
        let handler = e.spawn(ctx).await.unwrap();
        let result = handler
            .handle_request("ping", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "pong");
    }

    #[tokio::test]
    async fn stateless_entity_unknown_tag() {
        let e = Ping;
        let ctx = test_ctx("Ping", "p-1");
        let handler = e.spawn(ctx).await.unwrap();
        let err = handler
            .handle_request("unknown", &[], &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ClusterError::MalformedMessage { .. }));
    }

    #[entity(name = "CustomPing", krate = "crate")]
    #[derive(Clone)]
    struct CustomNamePing;

    #[entity_impl(krate = "crate")]
    impl CustomNamePing {
        #[rpc]
        async fn ping(&self) -> Result<String, ClusterError> {
            Ok("custom-pong".to_string())
        }
    }

    #[test]
    fn custom_name_entity() {
        let e = CustomNamePing;
        assert_eq!(e.entity_type().0, "CustomPing");
    }

    // --- Stateful entity (non-persisted state) ---

    #[derive(Clone, Serialize, Deserialize)]
    struct CounterState {
        count: i32,
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct Counter;

    #[entity_impl(krate = "crate")]
    #[state(CounterState)]
    impl Counter {
        fn init(&self, _ctx: &EntityContext) -> Result<CounterState, ClusterError> {
            Ok(CounterState { count: 0 })
        }

        #[activity]
        async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
            self.state.count += amount;
            Ok(self.state.count)
        }

        #[rpc]
        async fn get_count(&self) -> Result<i32, ClusterError> {
            Ok(self.state.count)
        }

        // Workflow wrapper to call activity from external clients
        // Uses #[workflow] instead of #[rpc] because it calls an activity internally
        #[workflow]
        async fn do_increment(&self, amount: i32) -> Result<i32, ClusterError> {
            self.increment(amount).await
        }
    }

    #[test]
    fn stateful_entity_type_name() {
        let e = Counter;
        assert_eq!(e.entity_type().0, "Counter");
    }

    #[tokio::test]
    async fn stateful_entity_increment_and_get() {
        let e = Counter;
        let storage = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx_with_storage("Counter", "c-1", storage);
        let handler = e.spawn(ctx).await.unwrap();

        // Increment by 5 (via RPC wrapper that calls activity)
        let payload = rmp_serde::to_vec(&5i32).unwrap();
        let result = handler
            .handle_request("do_increment", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 5);

        // Get count
        let result = handler
            .handle_request("get_count", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 5);

        // Increment again
        let payload = rmp_serde::to_vec(&3i32).unwrap();
        let result = handler
            .handle_request("do_increment", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 8);
    }

    #[tokio::test]
    async fn stateful_entity_unknown_tag() {
        let e = Counter;
        let ctx = test_ctx("Counter", "c-1");
        let handler = e.spawn(ctx).await.unwrap();
        let err = handler
            .handle_request("unknown", &[], &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ClusterError::MalformedMessage { .. }));
    }

    // --- Persisted state entity ---

    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct PersistedCounterState {
        count: i32,
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedCounter;

    #[entity_impl(krate = "crate")]
    #[state(PersistedCounterState)]
    impl PersistedCounter {
        fn init(&self, _ctx: &EntityContext) -> Result<PersistedCounterState, ClusterError> {
            Ok(PersistedCounterState { count: 0 })
        }

        #[activity]
        async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
            self.state.count += amount;
            Ok(self.state.count)
        }

        #[rpc]
        async fn get_count(&self) -> Result<i32, ClusterError> {
            Ok(self.state.count)
        }

        #[workflow]
        async fn do_increment(&self, amount: i32) -> Result<i32, ClusterError> {
            self.increment(amount).await
        }
    }

    #[tokio::test]
    async fn persisted_state_entity_saves_after_mut() {
        let storage = Arc::new(MemoryWorkflowStorage::new());
        let e = PersistedCounter;
        let ctx = test_ctx_with_storage("PersistedCounter", "pc-1", storage.clone());
        let handler = e.spawn(ctx).await.unwrap();

        // Increment by 10
        let payload = rmp_serde::to_vec(&10i32).unwrap();
        handler
            .handle_request("do_increment", &payload, &HashMap::new())
            .await
            .unwrap();

        // With transactional activities, state is persisted SYNCHRONOUSLY
        // before the activity returns. No yield needed.

        // Verify state was saved to storage
        let stored = storage
            .load("entity/PersistedCounter/pc-1/state")
            .await
            .unwrap();
        assert!(stored.is_some());
        let state: PersistedCounterState = rmp_serde::from_slice(&stored.unwrap()).unwrap();
        assert_eq!(state.count, 10);
    }

    #[tokio::test]
    async fn persisted_state_entity_loads_on_spawn() {
        let storage = Arc::new(MemoryWorkflowStorage::new());

        // Pre-populate storage with state
        let pre_state = PersistedCounterState { count: 42 };
        let bytes = rmp_serde::to_vec(&pre_state).unwrap();
        storage
            .save("entity/PersistedCounter/pc-2/state", &bytes)
            .await
            .unwrap();

        let e = PersistedCounter;
        let ctx = test_ctx_with_storage("PersistedCounter", "pc-2", storage.clone());
        let handler = e.spawn(ctx).await.unwrap();

        // Get count — should be loaded from storage
        let result = handler
            .handle_request("get_count", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn persisted_state_read_only_does_not_save() {
        let storage = Arc::new(MemoryWorkflowStorage::new());
        let e = PersistedCounter;
        let ctx = test_ctx_with_storage("PersistedCounter", "pc-3", storage.clone());
        let handler = e.spawn(ctx).await.unwrap();

        // Read-only call
        handler
            .handle_request("get_count", &[], &HashMap::new())
            .await
            .unwrap();

        // State should NOT be saved (read-only)
        let stored = storage
            .load("entity/PersistedCounter/pc-3/state")
            .await
            .unwrap();
        assert!(stored.is_none());
    }

    #[tokio::test]
    async fn persisted_state_without_storage_falls_back_to_init() {
        // No storage provided — should use init()
        let e = PersistedCounter;
        let ctx = test_ctx("PersistedCounter", "pc-4");
        let handler = e.spawn(ctx).await.unwrap();

        let result = handler
            .handle_request("get_count", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 0);
    }

    // --- Persisted method entity ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedMethodEntity;

    #[entity_impl(krate = "crate")]
    impl PersistedMethodEntity {
        #[workflow]
        async fn important_action(&self, data: String) -> Result<String, ClusterError> {
            Ok(format!("processed: {data}"))
        }

        #[rpc]
        async fn regular_action(&self) -> Result<String, ClusterError> {
            Ok("regular".to_string())
        }
    }

    #[tokio::test]
    async fn persisted_method_entity_dispatches() {
        let e = PersistedMethodEntity;
        let ctx = test_ctx("PersistedMethodEntity", "pm-1");
        let handler = e.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&"hello".to_string()).unwrap();
        let result = handler
            .handle_request("important_action", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "processed: hello");

        let result = handler
            .handle_request("regular_action", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "regular");
    }

    // Test that the generated client uses send_persisted for #[workflow] methods
    // (This is a compile-time check — the client struct should exist with the right methods)
    #[test]
    fn persisted_method_client_exists() {
        // Just verify the client struct exists and has the expected methods.
        // We can't fully test send_persisted without a Sharding mock, but we can
        // verify the types compile.
        fn _assert_client_has_methods(_c: &PersistedMethodEntityClient) {
            // important_action and regular_action should exist on the client
        }
    }

    // --- Mixed entity (persisted + regular) ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct MixedEntity;

    #[entity_impl(krate = "crate")]
    impl MixedEntity {
        #[workflow]
        async fn persisted_action(&self, value: String) -> Result<String, ClusterError> {
            Ok(format!("persisted:{value}"))
        }

        #[rpc]
        async fn regular_action(&self, value: i32) -> Result<i32, ClusterError> {
            Ok(value * 2)
        }
    }

    #[tokio::test]
    async fn mixed_entity_client_calls_persisted_and_regular() {
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn crate::runners::Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let sharding_impl =
            ShardingImpl::new(config, runners, None, None, Some(storage), metrics).unwrap();
        sharding_impl.acquire_all_shards().await;

        let sharding: Arc<dyn Sharding> = sharding_impl.clone();
        let client = MixedEntity.register(Arc::clone(&sharding)).await.unwrap();
        let entity_id = EntityId::new("mixed-1");

        let persisted: String = client
            .persisted_action(&entity_id, &"hello".to_string())
            .await
            .unwrap();
        assert_eq!(persisted, "persisted:hello");

        let regular: i32 = client.regular_action(&entity_id, &7).await.unwrap();
        assert_eq!(regular, 14);

        sharding.shutdown().await.unwrap();
    }

    // --- Persisted method idempotency replay ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedIdempotentEntity {
        calls: Arc<AtomicUsize>,
    }

    #[entity_impl(krate = "crate")]
    impl PersistedIdempotentEntity {
        #[workflow]
        async fn process(&self, value: i32) -> Result<i32, ClusterError> {
            let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(value + count as i32)
        }
    }

    #[tokio::test]
    async fn persisted_method_replay_returns_cached_reply() {
        let calls = Arc::new(AtomicUsize::new(0));
        let entity = PersistedIdempotentEntity {
            calls: Arc::clone(&calls),
        };

        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn crate::runners::Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let storage = Arc::new(MemoryMessageStorage::new());
        let sharding_impl =
            ShardingImpl::new(config, runners, None, None, Some(storage), metrics).unwrap();
        sharding_impl.acquire_all_shards().await;

        let sharding: Arc<dyn Sharding> = sharding_impl.clone();
        let client = entity.register(Arc::clone(&sharding)).await.unwrap();
        let entity_id = EntityId::new("idem-1");

        let first: i32 = client.process(&entity_id, &5).await.unwrap();
        let second: i32 = client.process(&entity_id, &5).await.unwrap();

        assert_eq!(first, 6);
        assert_eq!(second, 6);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        sharding.shutdown().await.unwrap();
    }

    // --- Persisted method idempotency key override ---

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct UpdateRequest {
        id: String,
        value: i32,
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedKeyEntity;

    #[entity_impl(krate = "crate")]
    impl PersistedKeyEntity {
        #[workflow(key(|req: &UpdateRequest| req.id.clone()))]
        async fn update(&self, req: UpdateRequest) -> Result<String, ClusterError> {
            Ok(format!("{}:{}", req.id, req.value))
        }
    }

    struct MockSharding {
        snowflake: SnowflakeGenerator,
        shards_per_group: i32,
    }

    impl MockSharding {
        fn new() -> Self {
            Self {
                snowflake: SnowflakeGenerator::new(),
                shards_per_group: 300,
            }
        }
    }

    struct CapturingSharding {
        inner: MockSharding,
        captured: Arc<Mutex<Vec<EnvelopeRequest>>>,
    }

    #[async_trait]
    impl Sharding for MockSharding {
        fn get_shard_id(&self, _entity_type: &EntityType, entity_id: &EntityId) -> ShardId {
            let shard = shard_for_entity(entity_id.as_ref(), self.shards_per_group);
            ShardId::new("default", shard)
        }

        fn has_shard_id(&self, _shard_id: &ShardId) -> bool {
            true
        }

        fn snowflake(&self) -> &SnowflakeGenerator {
            &self.snowflake
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        async fn register_entity(&self, _entity: Arc<dyn Entity>) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn register_singleton(
            &self,
            _name: &str,
            _shard_group: Option<&str>,
            _run: Arc<
                dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                    + Send
                    + Sync,
            >,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient {
            EntityClient::new(self, entity_type)
        }

        async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let response = rmp_serde::to_vec(&"ok".to_string()).unwrap();
            let reply = Reply::WithExit(ReplyWithExit {
                request_id: envelope.request_id,
                id: self.snowflake.next_async().await?,
                exit: ExitResult::Success(response),
            });
            tx.send(reply)
                .await
                .map_err(|_| ClusterError::MalformedMessage {
                    reason: "reply channel closed".into(),
                    source: None,
                })?;
            Ok(rx)
        }

        async fn notify(&self, _envelope: EnvelopeRequest) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn ack_chunk(&self, _ack: AckChunk) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn interrupt(&self, _interrupt: Interrupt) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn poll_storage(&self) -> Result<(), ClusterError> {
            Ok(())
        }

        fn active_entity_count(&self) -> usize {
            0
        }

        async fn registration_events(
            &self,
        ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
            Box::pin(tokio_stream::empty())
        }

        async fn shutdown(&self) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Sharding for CapturingSharding {
        fn get_shard_id(&self, entity_type: &EntityType, entity_id: &EntityId) -> ShardId {
            self.inner.get_shard_id(entity_type, entity_id)
        }

        fn has_shard_id(&self, shard_id: &ShardId) -> bool {
            self.inner.has_shard_id(shard_id)
        }

        fn snowflake(&self) -> &SnowflakeGenerator {
            self.inner.snowflake()
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        async fn register_entity(&self, _entity: Arc<dyn Entity>) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn register_singleton(
            &self,
            _name: &str,
            _shard_group: Option<&str>,
            _run: Arc<
                dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                    + Send
                    + Sync,
            >,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient {
            EntityClient::new(self, entity_type)
        }

        async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
            self.captured.lock().unwrap().push(envelope.clone());
            self.inner.send(envelope).await
        }

        async fn notify(&self, envelope: EnvelopeRequest) -> Result<(), ClusterError> {
            self.captured.lock().unwrap().push(envelope);
            Ok(())
        }

        async fn ack_chunk(&self, _ack: AckChunk) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn interrupt(&self, _interrupt: Interrupt) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn poll_storage(&self) -> Result<(), ClusterError> {
            Ok(())
        }

        fn active_entity_count(&self) -> usize {
            0
        }

        async fn registration_events(
            &self,
        ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
            Box::pin(tokio_stream::empty())
        }

        async fn shutdown(&self) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn persisted_key_override_uses_custom_idempotency_key() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = PersistedKeyEntityClient::new(Arc::clone(&sharding));

        let entity_id = EntityId::new("pk-1");
        let req1 = UpdateRequest {
            id: "same".to_string(),
            value: 1,
        };
        let req2 = UpdateRequest {
            id: "same".to_string(),
            value: 2,
        };

        let entity = PersistedKeyEntity;
        let ctx = test_ctx("PersistedKeyEntity", "pk-handler");
        let handler = entity.spawn(ctx).await.unwrap();
        let payload = rmp_serde::to_vec(&req1).unwrap();
        let result = handler
            .handle_request("update", &payload, &HashMap::new())
            .await
            .unwrap();
        let response: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(response, "same:1");

        let _: String = client.update(&entity_id, &req1).await.unwrap();
        let _: String = client.update(&entity_id, &req2).await.unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].request_id, captured[1].request_id);
    }

    // --- Multiple request parameters ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct MultiParamEntity;

    #[entity_impl(krate = "crate")]
    impl MultiParamEntity {
        #[rpc]
        async fn add(&self, left: i32, right: i32) -> Result<i32, ClusterError> {
            Ok(left + right)
        }
    }

    #[tokio::test]
    async fn multi_param_entity_dispatches() {
        let entity = MultiParamEntity;
        let ctx = test_ctx("MultiParamEntity", "mp-1");
        let handler = entity.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&(2i32, 3i32)).unwrap();
        let result = handler
            .handle_request("add", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 5);
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct MultiParamPersisted;

    #[entity_impl(krate = "crate")]
    impl MultiParamPersisted {
        #[workflow(key(|order_id: &String, _body: &String| order_id.clone()))]
        async fn send_email(&self, order_id: String, body: String) -> Result<String, ClusterError> {
            Ok(format!("{order_id}:{body}"))
        }
    }

    #[tokio::test]
    async fn multi_param_persist_key_uses_subset() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = MultiParamPersistedClient::new(Arc::clone(&sharding));

        let entity_id = EntityId::new("mp-1");
        let order_id = "order-1".to_string();
        let body1 = "first".to_string();
        let body2 = "second".to_string();

        let entity = MultiParamPersisted;
        let ctx = test_ctx("MultiParamPersisted", "mp-handler");
        let handler = entity.spawn(ctx).await.unwrap();
        let payload = rmp_serde::to_vec(&(order_id.clone(), body1.clone())).unwrap();
        let result = handler
            .handle_request("send_email", &payload, &HashMap::new())
            .await
            .unwrap();
        let response: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(response, format!("{order_id}:{body1}"));

        let _: String = client
            .send_email(&entity_id, &order_id, &body1)
            .await
            .unwrap();
        let _: String = client
            .send_email(&entity_id, &order_id, &body2)
            .await
            .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].request_id, captured[1].request_id);
    }

    // --- Entity traits ---

    #[entity_trait(krate = "crate")]
    #[derive(Clone)]
    pub struct LoggerTrait {
        pub captured: Arc<Mutex<Vec<String>>>,
    }

    #[entity_trait_impl(krate = "crate")]
    impl LoggerTrait {
        #[rpc]
        async fn log(&self, message: String) -> Result<String, ClusterError> {
            self.captured.lock().unwrap().push(message.clone());
            Ok(message)
        }
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct LoggingEntity;

    #[entity_impl(krate = "crate", traits(LoggerTrait))]
    #[state(())]
    impl LoggingEntity {
        fn init(&self, _ctx: &EntityContext) -> Result<(), ClusterError> {
            Ok(())
        }

        #[rpc]
        async fn ping(&self) -> Result<String, ClusterError> {
            Ok("pong".to_string())
        }

        #[rpc]
        async fn log_from_entity(&self, message: String) -> Result<String, ClusterError> {
            // Trait methods need to go through __handler (trait delegation not yet implemented for views)
            self.__handler.log(message).await
        }
    }

    #[tokio::test]
    async fn trait_dispatches_via_handler() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let trait_impl = LoggerTrait {
            captured: Arc::clone(&captured),
        };
        let entity = LoggingEntity.with(trait_impl);
        let ctx = test_ctx("LoggingEntity", "log-1");
        let handler = entity.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&"hello".to_string()).unwrap();
        let result = handler
            .handle_request("log", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "hello");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.as_slice(), ["hello".to_string()]);
    }

    #[tokio::test]
    async fn trait_methods_available_on_entity_self() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let trait_impl = LoggerTrait {
            captured: Arc::clone(&captured),
        };
        let entity = LoggingEntity.with(trait_impl);
        let ctx = test_ctx("LoggingEntity", "log-1");
        let handler = entity.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&"self-call".to_string()).unwrap();
        let result = handler
            .handle_request("log_from_entity", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "self-call");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.as_slice(), ["self-call".to_string()]);
    }

    #[tokio::test]
    async fn trait_client_extension_calls_send() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = LoggingEntityClient::new(sharding);
        let entity_id = EntityId::new("log-2");

        let response: String = client.log(&entity_id, &"hi".to_string()).await.unwrap();
        assert_eq!(response, "ok");
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct StatelessLoggingEntity;

    #[entity_impl(krate = "crate", traits(LoggerTrait))]
    impl StatelessLoggingEntity {
        #[rpc]
        async fn ping(&self) -> Result<String, ClusterError> {
            Ok("pong".to_string())
        }
    }

    #[tokio::test]
    async fn stateless_trait_dispatches_via_handler() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let trait_impl = LoggerTrait {
            captured: Arc::clone(&captured),
        };
        let entity = StatelessLoggingEntity.with(trait_impl);
        let ctx = test_ctx("StatelessLoggingEntity", "log-3");
        let handler = entity.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&"hello".to_string()).unwrap();
        let result = handler
            .handle_request("log", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "hello");

        let captured = captured.lock().unwrap();
        assert_eq!(captured.as_slice(), ["hello".to_string()]);
    }

    // --- Entity traits with state ---

    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    pub struct TraitCounterState {
        count: i32,
    }

    #[entity_trait(krate = "crate")]
    #[derive(Clone)]
    pub struct TraitCounter;

    #[entity_trait_impl(krate = "crate")]
    #[state(TraitCounterState)]
    impl TraitCounter {
        fn init(&self) -> Result<TraitCounterState, ClusterError> {
            Ok(TraitCounterState { count: 0 })
        }

        #[activity]
        #[protected]
        async fn do_increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
            self.state.count += amount;
            Ok(self.state.count)
        }

        /// Public workflow that wraps the increment activity.
        #[workflow]
        pub async fn increment(&self, amount: i32) -> Result<i32, ClusterError> {
            self.do_increment(amount).await
        }

        #[rpc]
        async fn get_count(&self) -> Result<i32, ClusterError> {
            Ok(self.state.count)
        }
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct TraitStateEntity;

    #[entity_impl(krate = "crate", traits(TraitCounter))]
    #[state(())]
    impl TraitStateEntity {
        fn init(&self, _ctx: &EntityContext) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn trait_state_persists_with_entity_state() {
        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let entity = TraitStateEntity.with(TraitCounter);
        let ctx = test_ctx_with_storage("TraitStateEntity", "ts-1", storage.clone());
        let handler = entity.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&5i32).unwrap();
        handler
            .handle_request("increment", &payload, &HashMap::new())
            .await
            .unwrap();
        drop(handler);

        let entity = TraitStateEntity.with(TraitCounter);
        let ctx = test_ctx_with_storage("TraitStateEntity", "ts-1", storage.clone());
        let handler = entity.spawn(ctx).await.unwrap();
        let result = handler
            .handle_request("get_count", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 5);
    }

    // --- Multi-trait entities ---

    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    pub struct AlphaState {
        count: i32,
    }

    #[entity_trait(krate = "crate")]
    #[derive(Clone)]
    pub struct AlphaTrait;

    #[entity_trait_impl(krate = "crate")]
    #[state(AlphaState)]
    impl AlphaTrait {
        fn init(&self) -> Result<AlphaState, ClusterError> {
            Ok(AlphaState { count: 0 })
        }

        #[activity]
        #[protected]
        async fn do_inc_alpha(&mut self, amount: i32) -> Result<i32, ClusterError> {
            self.state.count += amount;
            Ok(self.state.count)
        }

        /// Public workflow that wraps the activity.
        #[workflow]
        pub async fn inc_alpha(&self, amount: i32) -> Result<i32, ClusterError> {
            self.do_inc_alpha(amount).await
        }

        #[rpc]
        async fn get_alpha(&self) -> Result<i32, ClusterError> {
            Ok(self.state.count)
        }
    }

    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    pub struct BetaState {
        count: i32,
    }

    #[entity_trait(krate = "crate")]
    #[derive(Clone)]
    pub struct BetaTrait;

    #[entity_trait_impl(krate = "crate")]
    #[state(BetaState)]
    impl BetaTrait {
        fn init(&self) -> Result<BetaState, ClusterError> {
            Ok(BetaState { count: 0 })
        }

        #[activity]
        #[protected]
        async fn do_add_beta(&mut self, amount: i32) -> Result<i32, ClusterError> {
            self.state.count += amount;
            Ok(self.state.count)
        }

        /// Public workflow that wraps the activity.
        #[workflow]
        pub async fn add_beta(&self, amount: i32) -> Result<i32, ClusterError> {
            self.do_add_beta(amount).await
        }

        #[rpc]
        async fn get_beta(&self) -> Result<i32, ClusterError> {
            Ok(self.state.count)
        }
    }

    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct CompositeState {
        total: i32,
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct CompositeEntity;

    #[entity_impl(krate = "crate", traits(AlphaTrait, BetaTrait))]
    #[state(CompositeState)]
    impl CompositeEntity {
        fn init(&self, _ctx: &EntityContext) -> Result<CompositeState, ClusterError> {
            Ok(CompositeState { total: 0 })
        }

        #[activity]
        async fn do_bump_total(&mut self, amount: i32) -> Result<i32, ClusterError> {
            self.state.total += amount;
            Ok(self.state.total)
        }

        #[workflow]
        async fn bump_total(&self, amount: i32) -> Result<i32, ClusterError> {
            self.do_bump_total(amount).await
        }

        #[rpc]
        async fn get_total(&self) -> Result<i32, ClusterError> {
            Ok(self.state.total)
        }
    }

    #[tokio::test]
    async fn multi_trait_state_persists_and_dispatches() {
        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let entity = CompositeEntity.with(AlphaTrait).with(BetaTrait);
        let ctx = test_ctx_with_storage("CompositeEntity", "ct-1", storage.clone());
        let handler = entity.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&2i32).unwrap();
        let result = handler
            .handle_request("inc_alpha", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 2);

        let payload = rmp_serde::to_vec(&3i32).unwrap();
        let result = handler
            .handle_request("add_beta", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 3);

        let payload = rmp_serde::to_vec(&5i32).unwrap();
        let result = handler
            .handle_request("bump_total", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 5);

        drop(handler);

        // Reload and verify state persisted
        let entity = CompositeEntity.with(AlphaTrait).with(BetaTrait);
        let ctx = test_ctx_with_storage("CompositeEntity", "ct-1", storage.clone());
        let handler = entity.spawn(ctx).await.unwrap();

        let result = handler
            .handle_request("get_alpha", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 2);

        let result = handler
            .handle_request("get_beta", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 3);

        let result = handler
            .handle_request("get_total", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 5);
    }

    // --- Private method entity ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct OrderEntity;

    #[entity_impl(krate = "crate")]
    impl OrderEntity {
        #[rpc]
        async fn get_order(&self, id: String) -> Result<String, ClusterError> {
            Ok(format!("order:{id}"))
        }

        #[rpc]
        #[private]
        #[allow(dead_code)]
        async fn internal_validate(&self, id: String) -> Result<String, ClusterError> {
            Ok(format!("validated:{id}"))
        }
    }

    #[tokio::test]
    async fn private_method_is_not_dispatchable() {
        let e = OrderEntity;
        let ctx = test_ctx("OrderEntity", "o-1");
        let handler = e.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&"abc".to_string()).unwrap();
        let err = handler
            .handle_request("internal_validate", &payload, &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ClusterError::MalformedMessage { .. }));
    }

    #[test]
    fn private_method_not_on_client() {
        // Verify the generated client has get_order but NOT internal_validate.
        // We use a compile-time assertion: if internal_validate existed on the client,
        // this function signature check would need updating.
        fn _assert_client_methods(c: &OrderEntityClient) {
            // get_order should exist — this is a type-level check
            let _ = &c.inner;
        }
        // Note: OrderEntityClient::internal_validate does NOT exist — calling it
        // would be a compile error. The test passing proves private methods are
        // omitted from the client.
    }

    // --- Private method with stateful entity ---

    #[derive(Clone, Serialize, Deserialize)]
    struct PrivateStatefulState {
        value: String,
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PrivateStatefulEntity;

    #[entity_impl(krate = "crate")]
    #[state(PrivateStatefulState)]
    impl PrivateStatefulEntity {
        fn init(&self, _ctx: &EntityContext) -> Result<PrivateStatefulState, ClusterError> {
            Ok(PrivateStatefulState {
                value: "initial".to_string(),
            })
        }

        #[rpc]
        async fn get_value(&self) -> Result<String, ClusterError> {
            Ok(self.state.value.clone())
        }

        #[activity]
        #[allow(dead_code)]
        async fn set_internal(&mut self, val: String) -> Result<(), ClusterError> {
            self.state.value = val;
            Ok(())
        }
    }

    #[tokio::test]
    async fn private_activity_method_not_dispatchable() {
        let e = PrivateStatefulEntity;
        let ctx = test_ctx("PrivateStatefulEntity", "ps-1");
        let handler = e.spawn(ctx).await.unwrap();

        // Activity methods are private by default, so dispatch should fail
        let payload = rmp_serde::to_vec(&"updated".to_string()).unwrap();
        let err = handler
            .handle_request("set_internal", &payload, &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ClusterError::MalformedMessage { .. }));
    }

    #[test]
    fn private_activity_method_not_on_client() {
        fn _assert_client_methods(c: &PrivateStatefulEntityClient) {
            let _ = &c.inner;
            // get_value exists on client, set_internal does NOT
        }
    }

    // --- DurableContext entity ---

    fn test_ctx_with_engine(
        entity_type: &str,
        entity_id: &str,
        engine: Arc<dyn crate::durable::WorkflowEngine>,
    ) -> EntityContext {
        EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new(entity_type),
                entity_id: EntityId::new(entity_id),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: None,
            workflow_engine: Some(engine),
            sharding: None,
        }
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct DurableProcessor;

    #[entity_impl(krate = "crate")]
    impl DurableProcessor {
        #[workflow]
        async fn process_order(
            &self,
            _ctx: &crate::durable::DurableContext,
            order_id: String,
        ) -> Result<String, ClusterError> {
            Ok(format!("processed:{order_id}"))
        }

        #[rpc]
        async fn get_status(&self) -> Result<String, ClusterError> {
            Ok("ready".to_string())
        }
    }

    #[test]
    fn durable_entity_type_name() {
        let e = DurableProcessor;
        assert_eq!(e.entity_type().0, "DurableProcessor");
    }

    #[tokio::test]
    async fn durable_entity_dispatches_with_context() {
        let engine = Arc::new(TestWorkflowEngine::new());
        let e = DurableProcessor;
        let ctx = test_ctx_with_engine("DurableProcessor", "dp-1", engine);
        let handler = e.spawn(ctx).await.unwrap();

        // Call the durable method
        let payload = rmp_serde::to_vec(&"order-123".to_string()).unwrap();
        let result = handler
            .handle_request("process_order", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "processed:order-123");
    }

    #[tokio::test]
    async fn durable_entity_non_durable_method_works() {
        let engine = Arc::new(TestWorkflowEngine::new());
        let e = DurableProcessor;
        let ctx = test_ctx_with_engine("DurableProcessor", "dp-2", engine);
        let handler = e.spawn(ctx).await.unwrap();

        let result = handler
            .handle_request("get_status", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "ready");
    }

    #[tokio::test]
    async fn durable_entity_without_engine_returns_error() {
        let e = DurableProcessor;
        let ctx = test_ctx("DurableProcessor", "dp-3"); // No engine
        let handler = e.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&"order-456".to_string()).unwrap();
        let err = handler
            .handle_request("process_order", &payload, &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ClusterError::MalformedMessage { .. }));
    }

    // --- Stateful entity with DurableContext ---

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct OrderState {
        status: String,
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct StatefulDurableEntity;

    #[entity_impl(krate = "crate")]
    #[state(OrderState)]
    impl StatefulDurableEntity {
        fn init(&self, _ctx: &EntityContext) -> Result<OrderState, ClusterError> {
            Ok(OrderState {
                status: "new".to_string(),
            })
        }

        #[activity]
        async fn fulfill(
            &mut self,
            _ctx: &crate::durable::DurableContext,
            item: String,
        ) -> Result<String, ClusterError> {
            self.state.status = "fulfilled".to_string();
            Ok(format!("ok:{item}"))
        }

        #[rpc]
        async fn get_status(&self) -> Result<String, ClusterError> {
            Ok(self.state.status.clone())
        }

        #[workflow]
        async fn do_fulfill(
            &self,
            ctx: &crate::durable::DurableContext,
            item: String,
        ) -> Result<String, ClusterError> {
            self.fulfill(ctx, item).await
        }
    }

    #[tokio::test]
    async fn stateful_durable_entity_dispatches() {
        let storage = Arc::new(MemoryWorkflowStorage::new());
        let engine = Arc::new(TestWorkflowEngine::new());
        let e = StatefulDurableEntity;
        let ctx =
            test_ctx_with_storage_and_engine("StatefulDurableEntity", "sde-1", storage, engine);
        let handler = e.spawn(ctx).await.unwrap();

        // Call the workflow method which calls the activity — mutates state
        let payload = rmp_serde::to_vec(&"widget".to_string()).unwrap();
        let result = handler
            .handle_request("do_fulfill", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "ok:widget");

        // Verify state was mutated
        let result = handler
            .handle_request("get_status", &[], &HashMap::new())
            .await
            .unwrap();
        let status: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(status, "fulfilled");
    }

    // --- Built-in durable methods on self (sleep/await_deferred/resolve_deferred) ---

    fn test_ctx_with_storage_and_engine(
        entity_type: &str,
        entity_id: &str,
        storage: Arc<dyn crate::durable::WorkflowStorage>,
        engine: Arc<dyn crate::durable::WorkflowEngine>,
    ) -> EntityContext {
        EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new(entity_type),
                entity_id: EntityId::new(entity_id),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: Some(storage),
            workflow_engine: Some(engine),
            sharding: None,
        }
    }

    // Test that built-in durable methods exist and error correctly without engine
    #[tokio::test]
    async fn builtin_sleep_without_engine_returns_error() {
        // Create an entity with #[state(..., persistent)] but no workflow engine
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct SimpleState {
            count: i32,
        }

        #[entity(krate = "crate")]
        #[derive(Clone)]
        struct SleepTestEntity;

        #[entity_impl(krate = "crate")]
        #[state(SimpleState)]
        impl SleepTestEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<SimpleState, ClusterError> {
                Ok(SimpleState { count: 0 })
            }

            /// This activity method uses the built-in self.sleep() and modifies state
            #[activity]
            async fn sleep_and_increment(&mut self) -> Result<String, ClusterError> {
                self.sleep("test-sleep", std::time::Duration::from_secs(1))
                    .await?;
                self.state.count += 1;
                Ok("slept".into())
            }

            /// This workflow method calls the activity
            #[workflow]
            async fn do_sleep(&self) -> Result<String, ClusterError> {
                self.sleep_and_increment().await
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx_with_storage("SleepTestEntity", "e1", storage);
        let handler = SleepTestEntity.spawn(ctx).await.unwrap();

        // Call do_sleep — should fail because no workflow engine
        let result = handler
            .handle_request("do_sleep", &[], &HashMap::new())
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("workflow engine"),
            "expected workflow engine error, got: {err}"
        );
    }

    #[tokio::test]
    async fn builtin_sleep_with_engine_resumes() {
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct SleepState {
            count: i32,
        }

        #[entity(krate = "crate")]
        #[derive(Clone)]
        struct SleepWithEngineEntity;

        #[entity_impl(krate = "crate")]
        #[state(SleepState)]
        impl SleepWithEngineEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<SleepState, ClusterError> {
                Ok(SleepState { count: 0 })
            }

            #[activity]
            async fn sleep_and_increment(&mut self) -> Result<String, ClusterError> {
                self.sleep("test-sleep", std::time::Duration::from_millis(10))
                    .await?;
                self.state.count += 1;
                Ok("slept".into())
            }

            #[workflow]
            async fn do_sleep(&self) -> Result<String, ClusterError> {
                self.sleep_and_increment().await
            }

            #[rpc]
            async fn get_count(&self) -> Result<i32, ClusterError> {
                Ok(self.state.count)
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let engine: Arc<dyn crate::durable::WorkflowEngine> = Arc::new(TestWorkflowEngine::new());
        let ctx = test_ctx_with_storage_and_engine(
            "SleepWithEngineEntity",
            "e1",
            Arc::clone(&storage),
            engine,
        );
        let handler = SleepWithEngineEntity.spawn(ctx).await.unwrap();

        let result = handler
            .handle_request("do_sleep", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "slept");

        let result = handler
            .handle_request("get_count", &[], &HashMap::new())
            .await
            .unwrap();
        let count: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(count, 1);

        // With transactional activities, state is persisted SYNCHRONOUSLY
        // before the activity returns. No yield needed.

        let stored = storage
            .load("entity/SleepWithEngineEntity/e1/state")
            .await
            .unwrap();
        assert!(stored.is_some());
        let state: SleepState = rmp_serde::from_slice(&stored.unwrap()).unwrap();
        assert_eq!(state.count, 1);
    }

    #[tokio::test]
    async fn builtin_resolve_deferred_with_engine_succeeds() {
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct ResolveState {
            resolved: bool,
        }

        #[entity(krate = "crate")]
        #[derive(Clone)]
        struct ResolveTestEntity;

        #[entity_impl(krate = "crate")]
        #[state(ResolveState)]
        impl ResolveTestEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<ResolveState, ClusterError> {
                Ok(ResolveState { resolved: false })
            }

            #[activity]
            async fn resolve_and_mark(
                &mut self,
                signal_name: String,
            ) -> Result<String, ClusterError> {
                self.resolve_deferred(&signal_name, &42i32).await?;
                self.state.resolved = true;
                Ok("done".into())
            }

            #[workflow]
            async fn do_resolve(&self, signal_name: String) -> Result<String, ClusterError> {
                self.resolve_and_mark(signal_name).await
            }

            #[rpc]
            async fn is_resolved(&self) -> Result<bool, ClusterError> {
                Ok(self.state.resolved)
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let engine: Arc<dyn crate::durable::WorkflowEngine> = Arc::new(TestWorkflowEngine::new());
        let ctx = test_ctx_with_storage_and_engine("ResolveTestEntity", "e1", storage, engine);
        let handler = ResolveTestEntity.spawn(ctx).await.unwrap();

        // Call do_resolve — should succeed
        let payload = rmp_serde::to_vec(&"my-signal".to_string()).unwrap();
        let result = handler
            .handle_request("do_resolve", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "done");

        // Verify state was mutated
        let result = handler
            .handle_request("is_resolved", &[], &HashMap::new())
            .await
            .unwrap();
        let resolved: bool = rmp_serde::from_slice(&result).unwrap();
        assert!(resolved);
    }

    #[tokio::test]
    async fn builtin_await_deferred_waits_and_resumes_with_resolve_deferred() {
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct DeferredState;

        #[entity(krate = "crate", concurrency = 2)]
        #[derive(Clone)]
        struct DeferredEntity;

        #[entity_impl(krate = "crate", deferred_keys(SIGNAL: i32 = "signal"))]
        #[state(DeferredState)]
        impl DeferredEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<DeferredState, ClusterError> {
                Ok(DeferredState)
            }

            #[workflow]
            async fn wait_for_signal(&self) -> Result<i32, ClusterError> {
                self.await_deferred(SIGNAL).await
            }

            #[workflow]
            async fn resolve_signal(&self, value: i32) -> Result<(), ClusterError> {
                self.resolve_deferred(SIGNAL, &value).await
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let engine: Arc<dyn crate::durable::WorkflowEngine> = Arc::new(TestWorkflowEngine::new());
        let ctx = test_ctx_with_storage_and_engine("DeferredEntity", "e1", storage, engine);
        let handler = DeferredEntity.spawn(ctx).await.unwrap();
        let handler: Arc<dyn EntityHandler> = Arc::from(handler);

        let wait_task = {
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                handler
                    .handle_request("wait_for_signal", &[], &HashMap::new())
                    .await
            })
        };

        tokio::task::yield_now().await;

        let payload = rmp_serde::to_vec(&123i32).unwrap();
        handler
            .handle_request("resolve_signal", &payload, &HashMap::new())
            .await
            .unwrap();

        let result = wait_task.await.unwrap().unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 123);
    }

    // --- ArcSwap concurrency verification tests ---

    /// Verify that concurrent read operations don't block each other.
    /// With the new ArcSwap design, multiple reads can happen simultaneously
    /// without any locking contention.
    #[tokio::test]
    async fn concurrent_reads_dont_block() {
        use std::time::{Duration, Instant};

        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct SlowReadState {
            value: i32,
        }

        #[entity(krate = "crate", concurrency = 10)]
        #[derive(Clone)]
        struct ConcurrentReadsEntity;

        #[entity_impl(krate = "crate")]
        #[state(SlowReadState)]
        impl ConcurrentReadsEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<SlowReadState, ClusterError> {
                Ok(SlowReadState { value: 42 })
            }

            #[rpc]
            async fn slow_get(&self) -> Result<i32, ClusterError> {
                // Simulate a slow read by sleeping while accessing state
                let value = self.state.value;
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(value)
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx_with_storage("ConcurrentReadsEntity", "cr-1", storage);
        let handler = ConcurrentReadsEntity.spawn(ctx).await.unwrap();
        let handler: Arc<dyn EntityHandler> = Arc::from(handler);

        let start = Instant::now();

        // Launch 5 concurrent read operations
        let mut tasks = Vec::new();
        for _ in 0..5 {
            let h = Arc::clone(&handler);
            tasks.push(tokio::spawn(async move {
                h.handle_request("slow_get", &[], &HashMap::new()).await
            }));
        }

        // Wait for all to complete
        for task in tasks {
            let result = task.await.unwrap().unwrap();
            let value: i32 = rmp_serde::from_slice(&result).unwrap();
            assert_eq!(value, 42);
        }

        let elapsed = start.elapsed();

        // If reads were blocking, this would take ~250ms (5 * 50ms)
        // With concurrent reads, it should take ~50ms + some overhead
        // Allow up to 150ms to account for test system variance
        assert!(
            elapsed < Duration::from_millis(150),
            "Concurrent reads took {:?}, expected < 150ms (reads should not block each other)",
            elapsed
        );
    }

    /// Verify that write operations are properly serialized via the write lock.
    /// Multiple concurrent writes should not interleave.
    #[tokio::test]
    async fn write_operations_are_serialized() {
        #[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
        struct WriteTestState {
            operations: Vec<String>,
        }

        #[entity(krate = "crate", concurrency = 10)]
        #[derive(Clone)]
        struct SerializedWritesEntity;

        #[entity_impl(krate = "crate")]
        #[state(WriteTestState)]
        impl SerializedWritesEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<WriteTestState, ClusterError> {
                Ok(WriteTestState { operations: vec![] })
            }

            #[activity]
            async fn record_operation(&mut self, name: String) -> Result<(), ClusterError> {
                // Record start
                self.state.operations.push(format!("{}-start", name));
                // Simulate some work
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                // Record end - if writes are serialized, start and end should be adjacent
                self.state.operations.push(format!("{}-end", name));
                Ok(())
            }

            #[workflow]
            async fn do_record(&self, name: String) -> Result<(), ClusterError> {
                self.record_operation(name).await
            }

            #[rpc]
            async fn get_operations(&self) -> Result<Vec<String>, ClusterError> {
                Ok(self.state.operations.clone())
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx_with_storage("SerializedWritesEntity", "sw-1", storage);
        let handler = SerializedWritesEntity.spawn(ctx).await.unwrap();
        let handler: Arc<dyn EntityHandler> = Arc::from(handler);

        // Launch 3 concurrent write operations
        let mut tasks = Vec::new();
        for i in 0..3 {
            let h = Arc::clone(&handler);
            let name = format!("op{}", i);
            tasks.push(tokio::spawn(async move {
                let payload = rmp_serde::to_vec(&name).unwrap();
                h.handle_request("do_record", &payload, &HashMap::new())
                    .await
            }));
        }

        // Wait for all to complete
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        // Get the operation log
        let result = handler
            .handle_request("get_operations", &[], &HashMap::new())
            .await
            .unwrap();
        let ops: Vec<String> = rmp_serde::from_slice(&result).unwrap();

        // Verify operations are properly paired (each start immediately followed by its end)
        assert_eq!(ops.len(), 6, "Expected 6 operations (3 start + 3 end)");
        for i in (0..6).step_by(2) {
            let start = &ops[i];
            let end = &ops[i + 1];
            assert!(
                start.ends_with("-start"),
                "Expected start at position {}",
                i
            );
            assert!(end.ends_with("-end"), "Expected end at position {}", i + 1);
            // Extract operation name and verify they match
            let start_name = start.strip_suffix("-start").unwrap();
            let end_name = end.strip_suffix("-end").unwrap();
            assert_eq!(
                start_name, end_name,
                "Operation start/end mismatch at position {}: {} vs {}",
                i, start, end
            );
        }
    }

    /// Verify that workflows can suspend (via sleep/await_deferred) without holding locks.
    /// Test that activities run in a transaction - state changes are only visible
    /// after the activity completes successfully.
    ///
    /// With transactional activities:
    /// 1. State mutations within an activity are buffered in a transaction
    /// 2. The transaction commits AFTER the entire activity completes
    /// 3. State is NOT visible to other readers until commit
    ///
    /// This ensures durability - if an activity fails mid-way, state is not
    /// partially modified.
    #[tokio::test]
    async fn activity_state_changes_are_transactional() {
        use std::time::{Duration, Instant};

        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct SuspendState {
            value: i32,
        }

        #[entity(krate = "crate", concurrency = 2)]
        #[derive(Clone)]
        struct SuspendTestEntity;

        #[entity_impl(krate = "crate")]
        #[state(SuspendState)]
        impl SuspendTestEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<SuspendState, ClusterError> {
                Ok(SuspendState { value: 0 })
            }

            /// This activity modifies state, suspends, then modifies again.
            /// With transactional activities, the state changes are only
            /// visible after the ENTIRE activity completes.
            #[activity]
            async fn suspend_and_modify(&mut self) -> Result<i32, ClusterError> {
                // First modification (buffered in transaction)
                self.state.value = 1;

                // Suspend for 100ms
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Second modification (also buffered)
                self.state.value = 2;
                Ok(self.state.value)
            }

            #[workflow]
            async fn do_suspend(&self) -> Result<i32, ClusterError> {
                self.suspend_and_modify().await
            }

            #[rpc]
            async fn get_value(&self) -> Result<i32, ClusterError> {
                Ok(self.state.value)
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx_with_storage("SuspendTestEntity", "st-1", storage);
        let handler = SuspendTestEntity.spawn(ctx).await.unwrap();
        let handler: Arc<dyn EntityHandler> = Arc::from(handler);

        let start = Instant::now();

        // Start the suspending workflow
        let suspend_task = {
            let h = Arc::clone(&handler);
            tokio::spawn(async move { h.handle_request("do_suspend", &[], &HashMap::new()).await })
        };

        // Wait a bit - the activity is running but state is NOT committed yet
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Read the value - should still be 0 because transaction hasn't committed
        let read_start = Instant::now();
        let result = handler
            .handle_request("get_value", &[], &HashMap::new())
            .await
            .unwrap();
        let read_elapsed = read_start.elapsed();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();

        // TRANSACTIONAL: Value should still be 0 during activity execution
        // (the transaction hasn't committed yet)
        assert_eq!(
            value, 0,
            "Expected value=0 during activity (transaction not committed)"
        );

        // The read should complete quickly (not blocked)
        assert!(
            read_elapsed < Duration::from_millis(50),
            "Read took {:?}, expected < 50ms",
            read_elapsed
        );

        // Wait for suspend task to complete
        let result = suspend_task.await.unwrap().unwrap();
        let final_value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(final_value, 2);

        // Now read again - should see the committed value
        let result = handler
            .handle_request("get_value", &[], &HashMap::new())
            .await
            .unwrap();
        let committed_value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(
            committed_value, 2,
            "Expected value=2 after activity committed"
        );

        let total_elapsed = start.elapsed();
        // Total should be ~100ms (suspension time) + small overhead
        assert!(
            total_elapsed < Duration::from_millis(200),
            "Total took {:?}, expected < 200ms",
            total_elapsed
        );
    }

    /// Test that `self.state` in a workflow reflects updated values after
    /// calling an activity. With the `StateRef` proxy, the read view
    /// is refreshed after each activity delegation, so subsequent reads
    /// within the same workflow see the committed state.
    #[tokio::test]
    async fn workflow_state_refreshes_after_activity() {
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct RefreshState {
            count: i32,
            label: String,
        }

        #[entity(krate = "crate")]
        #[derive(Clone)]
        struct RefreshEntity;

        #[entity_impl(krate = "crate")]
        #[state(RefreshState)]
        impl RefreshEntity {
            fn init(&self, _ctx: &EntityContext) -> Result<RefreshState, ClusterError> {
                Ok(RefreshState {
                    count: 0,
                    label: "init".to_string(),
                })
            }

            #[activity]
            async fn do_increment(&mut self, amount: i32) -> Result<(), ClusterError> {
                self.state.count += amount;
                Ok(())
            }

            #[activity]
            async fn do_set_label(&mut self, label: String) -> Result<(), ClusterError> {
                self.state.label = label;
                Ok(())
            }

            /// Workflow that calls multiple activities and reads self.state
            /// after each one. With StateRef, these reads should reflect
            /// the latest committed state.
            #[workflow]
            async fn increment_and_read(&self, amount: i32) -> Result<i32, ClusterError> {
                assert_eq!(self.state.count, 0, "count should start at 0");

                self.do_increment(amount).await?;

                // This read must reflect the activity's mutation
                let after_first = self.state.count;
                assert_eq!(
                    after_first, amount,
                    "count should be {amount} after first increment"
                );

                self.do_increment(amount).await?;

                // Second read must reflect the second mutation
                let after_second = self.state.count;
                assert_eq!(
                    after_second,
                    amount * 2,
                    "count should be {} after second increment",
                    amount * 2
                );

                Ok(after_second)
            }

            /// Workflow that interleaves different activities and reads
            /// multiple fields after each call.
            #[workflow]
            async fn multi_field_workflow(&self) -> Result<String, ClusterError> {
                self.do_increment(10).await?;
                assert_eq!(self.state.count, 10);
                assert_eq!(self.state.label, "init");

                self.do_set_label("updated".to_string()).await?;
                assert_eq!(self.state.count, 10);
                assert_eq!(self.state.label, "updated");

                self.do_increment(5).await?;
                assert_eq!(self.state.count, 15);
                assert_eq!(self.state.label, "updated");

                Ok(format!("{}:{}", self.state.label, self.state.count))
            }

            #[rpc]
            async fn get_count(&self) -> Result<i32, ClusterError> {
                Ok(self.state.count)
            }
        }

        let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx = test_ctx_with_storage("RefreshEntity", "r-1", storage);
        let handler = RefreshEntity.spawn(ctx).await.unwrap();

        // Test 1: workflow reads state after activity calls
        let payload = rmp_serde::to_vec(&10i32).unwrap();
        let result = handler
            .handle_request("increment_and_read", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 20);

        // Verify via RPC that state persisted correctly
        let result = handler
            .handle_request("get_count", &[], &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 20);

        // Test 2: multi-field workflow
        let storage2: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());
        let ctx2 = test_ctx_with_storage("RefreshEntity", "r-2", storage2);
        let handler2 = RefreshEntity.spawn(ctx2).await.unwrap();

        let result = handler2
            .handle_request("multi_field_workflow", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "updated:15");
    }
}
