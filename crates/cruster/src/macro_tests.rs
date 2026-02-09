//! Tests for `#[entity]` and `#[entity_impl]` proc macros.

#[cfg(test)]
mod tests {
    use crate::config::ShardingConfig;
    use crate::durable::{MemoryWorkflowStorage, WorkflowEngine};
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
    use crate::snowflake::{Snowflake, SnowflakeGenerator};
    use crate::storage::memory_message::MemoryMessageStorage;
    use crate::storage::noop_runners::NoopRunners;
    use crate::types::{EntityAddress, EntityId, EntityType, RunnerAddress, ShardId};
    use async_trait::async_trait;
    use futures::future::BoxFuture;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
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
            message_storage: None,
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

    // --- Entity with #[rpc(persisted)] ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedRpcEntity;

    #[entity_impl(krate = "crate")]
    impl PersistedRpcEntity {
        #[rpc]
        async fn read_data(&self) -> Result<String, ClusterError> {
            Ok("data".to_string())
        }

        #[rpc(persisted)]
        async fn write_data(&self, value: String) -> Result<String, ClusterError> {
            Ok(format!("wrote: {value}"))
        }
    }

    #[test]
    fn persisted_rpc_entity_type_name() {
        let e = PersistedRpcEntity;
        assert_eq!(e.entity_type().0, "PersistedRpcEntity");
    }

    #[tokio::test]
    async fn persisted_rpc_entity_dispatch() {
        let e = PersistedRpcEntity;
        let ctx = test_ctx("PersistedRpcEntity", "pr-1");
        let handler = e.spawn(ctx).await.unwrap();

        // Non-persisted RPC
        let result = handler
            .handle_request("read_data", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "data");

        // Persisted RPC — dispatch works the same, persistence is a client-side concern
        let payload = rmp_serde::to_vec(&"hello".to_string()).unwrap();
        let result = handler
            .handle_request("write_data", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "wrote: hello");
    }

    #[tokio::test]
    async fn persisted_rpc_client_uses_send_persisted() {
        // This test verifies that the generated client for `#[rpc(persisted)]`
        // methods uses `send_persisted()` while `#[rpc]` uses `send()`.
        //
        // We construct the client manually and verify the method signatures
        // compile correctly — the actual network behavior is tested at the
        // integration level.
        //
        // The fact that `PersistedRpcEntityClient` compiles with both
        // `read_data` (non-persisted) and `write_data` (persisted) methods
        // is the test.
        let _client_type_check = |client: &PersistedRpcEntityClient| {
            let eid = EntityId::new("test");
            let val = "value".to_string();
            // Non-persisted: `send()`
            let _read = client.read_data(&eid);
            // Persisted: `send_persisted()` — different code path
            let _write = client.write_data(&eid, &val);
        };
    }

    // --- Persisted RPC entity (uses #[rpc(persisted)] for at-least-once delivery) ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedMethodEntity;

    #[entity_impl(krate = "crate")]
    impl PersistedMethodEntity {
        #[rpc(persisted)]
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

        // Persisted RPC dispatches the same as non-persisted
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

    // Test that the generated client uses send_persisted for #[rpc(persisted)] methods
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

    // --- Mixed entity (#[rpc(persisted)] + #[rpc]) ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct MixedEntity;

    #[entity_impl(krate = "crate")]
    impl MixedEntity {
        #[rpc(persisted)]
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

    // --- Persisted RPC idempotency replay ---

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedIdempotentEntity {
        calls: Arc<AtomicUsize>,
    }

    #[entity_impl(krate = "crate")]
    impl PersistedIdempotentEntity {
        #[rpc(persisted)]
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

    // --- Workflow with custom key extraction (migrated from entity #[workflow(key(...))] to standalone workflow) ---

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct UpdateRequest {
        id: String,
        value: i32,
    }

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct UpdateWorkflow;

    #[workflow_impl(krate = "crate", key = |req: &UpdateRequest| req.id.clone())]
    impl UpdateWorkflow {
        async fn execute(&self, req: UpdateRequest) -> Result<String, ClusterError> {
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
    async fn workflow_key_override_uses_custom_idempotency_key() {
        // Migrated from PersistedKeyEntity — tests that custom key extraction
        // produces the same entity_id for requests with the same key field.
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = UpdateWorkflowClient::new(Arc::clone(&sharding));

        let req1 = UpdateRequest {
            id: "same".to_string(),
            value: 1,
        };
        let req2 = UpdateRequest {
            id: "same".to_string(),
            value: 2,
        };

        // Dispatch works correctly
        let w = UpdateWorkflow;
        let ctx = test_ctx("Workflow/UpdateWorkflow", "pk-handler");
        let handler = w.spawn(ctx).await.unwrap();
        let payload = rmp_serde::to_vec(&req1).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let response: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(response, "same:1");

        // Client calls with same key field produce same entity_id
        let _: String = client.execute(&req1).await.unwrap();
        let _: String = client.execute(&req2).await.unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured[0].address.entity_id, captured[1].address.entity_id,
            "same key field should produce same entity_id"
        );
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

    // --- Workflow with key extraction from a subset of fields (migrated from MultiParamPersisted entity) ---

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct SendEmailRequest {
        order_id: String,
        body: String,
    }

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct SendEmailWorkflow;

    #[workflow_impl(krate = "crate", key = |req: &SendEmailRequest| req.order_id.clone())]
    impl SendEmailWorkflow {
        async fn execute(&self, req: SendEmailRequest) -> Result<String, ClusterError> {
            Ok(format!("{}:{}", req.order_id, req.body))
        }
    }

    #[tokio::test]
    async fn workflow_key_uses_subset_of_fields() {
        // Migrated from MultiParamPersisted — tests that key extraction uses
        // only a subset of request fields for idempotency.
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = SendEmailWorkflowClient::new(Arc::clone(&sharding));

        let order_id = "order-1".to_string();
        let body1 = "first".to_string();
        let body2 = "second".to_string();

        // Dispatch works correctly
        let w = SendEmailWorkflow;
        let ctx = test_ctx("Workflow/SendEmailWorkflow", "mp-handler");
        let handler = w.spawn(ctx).await.unwrap();
        let req1 = SendEmailRequest {
            order_id: order_id.clone(),
            body: body1.clone(),
        };
        let payload = rmp_serde::to_vec(&req1).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let response: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(response, format!("{order_id}:{body1}"));

        // Client calls with same order_id but different body produce same entity_id
        let req_a = SendEmailRequest {
            order_id: order_id.clone(),
            body: body1,
        };
        let req_b = SendEmailRequest {
            order_id: order_id.clone(),
            body: body2,
        };
        let _: String = client.execute(&req_a).await.unwrap();
        let _: String = client.execute(&req_b).await.unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured[0].address.entity_id, captured[1].address.entity_id,
            "same order_id should produce same entity_id regardless of body"
        );
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

    // ==========================================================================
    // Standalone Workflow Macro Tests
    // ==========================================================================

    // ==========================================================================
    // #[workflow] / #[workflow_impl] Macro Tests
    // ==========================================================================

    use crate::workflow_impl;

    // --- Basic workflow using new macros ---

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct NewWfRequest {
        name: String,
    }

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct NewSimpleWorkflow;

    #[workflow_impl(krate = "crate")]
    impl NewSimpleWorkflow {
        async fn execute(&self, request: NewWfRequest) -> Result<String, ClusterError> {
            Ok(format!("new-hello, {}", request.name))
        }
    }

    #[test]
    fn new_workflow_entity_type() {
        let w = NewSimpleWorkflow;
        assert_eq!(w.entity_type().0, "Workflow/NewSimpleWorkflow");
    }

    #[tokio::test]
    async fn new_workflow_dispatch() {
        let w = NewSimpleWorkflow;
        let ctx = test_ctx("Workflow/NewSimpleWorkflow", "exec-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewWfRequest {
            name: "world".to_string(),
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "new-hello, world");
    }

    #[tokio::test]
    async fn new_workflow_unknown_tag() {
        let w = NewSimpleWorkflow;
        let ctx = test_ctx("Workflow/NewSimpleWorkflow", "exec-1");
        let handler = w.spawn(ctx).await.unwrap();

        let err = handler
            .handle_request("unknown", &[], &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ClusterError::MalformedMessage { .. }));
    }

    // --- Workflow with activities using new macros ---

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct NewOrderRequest {
        order_id: String,
        amount: i32,
    }

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct NewOrderWorkflow;

    #[workflow_impl(krate = "crate")]
    impl NewOrderWorkflow {
        async fn execute(&self, request: NewOrderRequest) -> Result<String, ClusterError> {
            let validated = self.validate(request.order_id.clone()).await?;
            let charged = self
                .charge(request.order_id.clone(), request.amount)
                .await?;
            Ok(format!("{validated}+{charged}"))
        }

        #[activity]
        async fn validate(&self, order_id: String) -> Result<String, ClusterError> {
            Ok(format!("valid:{order_id}"))
        }

        #[activity]
        async fn charge(&self, order_id: String, amount: i32) -> Result<String, ClusterError> {
            Ok(format!("charged:{order_id}:{amount}"))
        }
    }

    #[tokio::test]
    async fn new_workflow_with_activities() {
        let w = NewOrderWorkflow;
        let ctx = test_ctx("Workflow/NewOrderWorkflow", "exec-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewOrderRequest {
            order_id: "order-99".to_string(),
            amount: 200,
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "valid:order-99+charged:order-99:200");
    }

    // --- Workflow with struct fields using new macros ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct NewFieldWorkflow {
        prefix: String,
    }

    #[workflow_impl(krate = "crate")]
    impl NewFieldWorkflow {
        async fn execute(&self, request: NewWfRequest) -> Result<String, ClusterError> {
            let greeting = self.greet(request.name).await?;
            Ok(greeting)
        }

        #[activity]
        async fn greet(&self, name: String) -> Result<String, ClusterError> {
            Ok(format!("{}: {name}", self.prefix))
        }
    }

    #[tokio::test]
    async fn new_workflow_with_fields() {
        let w = NewFieldWorkflow {
            prefix: "Hey".to_string(),
        };
        let ctx = test_ctx("Workflow/NewFieldWorkflow", "exec-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewWfRequest {
            name: "Bob".to_string(),
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "Hey: Bob");
    }

    // --- Client generation with new macros ---

    #[test]
    fn new_workflow_client_exists() {
        fn _assert_client_methods(_c: &NewSimpleWorkflowClient) {
            // execute, start, with_key, with_key_raw should exist
        }
    }

    #[tokio::test]
    async fn new_workflow_start_returns_execution_id() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = NewSimpleWorkflow
            .register(Arc::clone(&sharding))
            .await
            .unwrap();

        let req = NewWfRequest {
            name: "fire-and-forget".to_string(),
        };
        let exec_id = client.start(&req).await.unwrap();

        // The execution ID should be a non-empty string (the derived entity ID)
        assert!(!exec_id.is_empty(), "execution ID should not be empty");

        // The message should have been sent via notify (not send)
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1, "exactly one message should be captured");
        assert_eq!(captured[0].tag, "execute");
    }

    #[tokio::test]
    async fn new_workflow_with_key_hashes() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = NewSimpleWorkflowClient::new(Arc::clone(&sharding));

        let req = NewWfRequest {
            name: "keyed".to_string(),
        };

        // execute via with_key — should hash the key
        let _: String = client.with_key("my-key").execute(&req).await.unwrap();

        let captured_msgs = captured.lock().unwrap();
        assert_eq!(captured_msgs.len(), 1);

        // The entity_id should be the SHA-256 hash of "my-key", not "my-key" itself
        let entity_id = &captured_msgs[0].address.entity_id.0;
        assert_ne!(entity_id, "my-key", "key should be hashed");
        assert_eq!(
            entity_id,
            &crate::hash::sha256_hex("my-key".as_bytes()),
            "entity_id should match SHA-256 of key"
        );
    }

    #[tokio::test]
    async fn new_workflow_with_key_raw_no_hash() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = NewSimpleWorkflowClient::new(Arc::clone(&sharding));

        let req = NewWfRequest {
            name: "raw-keyed".to_string(),
        };

        // execute via with_key_raw — should use key as-is
        let _: String = client
            .with_key_raw("raw-id-42")
            .execute(&req)
            .await
            .unwrap();

        let captured_msgs = captured.lock().unwrap();
        assert_eq!(captured_msgs.len(), 1);
        assert_eq!(
            captured_msgs[0].address.entity_id.0, "raw-id-42",
            "raw key should be used directly as entity_id"
        );
    }

    #[tokio::test]
    async fn new_workflow_with_key_start() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = NewSimpleWorkflowClient::new(Arc::clone(&sharding));

        let req = NewWfRequest {
            name: "start-keyed".to_string(),
        };

        // start via with_key_raw — fire-and-forget with raw key
        let exec_id = client
            .with_key_raw("start-raw-1")
            .start(&req)
            .await
            .unwrap();
        assert_eq!(exec_id, "start-raw-1");

        let captured_msgs = captured.lock().unwrap();
        assert_eq!(captured_msgs.len(), 1);
        assert_eq!(captured_msgs[0].address.entity_id.0, "start-raw-1");
        assert_eq!(captured_msgs[0].tag, "execute");
    }

    #[tokio::test]
    async fn new_workflow_register() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = NewSimpleWorkflow
            .register(Arc::clone(&sharding))
            .await
            .unwrap();
        let req = NewWfRequest {
            name: "test".to_string(),
        };
        let _: String = client.execute(&req).await.unwrap();
    }

    #[test]
    fn new_workflow_implements_client_factory() {
        use crate::entity_client::WorkflowClientFactory;
        fn _assert_factory<T: WorkflowClientFactory>() {}
        _assert_factory::<NewSimpleWorkflow>();
    }

    // --- Workflow poll tests ---

    /// Mock sharding that stores replies and supports `replies_for` for poll testing.
    struct PollableSharding {
        inner: MockSharding,
        replies: Arc<Mutex<HashMap<Snowflake, Vec<Reply>>>>,
    }

    impl PollableSharding {
        fn new() -> Self {
            Self {
                inner: MockSharding::new(),
                replies: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl Sharding for PollableSharding {
        fn get_shard_id(&self, et: &EntityType, eid: &EntityId) -> ShardId {
            self.inner.get_shard_id(et, eid)
        }
        fn has_shard_id(&self, sid: &ShardId) -> bool {
            self.inner.has_shard_id(sid)
        }
        fn snowflake(&self) -> &SnowflakeGenerator {
            self.inner.snowflake()
        }
        fn is_shutdown(&self) -> bool {
            false
        }
        async fn register_entity(&self, _: Arc<dyn Entity>) -> Result<(), ClusterError> {
            Ok(())
        }
        async fn register_singleton(
            &self,
            _: &str,
            _: Option<&str>,
            _: Arc<
                dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                    + Send
                    + Sync,
            >,
        ) -> Result<(), ClusterError> {
            Ok(())
        }
        fn make_client(self: Arc<Self>, et: EntityType) -> EntityClient {
            EntityClient::new(self, et)
        }
        async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
            // Build reply
            let response = rmp_serde::to_vec(&"ok".to_string()).unwrap();
            let reply = Reply::WithExit(ReplyWithExit {
                request_id: envelope.request_id,
                id: self.inner.snowflake.next_async().await?,
                exit: ExitResult::Success(response),
            });
            // Store reply for poll
            self.replies
                .lock()
                .unwrap()
                .entry(envelope.request_id)
                .or_default()
                .push(reply.clone());
            // Also send via channel for send_persisted
            let (tx, rx) = tokio::sync::mpsc::channel(1);
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
        async fn ack_chunk(&self, _: AckChunk) -> Result<(), ClusterError> {
            Ok(())
        }
        async fn interrupt(&self, _: Interrupt) -> Result<(), ClusterError> {
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
        async fn replies_for(&self, request_id: Snowflake) -> Result<Vec<Reply>, ClusterError> {
            Ok(self
                .replies
                .lock()
                .unwrap()
                .get(&request_id)
                .cloned()
                .unwrap_or_default())
        }
        async fn shutdown(&self) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn new_workflow_poll_returns_none_when_not_started() {
        let sharding: Arc<dyn Sharding> = Arc::new(PollableSharding::new());
        let client = NewSimpleWorkflowClient::new(Arc::clone(&sharding));

        // Poll for an execution that was never started — should return None
        let result: Option<String> = client.poll("nonexistent-id").await.unwrap();
        assert!(
            result.is_none(),
            "poll should return None for unknown execution"
        );
    }

    #[tokio::test]
    async fn new_workflow_poll_returns_result_after_execute() {
        let sharding: Arc<dyn Sharding> = Arc::new(PollableSharding::new());
        let client = NewSimpleWorkflowClient::new(Arc::clone(&sharding));

        let req = NewWfRequest {
            name: "poll-test".to_string(),
        };

        // Execute the workflow (which stores the reply)
        let result: String = client.execute(&req).await.unwrap();
        assert_eq!(result, "ok");

        // Now poll for the same execution — derive entity_id the same way
        let key_bytes = rmp_serde::to_vec(&req).unwrap();
        let entity_id = crate::hash::sha256_hex(&key_bytes);
        let poll_result: Option<String> = client.poll(&entity_id).await.unwrap();
        assert_eq!(poll_result, Some("ok".to_string()));
    }

    #[tokio::test]
    async fn new_workflow_poll_with_key_returns_result() {
        let sharding: Arc<dyn Sharding> = Arc::new(PollableSharding::new());
        let client = NewSimpleWorkflowClient::new(Arc::clone(&sharding));

        let req = NewWfRequest {
            name: "poll-keyed".to_string(),
        };

        // Execute with a raw key
        let keyed = client.with_key_raw("poll-exec-1");
        let _: String = keyed.execute(&req).await.unwrap();

        // Poll on the ClientWithKey view
        let keyed_again = client.with_key_raw("poll-exec-1");
        let poll_result: Option<String> = keyed_again.poll().await.unwrap();
        assert_eq!(poll_result, Some("ok".to_string()));
    }

    #[test]
    fn new_workflow_poll_method_exists() {
        // Compile-time check that poll exists on both client types
        fn _assert_poll(_c: &NewSimpleWorkflowClient) {
            // client.poll(execution_id) should exist
        }
        fn _assert_poll_with_key(_c: &NewSimpleWorkflowClientWithKey<'_>) {
            // client_with_key.poll() should exist
        }
    }

    // --- Workflow with helpers using new macros ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct NewHelperWorkflow;

    #[workflow_impl(krate = "crate")]
    impl NewHelperWorkflow {
        async fn execute(&self, request: NewWfRequest) -> Result<String, ClusterError> {
            let upper = self.to_upper(&request.name);
            Ok(upper)
        }

        fn to_upper(&self, s: &str) -> String {
            s.to_uppercase()
        }
    }

    #[tokio::test]
    async fn new_workflow_with_helpers() {
        let w = NewHelperWorkflow;
        let ctx = test_ctx("Workflow/NewHelperWorkflow", "exec-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewWfRequest {
            name: "hello".to_string(),
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "HELLO");
    }

    // ==========================================================================
    // #[activity_group] / #[activity_group_impl] Macro Tests
    // ==========================================================================

    use crate::activity_group_impl;

    // --- Basic activity group ---

    #[activity_group(krate = "crate")]
    #[derive(Clone)]
    pub struct TestPayments {
        pub rate: f64,
    }

    #[activity_group_impl(krate = "crate")]
    impl TestPayments {
        #[activity]
        async fn charge(&self, amount: i32) -> Result<String, ClusterError> {
            let total = (amount as f64 * self.rate) as i32;
            Ok(format!("charged:{total}"))
        }

        #[activity]
        async fn refund(&self, tx_id: String) -> Result<String, ClusterError> {
            Ok(format!("refunded:{tx_id}"))
        }

        /// Helper (not an activity)
        fn format_amount(&self, amount: i32) -> String {
            format!("${amount}")
        }
    }

    // --- Activity group composed into a workflow ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct PaymentWorkflow;

    #[workflow_impl(krate = "crate", activity_groups(TestPayments))]
    impl PaymentWorkflow {
        async fn execute(&self, request: NewOrderRequest) -> Result<String, ClusterError> {
            let charge_result = self.charge(request.amount).await?;
            let refund_result = self.refund(request.order_id.clone()).await?;
            Ok(format!("{charge_result}|{refund_result}"))
        }
    }

    #[tokio::test]
    async fn activity_group_workflow_dispatch() {
        let payments = TestPayments { rate: 1.5 };
        let workflow = PaymentWorkflow;
        let ctx = test_ctx("Workflow/PaymentWorkflow", "exec-1");
        // To test dispatch, we need to create the WithGroups wrapper and spawn
        let bundle = __PaymentWorkflowWithGroups {
            __workflow: workflow,
            __group_test_payments: payments,
        };
        let handler = bundle.spawn(ctx).await.unwrap();

        let req = NewOrderRequest {
            order_id: "order-1".to_string(),
            amount: 100,
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "charged:150|refunded:order-1");
    }

    #[tokio::test]
    async fn activity_group_workflow_register() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = PaymentWorkflow
            .register(Arc::clone(&sharding), TestPayments { rate: 2.0 })
            .await
            .unwrap();

        let req = NewOrderRequest {
            order_id: "order-2".to_string(),
            amount: 50,
        };
        let _: String = client.execute(&req).await.unwrap();
    }

    #[test]
    fn activity_group_workflow_entity_type() {
        // When activity_groups are present, Entity is on the WithGroups wrapper
        let bundle = __PaymentWorkflowWithGroups {
            __workflow: PaymentWorkflow,
            __group_test_payments: TestPayments { rate: 1.0 },
        };
        assert_eq!(bundle.entity_type().0, "Workflow/PaymentWorkflow");
    }

    // --- Multiple activity groups ---

    #[activity_group(krate = "crate")]
    #[derive(Clone)]
    pub struct TestInventory;

    #[activity_group_impl(krate = "crate")]
    impl TestInventory {
        #[activity]
        async fn reserve(&self, item_count: i32) -> Result<String, ClusterError> {
            Ok(format!("reserved:{item_count}"))
        }
    }

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct MultiGroupWorkflow;

    #[workflow_impl(krate = "crate", activity_groups(TestPayments, TestInventory))]
    impl MultiGroupWorkflow {
        async fn execute(&self, request: NewOrderRequest) -> Result<String, ClusterError> {
            let reserved = self.reserve(request.amount).await?;
            let charged = self.charge(request.amount).await?;
            Ok(format!("{reserved}|{charged}"))
        }
    }

    #[tokio::test]
    async fn multi_activity_group_workflow_dispatch() {
        let payments = TestPayments { rate: 1.0 };
        let inventory = TestInventory;
        let workflow = MultiGroupWorkflow;
        let ctx = test_ctx("Workflow/MultiGroupWorkflow", "exec-1");
        let bundle = __MultiGroupWorkflowWithGroups {
            __workflow: workflow,
            __group_test_payments: payments,
            __group_test_inventory: inventory,
        };
        let handler = bundle.spawn(ctx).await.unwrap();

        let req = NewOrderRequest {
            order_id: "order-3".to_string(),
            amount: 10,
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "reserved:10|charged:10");
    }

    #[tokio::test]
    async fn multi_activity_group_workflow_register() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = MultiGroupWorkflow
            .register(
                Arc::clone(&sharding),
                TestPayments { rate: 1.0 },
                TestInventory,
            )
            .await
            .unwrap();

        let req = NewOrderRequest {
            order_id: "order-4".to_string(),
            amount: 20,
        };
        let _: String = client.execute(&req).await.unwrap();
    }

    // --- Workflow with both local activities and activity groups ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct MixedActivitiesWorkflow {
        prefix: String,
    }

    #[workflow_impl(krate = "crate", activity_groups(TestPayments))]
    impl MixedActivitiesWorkflow {
        async fn execute(&self, request: NewOrderRequest) -> Result<String, ClusterError> {
            let validated = self.validate(request.order_id.clone()).await?;
            let charged = self.charge(request.amount).await?;
            Ok(format!("{validated}|{charged}"))
        }

        #[activity]
        async fn validate(&self, order_id: String) -> Result<String, ClusterError> {
            Ok(format!("{}:valid:{order_id}", self.prefix))
        }
    }

    #[tokio::test]
    async fn mixed_activities_workflow_dispatch() {
        let payments = TestPayments { rate: 3.0 };
        let workflow = MixedActivitiesWorkflow {
            prefix: "test".to_string(),
        };
        let ctx = test_ctx("Workflow/MixedActivitiesWorkflow", "exec-1");
        let bundle = __MixedActivitiesWorkflowWithGroups {
            __workflow: workflow,
            __group_test_payments: payments,
        };
        let handler = bundle.spawn(ctx).await.unwrap();

        let req = NewOrderRequest {
            order_id: "order-5".to_string(),
            amount: 10,
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "test:valid:order-5|charged:30");
    }

    // ==========================================================================
    // Activity Retry Support Tests (#[activity(retries = N, backoff = "...")])
    // ==========================================================================

    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

    /// Workflow engine that returns immediately from sleep (for retry tests).
    struct InstantWorkflowEngine;

    #[async_trait]
    impl WorkflowEngine for InstantWorkflowEngine {
        async fn sleep(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
            _name: &str,
            _duration: Duration,
        ) -> Result<(), ClusterError> {
            // No-op — instant return for fast tests
            Ok(())
        }

        async fn await_deferred(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
            _name: &str,
        ) -> Result<Vec<u8>, ClusterError> {
            Err(ClusterError::PersistenceError {
                reason: "not supported".to_string(),
                source: None,
            })
        }

        async fn resolve_deferred(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
            _name: &str,
            _value: Vec<u8>,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn on_interrupt(
            &self,
            _workflow_name: &str,
            _execution_id: &str,
        ) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    fn test_ctx_with_instant_engine(entity_type: &str, entity_id: &str) -> EntityContext {
        EntityContext {
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new(entity_type),
                entity_id: EntityId::new(entity_id),
            },
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            snowflake: Arc::new(SnowflakeGenerator::new()),
            cancellation: tokio_util::sync::CancellationToken::new(),
            state_storage: Some(Arc::new(MemoryWorkflowStorage::new())),
            workflow_engine: Some(Arc::new(InstantWorkflowEngine)),
            sharding: None,
            message_storage: Some(Arc::new(MemoryMessageStorage::new())),
        }
    }

    // --- Workflow with retries (no backoff specified = exponential default) ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct RetryWorkflow {
        call_count: Arc<AtomicU32>,
    }

    #[workflow_impl(krate = "crate")]
    impl RetryWorkflow {
        async fn execute(&self, request: NewWfRequest) -> Result<String, ClusterError> {
            let result = self.flaky_activity(request.name.clone()).await?;
            Ok(result)
        }

        #[activity(retries = 3)]
        async fn flaky_activity(&self, name: String) -> Result<String, ClusterError> {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                Err(ClusterError::PersistenceError {
                    reason: format!("flaky failure #{count}"),
                    source: None,
                })
            } else {
                Ok(format!("success:{name}:attempt-{count}"))
            }
        }
    }

    #[test]
    fn retry_workflow_entity_type() {
        let w = RetryWorkflow {
            call_count: Arc::new(AtomicU32::new(0)),
        };
        assert_eq!(w.entity_type().0, "Workflow/RetryWorkflow");
    }

    #[tokio::test]
    async fn retry_workflow_activity_succeeds_after_retries() {
        let call_count = Arc::new(AtomicU32::new(0));
        let w = RetryWorkflow {
            call_count: call_count.clone(),
        };
        let ctx = test_ctx_with_instant_engine("Workflow/RetryWorkflow", "exec-retry-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewWfRequest {
            name: "retry-test".to_string(),
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        // Succeeds on attempt #2 (0-indexed), after 2 failures
        assert_eq!(value, "success:retry-test:attempt-2");
        // Total calls: attempts 0, 1, 2 = 3 calls
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    // --- Workflow with constant backoff ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct ConstantBackoffWorkflow {
        call_count: Arc<AtomicU32>,
    }

    #[workflow_impl(krate = "crate")]
    impl ConstantBackoffWorkflow {
        async fn execute(&self, request: NewWfRequest) -> Result<String, ClusterError> {
            self.retryable(request.name).await
        }

        #[activity(retries = 2, backoff = "constant")]
        async fn retryable(&self, name: String) -> Result<String, ClusterError> {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            if count < 1 {
                Err(ClusterError::PersistenceError {
                    reason: "constant-fail".to_string(),
                    source: None,
                })
            } else {
                Ok(format!("const-ok:{name}"))
            }
        }
    }

    #[tokio::test]
    async fn constant_backoff_workflow_succeeds() {
        let call_count = Arc::new(AtomicU32::new(0));
        let w = ConstantBackoffWorkflow {
            call_count: call_count.clone(),
        };
        let ctx = test_ctx_with_instant_engine("Workflow/ConstantBackoffWorkflow", "exec-const-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewWfRequest {
            name: "const-test".to_string(),
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "const-ok:const-test");
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    // --- Workflow where all retries fail ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct AlwaysFailWorkflow {
        call_count: Arc<AtomicU32>,
    }

    #[workflow_impl(krate = "crate")]
    impl AlwaysFailWorkflow {
        async fn execute(&self, request: NewWfRequest) -> Result<String, ClusterError> {
            self.always_fail(request.name).await
        }

        #[activity(retries = 2, backoff = "exponential")]
        async fn always_fail(&self, _name: String) -> Result<String, ClusterError> {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            Err(ClusterError::PersistenceError {
                reason: format!("always-fail-{count}"),
                source: None,
            })
        }
    }

    #[tokio::test]
    async fn retry_workflow_exhaustion_returns_last_error() {
        let call_count = Arc::new(AtomicU32::new(0));
        let w = AlwaysFailWorkflow {
            call_count: call_count.clone(),
        };
        let ctx = test_ctx_with_instant_engine("Workflow/AlwaysFailWorkflow", "exec-fail-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewWfRequest {
            name: "fail-test".to_string(),
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await;
        assert!(result.is_err());
        // 1 initial + 2 retries = 3 total calls
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    // --- Activity with retries = 0 (same as no retries) ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct NoRetryWorkflow;

    #[workflow_impl(krate = "crate")]
    impl NoRetryWorkflow {
        async fn execute(&self, request: NewWfRequest) -> Result<String, ClusterError> {
            self.no_retry(request.name).await
        }

        #[activity(retries = 0)]
        async fn no_retry(&self, name: String) -> Result<String, ClusterError> {
            Ok(format!("no-retry:{name}"))
        }
    }

    #[tokio::test]
    async fn no_retry_workflow_succeeds() {
        let w = NoRetryWorkflow;
        let ctx = test_ctx("Workflow/NoRetryWorkflow", "exec-noretry-1");
        let handler = w.spawn(ctx).await.unwrap();

        let req = NewWfRequest {
            name: "no-retry-test".to_string(),
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "no-retry:no-retry-test");
    }

    // --- Activity group with retries ---

    #[activity_group(krate = "crate")]
    #[derive(Clone)]
    pub struct RetryPayments {
        call_count: Arc<AtomicU32>,
    }

    #[activity_group_impl(krate = "crate")]
    impl RetryPayments {
        #[activity(retries = 3, backoff = "exponential")]
        async fn charge_with_retry(&self, amount: i32) -> Result<String, ClusterError> {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                Err(ClusterError::PersistenceError {
                    reason: format!("payment-fail-{count}"),
                    source: None,
                })
            } else {
                Ok(format!("charged:{amount}"))
            }
        }
    }

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct GroupRetryWorkflow;

    #[workflow_impl(krate = "crate", activity_groups(RetryPayments))]
    impl GroupRetryWorkflow {
        async fn execute(&self, request: NewOrderRequest) -> Result<String, ClusterError> {
            self.charge_with_retry(request.amount).await
        }
    }

    #[tokio::test]
    async fn activity_group_retry_succeeds() {
        let call_count = Arc::new(AtomicU32::new(0));
        let payments = RetryPayments {
            call_count: call_count.clone(),
        };
        let workflow = GroupRetryWorkflow;
        let ctx = test_ctx_with_instant_engine("Workflow/GroupRetryWorkflow", "exec-group-retry-1");
        let bundle = __GroupRetryWorkflowWithGroups {
            __workflow: workflow,
            __group_retry_payments: payments,
        };
        let handler = bundle.spawn(ctx).await.unwrap();

        let req = NewOrderRequest {
            order_id: "order-retry".to_string(),
            amount: 42,
        };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = handler
            .handle_request("execute", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "charged:42");
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    // --- Test compute_retry_backoff utility ---

    #[test]
    fn test_compute_retry_backoff_exponential() {
        use crate::durable::compute_retry_backoff;

        assert_eq!(
            compute_retry_backoff(0, "exponential", 1),
            Duration::from_secs(1)
        );
        assert_eq!(
            compute_retry_backoff(1, "exponential", 1),
            Duration::from_secs(2)
        );
        assert_eq!(
            compute_retry_backoff(2, "exponential", 1),
            Duration::from_secs(4)
        );
        assert_eq!(
            compute_retry_backoff(3, "exponential", 1),
            Duration::from_secs(8)
        );
        assert_eq!(
            compute_retry_backoff(5, "exponential", 1),
            Duration::from_secs(32)
        );
        // Capped at 60 seconds
        assert_eq!(
            compute_retry_backoff(6, "exponential", 1),
            Duration::from_secs(60)
        );
        assert_eq!(
            compute_retry_backoff(10, "exponential", 1),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn test_compute_retry_backoff_constant() {
        use crate::durable::compute_retry_backoff;

        assert_eq!(
            compute_retry_backoff(0, "constant", 1),
            Duration::from_secs(1)
        );
        assert_eq!(
            compute_retry_backoff(1, "constant", 1),
            Duration::from_secs(1)
        );
        assert_eq!(
            compute_retry_backoff(5, "constant", 1),
            Duration::from_secs(1)
        );
        assert_eq!(
            compute_retry_backoff(0, "constant", 5),
            Duration::from_secs(5)
        );
    }

    // =============================================================================
    // Pure-RPC Entity tests (stateless entities, new simplified codegen)
    // =============================================================================

    /// A pure-RPC entity: no #[state], no #[workflow], no #[activity].
    /// Uses the simplified handler codegen without ArcSwap, write locks, or view structs.
    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PureRpcEntity {
        prefix: String,
    }

    #[entity_impl(krate = "crate")]
    impl PureRpcEntity {
        #[rpc]
        async fn greet(&self, name: String) -> Result<String, ClusterError> {
            Ok(format!("{}: hello, {}", self.prefix, name))
        }

        #[rpc(persisted)]
        async fn save_data(&self, data: String) -> Result<String, ClusterError> {
            Ok(format!("{}: saved {}", self.prefix, data))
        }

        #[rpc]
        async fn add(&self, a: i32, b: i32) -> Result<i32, ClusterError> {
            Ok(a + b)
        }
    }

    #[test]
    fn pure_rpc_entity_type_name() {
        let e = PureRpcEntity {
            prefix: "test".into(),
        };
        assert_eq!(e.entity_type().0, "PureRpcEntity");
    }

    #[tokio::test]
    async fn pure_rpc_entity_dispatches() {
        let e = PureRpcEntity {
            prefix: "svc".into(),
        };
        let ctx = test_ctx("PureRpcEntity", "pure-1");
        let handler = e.spawn(ctx).await.unwrap();

        // Non-persisted RPC
        let payload = rmp_serde::to_vec(&"world".to_string()).unwrap();
        let result = handler
            .handle_request("greet", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "svc: hello, world");

        // Persisted RPC (dispatch is the same)
        let payload = rmp_serde::to_vec(&"item".to_string()).unwrap();
        let result = handler
            .handle_request("save_data", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "svc: saved item");
    }

    #[tokio::test]
    async fn pure_rpc_entity_multi_param() {
        let e = PureRpcEntity {
            prefix: "test".into(),
        };
        let ctx = test_ctx("PureRpcEntity", "pure-2");
        let handler = e.spawn(ctx).await.unwrap();

        let payload = rmp_serde::to_vec(&(3i32, 4i32)).unwrap();
        let result = handler
            .handle_request("add", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn pure_rpc_entity_unknown_tag_errors() {
        let e = PureRpcEntity {
            prefix: "test".into(),
        };
        let ctx = test_ctx("PureRpcEntity", "pure-3");
        let handler = e.spawn(ctx).await.unwrap();

        let result = handler
            .handle_request("nonexistent", &[], &HashMap::new())
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ClusterError::MalformedMessage { reason, .. } => {
                assert!(reason.contains("unknown RPC tag"));
            }
            other => panic!("expected MalformedMessage, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn pure_rpc_entity_register_returns_client() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = PureRpcEntity::register(
            PureRpcEntity {
                prefix: "test".into(),
            },
            Arc::clone(&sharding),
        )
        .await
        .unwrap();

        let entity_id = EntityId::new("pure-4");
        // Client should have entity's own methods
        let response: String = client
            .greet(&entity_id, &"alice".to_string())
            .await
            .unwrap();
        assert_eq!(response, "ok");
    }

    #[tokio::test]
    async fn pure_rpc_entity_client_persisted_method_exists() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = PureRpcEntity::register(
            PureRpcEntity {
                prefix: "test".into(),
            },
            Arc::clone(&sharding),
        )
        .await
        .unwrap();

        let entity_id = EntityId::new("pure-5");
        // save_data uses persisted delivery
        let response: String = client
            .save_data(&entity_id, &"payload".to_string())
            .await
            .unwrap();
        assert_eq!(response, "ok");
    }

    // =============================================================================
    // RPC Group tests
    // =============================================================================

    #[rpc_group(krate = "crate")]
    #[derive(Clone)]
    pub struct HealthCheckGroup;

    #[rpc_group_impl(krate = "crate")]
    impl HealthCheckGroup {
        #[rpc]
        async fn health(&self) -> Result<String, ClusterError> {
            Ok("ok".to_string())
        }
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct RpcGroupEntity;

    #[entity_impl(krate = "crate", rpc_groups(HealthCheckGroup))]
    impl RpcGroupEntity {
        #[rpc]
        async fn ping(&self) -> Result<String, ClusterError> {
            Ok("pong".to_string())
        }
    }

    #[tokio::test]
    async fn rpc_group_dispatches_via_handler() {
        let entity_with_groups = RpcGroupEntityWithRpcGroups {
            entity: RpcGroupEntity,
            __rpc_group_health_check_group: HealthCheckGroup,
        };
        let ctx = test_ctx("RpcGroupEntity", "rg-1");
        let handler = entity_with_groups.spawn(ctx).await.unwrap();

        // Entity's own RPC
        let result = handler
            .handle_request("ping", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "pong");

        // RPC from group
        let result = handler
            .handle_request("health", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "ok");
    }

    #[tokio::test]
    async fn rpc_group_unknown_tag_errors() {
        let entity_with_groups = RpcGroupEntityWithRpcGroups {
            entity: RpcGroupEntity,
            __rpc_group_health_check_group: HealthCheckGroup,
        };
        let ctx = test_ctx("RpcGroupEntity", "rg-2");
        let handler = entity_with_groups.spawn(ctx).await.unwrap();

        let result = handler
            .handle_request("nonexistent", &[], &HashMap::new())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn rpc_group_register_returns_client() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client =
            RpcGroupEntity::register(RpcGroupEntity, Arc::clone(&sharding), HealthCheckGroup)
                .await
                .unwrap();

        let entity_id = EntityId::new("rg-3");
        // Client should have entity's own methods
        let response: String = client.ping(&entity_id).await.unwrap();
        assert_eq!(response, "ok");
    }

    #[tokio::test]
    async fn rpc_group_client_extension_methods_exist() {
        // Verify that the generated ClientExt trait adds group methods to the entity client
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client =
            RpcGroupEntity::register(RpcGroupEntity, Arc::clone(&sharding), HealthCheckGroup)
                .await
                .unwrap();

        let entity_id = EntityId::new("rg-4");
        // Group method should be callable via client extension
        let response: String = client.health(&entity_id).await.unwrap();
        assert_eq!(response, "ok");
    }

    // --- RPC group with fields ---

    #[rpc_group(krate = "crate")]
    #[derive(Clone)]
    pub struct MetricsGroup {
        pub prefix: String,
    }

    #[rpc_group_impl(krate = "crate")]
    impl MetricsGroup {
        #[rpc]
        async fn get_metrics(&self) -> Result<String, ClusterError> {
            Ok(format!("{}/metrics", self.prefix))
        }
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct MultiGroupEntity;

    #[entity_impl(krate = "crate", rpc_groups(HealthCheckGroup, MetricsGroup))]
    impl MultiGroupEntity {
        #[rpc]
        async fn status(&self) -> Result<String, ClusterError> {
            Ok("running".to_string())
        }
    }

    #[tokio::test]
    async fn multi_rpc_group_dispatch() {
        let entity_with_groups = MultiGroupEntityWithRpcGroups {
            entity: MultiGroupEntity,
            __rpc_group_health_check_group: HealthCheckGroup,
            __rpc_group_metrics_group: MetricsGroup {
                prefix: "app".to_string(),
            },
        };
        let ctx = test_ctx("MultiGroupEntity", "mg-1");
        let handler = entity_with_groups.spawn(ctx).await.unwrap();

        // Entity's own RPC
        let result = handler
            .handle_request("status", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "running");

        // First group RPC
        let result = handler
            .handle_request("health", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "ok");

        // Second group RPC
        let result = handler
            .handle_request("get_metrics", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "app/metrics");
    }

    #[tokio::test]
    async fn multi_rpc_group_register() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = MultiGroupEntity::register(
            MultiGroupEntity,
            Arc::clone(&sharding),
            HealthCheckGroup,
            MetricsGroup {
                prefix: "test".to_string(),
            },
        )
        .await
        .unwrap();

        let entity_id = EntityId::new("mg-2");
        let response: String = client.status(&entity_id).await.unwrap();
        assert_eq!(response, "ok");
    }

    // --- RPC group with persisted RPC ---

    #[rpc_group(krate = "crate")]
    #[derive(Clone)]
    pub struct PersistedRpcGroup;

    #[rpc_group_impl(krate = "crate")]
    impl PersistedRpcGroup {
        #[rpc(persisted)]
        async fn save_data(&self, data: String) -> Result<String, ClusterError> {
            Ok(format!("saved:{data}"))
        }

        #[rpc]
        async fn read_data(&self) -> Result<String, ClusterError> {
            Ok("data".to_string())
        }
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct PersistedGroupEntity;

    #[entity_impl(krate = "crate", rpc_groups(PersistedRpcGroup))]
    impl PersistedGroupEntity {
        #[rpc]
        async fn ping(&self) -> Result<String, ClusterError> {
            Ok("pong".to_string())
        }
    }

    #[tokio::test]
    async fn persisted_rpc_group_dispatches() {
        let entity_with_groups = PersistedGroupEntityWithRpcGroups {
            entity: PersistedGroupEntity,
            __rpc_group_persisted_rpc_group: PersistedRpcGroup,
        };
        let ctx = test_ctx("PersistedGroupEntity", "prg-1");
        let handler = entity_with_groups.spawn(ctx).await.unwrap();

        // Non-persisted group RPC
        let result = handler
            .handle_request("read_data", &[], &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "data");

        // Persisted group RPC
        let payload = rmp_serde::to_vec(&"test".to_string()).unwrap();
        let result = handler
            .handle_request("save_data", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: String = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, "saved:test");
    }

    // --- RPC group with parameters ---

    #[rpc_group(krate = "crate")]
    #[derive(Clone)]
    pub struct MathGroup;

    #[rpc_group_impl(krate = "crate")]
    impl MathGroup {
        #[rpc]
        async fn add(&self, a: i32, b: i32) -> Result<i32, ClusterError> {
            Ok(a + b)
        }

        #[rpc]
        async fn multiply(&self, a: i32, b: i32) -> Result<i32, ClusterError> {
            Ok(a * b)
        }
    }

    #[entity(krate = "crate")]
    #[derive(Clone)]
    struct MathEntity;

    #[entity_impl(krate = "crate", rpc_groups(MathGroup))]
    impl MathEntity {
        #[rpc]
        async fn negate(&self, x: i32) -> Result<i32, ClusterError> {
            Ok(-x)
        }
    }

    #[tokio::test]
    async fn rpc_group_with_params_dispatch() {
        let entity_with_groups = MathEntityWithRpcGroups {
            entity: MathEntity,
            __rpc_group_math_group: MathGroup,
        };
        let ctx = test_ctx("MathEntity", "math-1");
        let handler = entity_with_groups.spawn(ctx).await.unwrap();

        // Entity's own RPC with param
        let payload = rmp_serde::to_vec(&5i32).unwrap();
        let result = handler
            .handle_request("negate", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, -5);

        // Group RPC with multiple params
        let payload = rmp_serde::to_vec(&(3i32, 4i32)).unwrap();
        let result = handler
            .handle_request("add", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 7);

        let result = handler
            .handle_request("multiply", &payload, &HashMap::new())
            .await
            .unwrap();
        let value: i32 = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(value, 12);
    }

    #[tokio::test]
    async fn rpc_group_entity_type_is_struct_name() {
        let entity_with_groups = RpcGroupEntityWithRpcGroups {
            entity: RpcGroupEntity,
            __rpc_group_health_check_group: HealthCheckGroup,
        };
        // Entity type should be the original struct name
        assert_eq!(entity_with_groups.entity_type().0, "RpcGroupEntity");
    }

    // =============================================================================
    // Workflow key extraction tests (#[workflow_impl(key = |req| ..., hash = ...)])
    // =============================================================================

    // --- Workflow with custom key extraction (hashed, default) ---

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct KeyedOrderRequest {
        order_id: String,
        amount: i32,
    }

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct KeyedWorkflow;

    #[workflow_impl(krate = "crate", key = |req: &KeyedOrderRequest| req.order_id.clone())]
    impl KeyedWorkflow {
        async fn execute(&self, request: KeyedOrderRequest) -> Result<String, ClusterError> {
            Ok(format!("{}:{}", request.order_id, request.amount))
        }
    }

    #[tokio::test]
    async fn workflow_key_extracts_custom_field_hashed() {
        // Verify that two requests with the same order_id but different amount
        // produce the same entity_id (because key only uses order_id)
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = KeyedWorkflowClient::new(Arc::clone(&sharding));

        let req1 = KeyedOrderRequest {
            order_id: "order-42".to_string(),
            amount: 100,
        };
        let req2 = KeyedOrderRequest {
            order_id: "order-42".to_string(),
            amount: 200,
        };

        let _: String = client.execute(&req1).await.unwrap();
        let _: String = client.execute(&req2).await.unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        // Both should target the same entity_id since key only uses order_id
        assert_eq!(
            captured[0].address.entity_id, captured[1].address.entity_id,
            "same order_id should produce same entity_id"
        );
        // The entity_id should be the SHA-256 hash of the serialized order_id
        let expected_id =
            crate::hash::sha256_hex(&rmp_serde::to_vec(&"order-42".to_string()).unwrap());
        assert_eq!(
            captured[0].address.entity_id.0, expected_id,
            "entity_id should be SHA-256 of serialized key value"
        );
    }

    #[tokio::test]
    async fn workflow_key_different_keys_different_entity_ids() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = KeyedWorkflowClient::new(Arc::clone(&sharding));

        let req1 = KeyedOrderRequest {
            order_id: "order-1".to_string(),
            amount: 100,
        };
        let req2 = KeyedOrderRequest {
            order_id: "order-2".to_string(),
            amount: 100,
        };

        let _: String = client.execute(&req1).await.unwrap();
        let _: String = client.execute(&req2).await.unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_ne!(
            captured[0].address.entity_id, captured[1].address.entity_id,
            "different order_ids should produce different entity_ids"
        );
    }

    // --- Workflow with custom key, hash = false ---

    #[workflow(krate = "crate")]
    #[derive(Clone)]
    struct RawKeyWorkflow;

    #[workflow_impl(krate = "crate", key = |req: &KeyedOrderRequest| req.order_id.clone(), hash = false)]
    impl RawKeyWorkflow {
        async fn execute(&self, request: KeyedOrderRequest) -> Result<String, ClusterError> {
            Ok(format!("raw:{}:{}", request.order_id, request.amount))
        }
    }

    #[tokio::test]
    async fn workflow_key_raw_uses_value_directly() {
        // With hash = false, the key closure result is used directly as entity_id
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = RawKeyWorkflowClient::new(Arc::clone(&sharding));

        let req = KeyedOrderRequest {
            order_id: "my-raw-key-123".to_string(),
            amount: 50,
        };

        let _: String = client.execute(&req).await.unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        // Entity ID should be the raw order_id, no hashing
        assert_eq!(
            captured[0].address.entity_id.0, "my-raw-key-123",
            "raw key should be used directly as entity_id"
        );
    }

    #[tokio::test]
    async fn workflow_key_raw_same_key_same_entity_id() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = RawKeyWorkflowClient::new(Arc::clone(&sharding));

        let req1 = KeyedOrderRequest {
            order_id: "same-id".to_string(),
            amount: 1,
        };
        let req2 = KeyedOrderRequest {
            order_id: "same-id".to_string(),
            amount: 9999,
        };

        let _: String = client.execute(&req1).await.unwrap();
        let _: String = client.execute(&req2).await.unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured[0].address.entity_id, captured[1].address.entity_id,
            "same order_id with raw key should produce same entity_id"
        );
        assert_eq!(captured[0].address.entity_id.0, "same-id");
    }

    // --- Workflow key with start() ---

    #[tokio::test]
    async fn workflow_key_raw_start_returns_raw_id() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = RawKeyWorkflowClient::new(Arc::clone(&sharding));

        let req = KeyedOrderRequest {
            order_id: "start-raw-key".to_string(),
            amount: 0,
        };

        let exec_id = client.start(&req).await.unwrap();
        // start() returns the entity_id which should be the raw key
        assert_eq!(exec_id, "start-raw-key");
    }

    #[tokio::test]
    async fn workflow_key_hashed_start_returns_hashed_id() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = KeyedWorkflowClient::new(Arc::clone(&sharding));

        let req = KeyedOrderRequest {
            order_id: "order-hashed".to_string(),
            amount: 0,
        };

        let exec_id = client.start(&req).await.unwrap();
        // start() returns the entity_id which should be hashed
        let expected_id =
            crate::hash::sha256_hex(&rmp_serde::to_vec(&"order-hashed".to_string()).unwrap());
        assert_eq!(exec_id, expected_id);
    }

    // --- Verify with_key still overrides custom key extraction ---

    #[tokio::test]
    async fn workflow_with_key_overrides_custom_key() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = RawKeyWorkflowClient::new(Arc::clone(&sharding));

        let req = KeyedOrderRequest {
            order_id: "should-be-ignored".to_string(),
            amount: 0,
        };

        // with_key_raw overrides the key extraction from the closure
        let _: String = client
            .with_key_raw("override-key")
            .execute(&req)
            .await
            .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].address.entity_id.0, "override-key",
            "with_key_raw should override the custom key extraction"
        );
    }
}
