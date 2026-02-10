//! Integration tests for ShardingImpl with real PostgreSQL storage.
//!
//! These tests exercise entity lifecycle, persisted message flow, and
//! storage-backed poll/resumption using a real PostgreSQL container.
//!
//! Run: `cargo test -p cruster --test sharding_integration`
//!
//! Requires Docker to be running.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cruster::config::ShardingConfig;
use cruster::entity::{Entity, EntityContext, EntityHandler};
use cruster::envelope::EnvelopeRequest;
use cruster::error::ClusterError;
use cruster::message_storage::{MessageStorage, SaveResult};
use cruster::metrics::ClusterMetrics;
use cruster::reply::{ExitResult, Reply, ReplyWithExit};
use cruster::sharding::Sharding;
use cruster::sharding_impl::ShardingImpl;
use cruster::snowflake::Snowflake;
use cruster::storage::noop_runners::NoopRunners;
use cruster::storage::sql_message::SqlMessageStorage;
use cruster::storage::sql_workflow::SqlWorkflowStorage;
use cruster::types::{EntityId, EntityType, ShardId};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ============================================================================
// Shared test infrastructure
// ============================================================================

/// Start a Postgres container and return a connected `PgPool` with migrations applied.
async fn setup_postgres() -> (testcontainers::ContainerAsync<Postgres>, sqlx::PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start postgres container");

    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get port");

    let url = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("failed to connect to postgres");

    let wf_storage = SqlWorkflowStorage::new(pool.clone());
    wf_storage.migrate().await.expect("migration failed");

    (container, pool)
}

/// Create a ShardingImpl with SqlMessageStorage, acquire all shards.
async fn make_sharding_with_storage(
    pool: sqlx::PgPool,
) -> (Arc<ShardingImpl>, Arc<SqlMessageStorage>) {
    let config = Arc::new(ShardingConfig {
        shard_groups: vec!["default".to_string()],
        shards_per_group: 10,
        ..Default::default()
    });
    let storage = Arc::new(SqlMessageStorage::new(pool).with_last_read_guard_interval(
        std::time::Duration::ZERO,
    ));
    let metrics = Arc::new(ClusterMetrics::unregistered());
    let s = ShardingImpl::new(
        config,
        Arc::new(NoopRunners),
        None,
        None,
        Some(storage.clone() as Arc<dyn MessageStorage>),
        metrics,
    )
    .unwrap();
    s.acquire_all_shards().await;
    (s, storage)
}

/// Create a bare ShardingImpl with no storage.
fn make_bare_sharding() -> Arc<ShardingImpl> {
    let config = Arc::new(ShardingConfig {
        shard_groups: vec!["default".to_string()],
        shards_per_group: 10,
        ..Default::default()
    });
    let metrics = Arc::new(ClusterMetrics::unregistered());
    ShardingImpl::new(
        config,
        Arc::new(NoopRunners),
        None,
        None,
        None,
        metrics,
    )
    .unwrap()
}

// ============================================================================
// Test entities
// ============================================================================

/// Simple echo entity — returns the payload as-is for "echo", empty for "ping".
struct EchoEntity;

struct EchoHandler;

#[async_trait]
impl Entity for EchoEntity {
    fn entity_type(&self) -> EntityType {
        EntityType::new("Echo")
    }

    async fn spawn(
        &self,
        _ctx: EntityContext,
    ) -> Result<Box<dyn EntityHandler>, ClusterError> {
        Ok(Box::new(EchoHandler))
    }
}

#[async_trait]
impl EntityHandler for EchoHandler {
    async fn handle_request(
        &self,
        tag: &str,
        payload: &[u8],
        _headers: &HashMap<String, String>,
    ) -> Result<Vec<u8>, ClusterError> {
        match tag {
            "echo" => Ok(payload.to_vec()),
            "ping" => Ok(vec![]),
            "fail" => Err(ClusterError::MalformedMessage {
                reason: "intentional failure".into(),
                source: None,
            }),
            other => Err(ClusterError::MalformedMessage {
                reason: format!("unknown tag: {other}"),
                source: None,
            }),
        }
    }
}

/// Stateful counter entity.
struct CounterEntity;

struct CounterHandler {
    count: AtomicUsize,
}

#[async_trait]
impl Entity for CounterEntity {
    fn entity_type(&self) -> EntityType {
        EntityType::new("Counter")
    }

    async fn spawn(
        &self,
        _ctx: EntityContext,
    ) -> Result<Box<dyn EntityHandler>, ClusterError> {
        Ok(Box::new(CounterHandler {
            count: AtomicUsize::new(0),
        }))
    }
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
                let amount: usize = rmp_serde::from_slice(payload).unwrap();
                let new_val = self.count.fetch_add(amount, Ordering::SeqCst) + amount;
                Ok(rmp_serde::to_vec(&new_val).unwrap())
            }
            "get" => {
                let val = self.count.load(Ordering::SeqCst);
                Ok(rmp_serde::to_vec(&val).unwrap())
            }
            _ => Err(ClusterError::MalformedMessage {
                reason: "unknown tag".into(),
                source: None,
            }),
        }
    }
}

fn make_envelope(
    s: &ShardingImpl,
    request_id: i64,
    entity_type: &str,
    entity_id: &str,
    tag: &str,
    payload: Vec<u8>,
    persisted: bool,
) -> EnvelopeRequest {
    let eid = EntityId::new(entity_id);
    let shard = s.get_shard_id(&EntityType::new(entity_type), &eid);
    EnvelopeRequest {
        request_id: Snowflake(request_id),
        address: cruster::types::EntityAddress {
            shard_id: shard,
            entity_type: EntityType::new(entity_type),
            entity_id: eid,
        },
        tag: tag.into(),
        payload,
        headers: HashMap::new(),
        span_id: None,
        trace_id: None,
        sampled: None,
        persisted,
        uninterruptible: Default::default(),
        deliver_at: None,
    }
}

// ============================================================================
// Entity Lifecycle Tests
// ============================================================================

mod entity_lifecycle {
    use super::*;

    #[tokio::test]
    async fn register_entity_and_send_request() {
        let s = make_bare_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let payload = rmp_serde::to_vec(&42i32).unwrap();
        let env = make_envelope(&s, 1000, "Echo", "e-1", "echo", payload.clone(), false);
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();

        match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => assert_eq!(data, &payload),
                ExitResult::Failure(msg) => panic!("unexpected failure: {msg}"),
            },
            Reply::Chunk(_) => panic!("unexpected chunk"),
        }
    }

    #[tokio::test]
    async fn stateful_entity_preserves_state() {
        let s = make_bare_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(CounterEntity)).await.unwrap();

        // Increment by 3
        let env = make_envelope(
            &s, 2000, "Counter", "c-1", "increment",
            rmp_serde::to_vec(&3usize).unwrap(), false,
        );
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        let val: usize = match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => rmp_serde::from_slice(data).unwrap(),
                _ => panic!("expected success"),
            },
            _ => panic!("expected WithExit"),
        };
        assert_eq!(val, 3);

        // Increment by 7 on the same entity
        let env = make_envelope(
            &s, 2001, "Counter", "c-1", "increment",
            rmp_serde::to_vec(&7usize).unwrap(), false,
        );
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        let val: usize = match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => rmp_serde::from_slice(data).unwrap(),
                _ => panic!("expected success"),
            },
            _ => panic!("expected WithExit"),
        };
        assert_eq!(val, 10);

        // Get current value
        let env = make_envelope(&s, 2002, "Counter", "c-1", "get", vec![], false);
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        let val: usize = match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => rmp_serde::from_slice(data).unwrap(),
                _ => panic!("expected success"),
            },
            _ => panic!("expected WithExit"),
        };
        assert_eq!(val, 10);
    }

    #[tokio::test]
    async fn multiple_entities_independent_state() {
        let s = make_bare_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(CounterEntity)).await.unwrap();

        // Increment entity A by 5
        let env = make_envelope(
            &s, 3000, "Counter", "a", "increment",
            rmp_serde::to_vec(&5usize).unwrap(), false,
        );
        let mut rx = s.send(env).await.unwrap();
        let _ = rx.recv().await.unwrap();

        // Increment entity B by 10
        let env = make_envelope(
            &s, 3001, "Counter", "b", "increment",
            rmp_serde::to_vec(&10usize).unwrap(), false,
        );
        let mut rx = s.send(env).await.unwrap();
        let _ = rx.recv().await.unwrap();

        // Check entity A: should be 5
        let env = make_envelope(&s, 3002, "Counter", "a", "get", vec![], false);
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        let val: usize = match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => rmp_serde::from_slice(data).unwrap(),
                _ => panic!("expected success"),
            },
            _ => panic!("expected WithExit"),
        };
        assert_eq!(val, 5);

        // Check entity B: should be 10
        let env = make_envelope(&s, 3003, "Counter", "b", "get", vec![], false);
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        let val: usize = match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => rmp_serde::from_slice(data).unwrap(),
                _ => panic!("expected success"),
            },
            _ => panic!("expected WithExit"),
        };
        assert_eq!(val, 10);
    }

    #[tokio::test]
    async fn notify_fire_and_forget() {
        let s = make_bare_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let env = make_envelope(&s, 4000, "Echo", "e-notify", "ping", vec![], false);
        s.notify(env).await.unwrap();

        // Give the entity time to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(s.active_entity_count() > 0);
    }

    #[tokio::test]
    async fn multiple_entity_types() {
        let s = make_bare_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();
        s.register_entity(Arc::new(CounterEntity)).await.unwrap();

        // Echo entity
        let env = make_envelope(&s, 5000, "Echo", "e-1", "echo", vec![1, 2, 3], false);
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => assert_eq!(data, &[1, 2, 3]),
                _ => panic!("expected success"),
            },
            _ => panic!("expected WithExit"),
        }

        // Counter entity
        let env = make_envelope(
            &s, 5001, "Counter", "c-1", "increment",
            rmp_serde::to_vec(&1usize).unwrap(), false,
        );
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        let val: usize = match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => rmp_serde::from_slice(data).unwrap(),
                _ => panic!("expected success"),
            },
            _ => panic!("expected WithExit"),
        };
        assert_eq!(val, 1);
    }

    #[tokio::test]
    async fn unknown_tag_returns_error() {
        let s = make_bare_sharding();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let env = make_envelope(&s, 6000, "Echo", "e-1", "nonexistent", vec![], false);
        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Failure(msg) => {
                    assert!(msg.contains("unknown tag"), "unexpected error: {msg}");
                }
                _ => panic!("expected failure"),
            },
            _ => panic!("expected WithExit"),
        }
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let s = make_bare_sharding();
        s.acquire_all_shards().await;
        assert!(!s.is_shutdown());

        s.shutdown().await.unwrap();
        assert!(s.is_shutdown());

        // Second shutdown should be fine
        s.shutdown().await.unwrap();
        assert!(s.is_shutdown());
    }
}

// ============================================================================
// Persisted Message Tests
// ============================================================================

mod persisted_messages {
    use super::*;

    #[tokio::test]
    async fn persisted_send_saves_to_storage() {
        let (_container, pool) = setup_postgres().await;
        let (s, storage) = make_sharding_with_storage(pool).await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let payload = rmp_serde::to_vec(&42i32).unwrap();
        let env = make_envelope(&s, 20000, "Echo", "e-1", "echo", payload.clone(), true);

        let mut rx = s.send(env).await.unwrap();
        let reply = rx.recv().await.unwrap();
        match &reply {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Success(data) => assert_eq!(data, &payload),
                ExitResult::Failure(msg) => panic!("unexpected failure: {msg}"),
            },
            _ => panic!("expected WithExit"),
        }

        // Reply should be persisted in storage
        let replies = storage.replies_for(Snowflake(20000)).await.unwrap();
        assert_eq!(replies.len(), 1);
    }

    #[tokio::test]
    async fn persisted_send_duplicate_returns_existing_reply() {
        let (_container, pool) = setup_postgres().await;
        let (s, _storage) = make_sharding_with_storage(pool).await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let env = make_envelope(&s, 21000, "Echo", "e-dup", "echo", vec![1, 2, 3], true);

        // First send
        let mut rx1 = s.send(env.clone()).await.unwrap();
        let _reply1 = rx1.recv().await.unwrap();

        // Second send (duplicate) — should return the stored reply
        let mut rx2 = s.send(env).await.unwrap();
        let reply2 = tokio::time::timeout(std::time::Duration::from_millis(500), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(reply2, Reply::WithExit(_)));
    }

    #[tokio::test]
    async fn persisted_send_duplicate_registers_reply_handler() {
        let (_container, pool) = setup_postgres().await;
        let (s, storage) = make_sharding_with_storage(pool).await;
        // Don't register the entity — the message will be saved but not processed.

        let env = make_envelope(&s, 22000, "Echo", "e-dup-no-reply", "echo", vec![1, 2, 3], true);

        // Save the request directly in storage (simulating another runner saving it)
        match storage.save_request(&env).await.unwrap() {
            SaveResult::Success => {}
            other => panic!("unexpected save result: {other:?}"),
        }

        // Send the duplicate — should register a reply handler
        let mut rx = s.send(env).await.unwrap();

        // Now save a reply externally — the handler should be notified
        let reply = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(22000),
            id: Snowflake(22001),
            exit: ExitResult::Success(vec![9, 9, 9]),
        });
        storage.save_reply(&reply).await.unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(received, Reply::WithExit(_)));
    }

    #[tokio::test]
    async fn persisted_notify_saves_to_storage() {
        let (_container, pool) = setup_postgres().await;
        let (s, storage) = make_sharding_with_storage(pool).await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let env = make_envelope(&s, 23000, "Echo", "e-notify", "ping", vec![], true);
        s.notify(env.clone()).await.unwrap();

        // Duplicate notify should be silently ignored (save returns Duplicate)
        s.notify(env).await.unwrap();

        // Verify the request was saved
        let result = storage.save_request(&make_envelope(&s, 23000, "Echo", "e-notify", "ping", vec![], true)).await.unwrap();
        assert!(matches!(result, SaveResult::Duplicate { .. }));
    }

    #[tokio::test]
    async fn non_persisted_send_does_not_save_to_storage() {
        let (_container, pool) = setup_postgres().await;
        let (s, storage) = make_sharding_with_storage(pool).await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        let env = make_envelope(&s, 24000, "Echo", "e-non-persist", "echo", vec![1], false);
        let mut rx = s.send(env).await.unwrap();
        let _reply = rx.recv().await.unwrap();

        // Non-persisted messages should NOT have replies in storage
        let replies = storage.replies_for(Snowflake(24000)).await.unwrap();
        assert!(replies.is_empty());
    }

    #[tokio::test]
    async fn poll_storage_dispatches_unprocessed_messages() {
        let (_container, pool) = setup_postgres().await;
        let (s, storage) = make_sharding_with_storage(pool).await;
        s.register_entity(Arc::new(EchoEntity)).await.unwrap();

        // Save a persisted envelope directly to storage (simulating another runner)
        let env = make_envelope(&s, 25000, "Echo", "e-poll", "echo", vec![7, 8, 9], true);
        storage.save_envelope(&env).await.unwrap();

        assert_eq!(s.active_entity_count(), 0);

        s.poll_storage().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(s.active_entity_count() > 0);
    }

    #[tokio::test]
    async fn poll_storage_skips_unregistered_entity_types() {
        let (_container, pool) = setup_postgres().await;
        let (s, storage) = make_sharding_with_storage(pool).await;
        // Don't register any entity

        let env = EnvelopeRequest {
            request_id: Snowflake(26000),
            address: cruster::types::EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("Unknown"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "x".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        storage.save_envelope(&env).await.unwrap();

        s.poll_storage().await.unwrap();
        assert_eq!(s.active_entity_count(), 0);
    }

    #[tokio::test]
    async fn poll_storage_saves_failure_reply_for_unregistered_entity_after_timeout() {
        let (_container, pool) = setup_postgres().await;
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_registration_timeout: std::time::Duration::from_millis(50),
            ..Default::default()
        });
        let storage = Arc::new(SqlMessageStorage::new(pool).with_last_read_guard_interval(
            std::time::Duration::ZERO,
        ));
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            Some(storage.clone() as Arc<dyn MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;

        let env = EnvelopeRequest {
            request_id: Snowflake(27000),
            address: cruster::types::EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("NeverRegistered"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "x".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        storage.save_envelope(&env).await.unwrap();

        // First poll — within timeout, message is skipped but tracked
        s.poll_storage().await.unwrap();
        let replies = storage.replies_for(Snowflake(27000)).await.unwrap();
        assert!(replies.is_empty(), "no failure reply yet before timeout");

        // Wait for timeout
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        // Second poll — timeout exceeded, failure reply should be saved
        s.poll_storage().await.unwrap();
        let replies = storage.replies_for(Snowflake(27000)).await.unwrap();
        assert_eq!(replies.len(), 1, "failure reply should be saved after timeout");
        match &replies[0] {
            Reply::WithExit(r) => match &r.exit {
                ExitResult::Failure(msg) => {
                    assert!(
                        msg.contains("NeverRegistered"),
                        "failure message should mention entity type: {msg}"
                    );
                }
                _ => panic!("expected Failure exit"),
            },
            _ => panic!("expected WithExit reply"),
        }
    }
}

// ============================================================================
// Resumption Tests
// ============================================================================

mod resumption {
    use super::*;

    /// Slow-processing entity with a tiny mailbox that will fill up.
    struct SlowEntity {
        handled: Arc<AtomicU32>,
    }

    struct SlowHandler {
        handled: Arc<AtomicU32>,
    }

    #[async_trait]
    impl Entity for SlowEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Slow")
        }
        fn mailbox_capacity(&self) -> Option<usize> {
            Some(1)
        }
        async fn spawn(
            &self,
            _ctx: EntityContext,
        ) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(SlowHandler {
                handled: Arc::clone(&self.handled),
            }))
        }
    }

    #[async_trait]
    impl EntityHandler for SlowHandler {
        async fn handle_request(
            &self,
            _tag: &str,
            _payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn mailbox_full_resumption_retries_delivery() {
        let (_container, pool) = setup_postgres().await;
        let handled = Arc::new(AtomicU32::new(0));

        let storage = Arc::new(
            SqlMessageStorage::new(pool)
                .with_last_read_guard_interval(std::time::Duration::ZERO),
        );
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_mailbox_capacity: 1,
            send_retry_interval: std::time::Duration::from_millis(20),
            storage_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        });
        let metrics =
            Arc::new(ClusterMetrics::new(&prometheus::Registry::new()).expect("valid metrics"));
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            Some(storage.clone() as Arc<dyn MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;

        let entity = SlowEntity {
            handled: handled.clone(),
        };
        s.register_entity(Arc::new(entity)).await.unwrap();

        let shard = ShardId::new("default", 0);
        let eid = EntityId::new("e1");
        for i in 0..5 {
            let envelope = EnvelopeRequest {
                request_id: Snowflake(30000 + i),
                address: cruster::types::EntityAddress {
                    shard_id: shard.clone(),
                    entity_type: EntityType::new("Slow"),
                    entity_id: eid.clone(),
                },
                tag: "work".into(),
                payload: vec![],
                headers: HashMap::new(),
                span_id: None,
                trace_id: None,
                sampled: None,
                persisted: false,
                uninterruptible: Default::default(),
                deliver_at: None,
            };
            storage.save_request(&envelope).await.unwrap();
        }

        s.poll_storage().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let count = handled.load(Ordering::SeqCst);
        assert!(
            count >= 3,
            "expected at least 3 messages handled (got {count}), \
             resumption should retry MailboxFull messages"
        );
    }

    #[tokio::test]
    async fn max_retries_exhaustion_dead_letters_message() {
        let (_container, pool) = setup_postgres().await;

        /// Entity whose handler blocks forever so the mailbox never drains.
        struct BlockingEntity;
        struct NeverHandler;

        #[async_trait]
        impl Entity for BlockingEntity {
            fn entity_type(&self) -> EntityType {
                EntityType::new("Blocking")
            }
            fn mailbox_capacity(&self) -> Option<usize> {
                Some(1)
            }
            async fn spawn(
                &self,
                _ctx: EntityContext,
            ) -> Result<Box<dyn EntityHandler>, ClusterError> {
                Ok(Box::new(NeverHandler))
            }
        }

        #[async_trait]
        impl EntityHandler for NeverHandler {
            async fn handle_request(
                &self,
                _tag: &str,
                _payload: &[u8],
                _headers: &HashMap<String, String>,
            ) -> Result<Vec<u8>, ClusterError> {
                futures::future::pending::<()>().await;
                Ok(vec![])
            }
        }

        let storage = Arc::new(
            SqlMessageStorage::new(pool)
                .with_last_read_guard_interval(std::time::Duration::ZERO),
        );
        let config = Arc::new(ShardingConfig {
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            entity_mailbox_capacity: 1,
            send_retry_interval: std::time::Duration::from_millis(5),
            storage_poll_interval: std::time::Duration::from_millis(50),
            storage_resumption_max_retries: 3,
            ..Default::default()
        });
        let metrics =
            Arc::new(ClusterMetrics::new(&prometheus::Registry::new()).expect("valid metrics"));
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            Some(storage.clone() as Arc<dyn MessageStorage>),
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        s.register_entity(Arc::new(BlockingEntity)).await.unwrap();

        let shard = ShardId::new("default", 0);
        let eid = EntityId::new("e1");
        for i in 0..10 {
            let envelope = EnvelopeRequest {
                request_id: Snowflake(31000 + i),
                address: cruster::types::EntityAddress {
                    shard_id: shard.clone(),
                    entity_type: EntityType::new("Blocking"),
                    entity_id: eid.clone(),
                },
                tag: "work".into(),
                payload: vec![],
                headers: HashMap::new(),
                span_id: None,
                trace_id: None,
                sampled: None,
                persisted: false,
                uninterruptible: Default::default(),
                deliver_at: None,
            };
            storage.save_request(&envelope).await.unwrap();
        }

        s.poll_storage().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let mut found_dead_letter = false;
        for i in 0..10 {
            let target_id = Snowflake(31000 + i);
            let replies = storage.replies_for(target_id).await.unwrap();
            if replies.iter().any(|r| {
                matches!(
                    r,
                    Reply::WithExit(ReplyWithExit {
                        exit: ExitResult::Failure(reason),
                        ..
                    }) if reason.contains("retry limit exhausted")
                )
            }) {
                found_dead_letter = true;
                break;
            }
        }

        assert!(
            found_dead_letter,
            "expected at least one dead-lettered message after resumption retry exhaustion"
        );
    }
}
