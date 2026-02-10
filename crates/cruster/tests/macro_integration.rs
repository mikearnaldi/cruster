//! Integration tests for macro-generated workflow and activity dispatch.
//!
//! These tests were migrated from macro_tests.rs (where they required
//! `#[ignore]` because they need real SQL storage for activity execution).
//!
//! Run: `cargo test -p cruster --test macro_integration`
//!
//! Requires Docker to be running.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cruster::__internal::{WorkflowEngine, WorkflowStorage};
use cruster::entity::{Entity, EntityContext};
use cruster::error::ClusterError;
use cruster::prelude::*;
use cruster::snowflake::SnowflakeGenerator;
use cruster::storage::sql_message::SqlMessageStorage;
use cruster::storage::sql_workflow::SqlWorkflowStorage;

use cruster::types::{EntityAddress, EntityId, EntityType, RunnerAddress, ShardId};

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

fn test_ctx(pool: &sqlx::PgPool, entity_type: &str, entity_id: &str) -> EntityContext {
    EntityContext {
        address: EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new(entity_type),
            entity_id: EntityId::new(entity_id),
        },
        runner_address: RunnerAddress::new("127.0.0.1", 9000),
        snowflake: Arc::new(SnowflakeGenerator::new()),
        cancellation: tokio_util::sync::CancellationToken::new(),
        state_storage: Some(
            Arc::new(SqlWorkflowStorage::new(pool.clone())) as Arc<dyn WorkflowStorage>
        ),
        workflow_engine: Some(Arc::new(InstantWorkflowEngine) as Arc<dyn WorkflowEngine>),
        sharding: None,
        message_storage: Some(Arc::new(SqlMessageStorage::new(pool.clone()))
            as Arc<dyn cruster::message_storage::MessageStorage>),
    }
}

/// Workflow engine that returns immediately from sleep (for retry tests).
struct InstantWorkflowEngine;

#[async_trait::async_trait]
impl WorkflowEngine for InstantWorkflowEngine {
    async fn sleep(
        &self,
        _workflow_name: &str,
        _execution_id: &str,
        _name: &str,
        _duration: Duration,
    ) -> Result<(), ClusterError> {
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

// ============================================================================
// Workflow request types
// ============================================================================

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct WfRequest {
    name: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct OrderRequest {
    order_id: String,
    amount: i32,
}

// ============================================================================
// Workflow with activities
// ============================================================================

#[workflow]
#[derive(Clone)]
struct OrderWorkflow;

#[workflow_impl]
impl OrderWorkflow {
    async fn execute(&self, request: OrderRequest) -> Result<String, ClusterError> {
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
async fn workflow_with_activities() {
    let (_container, pool) = setup_postgres().await;
    let w = OrderWorkflow;
    let ctx = test_ctx(&pool, "Workflow/OrderWorkflow", "exec-1");
    let handler = w.spawn(ctx).await.unwrap();

    let req = OrderRequest {
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

// ============================================================================
// Workflow with struct fields
// ============================================================================

#[workflow]
#[derive(Clone)]
struct FieldWorkflow {
    prefix: String,
}

#[workflow_impl]
impl FieldWorkflow {
    async fn execute(&self, request: WfRequest) -> Result<String, ClusterError> {
        let greeting = self.greet(request.name).await?;
        Ok(greeting)
    }

    #[activity]
    async fn greet(&self, name: String) -> Result<String, ClusterError> {
        Ok(format!("{}: {name}", self.prefix))
    }
}

#[tokio::test]
async fn workflow_with_fields() {
    let (_container, pool) = setup_postgres().await;
    let w = FieldWorkflow {
        prefix: "Hey".to_string(),
    };
    let ctx = test_ctx(&pool, "Workflow/FieldWorkflow", "exec-1");
    let handler = w.spawn(ctx).await.unwrap();

    let req = WfRequest {
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

// ============================================================================
// Activity group composed into a workflow
// ============================================================================

#[activity_group]
#[derive(Clone)]
pub struct Payments {
    rate: f64,
}

#[activity_group_impl]
impl Payments {
    #[activity]
    async fn charge(&self, amount: i32) -> Result<String, ClusterError> {
        let total = (amount as f64 * self.rate) as i32;
        Ok(format!("charged:{total}"))
    }

    #[activity]
    async fn refund(&self, tx_id: String) -> Result<String, ClusterError> {
        Ok(format!("refunded:{tx_id}"))
    }
}

#[workflow]
#[derive(Clone)]
struct PaymentWorkflow;

#[workflow_impl(activity_groups(Payments))]
impl PaymentWorkflow {
    async fn execute(&self, request: OrderRequest) -> Result<String, ClusterError> {
        let charge_result = self.charge(request.amount).await?;
        let refund_result = self.refund(request.order_id.clone()).await?;
        Ok(format!("{charge_result}|{refund_result}"))
    }
}

#[tokio::test]
async fn activity_group_workflow_dispatch() {
    let (_container, pool) = setup_postgres().await;
    let payments = Payments { rate: 1.5 };
    let workflow = PaymentWorkflow;
    let ctx = test_ctx(&pool, "Workflow/PaymentWorkflow", "exec-1");
    let bundle = __PaymentWorkflowWithGroups {
        __workflow: workflow,
        __group_payments: payments,
    };
    let handler = bundle.spawn(ctx).await.unwrap();

    let req = OrderRequest {
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

// ============================================================================
// Multiple activity groups
// ============================================================================

#[activity_group]
#[derive(Clone)]
pub struct Inventory;

#[activity_group_impl]
impl Inventory {
    #[activity]
    async fn reserve(&self, item_count: i32) -> Result<String, ClusterError> {
        Ok(format!("reserved:{item_count}"))
    }
}

#[workflow]
#[derive(Clone)]
struct MultiGroupWorkflow;

#[workflow_impl(activity_groups(Payments, Inventory))]
impl MultiGroupWorkflow {
    async fn execute(&self, request: OrderRequest) -> Result<String, ClusterError> {
        let reserved = self.reserve(request.amount).await?;
        let charged = self.charge(request.amount).await?;
        Ok(format!("{reserved}|{charged}"))
    }
}

#[tokio::test]
async fn multi_activity_group_workflow_dispatch() {
    let (_container, pool) = setup_postgres().await;
    let payments = Payments { rate: 1.0 };
    let inventory = Inventory;
    let workflow = MultiGroupWorkflow;
    let ctx = test_ctx(&pool, "Workflow/MultiGroupWorkflow", "exec-1");
    let bundle = __MultiGroupWorkflowWithGroups {
        __workflow: workflow,
        __group_payments: payments,
        __group_inventory: inventory,
    };
    let handler = bundle.spawn(ctx).await.unwrap();

    let req = OrderRequest {
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

// ============================================================================
// Workflow with both local activities and activity groups
// ============================================================================

#[workflow]
#[derive(Clone)]
struct MixedActivitiesWorkflow {
    prefix: String,
}

#[workflow_impl(activity_groups(Payments))]
impl MixedActivitiesWorkflow {
    async fn execute(&self, request: OrderRequest) -> Result<String, ClusterError> {
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
    let (_container, pool) = setup_postgres().await;
    let payments = Payments { rate: 3.0 };
    let workflow = MixedActivitiesWorkflow {
        prefix: "test".to_string(),
    };
    let ctx = test_ctx(&pool, "Workflow/MixedActivitiesWorkflow", "exec-1");
    let bundle = __MixedActivitiesWorkflowWithGroups {
        __workflow: workflow,
        __group_payments: payments,
    };
    let handler = bundle.spawn(ctx).await.unwrap();

    let req = OrderRequest {
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

// ============================================================================
// Workflow with retries (exponential default)
// ============================================================================

#[workflow]
#[derive(Clone)]
struct RetryWorkflow {
    call_count: Arc<AtomicU32>,
}

#[workflow_impl]
impl RetryWorkflow {
    async fn execute(&self, request: WfRequest) -> Result<String, ClusterError> {
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

#[tokio::test]
async fn retry_workflow_activity_succeeds_after_retries() {
    let (_container, pool) = setup_postgres().await;
    let call_count = Arc::new(AtomicU32::new(0));
    let w = RetryWorkflow {
        call_count: call_count.clone(),
    };
    let ctx = test_ctx(&pool, "Workflow/RetryWorkflow", "exec-retry-1");
    let handler = w.spawn(ctx).await.unwrap();

    let req = WfRequest {
        name: "retry-test".to_string(),
    };
    let payload = rmp_serde::to_vec(&req).unwrap();
    let result = handler
        .handle_request("execute", &payload, &HashMap::new())
        .await
        .unwrap();
    let value: String = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(value, "success:retry-test:attempt-2");
    assert_eq!(call_count.load(Ordering::SeqCst), 3);
}

// ============================================================================
// Workflow with constant backoff
// ============================================================================

#[workflow]
#[derive(Clone)]
struct ConstantBackoffWorkflow {
    call_count: Arc<AtomicU32>,
}

#[workflow_impl]
impl ConstantBackoffWorkflow {
    async fn execute(&self, request: WfRequest) -> Result<String, ClusterError> {
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
    let (_container, pool) = setup_postgres().await;
    let call_count = Arc::new(AtomicU32::new(0));
    let w = ConstantBackoffWorkflow {
        call_count: call_count.clone(),
    };
    let ctx = test_ctx(&pool, "Workflow/ConstantBackoffWorkflow", "exec-const-1");
    let handler = w.spawn(ctx).await.unwrap();

    let req = WfRequest {
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

// ============================================================================
// Workflow where all retries fail
// ============================================================================

#[workflow]
#[derive(Clone)]
struct AlwaysFailWorkflow {
    call_count: Arc<AtomicU32>,
}

#[workflow_impl]
impl AlwaysFailWorkflow {
    async fn execute(&self, request: WfRequest) -> Result<String, ClusterError> {
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
    let (_container, pool) = setup_postgres().await;
    let call_count = Arc::new(AtomicU32::new(0));
    let w = AlwaysFailWorkflow {
        call_count: call_count.clone(),
    };
    let ctx = test_ctx(&pool, "Workflow/AlwaysFailWorkflow", "exec-fail-1");
    let handler = w.spawn(ctx).await.unwrap();

    let req = WfRequest {
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

// ============================================================================
// Activity with retries = 0 (same as no retries)
// ============================================================================

#[workflow]
#[derive(Clone)]
struct NoRetryWorkflow;

#[workflow_impl]
impl NoRetryWorkflow {
    async fn execute(&self, request: WfRequest) -> Result<String, ClusterError> {
        self.no_retry(request.name).await
    }

    #[activity(retries = 0)]
    async fn no_retry(&self, name: String) -> Result<String, ClusterError> {
        Ok(format!("no-retry:{name}"))
    }
}

#[tokio::test]
async fn no_retry_workflow_succeeds() {
    let (_container, pool) = setup_postgres().await;
    let w = NoRetryWorkflow;
    // Note: uses test_ctx WITHOUT InstantWorkflowEngine, since no retries are needed.
    // Actually we still need state_storage for the activity to commit.
    let ctx = test_ctx(&pool, "Workflow/NoRetryWorkflow", "exec-noretry-1");
    let handler = w.spawn(ctx).await.unwrap();

    let req = WfRequest {
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

// ============================================================================
// Activity group with retries
// ============================================================================

#[activity_group]
#[derive(Clone)]
pub struct RetryPayments {
    call_count: Arc<AtomicU32>,
}

#[activity_group_impl]
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

#[workflow]
#[derive(Clone)]
struct GroupRetryWorkflow;

#[workflow_impl(activity_groups(RetryPayments))]
impl GroupRetryWorkflow {
    async fn execute(&self, request: OrderRequest) -> Result<String, ClusterError> {
        self.charge_with_retry(request.amount).await
    }
}

#[tokio::test]
async fn activity_group_retry_succeeds() {
    let (_container, pool) = setup_postgres().await;
    let call_count = Arc::new(AtomicU32::new(0));
    let payments = RetryPayments {
        call_count: call_count.clone(),
    };
    let workflow = GroupRetryWorkflow;
    let ctx = test_ctx(&pool, "Workflow/GroupRetryWorkflow", "exec-group-retry-1");
    let bundle = __GroupRetryWorkflowWithGroups {
        __workflow: workflow,
        __group_retry_payments: payments,
    };
    let handler = bundle.spawn(ctx).await.unwrap();

    let req = OrderRequest {
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

// ============================================================================
// Persisted RPC entity tests (mixed persisted + regular, idempotency)
// ============================================================================

use cruster::config::ShardingConfig;
use cruster::message_storage::MessageStorage;
use cruster::metrics::ClusterMetrics;
use cruster::sharding::Sharding;
use cruster::sharding_impl::ShardingImpl;
use cruster::storage::noop_runners::NoopRunners;
use std::sync::atomic::AtomicUsize;

#[entity]
#[derive(Clone)]
struct MixedEntity;

#[entity_impl]
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
    let (_container, pool) = setup_postgres().await;
    let config = Arc::new(ShardingConfig {
        shard_groups: vec!["default".to_string()],
        shards_per_group: 10,
        ..Default::default()
    });
    let runners: Arc<dyn cruster::runners::Runners> = Arc::new(NoopRunners);
    let metrics = Arc::new(ClusterMetrics::unregistered());
    let storage: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
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

#[entity]
#[derive(Clone)]
struct PersistedIdempotentEntity {
    calls: Arc<AtomicUsize>,
}

#[entity_impl]
impl PersistedIdempotentEntity {
    #[rpc(persisted)]
    async fn process(&self, value: i32) -> Result<i32, ClusterError> {
        let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(value + count as i32)
    }
}

#[tokio::test]
async fn persisted_method_replay_returns_cached_reply() {
    let (_container, pool) = setup_postgres().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let entity = PersistedIdempotentEntity {
        calls: Arc::clone(&calls),
    };

    let config = Arc::new(ShardingConfig {
        shard_groups: vec!["default".to_string()],
        shards_per_group: 10,
        ..Default::default()
    });
    let runners: Arc<dyn cruster::runners::Runners> = Arc::new(NoopRunners);
    let metrics = Arc::new(ClusterMetrics::unregistered());
    let storage: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
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

// ============================================================================
// Activity self.tx rollback on error — standalone workflow
// ============================================================================

#[workflow]
#[derive(Clone)]
struct TxRollbackWorkflow;

#[workflow_impl(key = |req: &TxRollbackRequest| format!("{}/{}", req.entity_id, req.run_id), hash = false)]
impl TxRollbackWorkflow {
    async fn execute(&self, request: TxRollbackRequest) -> Result<i64, ClusterError> {
        self.do_write(request.entity_id, request.should_fail).await
    }

    #[activity]
    async fn do_write(&self, entity_id: String, should_fail: bool) -> Result<i64, ClusterError> {
        // Write via self.tx — should be rolled back if we return Err
        let result: (i64,) = sqlx::query_as(
            "INSERT INTO tx_rollback_test (entity_id, counter)
             VALUES ($1, 1)
             ON CONFLICT (entity_id) DO UPDATE SET counter = tx_rollback_test.counter + 1
             RETURNING counter",
        )
        .bind(&entity_id)
        .fetch_one(&self.tx)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("do_write failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        if should_fail {
            return Err(ClusterError::PersistenceError {
                reason: "intentional failure".to_string(),
                source: None,
            });
        }

        Ok(result.0)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct TxRollbackRequest {
    entity_id: String,
    run_id: String,
    should_fail: bool,
}

#[tokio::test]
async fn activity_tx_rolls_back_on_error() {
    let (_container, pool) = setup_postgres().await;

    // Create test table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS tx_rollback_test (
            entity_id TEXT PRIMARY KEY,
            counter BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    let w = TxRollbackWorkflow;
    let ctx = test_ctx(&pool, "Workflow/TxRollbackWorkflow", "rollback-test-1");
    let handler = w.spawn(ctx).await.unwrap();

    // 1. Successful write — counter should be 1
    let req = TxRollbackRequest {
        entity_id: "rb-entity-1".to_string(),
        run_id: "run-1".to_string(),
        should_fail: false,
    };
    let payload = rmp_serde::to_vec(&req).unwrap();
    let result = handler
        .handle_request("execute", &payload, &HashMap::new())
        .await
        .unwrap();
    let value: i64 = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(value, 1, "first successful write should set counter to 1");

    // Verify in DB
    let row: (i64,) =
        sqlx::query_as("SELECT counter FROM tx_rollback_test WHERE entity_id = 'rb-entity-1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, 1);

    // 2. Failing write — counter should NOT change (rolled back)
    // Each workflow execution needs a fresh entity context to avoid journal cache collisions.
    let ctx2 = test_ctx(&pool, "Workflow/TxRollbackWorkflow", "rollback-test-2");
    let handler2 = TxRollbackWorkflow.spawn(ctx2).await.unwrap();
    let req = TxRollbackRequest {
        entity_id: "rb-entity-1".to_string(),
        run_id: "run-2".to_string(),
        should_fail: true,
    };
    let payload = rmp_serde::to_vec(&req).unwrap();
    let result = handler2
        .handle_request("execute", &payload, &HashMap::new())
        .await;
    assert!(result.is_err(), "failing activity should propagate error");

    // Counter should still be 1 — the UPSERT was rolled back
    let row: (i64,) =
        sqlx::query_as("SELECT counter FROM tx_rollback_test WHERE entity_id = 'rb-entity-1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        row.0, 1,
        "counter should still be 1 after failed activity (tx rolled back)"
    );

    // 3. Another successful write — counter should be 2 (not 3)
    let ctx3 = test_ctx(&pool, "Workflow/TxRollbackWorkflow", "rollback-test-3");
    let handler3 = TxRollbackWorkflow.spawn(ctx3).await.unwrap();
    let req = TxRollbackRequest {
        entity_id: "rb-entity-1".to_string(),
        run_id: "run-3".to_string(),
        should_fail: false,
    };
    let payload = rmp_serde::to_vec(&req).unwrap();
    let result = handler3
        .handle_request("execute", &payload, &HashMap::new())
        .await
        .unwrap();
    let value: i64 = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(
        value, 2,
        "second successful write should set counter to 2 (failed write was rolled back)"
    );
}

// ============================================================================
// Activity group self.tx rollback on error
// ============================================================================

#[activity_group]
#[derive(Clone)]
pub struct TxRollbackGroup;

#[activity_group_impl]
impl TxRollbackGroup {
    #[activity]
    async fn write_and_maybe_fail(
        &self,
        entity_id: String,
        should_fail: bool,
    ) -> Result<i64, ClusterError> {
        let result: (i64,) = sqlx::query_as(
            "INSERT INTO tx_rollback_group_test (entity_id, counter)
             VALUES ($1, 1)
             ON CONFLICT (entity_id) DO UPDATE SET counter = tx_rollback_group_test.counter + 1
             RETURNING counter",
        )
        .bind(&entity_id)
        .fetch_one(&self.tx)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("group write failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        if should_fail {
            return Err(ClusterError::PersistenceError {
                reason: "intentional group failure".to_string(),
                source: None,
            });
        }

        Ok(result.0)
    }
}

#[workflow]
#[derive(Clone)]
struct GroupTxRollbackWorkflow;

#[workflow_impl(
    key = |req: &TxRollbackRequest| format!("{}/{}", req.entity_id, req.run_id),
    hash = false,
    activity_groups(TxRollbackGroup)
)]
impl GroupTxRollbackWorkflow {
    async fn execute(&self, request: TxRollbackRequest) -> Result<i64, ClusterError> {
        self.write_and_maybe_fail(request.entity_id, request.should_fail)
            .await
    }
}

#[tokio::test]
async fn activity_group_tx_rolls_back_on_error() {
    let (_container, pool) = setup_postgres().await;

    // Create test table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS tx_rollback_group_test (
            entity_id TEXT PRIMARY KEY,
            counter BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ctx = test_ctx(
        &pool,
        "Workflow/GroupTxRollbackWorkflow",
        "group-rollback-1",
    );
    let bundle = __GroupTxRollbackWorkflowWithGroups {
        __workflow: GroupTxRollbackWorkflow,
        __group_tx_rollback_group: TxRollbackGroup,
    };
    let handler = bundle.spawn(ctx).await.unwrap();

    // 1. Successful write
    let req = TxRollbackRequest {
        entity_id: "grb-entity-1".to_string(),
        run_id: "run-1".to_string(),
        should_fail: false,
    };
    let payload = rmp_serde::to_vec(&req).unwrap();
    let result = handler
        .handle_request("execute", &payload, &HashMap::new())
        .await
        .unwrap();
    let value: i64 = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(value, 1);

    // 2. Failing write — should be rolled back
    // Each workflow execution needs a fresh entity context to avoid journal cache collisions.
    let ctx2 = test_ctx(
        &pool,
        "Workflow/GroupTxRollbackWorkflow",
        "group-rollback-2",
    );
    let bundle2 = __GroupTxRollbackWorkflowWithGroups {
        __workflow: GroupTxRollbackWorkflow,
        __group_tx_rollback_group: TxRollbackGroup,
    };
    let handler2 = bundle2.spawn(ctx2).await.unwrap();
    let req = TxRollbackRequest {
        entity_id: "grb-entity-1".to_string(),
        run_id: "run-2".to_string(),
        should_fail: true,
    };
    let payload = rmp_serde::to_vec(&req).unwrap();
    let result = handler2
        .handle_request("execute", &payload, &HashMap::new())
        .await;
    assert!(result.is_err());

    // Counter should still be 1
    let row: (i64,) = sqlx::query_as(
        "SELECT counter FROM tx_rollback_group_test WHERE entity_id = 'grb-entity-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row.0, 1,
        "activity group: counter should still be 1 after failed activity (tx rolled back)"
    );

    // 3. Successful write after failure — counter should be 2
    let ctx3 = test_ctx(
        &pool,
        "Workflow/GroupTxRollbackWorkflow",
        "group-rollback-3",
    );
    let bundle3 = __GroupTxRollbackWorkflowWithGroups {
        __workflow: GroupTxRollbackWorkflow,
        __group_tx_rollback_group: TxRollbackGroup,
    };
    let handler3 = bundle3.spawn(ctx3).await.unwrap();
    let req = TxRollbackRequest {
        entity_id: "grb-entity-1".to_string(),
        run_id: "run-3".to_string(),
        should_fail: false,
    };
    let payload = rmp_serde::to_vec(&req).unwrap();
    let result = handler3
        .handle_request("execute", &payload, &HashMap::new())
        .await
        .unwrap();
    let value: i64 = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(value, 2);
}
