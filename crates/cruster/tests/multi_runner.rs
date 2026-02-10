//! Multi-runner integration tests using etcd + PostgreSQL via testcontainers.
//!
//! These tests spin up real etcd and PostgreSQL containers, wire up multiple
//! `ShardingImpl` runners with gRPC transport, and verify:
//! - Shard distribution across runners
//! - Entity message dispatch and handling
//! - Shard failover when a runner shuts down
//! - Message re-delivery to surviving runners
//!
//! Run: `cargo test -p cruster --features etcd --test multi_runner`
//!
//! Requires Docker to be running.

#![cfg(feature = "etcd")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cruster::config::ShardingConfig;
use cruster::entity::{Entity, EntityContext, EntityHandler};
use cruster::envelope::EnvelopeRequest;
use cruster::message_storage::{MessageStorage, SaveResult};
use cruster::metrics::ClusterMetrics;
use cruster::sharding::Sharding;
use cruster::sharding_impl::ShardingImpl;
use cruster::snowflake::Snowflake;
use cruster::storage::etcd_runner::EtcdRunnerStorage;
use cruster::storage::sql_message::SqlMessageStorage;
use cruster::storage::sql_workflow::SqlWorkflowStorage;
use cruster::transport::grpc::{GrpcRunnerHealth, GrpcRunnerServer, GrpcRunners};
use cruster::types::{EntityAddress, EntityId, EntityType, RunnerAddress, ShardId};

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

// ============================================================================
// Test entity: PingEntity
// ============================================================================

/// Global counter to track how many times handlers are invoked across runners.
static GLOBAL_HANDLE_COUNT: AtomicU32 = AtomicU32::new(0);

struct PingEntity;

#[async_trait]
impl Entity for PingEntity {
    fn entity_type(&self) -> EntityType {
        EntityType::new("PingEntity")
    }

    async fn spawn(
        &self,
        _ctx: EntityContext,
    ) -> Result<Box<dyn EntityHandler>, cruster::error::ClusterError> {
        Ok(Box::new(PingHandler))
    }
}

struct PingHandler;

#[async_trait]
impl EntityHandler for PingHandler {
    async fn handle_request(
        &self,
        tag: &str,
        payload: &[u8],
        _headers: &HashMap<String, String>,
    ) -> Result<Vec<u8>, cruster::error::ClusterError> {
        GLOBAL_HANDLE_COUNT.fetch_add(1, Ordering::SeqCst);
        match tag {
            "ping" => {
                let input: String = rmp_serde::from_slice(payload).unwrap_or_default();
                let response = format!("pong:{input}");
                Ok(rmp_serde::to_vec(&response).unwrap())
            }
            _ => Err(cruster::error::ClusterError::MalformedMessage {
                reason: format!("unknown tag: {tag}"),
                source: None,
            }),
        }
    }
}

// ============================================================================
// Infrastructure helpers
// ============================================================================

async fn setup_postgres() -> (ContainerAsync<Postgres>, sqlx::PgPool) {
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

    // Run migrations once
    let wf_storage = SqlWorkflowStorage::new(pool.clone());
    wf_storage.migrate().await.expect("migration failed");

    (container, pool)
}

async fn setup_etcd() -> (ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_env_var("ETCD_ADVERTISE_CLIENT_URLS", "http://0.0.0.0:2379")
        .with_env_var("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379");

    let container = image.start().await.expect("failed to start etcd container");

    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(2379)
        .await
        .expect("failed to get port");

    let endpoint = format!("http://{}:{}", host, port);
    (container, endpoint)
}

/// A fully wired runner with ShardingImpl, gRPC server, and shutdown handle.
struct TestRunner {
    sharding: Arc<ShardingImpl>,
    _grpc_shutdown: tokio::sync::oneshot::Sender<()>,
}

impl TestRunner {
    async fn new(
        name: &str,
        grpc_port: u16,
        pool: sqlx::PgPool,
        etcd_endpoint: &str,
        etcd_prefix: &str,
        lease_ttl: i64,
        shards_per_group: i32,
    ) -> Self {
        let runner_address = RunnerAddress::new("127.0.0.1", grpc_port);

        // Message storage (shared DB)
        let message_storage = Arc::new(SqlMessageStorage::new(pool.clone()));

        // Workflow state storage
        let state_storage: Arc<dyn cruster::__internal::WorkflowStorage> =
            Arc::new(SqlWorkflowStorage::new(pool.clone()));

        // Workflow engine
        let workflow_engine: Arc<dyn cruster::__internal::WorkflowEngine> =
            Arc::new(cruster::__internal::SqlWorkflowEngine::new(pool.clone()));

        // etcd runner storage
        let etcd_client = etcd_client::Client::connect([etcd_endpoint], None)
            .await
            .unwrap_or_else(|e| panic!("{name}: failed to connect to etcd: {e}"));
        let runner_storage = Arc::new(EtcdRunnerStorage::new(etcd_client, etcd_prefix, lease_ttl));

        // gRPC transport
        let grpc_runners = Arc::new(GrpcRunners::new());
        let runner_health = Arc::new(GrpcRunnerHealth::new(grpc_runners.clone()));

        // Config with fast intervals for testing
        let config = Arc::new(ShardingConfig {
            runner_address: runner_address.clone(),
            shard_groups: vec!["default".to_string()],
            shards_per_group,
            shard_rebalance_retry_interval: Duration::from_secs(1),
            shard_rebalance_debounce: Duration::from_millis(500),
            storage_poll_interval: Duration::from_millis(200),
            entity_registration_timeout: Duration::from_secs(5),
            ..Default::default()
        });

        // Create ShardingImpl
        let sharding = ShardingImpl::new_with_engines(
            config,
            grpc_runners,
            Some(runner_storage),
            Some(runner_health),
            Some(message_storage),
            Some(state_storage),
            Some(workflow_engine),
            Arc::new(ClusterMetrics::unregistered()),
        )
        .unwrap_or_else(|e| panic!("{name}: failed to create ShardingImpl: {e}"));

        // Start multi-runner background loops
        sharding
            .start()
            .await
            .unwrap_or_else(|e| panic!("{name}: failed to start sharding: {e}"));

        // Start gRPC server
        let grpc_server = GrpcRunnerServer::new(sharding.clone());
        let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", grpc_port).parse().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let runner_name = name.to_string();

        tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(grpc_server.into_service())
                .serve_with_shutdown(grpc_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                tracing::error!("{}: gRPC server error: {}", runner_name, e);
            }
        });

        TestRunner {
            sharding,
            _grpc_shutdown: shutdown_tx,
        }
    }

    async fn register_entity(&self, entity: impl Entity + 'static) {
        self.sharding
            .register_entity(Arc::new(entity))
            .await
            .expect("failed to register entity");
    }

    /// Graceful shutdown — releases all shard locks immediately.
    async fn shutdown(self) {
        self.sharding.shutdown().await.ok();
        // Drop the gRPC shutdown sender to stop the server
        drop(self._grpc_shutdown);
    }
}

/// Wait until a runner owns at least `min_shards` shards.
async fn wait_for_shards(
    sharding: &Arc<ShardingImpl>,
    min_shards: usize,
    timeout: Duration,
) -> usize {
    let start = std::time::Instant::now();
    loop {
        let count = sharding.owned_shard_count().await;
        if count >= min_shards {
            return count;
        }
        if start.elapsed() > timeout {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Two runners register with etcd and share the shard space.
/// Both should own some shards (roughly 50/50 for equal weights).
#[tokio::test]
async fn two_runners_distribute_shards() {
    let (_pg_container, pool) = setup_postgres().await;
    let (_etcd_container, etcd_endpoint) = setup_etcd().await;

    let shards_per_group = 30;

    let runner_a = TestRunner::new(
        "runner-a",
        19001,
        pool.clone(),
        &etcd_endpoint,
        "/test-shards/",
        10,
        shards_per_group,
    )
    .await;

    let runner_b = TestRunner::new(
        "runner-b",
        19002,
        pool.clone(),
        &etcd_endpoint,
        "/test-shards/",
        10,
        shards_per_group,
    )
    .await;

    // Wait for shard acquisition to stabilize
    tokio::time::sleep(Duration::from_secs(5)).await;

    let count_a = runner_a.sharding.owned_shard_count().await;
    let count_b = runner_b.sharding.owned_shard_count().await;

    eprintln!("runner-a owns {count_a} shards, runner-b owns {count_b} shards");

    assert!(count_a > 0, "runner-a should own some shards");
    assert!(count_b > 0, "runner-b should own some shards");
    assert_eq!(
        count_a + count_b,
        shards_per_group as usize,
        "total shards should equal shards_per_group"
    );

    runner_a.shutdown().await;
    runner_b.shutdown().await;
}

/// When runner-a shuts down, runner-b acquires all of runner-a's shards.
#[tokio::test]
async fn shards_rebalance_on_runner_shutdown() {
    let (_pg_container, pool) = setup_postgres().await;
    let (_etcd_container, etcd_endpoint) = setup_etcd().await;

    let shards_per_group = 20;

    let runner_a = TestRunner::new(
        "runner-a",
        19003,
        pool.clone(),
        &etcd_endpoint,
        "/test-rebalance/",
        10,
        shards_per_group,
    )
    .await;

    let runner_b = TestRunner::new(
        "runner-b",
        19004,
        pool.clone(),
        &etcd_endpoint,
        "/test-rebalance/",
        10,
        shards_per_group,
    )
    .await;

    // Wait for initial distribution
    tokio::time::sleep(Duration::from_secs(5)).await;

    let count_a = runner_a.sharding.owned_shard_count().await;
    let count_b_before = runner_b.sharding.owned_shard_count().await;
    eprintln!("before shutdown: runner-a={count_a}, runner-b={count_b_before}");

    assert!(count_a > 0, "runner-a should own shards");
    assert!(count_b_before > 0, "runner-b should own shards");

    // Shut down runner-a — this calls release_all which revokes the etcd lease,
    // atomically freeing all shard locks
    let sharding_b = runner_a.sharding.clone();
    runner_a.shutdown().await;
    drop(sharding_b);

    // Wait for runner-b to acquire the freed shards
    let count_b_after = wait_for_shards(
        &runner_b.sharding,
        shards_per_group as usize,
        Duration::from_secs(15),
    )
    .await;

    eprintln!("after shutdown: runner-b={count_b_after}");

    assert_eq!(
        count_b_after, shards_per_group as usize,
        "runner-b should own all shards after runner-a shuts down"
    );

    runner_b.shutdown().await;
}

/// Full workflow failover test:
/// 1. Two runners start with a PingEntity registered on both
/// 2. A persisted message is saved to SQL storage
/// 3. The runner that owns the message's shard handles it
/// 4. After shutdown of that runner, the other runner acquires the shard
/// 5. A new persisted message to the same shard is handled by the surviving runner
#[tokio::test]
async fn message_failover_on_runner_shutdown() {
    let (_pg_container, pool) = setup_postgres().await;
    let (_etcd_container, etcd_endpoint) = setup_etcd().await;

    GLOBAL_HANDLE_COUNT.store(0, Ordering::SeqCst);

    let shards_per_group = 10;

    let runner_a = TestRunner::new(
        "runner-a",
        19005,
        pool.clone(),
        &etcd_endpoint,
        "/test-failover/",
        5, // short TTL
        shards_per_group,
    )
    .await;

    let runner_b = TestRunner::new(
        "runner-b",
        19006,
        pool.clone(),
        &etcd_endpoint,
        "/test-failover/",
        5,
        shards_per_group,
    )
    .await;

    // Register PingEntity on both runners
    runner_a.register_entity(PingEntity).await;
    runner_b.register_entity(PingEntity).await;

    // Wait for shard acquisition
    tokio::time::sleep(Duration::from_secs(5)).await;

    let count_a = runner_a.sharding.owned_shard_count().await;
    let count_b = runner_b.sharding.owned_shard_count().await;
    eprintln!("initial: runner-a={count_a}, runner-b={count_b}");

    // Save a persisted message directly into SQL storage.
    // Use shard 0 — we'll check which runner owns it.
    let msg_storage = SqlMessageStorage::new(pool.clone());
    let shard_id = ShardId::new("default", 0);

    let envelope = EnvelopeRequest {
        request_id: Snowflake(90001),
        address: EntityAddress {
            shard_id: shard_id.clone(),
            entity_type: EntityType::new("PingEntity"),
            entity_id: EntityId::new("failover-test-1"),
        },
        tag: "ping".into(),
        payload: rmp_serde::to_vec(&"hello".to_string()).unwrap(),
        headers: HashMap::new(),
        span_id: None,
        trace_id: None,
        sampled: None,
        persisted: true,
        uninterruptible: Default::default(),
        deliver_at: None,
    };

    let save_result = msg_storage.save_request(&envelope).await.unwrap();
    assert!(
        matches!(save_result, SaveResult::Success),
        "message should be saved"
    );

    // Wait for the message to be picked up and processed
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the message was handled
    let handle_count = GLOBAL_HANDLE_COUNT.load(Ordering::SeqCst);
    eprintln!("handle_count after first message: {handle_count}");
    assert!(
        handle_count >= 1,
        "message should have been handled by one of the runners"
    );

    // Check which runner owns shard 0
    let a_owns_shard0 = runner_a.sharding.owns_shard(&shard_id).await;
    let b_owns_shard0 = runner_b.sharding.owns_shard(&shard_id).await;
    eprintln!("shard 0 ownership: runner-a={a_owns_shard0}, runner-b={b_owns_shard0}");

    // Shut down the runner that owns shard 0
    let surviving_sharding;
    if a_owns_shard0 {
        eprintln!("shutting down runner-a (owns shard 0)");
        surviving_sharding = runner_b.sharding.clone();
        runner_a.shutdown().await;
    } else {
        eprintln!("shutting down runner-b (owns shard 0)");
        surviving_sharding = runner_a.sharding.clone();
        runner_b.shutdown().await;
    }

    // Wait for the surviving runner to acquire all shards
    let final_count = wait_for_shards(
        &surviving_sharding,
        shards_per_group as usize,
        Duration::from_secs(15),
    )
    .await;

    eprintln!("surviving runner now owns {final_count} shards");
    assert_eq!(
        final_count, shards_per_group as usize,
        "surviving runner should own all shards"
    );

    // Verify shard 0 is now owned by the surviving runner
    let surviving_owns_shard0 = surviving_sharding.owns_shard(&shard_id).await;
    assert!(
        surviving_owns_shard0,
        "surviving runner should own shard 0 after failover"
    );

    // Send a NEW message to the same shard — it should be handled by the surviving runner
    GLOBAL_HANDLE_COUNT.store(0, Ordering::SeqCst);

    let envelope2 = EnvelopeRequest {
        request_id: Snowflake(90002),
        address: EntityAddress {
            shard_id: shard_id.clone(),
            entity_type: EntityType::new("PingEntity"),
            entity_id: EntityId::new("failover-test-2"),
        },
        tag: "ping".into(),
        payload: rmp_serde::to_vec(&"after-failover".to_string()).unwrap(),
        headers: HashMap::new(),
        span_id: None,
        trace_id: None,
        sampled: None,
        persisted: true,
        uninterruptible: Default::default(),
        deliver_at: None,
    };

    msg_storage.save_request(&envelope2).await.unwrap();

    // Wait for the message to be picked up
    tokio::time::sleep(Duration::from_secs(3)).await;

    let handle_count2 = GLOBAL_HANDLE_COUNT.load(Ordering::SeqCst);
    eprintln!("handle_count after failover message: {handle_count2}");
    assert!(
        handle_count2 >= 1,
        "message should be handled by surviving runner after failover"
    );

    // Verify the reply exists
    let replies = msg_storage.replies_for(Snowflake(90002)).await.unwrap();
    assert!(
        !replies.is_empty(),
        "reply should exist for the post-failover message"
    );

    // Graceful cleanup of the surviving runner
    surviving_sharding.shutdown().await.ok();
}
