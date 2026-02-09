//! Cluster Tests - E2E test runner for cruster features.
//!
//! This binary joins a cluster, registers test entities, and exposes an HTTP API
//! for bash scripts to test cluster behavior.
//!
//! ## Required Environment Variables
//!
//! ```bash
//! POSTGRES_URL=postgres://user:pass@localhost/cluster \
//! ETCD_ENDPOINTS=localhost:2379 \
//! RUNNER_ADDRESS=localhost:9000 \
//! cargo run --package cluster-tests
//! ```

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use cruster::config::ShardingConfig;
use cruster::metrics::ClusterMetrics;
use cruster::sharding::Sharding;
use cruster::sharding_impl::ShardingImpl;
use cruster::storage::etcd_runner::EtcdRunnerStorage;
use cruster::storage::sql_message::SqlMessageStorage;
use cruster::storage::sql_workflow::SqlWorkflowStorage;
use cruster::storage::sql_workflow_engine::SqlWorkflowEngine;
use cruster::transport::grpc::{GrpcRunnerHealth, GrpcRunnerServer, GrpcRunners};
use cruster::types::RunnerAddress;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod api;
mod entities;

use api::{create_router, AppState};
use entities::{
    ActivityTest, Auditable, Counter, CrossEntity, KVStore, SingletonManager, SqlActivityTest,
    StatelessCounter, TimerTest, TraitTest, Versioned, WorkflowTest,
};

/// Parse a "host:port" string into a RunnerAddress.
fn parse_runner_address(s: &str) -> Option<RunnerAddress> {
    let parts: Vec<&str> = s.rsplitn(2, ':').collect();
    if parts.len() != 2 {
        return None;
    }
    let port: u16 = parts[0].parse().ok()?;
    let host = parts[1].to_string();
    Some(RunnerAddress::new(host, port))
}

/// CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "cluster-tests")]
#[command(about = "E2E test runner for cruster features")]
struct Args {
    /// HTTP API listen address.
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    listen_addr: String,

    /// PostgreSQL connection string (required).
    #[arg(long, env = "POSTGRES_URL")]
    postgres_url: String,

    /// etcd endpoints, comma-separated (required).
    #[arg(long, env = "ETCD_ENDPOINTS")]
    etcd_endpoints: String,

    /// Runner address for this instance, host:port (required).
    #[arg(long, env = "RUNNER_ADDRESS")]
    runner_address: String,

    /// gRPC server port for inter-runner communication.
    /// Defaults to the port from runner_address.
    #[arg(long, env = "GRPC_PORT")]
    grpc_port: Option<u16>,
}

/// Cluster components.
struct Cluster {
    sharding: Arc<ShardingImpl>,
    config: Arc<ShardingConfig>,
    pool: sqlx::PgPool,
    _grpc_shutdown: tokio::sync::oneshot::Sender<()>,
}

impl Cluster {
    fn sharding(&self) -> Arc<dyn Sharding> {
        self.sharding.clone()
    }

    fn config(&self) -> Arc<ShardingConfig> {
        self.config.clone()
    }

    fn pool(&self) -> sqlx::PgPool {
        self.pool.clone()
    }

    async fn shutdown(self) -> Result<()> {
        // Dropping the sender signals shutdown to the gRPC server
        drop(self._grpc_shutdown);
        self.sharding.shutdown().await?;
        Ok(())
    }
}

/// Create a cluster with Postgres and etcd backends.
async fn create_cluster(
    postgres_url: &str,
    etcd_endpoints: &str,
    runner_address: RunnerAddress,
    grpc_port: u16,
) -> Result<Cluster> {
    tracing::info!("Connecting to PostgreSQL: {}", postgres_url);
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(postgres_url)
        .await?;

    // Run migrations
    tracing::info!("Running database migrations...");
    let message_storage = Arc::new(SqlMessageStorage::new(pool.clone()));
    message_storage.migrate().await?;

    let state_storage = Arc::new(SqlWorkflowStorage::new(pool.clone()));
    let workflow_engine = Arc::new(SqlWorkflowEngine::new(pool.clone()));

    // Parse etcd endpoints
    let endpoints: Vec<String> = etcd_endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    tracing::info!("Connecting to etcd: {:?}", endpoints);

    let etcd_client = etcd_client::Client::connect(endpoints, None).await?;
    let runner_storage = Arc::new(EtcdRunnerStorage::new(
        etcd_client,
        "/cluster-tests/",
        30, // lease TTL in seconds
    ));

    // Create gRPC transport
    let grpc_runners = Arc::new(GrpcRunners::new());
    let runner_health = Arc::new(GrpcRunnerHealth::new(grpc_runners.clone()));

    // Create sharding config
    let config = Arc::new(ShardingConfig {
        runner_address: runner_address.clone(),
        shard_groups: vec!["default".to_string()],
        shards_per_group: 300,
        ..Default::default()
    });

    // Create sharding instance
    tracing::info!("Creating sharding instance for runner: {}", runner_address);
    let sharding = ShardingImpl::new_with_engines(
        config.clone(),
        grpc_runners,
        Some(runner_storage.clone()),
        Some(runner_health),
        Some(message_storage),
        Some(state_storage),
        Some(workflow_engine),
        Arc::new(ClusterMetrics::unregistered()),
    )?;

    // Start multi-runner background loops
    tracing::info!("Starting sharding background loops...");
    sharding.start().await?;

    // Start gRPC server for inter-runner communication
    let grpc_server = GrpcRunnerServer::new(sharding.clone());
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", grpc_port).parse()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tracing::info!("Starting gRPC server on {}", grpc_addr);
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(grpc_server.into_service())
            .serve_with_shutdown(grpc_addr, async {
                let _ = shutdown_rx.await;
            })
            .await
        {
            tracing::error!("gRPC server error: {}", e);
        }
    });

    Ok(Cluster {
        sharding,
        config,
        pool,
        _grpc_shutdown: shutdown_tx,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive("cluster_tests=info".parse()?))
        .init();

    let args = Args::parse();
    tracing::info!("Cluster Tests starting...");
    tracing::info!("Listen address: {}", args.listen_addr);

    let runner_address = parse_runner_address(&args.runner_address).ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid RUNNER_ADDRESS format (expected host:port): {}",
            args.runner_address
        )
    })?;

    let grpc_port = args.grpc_port.unwrap_or(runner_address.port);

    tracing::info!("Running with Postgres + etcd, runner: {}", runner_address);

    let cluster = create_cluster(
        &args.postgres_url,
        &args.etcd_endpoints,
        runner_address,
        grpc_port,
    )
    .await?;

    let sharding = cluster.sharding();

    // Register entities and get typed clients
    let counter_client = Counter
        .register(sharding.clone())
        .await
        .expect("failed to register Counter entity");
    tracing::info!("Registered Counter entity");

    let kv_store_client = KVStore
        .register(sharding.clone())
        .await
        .expect("failed to register KVStore entity");
    tracing::info!("Registered KVStore entity");

    let workflow_test_client = WorkflowTest
        .register(sharding.clone())
        .await
        .expect("failed to register WorkflowTest entity");
    tracing::info!("Registered WorkflowTest entity");

    let activity_test_client = ActivityTest
        .register(sharding.clone())
        .await
        .expect("failed to register ActivityTest entity");
    tracing::info!("Registered ActivityTest entity");

    let trait_test_client = TraitTest
        .register(sharding.clone(), Auditable, Versioned)
        .await
        .expect("failed to register TraitTest entity");
    tracing::info!("Registered TraitTest entity");

    let timer_test_client = TimerTest
        .register(sharding.clone())
        .await
        .expect("failed to register TimerTest entity");
    tracing::info!("Registered TimerTest entity");

    let cross_entity_client = CrossEntity
        .register(sharding.clone())
        .await
        .expect("failed to register CrossEntity entity");
    tracing::info!("Registered CrossEntity entity");

    let sql_activity_test_client = SqlActivityTest
        .register(sharding.clone())
        .await
        .expect("failed to register SqlActivityTest entity");
    tracing::info!("Registered SqlActivityTest entity");

    let stateless_counter_client = StatelessCounter {
        pool: cluster.pool(),
    }
    .register(sharding.clone())
    .await
    .expect("failed to register StatelessCounter entity");
    tracing::info!("Registered StatelessCounter entity");

    // Register the singleton using cluster's register_singleton feature
    let singleton_manager = Arc::new(SingletonManager::new(cluster.pool()));
    singleton_manager
        .init_schema()
        .await
        .expect("failed to initialize singleton schema");
    singleton_manager
        .register(sharding.clone())
        .await
        .expect("failed to register SingletonTest singleton");
    tracing::info!("Registered SingletonTest singleton");

    // Get config info for debug endpoints
    let config = cluster.config();
    let shard_groups = config.shard_groups.clone();
    let shards_per_group = config.shards_per_group;

    // Create shared application state
    let app_state = Arc::new(AppState {
        counter_client,
        kv_store_client,
        workflow_test_client,
        activity_test_client,
        trait_test_client,
        timer_test_client,
        cross_entity_client,
        sql_activity_test_client,
        stateless_counter_client,
        singleton_manager,
        sharding,
        shard_groups,
        shards_per_group,
        registered_entity_types: vec![
            "Counter".to_string(),
            "KVStore".to_string(),
            "WorkflowTest".to_string(),
            "ActivityTest".to_string(),
            "TraitTest".to_string(),
            "TimerTest".to_string(),
            "CrossEntity".to_string(),
            "SqlActivityTest".to_string(),
            "StatelessCounter".to_string(),
            "SingletonTest (singleton)".to_string(),
        ],
        pool: cluster.pool(),
    });

    // Create HTTP router
    let app = create_router(app_state);

    // Start HTTP server with graceful shutdown on SIGTERM/SIGINT
    let listener = tokio::net::TcpListener::bind(&args.listen_addr).await?;
    tracing::info!("HTTP API listening on {}", args.listen_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let ctrl_c = async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install Ctrl+C handler");
            };

            #[cfg(unix)]
            let terminate = async {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install signal handler")
                    .recv()
                    .await;
            };

            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {
                    tracing::info!("Received Ctrl+C, initiating graceful shutdown");
                },
                _ = terminate => {
                    tracing::info!("Received SIGTERM, initiating graceful shutdown");
                },
            }
        })
        .await?;

    // Cleanup - this now runs after SIGTERM/SIGINT
    tracing::info!("Shutting down cluster...");
    cluster.shutdown().await?;
    tracing::info!("Cluster Tests shutdown complete");
    Ok(())
}
