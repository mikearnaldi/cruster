//! Production-grade single-node cluster with durable message storage.
//!
//! Uses [`SqlMessageStorage`] for persistent messages, noop runners (no remote
//! communication), and noop health checks. All shards are immediately owned.
//! Suitable for durable single-node deployments (workflows, cron, etc.).
//!
//! This module requires the `sql` feature.
//!
//! # Example
//!
//! ```text
//! let pool = PgPool::connect("postgres://localhost/cluster").await?;
//! let runner = SingleRunner::new(pool).await?;
//! let client = runner.register(MyEntity).await?;
//! let response: String = client.send(&EntityId::new("e-1"), "greet", &"hello").await?;
//! runner.shutdown().await?;
//! ```

use std::sync::Arc;

use sqlx::postgres::PgPool;

use crate::config::ShardingConfig;
use crate::entity::Entity;
use crate::entity_client::EntityClient;
use crate::error::ClusterError;
use crate::metrics::ClusterMetrics;
use crate::sharding::Sharding;
use crate::sharding_impl::ShardingImpl;
use crate::storage::noop_runners::NoopRunners;
use crate::storage::sql_message::SqlMessageStorage;

/// A single-node cluster with SQL-backed durable message storage.
///
/// Wraps a [`ShardingImpl`] with [`SqlMessageStorage`] for persistence,
/// noop runners, and noop health checks. All configured shards are
/// immediately owned.
pub struct SingleRunner {
    sharding: Arc<ShardingImpl>,
    config: Arc<ShardingConfig>,
}

impl SingleRunner {
    /// Create a single-node durable cluster with default configuration.
    ///
    /// Runs database migrations before starting.
    pub async fn new(pool: PgPool) -> Result<Self, ClusterError> {
        Self::with_config(pool, ShardingConfig::default()).await
    }

    /// Create a single-node durable cluster with custom configuration.
    ///
    /// Runs database migrations before starting.
    pub async fn with_config(pool: PgPool, config: ShardingConfig) -> Result<Self, ClusterError> {
        let message_storage = Arc::new(
            SqlMessageStorage::with_max_retries(pool, config.storage_message_max_retries)
                .with_batch_limit(config.storage_inbox_size as u32)
                .with_last_read_guard_interval(config.last_read_guard_interval),
        );
        message_storage.migrate().await?;

        let config = Arc::new(config);
        let runners = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let sharding = ShardingImpl::new(
            Arc::clone(&config),
            runners,
            None,
            None,
            Some(message_storage),
            metrics,
        )?;
        sharding.acquire_all_shards().await;

        Ok(Self { sharding, config })
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
