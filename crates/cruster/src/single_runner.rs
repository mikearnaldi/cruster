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
//! let runner = SingleRunner::builder(pool).build().await?;
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

/// Builder for configuring a single-node durable cluster.
pub struct SingleRunnerBuilder {
    pool: PgPool,
    config: ShardingConfig,
    migrations_table: Option<String>,
}

impl SingleRunnerBuilder {
    /// Create a builder using default configuration.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            config: ShardingConfig::default(),
            migrations_table: None,
        }
    }

    /// Use a custom sharding configuration.
    pub fn config(mut self, config: ShardingConfig) -> Self {
        self.config = config;
        self
    }

    /// Use a custom table to track applied framework migrations.
    pub fn migrations_table(mut self, migrations_table: impl Into<String>) -> Self {
        self.migrations_table = Some(migrations_table.into());
        self
    }

    /// Create the single-node durable cluster.
    pub async fn build(self) -> Result<SingleRunner, ClusterError> {
        {
            let storage = crate::storage::Storage::builder(&self.pool);
            if let Some(migrations_table) = self.migrations_table.as_deref() {
                storage.migrations_table(migrations_table).migrate().await?;
            } else {
                storage.migrate().await?;
            }
        }

        let message_storage = Arc::new(
            SqlMessageStorage::with_max_retries(self.pool, self.config.storage_message_max_retries)
                .with_batch_limit(self.config.storage_inbox_size as u32)
                .with_last_read_guard_interval(self.config.last_read_guard_interval),
        );

        let config = Arc::new(self.config);
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

        Ok(SingleRunner { sharding, config })
    }
}

impl SingleRunner {
    /// Start configuring a single-node durable cluster.
    pub fn builder(pool: PgPool) -> SingleRunnerBuilder {
        SingleRunnerBuilder::new(pool)
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
