pub mod noop_health;
pub mod noop_runners;

pub mod sql_message;

pub mod sql_workflow_journal;

pub mod sql_workflow_runtime;

use sqlx::postgres::PgPool;

use crate::error::ClusterError;

/// Builder for configuring SQL storage operations.
pub struct Storage<'a> {
    pool: &'a PgPool,
    migrations_table: Option<&'a str>,
}

impl<'a> Storage<'a> {
    /// Start configuring storage operations for the provided Postgres pool.
    pub fn builder(pool: &'a PgPool) -> Self {
        Self::new(pool)
    }

    /// Configure storage operations for the provided Postgres pool.
    pub fn new(pool: &'a PgPool) -> Self {
        Self {
            pool,
            migrations_table: None,
        }
    }

    /// Use a custom table to track applied framework migrations.
    pub fn migrations_table(mut self, migrations_table: &'a str) -> Self {
        self.migrations_table = Some(migrations_table);
        self
    }

    /// Run all framework SQL migrations for cruster storage backends.
    pub async fn migrate(&self) -> Result<(), ClusterError> {
        let mut migrator = sqlx::migrate!();
        if let Some(migrations_table) = self.migrations_table {
            migrator.dangerous_set_table_name(migrations_table.to_owned());
        }

        migrator
            .run(self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to run framework SQL migrations: {e}"),
                source: Some(Box::new(e)),
            })
    }
}

/// Run all framework SQL migrations for cruster storage backends.
pub async fn migrate(pool: &PgPool) -> Result<(), ClusterError> {
    Storage::new(pool).migrate().await
}

#[cfg(feature = "etcd")]
pub mod etcd_runner;
