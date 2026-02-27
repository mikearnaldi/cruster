pub mod noop_health;
pub mod noop_runners;

pub(crate) mod sql_migrations;

pub mod sql_message;

pub mod sql_workflow_journal;

pub mod sql_workflow_runtime;

use sqlx::postgres::PgPool;

use crate::error::ClusterError;

/// Run all framework SQL migrations for cruster storage backends.
pub async fn migrate(pool: &PgPool) -> Result<(), ClusterError> {
    sql_migrations::run_cruster_migrations(pool).await
}

#[cfg(feature = "etcd")]
pub mod etcd_runner;
