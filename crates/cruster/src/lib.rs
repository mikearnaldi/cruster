//! Cruster — stateless entity framework with durable workflows.
//!
//! Entities are addressable stateless RPC handlers. Workflows provide durable orchestration.
//! State management is the application's responsibility (use your database directly).

pub mod config;
pub mod cron;

pub mod detachment;
pub mod entity;
pub mod entity_client;
pub mod entity_manager;
pub mod entity_reaper;
pub mod entity_resource;
pub mod envelope;
pub mod error;
pub mod hash;
pub mod message;
pub mod message_storage;
pub mod metrics;
#[cfg(feature = "otel")]
pub mod otel;
pub mod reply;
pub mod resource_map;
pub mod runner;
pub mod runner_health;
pub mod runner_storage;
pub mod runners;
pub mod schema;
pub mod shard_assigner;
pub mod sharding;
pub mod sharding_impl;
pub mod single_runner;
pub mod singleton;
pub mod snowflake;
pub mod state_guard;
pub mod storage;
pub mod transport;
pub mod types;

/// Re-export proc macros for entity definition.
pub use cruster_macros::{entity, entity_impl};

/// Re-export deprecated entity_trait macros (emit compile errors directing to replacements).
pub use cruster_macros::{entity_trait, entity_trait_impl};

/// Re-export proc macros for standalone workflow definition.
/// `#[workflow]` is dual-purpose (struct-level -> standalone workflow, method-level -> marker).
/// `#[workflow_impl]` is used on the workflow impl block.
pub use cruster_macros::workflow_impl;

/// Re-export proc macros for activity group definition.
/// `#[activity_group]` marks a struct as an activity group.
/// `#[activity_group_impl]` processes the impl block.
pub use cruster_macros::{activity_group, activity_group_impl};

/// Re-export proc macros for RPC group definition.
/// `#[rpc_group]` marks a struct as an RPC group.
/// `#[rpc_group_impl]` processes the impl block.
pub use cruster_macros::{rpc_group, rpc_group_impl};

/// Prelude module for convenient glob imports.
///
/// This module re-exports all commonly used items including proc-macro attributes.
/// Use `use cruster::prelude::*;` to import everything needed for entity and workflow definitions.
///
/// # Example
///
/// ```text
/// use cruster::prelude::*;
///
/// #[entity]
/// #[derive(Clone)]
/// struct MyEntity {
///     db: PgPool,
/// }
///
/// #[entity_impl]
/// impl MyEntity {
///     #[rpc]
///     async fn get_value(&self, key: String) -> Result<String, ClusterError> {
///         // Query your database directly
///         todo!()
///     }
///
///     #[rpc(persisted)]
///     async fn set_value(&self, key: String, value: String) -> Result<(), ClusterError> {
///         // Write to your database directly
///         todo!()
///     }
/// }
/// ```
pub mod prelude {
    // Main macros
    pub use cruster_macros::{entity, entity_impl};

    // Deprecated macros (emit compile errors)
    pub use cruster_macros::{entity_trait, entity_trait_impl};

    // Workflow impl macro (workflow is already exported as helper attribute)
    pub use cruster_macros::workflow_impl;

    // Activity group macros
    pub use cruster_macros::{activity_group, activity_group_impl};

    // RPC group macros
    pub use cruster_macros::{rpc_group, rpc_group_impl};

    // Helper attribute macros (for IDE autocomplete and documentation)
    pub use cruster_macros::{activity, private, protected, public, rpc, state, workflow};

    // Common types
    pub use crate::entity::{Entity, EntityContext, EntityHandler};
    pub use crate::entity_client::WorkflowClientFactory;
    pub use crate::error::ClusterError;

    // Activity scope for transactional activities
    pub use crate::state_guard::ActivityScope;
    pub use crate::state_guard::SqlTransactionHandle;
}
mod durable;

#[doc(hidden)]
pub mod __internal {
    pub use crate::durable::{
        compute_retry_backoff, DeferredKey, DeferredKeyLike, DurableContext, StorageTransaction,
        WorkflowEngine, WorkflowScope, WorkflowStorage,
    };
    pub use crate::envelope::REQUEST_ID_HEADER_KEY;
    pub use crate::message_storage::MessageStorage;
    pub use crate::state_guard::ActivityScope;
    pub use crate::state_guard::ActivityTx;
    pub use crate::state_guard::SqlTransactionHandle;
    pub use crate::storage::sql_workflow::save_journal_entry;
    pub use crate::storage::sql_workflow_engine::SqlWorkflowEngine;
}
#[cfg(test)]
mod macro_tests;
