//! Autopilot cluster entity framework.
//!
//! ```compile_fail
//! use cruster::{entity, entity_impl};
//! use cruster::error::ClusterError;
//!
//! struct NotSerializable {
//!     value: std::cell::Cell<i32>,
//! }
//!
//! #[entity]
//! #[derive(Clone)]
//! struct BadEntity;
//!
//! #[entity_impl]
//! impl BadEntity {
//!     #[workflow]
//!     async fn bad(&self, value: NotSerializable) -> Result<(), ClusterError> {
//!         let _ = value;
//!         Ok(())
//!     }
//! }
//! ```

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
#[cfg(feature = "sql")]
pub mod single_runner;
pub mod singleton;
pub mod snowflake;
pub mod state_guard;
pub mod storage;
pub mod testing;
pub mod transport;
pub mod types;

/// Re-export proc macros for entity definition.
pub use cruster_macros::{entity, entity_impl, entity_trait, entity_trait_impl};

/// Prelude module for convenient glob imports.
///
/// This module re-exports all commonly used items including proc-macro attributes.
/// Use `use cruster::prelude::*;` to import everything needed for entity definitions.
///
/// # Example
///
/// ```text
/// use cruster::prelude::*;
///
/// #[entity]
/// #[derive(Clone)]
/// struct Counter;
///
/// #[entity_impl]
/// #[state(CounterState)]
/// impl Counter {
///     fn init(&self, _ctx: &EntityContext) -> Result<CounterState, ClusterError> {
///         Ok(CounterState { count: 0 })
///     }
///
///     #[activity]
///     async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
///         self.state.count += amount;
///         Ok(self.state.count)
///     }
///
///     #[rpc]
///     async fn get_count(&self) -> Result<i32, ClusterError> {
///         Ok(self.state.count)
///     }
/// }
/// ```
pub mod prelude {
    // Main macros
    pub use cruster_macros::{entity, entity_impl, entity_trait, entity_trait_impl};

    // Helper attribute macros (for IDE autocomplete and documentation)
    pub use cruster_macros::{activity, private, protected, public, rpc, state, workflow};

    // Common types
    pub use crate::entity::{Entity, EntityContext, EntityHandler};
    pub use crate::error::ClusterError;
}
mod durable;

#[doc(hidden)]
pub mod __internal {
    pub use crate::durable::{
        DeferredKey, DeferredKeyLike, DurableContext, MemoryWorkflowEngine, MemoryWorkflowStorage,
        StorageTransaction, WorkflowEngine, WorkflowStorage,
    };
    #[cfg(feature = "sql")]
    pub use crate::state_guard::SqlTransactionHandle;
    pub use crate::state_guard::{ActivityScope, StateMutGuard, StateRef, TraitStateMutGuard};
    #[cfg(feature = "sql")]
    pub use crate::storage::sql_workflow_engine::SqlWorkflowEngine;
}
#[cfg(test)]
mod macro_tests;
