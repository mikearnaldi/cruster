//! Entity definitions for cluster tests.
//!
//! ## Entities
//!
//! - `Counter` - Basic state persistence and simple RPCs
//! - `KVStore` - Key-value operations
//! - `WorkflowTest` - Durable workflow testing
//! - `ActivityTest` - Activity journaling
//! - `TraitTest` - Entity trait composition
//! - `TimerTest` - Timer/scheduling
//! - `CrossEntity` - Entity-to-entity calls
//! - `SingletonTest` - Singleton entity behavior
//! - `SqlActivityTest` - SQL execution within activity transactions
//! - `StatelessCounter` - Pure-RPC stateless entity (new API)

pub mod activity_test;
pub mod counter;
pub mod cross_entity;
pub mod kv_store;
pub mod singleton_test;
pub mod sql_activity_test;
pub mod stateless_counter;
pub mod timer_test;
pub mod trait_test;
pub mod workflow_test;

pub use activity_test::{
    ActivityRecord, ActivityTest, ActivityTestClient, RunWithActivitiesRequest,
};
pub use counter::{Counter, CounterClient, DecrementRequest, IncrementRequest};
pub use cross_entity::{
    ClearMessagesRequest, CrossEntity, CrossEntityClient, Message, PingRequest, ReceiveRequest,
    ResetPingCountRequest,
};
pub use kv_store::{DeleteRequest, GetRequest, KVStore, KVStoreClient, SetRequest};
pub use singleton_test::{SingletonManager, SingletonState};
pub use sql_activity_test::{
    FailingTransferRequest, GetSqlCountRequest, SqlActivityTest, SqlActivityTestClient,
    SqlActivityTestState, TransferRequest,
};
pub use timer_test::{
    CancelTimerRequest, ClearFiresRequest, PendingTimer, ScheduleTimerRequest, TimerFire,
    TimerTest, TimerTestClient,
};
pub use trait_test::{AuditEntry, Auditable, TraitTest, TraitTestClient, UpdateRequest, Versioned};
pub use stateless_counter::{
    StatelessCounter, StatelessCounterClient, StatelessDecrementRequest, StatelessGetRequest,
    StatelessIncrementRequest, StatelessResetRequest,
};
pub use workflow_test::{
    GetExecutionRequest, RunFailingWorkflowRequest, RunLongWorkflowRequest,
    RunSimpleWorkflowRequest, WorkflowExecution, WorkflowTest, WorkflowTestClient,
};
