//! Entity definitions for cluster tests.
//!
//! ## Entities
//!
//! - `Counter` - Basic state persistence and simple RPCs
//! - `KVStore` - Key-value operations
//! - `WorkflowTest` - Durable workflow testing
//! - `ActivityTest` - Activity journaling
//! - `ActivityGroupTest` - Activity group composition in workflows
//! - `TraitTest` - Entity trait composition
//! - `TimerTest` - Timer/scheduling
//! - `CrossEntity` - Entity-to-entity calls
//! - `SingletonTest` - Singleton entity behavior
//! - `SqlActivityTest` - SQL execution within activity transactions
//! - `StatelessCounter` - Pure-RPC stateless entity (new API)

pub mod activity_group_test;
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

pub use activity_group_test::{
    ActivityGroupTest, ActivityGroupTestClient, GetOrderStepsRequest, Inventory, OrderResult,
    OrderStep, OrderWorkflow, OrderWorkflowClient, Payments, ProcessOrderRequest,
};
pub use activity_test::{
    ActivityRecord, ActivityTest, ActivityTestClient, ActivityWorkflow, ActivityWorkflowClient,
    GetActivityLogRequest, RunWithActivitiesRequest,
};
pub use counter::{
    Counter, CounterClient, DecrementRequest, GetCounterRequest, IncrementRequest,
    ResetCounterRequest,
};
pub use cross_entity::{
    ClearMessagesRequest, CrossEntity, CrossEntityClient, GetMessagesRequest, Message, PingRequest,
    ReceiveRequest, ResetPingCountRequest,
};
pub use kv_store::{
    ClearRequest, DeleteRequest, GetRequest, KVStore, KVStoreClient, ListKeysRequest, SetRequest,
};
pub use singleton_test::{SingletonManager, SingletonState};
pub use sql_activity_test::{
    FailingTransferRequest, GetSqlCountRequest, GetStateRequest, SqlActivityTest,
    SqlActivityTestClient, SqlActivityTestState, SqlCountWorkflow, SqlCountWorkflowClient,
    SqlFailingTransferWorkflow, SqlFailingTransferWorkflowClient, SqlTransferWorkflow,
    SqlTransferWorkflowClient, TransferRequest,
};
pub use stateless_counter::{
    StatelessCounter, StatelessCounterClient, StatelessDecrementRequest, StatelessGetRequest,
    StatelessIncrementRequest, StatelessResetRequest,
};
pub use timer_test::{
    CancelTimerRequest, ClearFiresRequest, GetPendingTimersRequest, GetTimerFiresRequest,
    PendingTimer, ScheduleTimerRequest, ScheduleTimerWorkflow, ScheduleTimerWorkflowClient,
    TimerFire, TimerTest, TimerTestClient,
};
pub use trait_test::{
    AuditEntry, Auditable, GetAuditLogRequest, GetTraitDataRequest, GetVersionRequest, TraitTest,
    TraitTestClient, UpdateRequest, Versioned,
};
pub use workflow_test::{
    FailingWorkflow, FailingWorkflowClient, GetExecutionRequest, ListExecutionsRequest,
    LongWorkflow, LongWorkflowClient, RunFailingWorkflowRequest, RunLongWorkflowRequest,
    RunSimpleWorkflowRequest, SimpleWorkflow, SimpleWorkflowClient, WorkflowExecution,
    WorkflowTest, WorkflowTestClient,
};
