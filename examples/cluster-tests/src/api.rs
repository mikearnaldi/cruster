//! HTTP API route handlers for cluster tests.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use cruster::sharding::Sharding;
use cruster::types::EntityId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::entities::{
    ActivityRecord, ActivityTestClient, ActivityWorkflowClient, AuditEntry, CancelTimerRequest,
    ClearFiresRequest, ClearMessagesRequest, ClearRequest, CounterClient, CrossEntityClient,
    DecrementRequest, DeleteRequest, FailingTransferRequest, FailingWorkflowClient,
    GetActivityLogRequest, GetCounterRequest, GetExecutionRequest, GetMessagesRequest,
    GetRequest, GetSqlCountRequest, IncrementRequest, KVStoreClient,
    ListExecutionsRequest, ListKeysRequest, LongWorkflowClient, Message, PendingTimer, PingRequest,
    ReceiveRequest, ResetCounterRequest, ResetPingCountRequest, RunFailingWorkflowRequest,
    RunLongWorkflowRequest, RunSimpleWorkflowRequest, RunWithActivitiesRequest,
    ScheduleTimerRequest, SetRequest, SimpleWorkflowClient, SingletonManager, SingletonState,
    SqlActivityTestClient, SqlActivityTestState, StatelessCounterClient, StatelessDecrementRequest,
    StatelessGetRequest, StatelessIncrementRequest, StatelessResetRequest, TimerFire,
    TimerTestClient, TraitTestClient, TransferRequest, UpdateRequest, WorkflowExecution,
    WorkflowTestClient,
};
// Import trait client extensions to make trait methods available on TraitTestClient
use crate::entities::trait_test::{AuditableClientExt, VersionedClientExt};

/// Shared application state.
pub struct AppState {
    /// Counter entity client.
    pub counter_client: CounterClient,
    /// KVStore entity client.
    pub kv_store_client: KVStoreClient,
    /// WorkflowTest entity client (for read queries).
    pub workflow_test_client: WorkflowTestClient,
    /// SimpleWorkflow client (standalone workflow).
    pub simple_workflow_client: SimpleWorkflowClient,
    /// FailingWorkflow client (standalone workflow).
    pub failing_workflow_client: FailingWorkflowClient,
    /// LongWorkflow client (standalone workflow).
    pub long_workflow_client: LongWorkflowClient,
    /// ActivityTest entity client (for read queries).
    pub activity_test_client: ActivityTestClient,
    /// ActivityWorkflow client (standalone workflow).
    pub activity_workflow_client: ActivityWorkflowClient,
    /// TraitTest entity client.
    pub trait_test_client: TraitTestClient,
    /// TimerTest entity client.
    pub timer_test_client: TimerTestClient,
    /// CrossEntity entity client.
    pub cross_entity_client: CrossEntityClient,
    /// SqlActivityTest entity client.
    pub sql_activity_test_client: SqlActivityTestClient,
    /// StatelessCounter entity client (pure-RPC, new API).
    pub stateless_counter_client: StatelessCounterClient,
    /// Singleton manager (uses cluster's register_singleton feature).
    pub singleton_manager: Arc<SingletonManager>,
    /// Reference to the sharding implementation for debug endpoints.
    pub sharding: Arc<dyn Sharding>,
    /// Shard groups from config.
    pub shard_groups: Vec<String>,
    /// Number of shards per group from config.
    pub shards_per_group: i32,
    /// List of registered entity types.
    pub registered_entity_types: Vec<String>,
    /// Database pool for debug queries.
    pub pool: sqlx::PgPool,
}

/// Health check response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Status string.
    pub status: String,
}

/// Create the HTTP router with all routes.
pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health check
        .route("/health", get(health_check))
        // Counter routes
        .route("/counter/:id", get(counter_get))
        .route("/counter/:id/increment", post(counter_increment))
        .route("/counter/:id/decrement", post(counter_decrement))
        .route("/counter/:id/reset", post(counter_reset))
        // KVStore routes
        .route("/kv/:id/set", post(kv_set))
        .route("/kv/:id/get/:key", get(kv_get))
        .route("/kv/:id/delete/:key", delete(kv_delete))
        .route("/kv/:id/keys", get(kv_list_keys))
        .route("/kv/:id/clear", post(kv_clear))
        // WorkflowTest routes
        .route("/workflow/:id/run-simple", post(workflow_run_simple))
        .route("/workflow/:id/run-failing", post(workflow_run_failing))
        .route("/workflow/:id/run-long", post(workflow_run_long))
        .route(
            "/workflow/:id/execution/:exec_id",
            get(workflow_get_execution),
        )
        .route("/workflow/:id/executions", get(workflow_list_executions))
        // ActivityTest routes
        .route("/activity/:id/run", post(activity_run))
        .route("/activity/:id/log", get(activity_get_log))
        // SqlActivityTest routes
        .route("/sql-activity/:id/transfer", post(sql_activity_transfer))
        .route(
            "/sql-activity/:id/failing-transfer",
            post(sql_activity_failing_transfer),
        )
        .route("/sql-activity/:id/state", get(sql_activity_get_state))
        .route(
            "/sql-activity/:id/sql-count",
            get(sql_activity_get_sql_count),
        )
        // TraitTest routes
        .route("/trait/:id", get(trait_get))
        .route("/trait/:id/update", post(trait_update))
        .route("/trait/:id/audit-log", get(trait_get_audit_log))
        .route("/trait/:id/version", get(trait_get_version))
        // TimerTest routes
        .route("/timer/:id/schedule", post(timer_schedule))
        .route("/timer/:id/cancel", post(timer_cancel))
        .route("/timer/:id/fires", get(timer_get_fires))
        .route("/timer/:id/clear-fires", post(timer_clear_fires))
        .route("/timer/:id/pending", get(timer_get_pending))
        // CrossEntity routes
        .route("/cross/:id/send", post(cross_send))
        .route("/cross/:id/receive", post(cross_receive))
        .route("/cross/:id/messages", get(cross_get_messages))
        .route("/cross/:id/clear", post(cross_clear_messages))
        .route("/cross/:id/ping-pong", post(cross_ping_pong))
        // StatelessCounter routes (pure-RPC, new API)
        .route("/stateless-counter/:id", get(stateless_counter_get))
        .route(
            "/stateless-counter/:id/increment",
            post(stateless_counter_increment),
        )
        .route(
            "/stateless-counter/:id/decrement",
            post(stateless_counter_decrement),
        )
        .route(
            "/stateless-counter/:id/reset",
            post(stateless_counter_reset),
        )
        // SingletonTest routes (uses cluster's register_singleton feature)
        .route("/singleton/state", get(singleton_state))
        .route("/singleton/tick-count", get(singleton_tick_count))
        .route("/singleton/current-runner", get(singleton_current_runner))
        .route("/singleton/reset", post(singleton_reset))
        // Debug routes
        .route("/debug/shards", get(debug_shards))
        .route("/debug/entities", get(debug_entities))
        .route("/debug/messages", get(debug_messages))
        .route("/debug/replies", get(debug_replies))
        .route("/debug/journal", get(debug_journal))
        .with_state(state)
}

/// Health check endpoint.
async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

/// Get current counter value.
async fn counter_get(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    let value = state
        .counter_client
        .get(
            &entity_id,
            &GetCounterRequest {
                entity_id: id.clone(),
            },
        )
        .await?;
    Ok(Json(value))
}

/// Increment counter by given amount.
async fn counter_increment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(amount): Json<i64>,
) -> Result<Json<i64>, AppError> {
    tracing::info!("counter_increment called: id={}, amount={}", id, amount);
    let entity_id = EntityId::new(&id);
    let value = state
        .counter_client
        .increment(
            &entity_id,
            &IncrementRequest {
                entity_id: id.clone(),
                amount,
            },
        )
        .await?;
    tracing::info!("counter_increment completed: id={}, value={}", id, value);
    Ok(Json(value))
}

/// Decrement counter by given amount.
async fn counter_decrement(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(amount): Json<i64>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    let value = state
        .counter_client
        .decrement(
            &entity_id,
            &DecrementRequest {
                entity_id: id.clone(),
                amount,
            },
        )
        .await?;
    Ok(Json(value))
}

/// Reset counter to zero.
async fn counter_reset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .counter_client
        .reset(
            &entity_id,
            &ResetCounterRequest {
                entity_id: id.clone(),
            },
        )
        .await?;
    Ok(Json(()))
}

// ============================================================================
// KVStore handlers
// ============================================================================

/// Request body for setting a key-value pair.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvSetBody {
    /// Key to set.
    pub key: String,
    /// Value to set.
    pub value: serde_json::Value,
}

/// Set a key-value pair.
async fn kv_set(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<KvSetBody>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .kv_store_client
        .set(
            &entity_id,
            &SetRequest {
                entity_id: id,
                key: body.key,
                value: body.value,
            },
        )
        .await?;
    Ok(Json(()))
}

/// Get a value by key.
async fn kv_get(
    State(state): State<Arc<AppState>>,
    Path((id, key)): Path<(String, String)>,
) -> Result<Json<Option<serde_json::Value>>, AppError> {
    let entity_id = EntityId::new(&id);
    let value = state
        .kv_store_client
        .get(
            &entity_id,
            &GetRequest {
                entity_id: id,
                key,
            },
        )
        .await?;
    Ok(Json(value))
}

/// Delete a key.
async fn kv_delete(
    State(state): State<Arc<AppState>>,
    Path((id, key)): Path<(String, String)>,
) -> Result<Json<bool>, AppError> {
    let entity_id = EntityId::new(&id);
    let deleted = state
        .kv_store_client
        .delete(
            &entity_id,
            &DeleteRequest {
                entity_id: id,
                key,
            },
        )
        .await?;
    Ok(Json(deleted))
}

/// List all keys.
async fn kv_list_keys(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<String>>, AppError> {
    let entity_id = EntityId::new(&id);
    let keys = state
        .kv_store_client
        .list_keys(
            &entity_id,
            &ListKeysRequest {
                entity_id: id,
            },
        )
        .await?;
    Ok(Json(keys))
}

/// Clear all data.
async fn kv_clear(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .kv_store_client
        .clear(
            &entity_id,
            &ClearRequest {
                entity_id: id,
            },
        )
        .await?;
    Ok(Json(()))
}

// ============================================================================
// WorkflowTest handlers
// ============================================================================

/// Request body for running a simple workflow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowRunSimpleBody {
    /// Execution ID.
    pub exec_id: String,
}

/// Request body for running a failing workflow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowRunFailingBody {
    /// Execution ID.
    pub exec_id: String,
    /// Step number to fail at.
    pub fail_at: usize,
}

/// Request body for running a long workflow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowRunLongBody {
    /// Execution ID.
    pub exec_id: String,
    /// Number of steps.
    pub steps: usize,
}

/// Run a simple workflow with 3 steps.
async fn workflow_run_simple(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<WorkflowRunSimpleBody>,
) -> Result<Json<String>, AppError> {
    let result = state
        .simple_workflow_client
        .execute(&RunSimpleWorkflowRequest {
            entity_id: id,
            exec_id: body.exec_id,
        })
        .await?;
    Ok(Json(result))
}

/// Run a workflow that fails at a specific step.
async fn workflow_run_failing(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<WorkflowRunFailingBody>,
) -> Result<Json<String>, AppError> {
    let result = state
        .failing_workflow_client
        .execute(&RunFailingWorkflowRequest {
            entity_id: id,
            exec_id: body.exec_id,
            fail_at: body.fail_at,
        })
        .await?;
    Ok(Json(result))
}

/// Run a long workflow with N steps.
async fn workflow_run_long(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<WorkflowRunLongBody>,
) -> Result<Json<String>, AppError> {
    let result = state
        .long_workflow_client
        .execute(&RunLongWorkflowRequest {
            entity_id: id,
            exec_id: body.exec_id,
            steps: body.steps,
        })
        .await?;
    Ok(Json(result))
}

/// Get a specific workflow execution.
async fn workflow_get_execution(
    State(state): State<Arc<AppState>>,
    Path((id, exec_id)): Path<(String, String)>,
) -> Result<Json<Option<WorkflowExecution>>, AppError> {
    let entity_id = EntityId::new(&id);
    let execution = state
        .workflow_test_client
        .get_execution(
            &entity_id,
            &GetExecutionRequest {
                entity_id: id,
                exec_id,
            },
        )
        .await?;
    Ok(Json(execution))
}

/// List all workflow executions.
async fn workflow_list_executions(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<WorkflowExecution>>, AppError> {
    let entity_id = EntityId::new(&id);
    let executions = state
        .workflow_test_client
        .list_executions(
            &entity_id,
            &ListExecutionsRequest {
                entity_id: id,
            },
        )
        .await?;
    Ok(Json(executions))
}

// ============================================================================
// ActivityTest handlers
// ============================================================================

/// Request body for running a workflow with activities.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityRunBody {
    /// Execution ID.
    pub exec_id: String,
}

/// Run a workflow with multiple activities.
async fn activity_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<ActivityRunBody>,
) -> Result<Json<Vec<String>>, AppError> {
    let result = state
        .activity_workflow_client
        .execute(&RunWithActivitiesRequest {
            entity_id: id,
            exec_id: body.exec_id,
        })
        .await?;
    Ok(Json(result))
}

/// Get the activity log.
async fn activity_get_log(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<ActivityRecord>>, AppError> {
    let entity_id = EntityId::new(&id);
    let log = state
        .activity_test_client
        .get_activity_log(
            &entity_id,
            &GetActivityLogRequest {
                entity_id: id,
            },
        )
        .await?;
    Ok(Json(log))
}

// ============================================================================
// SqlActivityTest handlers
// ============================================================================

/// Request body for making a transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlActivityTransferBody {
    /// Target entity ID.
    pub to_entity: String,
    /// Amount to transfer.
    pub amount: i64,
}

/// Make a transfer that records in both state and SQL table.
async fn sql_activity_transfer(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SqlActivityTransferBody>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    let count = state
        .sql_activity_test_client
        .transfer(
            &entity_id,
            &TransferRequest {
                to_entity: body.to_entity,
                amount: body.amount,
            },
        )
        .await?;
    Ok(Json(count))
}

/// Make a transfer that will fail (tests rollback).
async fn sql_activity_failing_transfer(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SqlActivityTransferBody>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    let count = state
        .sql_activity_test_client
        .failing_transfer(
            &entity_id,
            &FailingTransferRequest {
                to_entity: body.to_entity,
                amount: body.amount,
            },
        )
        .await?;
    Ok(Json(count))
}

/// Get the current entity state.
async fn sql_activity_get_state(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<SqlActivityTestState>, AppError> {
    let entity_id = EntityId::new(&id);
    let entity_state = state.sql_activity_test_client.get_state(&entity_id).await?;
    Ok(Json(entity_state))
}

/// Get the transfer count from SQL table (verifies SQL writes).
async fn sql_activity_get_sql_count(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    // Generate unique query ID to prevent workflow caching
    let query_id = format!(
        "query-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let count = state
        .sql_activity_test_client
        .get_transfer_count_from_sql(&entity_id, &GetSqlCountRequest { query_id })
        .await?;
    Ok(Json(count))
}

// ============================================================================
// TraitTest handlers
// ============================================================================

/// Request body for updating trait test data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraitUpdateBody {
    /// The new data value.
    pub data: String,
}

/// Get current data value.
async fn trait_get(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<String>, AppError> {
    let entity_id = EntityId::new(&id);
    let value = state.trait_test_client.get(&entity_id).await?;
    Ok(Json(value))
}

/// Update data value (logs via Auditable, bumps Versioned).
async fn trait_update(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TraitUpdateBody>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .trait_test_client
        .update(&entity_id, &UpdateRequest { data: body.data })
        .await?;
    Ok(Json(()))
}

/// Get audit log from Auditable trait.
async fn trait_get_audit_log(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<AuditEntry>>, AppError> {
    let entity_id = EntityId::new(&id);
    let log = state.trait_test_client.get_audit_log(&entity_id).await?;
    Ok(Json(log))
}

/// Get version from Versioned trait.
async fn trait_get_version(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<u64>, AppError> {
    let entity_id = EntityId::new(&id);
    let version = state.trait_test_client.get_version(&entity_id).await?;
    Ok(Json(version))
}

// ============================================================================
// TimerTest handlers
// ============================================================================

/// Request body for scheduling a timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerScheduleBody {
    /// Unique timer identifier.
    pub timer_id: String,
    /// Delay in milliseconds.
    pub delay_ms: u64,
}

/// Request body for cancelling a timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerCancelBody {
    /// Timer ID to cancel.
    pub timer_id: String,
}

/// Schedule a timer with the given delay.
async fn timer_schedule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TimerScheduleBody>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .timer_test_client
        .schedule_timer(
            &entity_id,
            &ScheduleTimerRequest {
                timer_id: body.timer_id,
                delay_ms: body.delay_ms,
            },
        )
        .await?;
    Ok(Json(()))
}

/// Cancel a pending timer.
async fn timer_cancel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TimerCancelBody>,
) -> Result<Json<bool>, AppError> {
    let entity_id = EntityId::new(&id);
    let cancelled = state
        .timer_test_client
        .cancel_timer(
            &entity_id,
            &CancelTimerRequest {
                timer_id: body.timer_id,
            },
        )
        .await?;
    Ok(Json(cancelled))
}

/// Get the list of fired timers.
async fn timer_get_fires(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<TimerFire>>, AppError> {
    let entity_id = EntityId::new(&id);
    let fires = state.timer_test_client.get_timer_fires(&entity_id).await?;
    Ok(Json(fires))
}

/// Clear the timer fire history.
async fn timer_clear_fires(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    // Generate unique request ID so each clear is a new workflow execution
    let request_id = format!("clear-{}-{}", id, chrono::Utc::now().timestamp_millis());
    state
        .timer_test_client
        .clear_fires(&entity_id, &ClearFiresRequest { request_id })
        .await?;
    Ok(Json(()))
}

/// Get the list of pending timers.
async fn timer_get_pending(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<PendingTimer>>, AppError> {
    let entity_id = EntityId::new(&id);
    let pending = state
        .timer_test_client
        .get_pending_timers(&entity_id)
        .await?;
    Ok(Json(pending))
}

// ============================================================================
// CrossEntity handlers
// ============================================================================

/// Request body for sending a message to another entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrossSendBody {
    /// Target entity type (currently only "CrossEntity" is supported).
    pub target_type: String,
    /// Target entity ID.
    pub target_id: String,
    /// Message content.
    pub message: String,
}

/// Request body for receiving a message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrossReceiveBody {
    /// The entity that sent the message.
    pub from: String,
    /// The message content.
    pub message: String,
}

/// Request body for ping-pong operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrossPingPongBody {
    /// Partner entity ID.
    pub partner_id: String,
    /// Number of ping-pongs to perform.
    pub count: u32,
}

/// Send a message to another entity.
///
/// This handler orchestrates the cross-entity communication by:
/// 1. Receiving the send request from entity A
/// 2. Calling receive on entity B with the message
async fn cross_send(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<CrossSendBody>,
) -> Result<Json<()>, AppError> {
    // Currently only CrossEntity-to-CrossEntity communication is supported
    if body.target_type != "CrossEntity" {
        return Err(anyhow::anyhow!(
            "Only CrossEntity target type is supported, got: {}",
            body.target_type
        )
        .into());
    }

    // Send the message to the target entity
    let target_entity_id = EntityId::new(&body.target_id);
    state
        .cross_entity_client
        .receive(
            &target_entity_id,
            &ReceiveRequest {
                entity_id: body.target_id.clone(),
                from: id,
                message: body.message,
            },
        )
        .await?;

    Ok(Json(()))
}

/// Receive a message from another entity.
async fn cross_receive(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<CrossReceiveBody>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .cross_entity_client
        .receive(
            &entity_id,
            &ReceiveRequest {
                entity_id: id,
                from: body.from,
                message: body.message,
            },
        )
        .await?;
    Ok(Json(()))
}

/// Get all messages received by an entity.
async fn cross_get_messages(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<Message>>, AppError> {
    let entity_id = EntityId::new(&id);
    let messages = state
        .cross_entity_client
        .get_messages(
            &entity_id,
            &GetMessagesRequest {
                entity_id: id,
            },
        )
        .await?;
    Ok(Json(messages))
}

/// Clear all messages for an entity.
async fn cross_clear_messages(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .cross_entity_client
        .clear_messages(
            &entity_id,
            &ClearMessagesRequest {
                entity_id: id,
            },
        )
        .await?;
    Ok(Json(()))
}

/// Perform a ping-pong sequence between two entities.
///
/// This orchestrates multiple back-and-forth calls between entity A (the caller)
/// and entity B (the partner). Each round:
/// 1. Entity A sends a ping to entity B with the current count
/// 2. Entity B records the ping and returns the count
///
/// Returns the final count after all ping-pongs complete.
async fn cross_ping_pong(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<CrossPingPongBody>,
) -> Result<Json<u32>, AppError> {
    let entity_a_id = EntityId::new(&id);
    let entity_b_id = EntityId::new(&body.partner_id);

    // Reset both entities' ping counts
    state
        .cross_entity_client
        .reset_ping_count(
            &entity_a_id,
            &ResetPingCountRequest {
                entity_id: id.clone(),
            },
        )
        .await?;
    state
        .cross_entity_client
        .reset_ping_count(
            &entity_b_id,
            &ResetPingCountRequest {
                entity_id: body.partner_id.clone(),
            },
        )
        .await?;

    // Perform the ping-pong sequence
    let mut current_count = 0u32;
    for _ in 0..body.count {
        // A pings B
        current_count += 1;
        state
            .cross_entity_client
            .ping(
                &entity_b_id,
                &PingRequest {
                    entity_id: body.partner_id.clone(),
                    count: current_count,
                },
            )
            .await?;

        // B pings A
        current_count += 1;
        state
            .cross_entity_client
            .ping(
                &entity_a_id,
                &PingRequest {
                    entity_id: id.clone(),
                    count: current_count,
                },
            )
            .await?;
    }

    // Return the final count (should be count * 2)
    Ok(Json(current_count))
}

// ============================================================================
// SingletonTest handlers (uses cluster's register_singleton feature)
// ============================================================================

/// Get the full singleton state from PostgreSQL.
async fn singleton_state(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Option<SingletonState>>, AppError> {
    let singleton_state = state.singleton_manager.get_state().await?;
    Ok(Json(singleton_state))
}

/// Get the current tick count.
///
/// The singleton increments this counter every second.
async fn singleton_tick_count(State(state): State<Arc<AppState>>) -> Result<Json<i64>, AppError> {
    let count = state.singleton_manager.get_tick_count().await?;
    Ok(Json(count))
}

/// Get the current runner ID hosting the singleton.
async fn singleton_current_runner(
    State(state): State<Arc<AppState>>,
) -> Result<Json<String>, AppError> {
    let runner = state.singleton_manager.get_current_runner().await?;
    Ok(Json(runner))
}

/// Reset the singleton state (for testing).
async fn singleton_reset(State(state): State<Arc<AppState>>) -> Result<Json<()>, AppError> {
    state.singleton_manager.reset().await?;
    Ok(Json(()))
}

// ============================================================================
// StatelessCounter handlers (pure-RPC, new API)
// ============================================================================

/// Get current stateless counter value.
async fn stateless_counter_get(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    let value = state
        .stateless_counter_client
        .get(&entity_id, &StatelessGetRequest { entity_id: id })
        .await?;
    Ok(Json(value))
}

/// Increment stateless counter by given amount.
async fn stateless_counter_increment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(amount): Json<i64>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    let value = state
        .stateless_counter_client
        .increment(
            &entity_id,
            &StatelessIncrementRequest {
                entity_id: id,
                amount,
            },
        )
        .await?;
    Ok(Json(value))
}

/// Decrement stateless counter by given amount.
async fn stateless_counter_decrement(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(amount): Json<i64>,
) -> Result<Json<i64>, AppError> {
    let entity_id = EntityId::new(&id);
    let value = state
        .stateless_counter_client
        .decrement(
            &entity_id,
            &StatelessDecrementRequest {
                entity_id: id,
                amount,
            },
        )
        .await?;
    Ok(Json(value))
}

/// Reset stateless counter to zero.
async fn stateless_counter_reset(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<()>, AppError> {
    let entity_id = EntityId::new(&id);
    state
        .stateless_counter_client
        .reset(&entity_id, &StatelessResetRequest { entity_id: id })
        .await?;
    Ok(Json(()))
}

// ============================================================================
// Debug handlers
// ============================================================================

/// Information about shard configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardInfo {
    /// Total number of shards across all groups.
    pub total_shards: i32,
    /// Number of shards per group.
    pub shards_per_group: i32,
    /// Configured shard groups.
    pub shard_groups: Vec<String>,
}

/// Information about registered entities.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntityInfo {
    /// Registered entity types.
    pub entity_types: Vec<String>,
    /// Number of active entity instances.
    pub active_count: usize,
}

/// Get shard configuration information.
async fn debug_shards(State(state): State<Arc<AppState>>) -> Json<ShardInfo> {
    let total_shards = state.shards_per_group * state.shard_groups.len() as i32;
    Json(ShardInfo {
        total_shards,
        shards_per_group: state.shards_per_group,
        shard_groups: state.shard_groups.clone(),
    })
}

/// Get registered entity information.
async fn debug_entities(State(state): State<Arc<AppState>>) -> Json<EntityInfo> {
    Json(EntityInfo {
        entity_types: state.registered_entity_types.clone(),
        active_count: state.sharding.active_entity_count(),
    })
}

// ============================================================================
// Debug DB query handlers
// ============================================================================

/// Query parameters for debug message queries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugMessageQuery {
    /// Filter by entity type.
    pub entity_type: Option<String>,
    /// Filter by entity ID.
    pub entity_id: Option<String>,
    /// Filter by processed status.
    pub processed: Option<bool>,
    /// Filter by tag (message method name).
    pub tag: Option<String>,
    /// Maximum number of results (default 100).
    pub limit: Option<i64>,
}

/// A message record from the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugMessageRecord {
    /// Message request ID.
    pub request_id: i64,
    /// Shard group.
    pub shard_group: String,
    /// Shard ID.
    pub shard_id: i32,
    /// Entity type.
    pub entity_type: String,
    /// Entity ID.
    pub entity_id: String,
    /// Message tag/method name.
    pub tag: String,
    /// Whether this is a request (vs fire-and-forget).
    pub is_request: bool,
    /// Whether the message has been processed.
    pub processed: bool,
    /// Number of delivery retries.
    pub retry_count: i32,
    /// Creation timestamp.
    pub created_at: String,
}

/// Query messages from the database.
async fn debug_messages(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<DebugMessageQuery>,
) -> Result<Json<Vec<DebugMessageRecord>>, AppError> {
    let limit = params.limit.unwrap_or(100);

    let rows = sqlx::query(
        r#"
        SELECT request_id, shard_group, shard_id, entity_type, entity_id,
               tag, is_request, processed, retry_count, created_at
        FROM cluster_messages
        WHERE ($1::text IS NULL OR entity_type = $1)
          AND ($2::text IS NULL OR entity_id = $2)
          AND ($3::boolean IS NULL OR processed = $3)
          AND ($4::text IS NULL OR tag = $4)
        ORDER BY created_at DESC
        LIMIT $5
        "#,
    )
    .bind(&params.entity_type)
    .bind(&params.entity_id)
    .bind(params.processed)
    .bind(&params.tag)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| anyhow::anyhow!("debug messages query failed: {e}"))?;

    let records: Vec<DebugMessageRecord> = rows
        .iter()
        .map(|row| {
            use sqlx::Row;
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
            DebugMessageRecord {
                request_id: row.get("request_id"),
                shard_group: row.get("shard_group"),
                shard_id: row.get("shard_id"),
                entity_type: row.get("entity_type"),
                entity_id: row.get("entity_id"),
                tag: row.get("tag"),
                is_request: row.get("is_request"),
                processed: row.get("processed"),
                retry_count: row.get("retry_count"),
                created_at: created_at.to_rfc3339(),
            }
        })
        .collect();

    Ok(Json(records))
}

/// Query parameters for debug reply queries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugReplyQuery {
    /// Filter by request ID.
    pub request_id: Option<i64>,
    /// Filter by exit status only.
    pub is_exit: Option<bool>,
    /// Maximum number of results (default 100).
    pub limit: Option<i64>,
}

/// A reply record from the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugReplyRecord {
    /// Reply ID.
    pub id: i64,
    /// Associated request ID.
    pub request_id: i64,
    /// Reply sequence number.
    pub sequence: i32,
    /// Whether this is a final exit reply.
    pub is_exit: bool,
    /// Creation timestamp.
    pub created_at: String,
}

/// Query replies from the database.
async fn debug_replies(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<DebugReplyQuery>,
) -> Result<Json<Vec<DebugReplyRecord>>, AppError> {
    let limit = params.limit.unwrap_or(100);

    let rows = sqlx::query(
        r#"
        SELECT id, request_id, sequence, is_exit, created_at
        FROM cluster_replies
        WHERE ($1::bigint IS NULL OR request_id = $1)
          AND ($2::boolean IS NULL OR is_exit = $2)
        ORDER BY created_at DESC
        LIMIT $3
        "#,
    )
    .bind(params.request_id)
    .bind(params.is_exit)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| anyhow::anyhow!("debug replies query failed: {e}"))?;

    let records: Vec<DebugReplyRecord> = rows
        .iter()
        .map(|row| {
            use sqlx::Row;
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
            DebugReplyRecord {
                id: row.get("id"),
                request_id: row.get("request_id"),
                sequence: row.get("sequence"),
                is_exit: row.get("is_exit"),
                created_at: created_at.to_rfc3339(),
            }
        })
        .collect();

    Ok(Json(records))
}

/// Query parameters for debug workflow journal queries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugJournalQuery {
    /// Filter by key prefix.
    pub prefix: Option<String>,
    /// Filter to only completed entries.
    pub completed: Option<bool>,
    /// Maximum number of results (default 100).
    pub limit: Option<i64>,
}

/// A workflow journal record from the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugJournalRecord {
    /// Journal key.
    pub key: String,
    /// Size of the stored value in bytes.
    pub value_size: i32,
    /// Creation timestamp.
    pub created_at: String,
    /// Last update timestamp.
    pub updated_at: String,
    /// Completion timestamp (null if not completed).
    pub completed_at: Option<String>,
}

/// Query workflow journal entries from the database.
async fn debug_journal(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<DebugJournalQuery>,
) -> Result<Json<Vec<DebugJournalRecord>>, AppError> {
    let limit = params.limit.unwrap_or(100);
    let prefix_pattern = params
        .prefix
        .as_ref()
        .map(|p| format!("{}%", p))
        .unwrap_or_else(|| "%".to_string());

    let rows = sqlx::query(
        r#"
        SELECT key, octet_length(value) as value_size, created_at, updated_at, completed_at
        FROM cluster_workflow_journal
        WHERE key LIKE $1
          AND ($2::boolean IS NULL OR ($2 = TRUE AND completed_at IS NOT NULL) OR ($2 = FALSE AND completed_at IS NULL))
        ORDER BY created_at DESC
        LIMIT $3
        "#,
    )
    .bind(&prefix_pattern)
    .bind(params.completed)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| anyhow::anyhow!("debug journal query failed: {e}"))?;

    let records: Vec<DebugJournalRecord> = rows
        .iter()
        .map(|row| {
            use sqlx::Row;
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
            let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");
            let completed_at: Option<chrono::DateTime<chrono::Utc>> = row.get("completed_at");
            DebugJournalRecord {
                key: row.get("key"),
                value_size: row.get("value_size"),
                created_at: created_at.to_rfc3339(),
                updated_at: updated_at.to_rfc3339(),
                completed_at: completed_at.map(|t| t.to_rfc3339()),
            }
        })
        .collect();

    Ok(Json(records))
}

/// Application error type.
pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": self.0.to_string()
            })),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
