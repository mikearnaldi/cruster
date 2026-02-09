//! WorkflowTest - tests durable workflow execution and replay.
//!
//! Migrated from old stateful entity API to new pure-RPC entity + standalone workflows.
//!
//! ## Architecture
//! - `WorkflowTest` entity: pure-RPC for read queries (get_execution, list_executions)
//! - `SimpleWorkflow`: standalone workflow with 3 sequential activities
//! - `FailingWorkflow`: standalone workflow that fails at a configurable step
//! - `LongWorkflow`: standalone workflow with N steps
//!
//! All execution state is stored in PostgreSQL `workflow_test_executions` table.

use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Record of a single workflow execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowExecution {
    /// Unique identifier for this execution.
    pub id: String,
    /// Steps that have been completed.
    pub steps_completed: Vec<String>,
    /// Final result if workflow completed.
    pub result: Option<String>,
}

// ============================================================================
// WorkflowTest entity - pure-RPC for reading execution data
// ============================================================================

/// WorkflowTest entity for querying workflow execution data.
///
/// Uses the new stateless entity API - state is stored directly in PostgreSQL
/// via the `workflow_test_executions` table.
///
/// ## RPCs
/// - `get_execution(entity_id, exec_id)` - Get a specific execution
/// - `list_executions(entity_id)` - List all executions for an entity
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct WorkflowTest {
    /// Database pool for direct state queries.
    pub pool: PgPool,
}

/// Request to get a specific execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetExecutionRequest {
    /// Entity ID that owns the execution.
    pub entity_id: String,
    /// Execution ID to retrieve.
    pub exec_id: String,
}

/// Request to list all executions for an entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListExecutionsRequest {
    /// Entity ID to list executions for.
    pub entity_id: String,
}

#[entity_impl]
impl WorkflowTest {
    /// Get a specific workflow execution.
    #[rpc]
    pub async fn get_execution(
        &self,
        request: GetExecutionRequest,
    ) -> Result<Option<WorkflowExecution>, ClusterError> {
        let row: Option<(String, Vec<String>, Option<String>)> = sqlx::query_as(
            "SELECT id, steps_completed, result FROM workflow_test_executions
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&request.entity_id)
        .bind(&request.exec_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_execution failed: {e}"),
            source: None,
        })?;

        Ok(row.map(|(id, steps_completed, result)| WorkflowExecution {
            id,
            steps_completed,
            result,
        }))
    }

    /// List all workflow executions for this entity.
    #[rpc]
    pub async fn list_executions(
        &self,
        request: ListExecutionsRequest,
    ) -> Result<Vec<WorkflowExecution>, ClusterError> {
        let rows: Vec<(String, Vec<String>, Option<String>)> = sqlx::query_as(
            "SELECT id, steps_completed, result FROM workflow_test_executions
             WHERE entity_id = $1
             ORDER BY id",
        )
        .bind(&request.entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("list_executions failed: {e}"),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(|(id, steps_completed, result)| WorkflowExecution {
                id,
                steps_completed,
                result,
            })
            .collect())
    }
}

// ============================================================================
// SimpleWorkflow - standalone workflow with 3 sequential activities
// ============================================================================

/// Request to run a simple workflow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunSimpleWorkflowRequest {
    /// Entity ID (scoping key for executions).
    pub entity_id: String,
    /// Unique execution ID.
    pub exec_id: String,
}

/// Simple workflow that creates an execution, runs 3 steps, and marks completed.
#[workflow]
#[derive(Clone)]
pub struct SimpleWorkflow {
    /// Database pool for writing execution data.
    pub pool: PgPool,
}

#[workflow_impl(key = |req: &RunSimpleWorkflowRequest| format!("{}/{}", req.entity_id, req.exec_id), hash = false)]
impl SimpleWorkflow {
    async fn execute(&self, request: RunSimpleWorkflowRequest) -> Result<String, ClusterError> {
        let entity_id = request.entity_id.clone();
        let exec_id = request.exec_id.clone();

        // Create execution record
        self.create_execution(entity_id.clone(), exec_id.clone())
            .await?;

        // Execute steps 1-3
        self.complete_step(entity_id.clone(), exec_id.clone(), "step1".to_string())
            .await?;
        self.complete_step(entity_id.clone(), exec_id.clone(), "step2".to_string())
            .await?;
        self.complete_step(entity_id.clone(), exec_id.clone(), "step3".to_string())
            .await?;

        // Mark as completed
        let result = format!("completed:{}", exec_id);
        self.mark_completed(entity_id, exec_id, result.clone())
            .await?;

        Ok(result)
    }

    #[activity]
    async fn create_execution(
        &self,
        entity_id: String,
        exec_id: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO workflow_test_executions (id, entity_id, steps_completed, result)
             VALUES ($1, $2, '{}', NULL)
             ON CONFLICT (entity_id, id) DO NOTHING",
        )
        .bind(&exec_id)
        .bind(&entity_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("create_execution failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn complete_step(
        &self,
        entity_id: String,
        exec_id: String,
        step_name: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "UPDATE workflow_test_executions
             SET steps_completed = array_append(steps_completed, $3)
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&entity_id)
        .bind(&exec_id)
        .bind(&step_name)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("complete_step failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn mark_completed(
        &self,
        entity_id: String,
        exec_id: String,
        result: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "UPDATE workflow_test_executions SET result = $3
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&entity_id)
        .bind(&exec_id)
        .bind(&result)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("mark_completed failed: {e}"),
            source: None,
        })?;
        Ok(())
    }
}

// ============================================================================
// FailingWorkflow - standalone workflow that fails at a configurable step
// ============================================================================

/// Request to run a workflow that fails at a specific step.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunFailingWorkflowRequest {
    /// Entity ID (scoping key for executions).
    pub entity_id: String,
    /// Unique execution ID.
    pub exec_id: String,
    /// Step number to fail at (0-indexed).
    pub fail_at: usize,
}

/// Workflow that creates an execution, runs steps, and fails at a configurable point.
#[workflow]
#[derive(Clone)]
pub struct FailingWorkflow {
    /// Database pool for writing execution data.
    pub pool: PgPool,
}

#[workflow_impl(key = |req: &RunFailingWorkflowRequest| format!("{}/{}", req.entity_id, req.exec_id), hash = false)]
impl FailingWorkflow {
    async fn execute(&self, request: RunFailingWorkflowRequest) -> Result<String, ClusterError> {
        let entity_id = request.entity_id.clone();
        let exec_id = request.exec_id.clone();
        let fail_at = request.fail_at;

        // Create execution record
        self.create_execution(entity_id.clone(), exec_id.clone())
            .await?;

        // Execute steps until failure
        for i in 0..3 {
            if i == fail_at {
                // Mark as failed
                self.mark_failed(entity_id.clone(), exec_id.clone(), i)
                    .await?;
                return Err(ClusterError::MalformedMessage {
                    reason: format!("Intentional failure at step {}", i),
                    source: None,
                });
            }
            self.complete_step(entity_id.clone(), exec_id.clone(), format!("step{}", i))
                .await?;
        }

        // Mark as completed
        let result = format!("completed:{}", exec_id);
        self.mark_completed(entity_id, exec_id, result.clone())
            .await?;

        Ok(result)
    }

    #[activity]
    async fn create_execution(
        &self,
        entity_id: String,
        exec_id: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO workflow_test_executions (id, entity_id, steps_completed, result)
             VALUES ($1, $2, '{}', NULL)
             ON CONFLICT (entity_id, id) DO NOTHING",
        )
        .bind(&exec_id)
        .bind(&entity_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("create_execution failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn complete_step(
        &self,
        entity_id: String,
        exec_id: String,
        step_name: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "UPDATE workflow_test_executions
             SET steps_completed = array_append(steps_completed, $3)
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&entity_id)
        .bind(&exec_id)
        .bind(&step_name)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("complete_step failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn mark_completed(
        &self,
        entity_id: String,
        exec_id: String,
        result: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "UPDATE workflow_test_executions SET result = $3
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&entity_id)
        .bind(&exec_id)
        .bind(&result)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("mark_completed failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn mark_failed(
        &self,
        entity_id: String,
        exec_id: String,
        step: usize,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "UPDATE workflow_test_executions SET result = $3
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&entity_id)
        .bind(&exec_id)
        .bind(format!("failed:step{}", step))
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("mark_failed failed: {e}"),
            source: None,
        })?;
        Ok(())
    }
}

// ============================================================================
// LongWorkflow - standalone workflow with N steps
// ============================================================================

/// Request to run a long workflow with N steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunLongWorkflowRequest {
    /// Entity ID (scoping key for executions).
    pub entity_id: String,
    /// Unique execution ID.
    pub exec_id: String,
    /// Number of steps to execute.
    pub steps: usize,
}

/// Workflow that creates an execution and runs a configurable number of steps.
#[workflow]
#[derive(Clone)]
pub struct LongWorkflow {
    /// Database pool for writing execution data.
    pub pool: PgPool,
}

#[workflow_impl(key = |req: &RunLongWorkflowRequest| format!("{}/{}", req.entity_id, req.exec_id), hash = false)]
impl LongWorkflow {
    async fn execute(&self, request: RunLongWorkflowRequest) -> Result<String, ClusterError> {
        let entity_id = request.entity_id.clone();
        let exec_id = request.exec_id.clone();
        let steps = request.steps;

        // Create execution record
        self.create_execution(entity_id.clone(), exec_id.clone())
            .await?;

        // Execute all steps
        for i in 0..steps {
            self.complete_step(entity_id.clone(), exec_id.clone(), format!("step{}", i))
                .await?;
        }

        // Mark as completed
        let result = format!("completed:{}", exec_id);
        self.mark_completed(entity_id, exec_id, result.clone())
            .await?;

        Ok(result)
    }

    #[activity]
    async fn create_execution(
        &self,
        entity_id: String,
        exec_id: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO workflow_test_executions (id, entity_id, steps_completed, result)
             VALUES ($1, $2, '{}', NULL)
             ON CONFLICT (entity_id, id) DO NOTHING",
        )
        .bind(&exec_id)
        .bind(&entity_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("create_execution failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn complete_step(
        &self,
        entity_id: String,
        exec_id: String,
        step_name: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "UPDATE workflow_test_executions
             SET steps_completed = array_append(steps_completed, $3)
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&entity_id)
        .bind(&exec_id)
        .bind(&step_name)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("complete_step failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    #[activity]
    async fn mark_completed(
        &self,
        entity_id: String,
        exec_id: String,
        result: String,
    ) -> Result<(), ClusterError> {
        sqlx::query(
            "UPDATE workflow_test_executions SET result = $3
             WHERE entity_id = $1 AND id = $2",
        )
        .bind(&entity_id)
        .bind(&exec_id)
        .bind(&result)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("mark_completed failed: {e}"),
            source: None,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_execution_serialization() {
        let execution = WorkflowExecution {
            id: "test-exec".to_string(),
            steps_completed: vec!["step1".to_string(), "step2".to_string()],
            result: Some("completed".to_string()),
        };

        let json = serde_json::to_string(&execution).unwrap();
        let parsed: WorkflowExecution = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, "test-exec");
        assert_eq!(parsed.steps_completed.len(), 2);
        assert_eq!(parsed.result, Some("completed".to_string()));
    }

    #[test]
    fn test_run_simple_workflow_request_serialization() {
        let req = RunSimpleWorkflowRequest {
            entity_id: "wf-1".to_string(),
            exec_id: "exec-1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: RunSimpleWorkflowRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "wf-1");
        assert_eq!(parsed.exec_id, "exec-1");
    }

    #[test]
    fn test_run_failing_workflow_request_serialization() {
        let req = RunFailingWorkflowRequest {
            entity_id: "wf-1".to_string(),
            exec_id: "exec-1".to_string(),
            fail_at: 2,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: RunFailingWorkflowRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "wf-1");
        assert_eq!(parsed.exec_id, "exec-1");
        assert_eq!(parsed.fail_at, 2);
    }

    #[test]
    fn test_run_long_workflow_request_serialization() {
        let req = RunLongWorkflowRequest {
            entity_id: "wf-1".to_string(),
            exec_id: "exec-1".to_string(),
            steps: 10,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: RunLongWorkflowRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "wf-1");
        assert_eq!(parsed.exec_id, "exec-1");
        assert_eq!(parsed.steps, 10);
    }
}
