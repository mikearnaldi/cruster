//! ActivityTest - tests activity journaling and transactional state.
//!
//! Migrated from old stateful entity API to new pure-RPC entity + standalone workflow.
//!
//! ## Architecture
//! - `ActivityTest` entity: pure-RPC for reading activity logs (get_activity_log)
//! - `ActivityWorkflow`: standalone workflow with multiple activities that write to PG
//!
//! All activity state is stored in PostgreSQL `activity_test_logs` table.

use chrono::{DateTime, Utc};

use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Record of a single activity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityRecord {
    /// Unique identifier for this activity.
    pub id: String,
    /// Action that was performed.
    pub action: String,
    /// When the activity occurred.
    pub timestamp: DateTime<Utc>,
}

// ============================================================================
// ActivityTest entity - pure-RPC for reading activity log data
// ============================================================================

/// ActivityTest entity for querying activity log data.
///
/// Uses the new stateless entity API - state is stored directly in PostgreSQL
/// via the `activity_test_logs` table.
///
/// ## RPCs
/// - `get_activity_log(entity_id)` - Get the activity log for an entity
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct ActivityTest {
    /// Database pool for direct state queries.
    pub pool: PgPool,
}

/// Request to get the activity log for an entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetActivityLogRequest {
    /// Entity ID to get logs for.
    pub entity_id: String,
}

#[entity_impl]
impl ActivityTest {
    /// Get the activity log.
    #[rpc]
    pub async fn get_activity_log(
        &self,
        request: GetActivityLogRequest,
    ) -> Result<Vec<ActivityRecord>, ClusterError> {
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT id, action, timestamp FROM activity_test_logs
             WHERE entity_id = $1
             ORDER BY timestamp ASC",
        )
        .bind(&request.entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_activity_log failed: {e}"),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(|(id, action, timestamp)| ActivityRecord {
                id,
                action,
                timestamp,
            })
            .collect())
    }
}

// ============================================================================
// ActivityWorkflow - standalone workflow with multiple activities
// ============================================================================

/// Request to run a workflow with activities.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunWithActivitiesRequest {
    /// Entity ID (scoping key for activity logs).
    pub entity_id: String,
    /// Unique execution ID.
    pub exec_id: String,
}

/// Request to log an activity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogActivityRequest {
    /// Entity ID to log activity for.
    pub entity_id: String,
    /// Activity ID.
    pub id: String,
    /// Action performed.
    pub action: String,
}

/// Request to simulate an external call.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalCallRequest {
    /// URL to call (simulated).
    pub url: String,
}

/// Workflow that runs multiple activities to test journaling and replay.
///
/// Activities use `&self.tx` for transactional DB writes — no `pool` field needed.
#[workflow]
#[derive(Clone)]
pub struct ActivityWorkflow;

#[workflow_impl(key = |req: &RunWithActivitiesRequest| format!("{}/{}", req.entity_id, req.exec_id), hash = false)]
impl ActivityWorkflow {
    async fn execute(
        &self,
        request: RunWithActivitiesRequest,
    ) -> Result<Vec<String>, ClusterError> {
        let entity_id = request.entity_id.clone();
        let exec_id = request.exec_id.clone();
        let mut results = Vec::new();

        // Activity 1: Log the start of the workflow
        self.log_activity(LogActivityRequest {
            entity_id: entity_id.clone(),
            id: format!("{}-start", exec_id),
            action: "workflow_started".to_string(),
        })
        .await?;
        results.push(format!("{}-start", exec_id));

        // Activity 2: Simulate an external call
        let external_result = self
            .external_call(ExternalCallRequest {
                url: "https://api.example.com/process".to_string(),
            })
            .await?;
        results.push(external_result);

        // Activity 3: Log a processing step
        self.log_activity(LogActivityRequest {
            entity_id: entity_id.clone(),
            id: format!("{}-process", exec_id),
            action: "data_processed".to_string(),
        })
        .await?;
        results.push(format!("{}-process", exec_id));

        // Activity 4: Another external call
        let external_result2 = self
            .external_call(ExternalCallRequest {
                url: "https://api.example.com/complete".to_string(),
            })
            .await?;
        results.push(external_result2);

        // Activity 5: Log the completion
        self.log_activity(LogActivityRequest {
            entity_id: entity_id.clone(),
            id: format!("{}-complete", exec_id),
            action: "workflow_completed".to_string(),
        })
        .await?;
        results.push(format!("{}-complete", exec_id));

        Ok(results)
    }

    /// Log an activity to the activity log.
    ///
    /// Writes the activity record to PostgreSQL via the framework transaction.
    /// On replay, this should be journaled and not re-executed.
    #[activity]
    async fn log_activity(&self, request: LogActivityRequest) -> Result<(), ClusterError> {
        sqlx::query(
            "INSERT INTO activity_test_logs (id, entity_id, action, timestamp)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (entity_id, id) DO NOTHING",
        )
        .bind(&request.id)
        .bind(&request.entity_id)
        .bind(&request.action)
        .execute(&self.tx)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("log_activity failed: {e}"),
            source: None,
        })?;
        Ok(())
    }

    /// Simulate an external call (side effect).
    ///
    /// On replay, the result should be replayed from the journal rather than re-executing.
    #[activity]
    async fn external_call(&self, request: ExternalCallRequest) -> Result<String, ClusterError> {
        // Simulate the external call by returning a deterministic result
        // In a real scenario, this would make an HTTP request
        Ok(format!("response_from_{}", request.url.replace('/', "_")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activity_record_serialization() {
        let record = ActivityRecord {
            id: "test-activity".to_string(),
            action: "test_action".to_string(),
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: ActivityRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, "test-activity");
        assert_eq!(parsed.action, "test_action");
    }

    #[test]
    fn test_run_with_activities_request_serialization() {
        let req = RunWithActivitiesRequest {
            entity_id: "act-1".to_string(),
            exec_id: "exec-1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: RunWithActivitiesRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "act-1");
        assert_eq!(parsed.exec_id, "exec-1");
    }
}
