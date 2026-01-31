//! ActivityTest entity - entity for testing activity journaling and transactional state.
//!
//! This entity provides activity operations to test:
//! - Activities are journaled
//! - Activities replay correctly (not re-executed)
//! - Activity state mutations are transactional

use chrono::{DateTime, Utc};
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

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

/// State for an ActivityTest entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ActivityTestState {
    /// Log of all activities.
    pub activity_log: Vec<ActivityRecord>,
}

/// ActivityTest entity for testing activity journaling.
///
/// ## State (Persisted)
/// - activity_log: `Vec<ActivityRecord>` - all logged activities
///
/// ## Workflows
/// - `run_with_activities(exec_id)` - Run workflow with multiple activities
///
/// ## Activities
/// - `log_activity(id, action)` - Log an activity (mutates state)
/// - `external_call(url)` - Simulate external side effect
///
/// ## RPCs
/// - `get_activity_log()` - Get the activity log
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct ActivityTest;

/// Request to run a workflow with activities.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunWithActivitiesRequest {
    /// Unique execution ID.
    pub exec_id: String,
}

/// Request to log an activity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogActivityRequest {
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

#[entity_impl]
#[state(ActivityTestState)]
impl ActivityTest {
    fn init(&self, _ctx: &EntityContext) -> Result<ActivityTestState, ClusterError> {
        Ok(ActivityTestState::default())
    }

    /// Run a workflow with multiple activities.
    ///
    /// This workflow logs several activities and makes simulated external calls
    /// to test activity journaling and replay behavior.
    #[workflow]
    pub async fn run_with_activities(
        &self,
        request: RunWithActivitiesRequest,
    ) -> Result<Vec<String>, ClusterError> {
        let exec_id = request.exec_id;
        let mut results = Vec::new();

        // Activity 1: Log the start of the workflow
        self.log_activity(LogActivityRequest {
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
            id: format!("{}-complete", exec_id),
            action: "workflow_completed".to_string(),
        })
        .await?;
        results.push(format!("{}-complete", exec_id));

        Ok(results)
    }

    /// Log an activity to the activity log.
    ///
    /// This activity mutates the entity state by appending to the activity log.
    /// On replay, this should be deterministic and not re-execute.
    #[activity]
    async fn log_activity(&mut self, request: LogActivityRequest) -> Result<(), ClusterError> {
        let record = ActivityRecord {
            id: request.id,
            action: request.action,
            timestamp: Utc::now(),
        };
        self.state.activity_log.push(record);
        Ok(())
    }

    /// Simulate an external call (side effect).
    ///
    /// This activity simulates calling an external service. On replay, the
    /// result should be replayed from the journal rather than re-executing.
    #[activity]
    async fn external_call(
        &mut self,
        request: ExternalCallRequest,
    ) -> Result<String, ClusterError> {
        // Simulate the external call by returning a deterministic result
        // In a real scenario, this would make an HTTP request
        Ok(format!("response_from_{}", request.url.replace('/', "_")))
    }

    /// Get the activity log.
    #[rpc]
    pub async fn get_activity_log(&self) -> Result<Vec<ActivityRecord>, ClusterError> {
        Ok(self.state.activity_log.clone())
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
    fn test_activity_test_state_default() {
        let state = ActivityTestState::default();
        assert!(state.activity_log.is_empty());
    }

    #[test]
    fn test_activity_test_state_serialization() {
        let mut state = ActivityTestState::default();
        state.activity_log.push(ActivityRecord {
            id: "act1".to_string(),
            action: "action1".to_string(),
            timestamp: Utc::now(),
        });

        let json = serde_json::to_string(&state).unwrap();
        let parsed: ActivityTestState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.activity_log.len(), 1);
        assert_eq!(parsed.activity_log[0].id, "act1");
    }
}
