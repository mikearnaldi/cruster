//! SqlActivityTest - tests executing arbitrary SQL within activity transactions.
//!
//! Migrated from old stateful entity API to new pure-RPC entity + standalone workflows.
//!
//! ## Architecture
//! - `SqlActivityTest` entity: pure-RPC for reading state from PG (`sql_activity_test_state` table)
//! - `SqlTransferWorkflow`: standalone workflow with activity that writes to both
//!   `sql_activity_test_state` and `sql_activity_test_transfers` atomically via `self.tx()`
//! - `SqlFailingTransferWorkflow`: standalone workflow with activity that writes then fails,
//!   testing that both state and transfer writes are rolled back
//! - `SqlCountWorkflow`: standalone workflow with activity that queries the transfers table
//!   via `self.tx()`
//!
//! All state is stored in PostgreSQL. The key feature being tested is that
//! `self.tx()` provides a SQL transaction handle for executing arbitrary SQL
//! within the same transaction as journal writes, ensuring atomicity.


use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// State for SqlActivityTest entity (stored in PG `sql_activity_test_state` table).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SqlActivityTestState {
    /// The entity ID.
    pub entity_id: String,
    /// Number of transfers made by this entity.
    pub transfer_count: i64,
    /// Total amount transferred.
    pub total_transferred: i64,
}

// ============================================================================
// SqlActivityTest entity - pure-RPC for reading state
// ============================================================================

/// SqlActivityTest entity for querying state data.
///
/// Uses the new stateless entity API - state is stored directly in PostgreSQL
/// via the `sql_activity_test_state` table.
///
/// ## RPCs
/// - `get_state(entity_id)` - Get the transfer state for an entity
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct SqlActivityTest {
    /// Database pool for direct state queries.
    pub pool: PgPool,
}

/// Request to get state for an entity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetStateRequest {
    /// Entity ID to get state for.
    pub entity_id: String,
}

#[entity_impl]
impl SqlActivityTest {
    /// Get current state from PG.
    #[rpc]
    pub async fn get_state(
        &self,
        request: GetStateRequest,
    ) -> Result<SqlActivityTestState, ClusterError> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "SELECT transfer_count, total_transferred FROM sql_activity_test_state
             WHERE entity_id = $1",
        )
        .bind(&request.entity_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_state failed: {e}"),
            source: None,
        })?;

        match row {
            Some((transfer_count, total_transferred)) => Ok(SqlActivityTestState {
                entity_id: request.entity_id,
                transfer_count,
                total_transferred,
            }),
            None => Ok(SqlActivityTestState {
                entity_id: request.entity_id,
                transfer_count: 0,
                total_transferred: 0,
            }),
        }
    }
}

// ============================================================================
// SqlTransferWorkflow - tests atomic SQL writes in activities
// ============================================================================

/// Request to make a transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferRequest {
    /// Source entity ID.
    pub entity_id: String,
    /// Target entity ID.
    pub to_entity: String,
    /// Amount to transfer.
    pub amount: i64,
}

/// Workflow that performs a transfer using `self.tx()`.
///
/// The activity writes to both `sql_activity_test_state` and
/// `sql_activity_test_transfers` in the same transaction, ensuring atomicity.
#[workflow]
#[derive(Clone)]
pub struct SqlTransferWorkflow;

#[workflow_impl(key = |req: &TransferRequest| format!("{}/transfer/{}/{}", req.entity_id, req.to_entity, req.amount))]
impl SqlTransferWorkflow {
    async fn execute(&self, request: TransferRequest) -> Result<i64, ClusterError> {
        self.do_transfer(request.entity_id, request.to_entity, request.amount)
            .await
    }

    /// Activity that records a transfer in both the state table AND the transfers table.
    ///
    /// Both operations happen in the same SQL transaction (via `self.tx()`):
    /// - UPSERT into `sql_activity_test_state` (transfer_count, total_transferred)
    /// - INSERT into `sql_activity_test_transfers`
    #[activity]
    async fn do_transfer(
        &mut self,
        entity_id: String,
        to_entity: String,
        amount: i64,
    ) -> Result<i64, ClusterError> {
        // Upsert state
        #[derive(sqlx::FromRow)]
        struct CountResult {
            transfer_count: i64,
        }

        let result: CountResult = sqlx::query_as(
            "INSERT INTO sql_activity_test_state (entity_id, transfer_count, total_transferred)
             VALUES ($1, 1, $2)
             ON CONFLICT (entity_id) DO UPDATE SET
               transfer_count = sql_activity_test_state.transfer_count + 1,
               total_transferred = sql_activity_test_state.total_transferred + $2
             RETURNING transfer_count",
        )
        .bind(&entity_id)
        .bind(amount)
        .fetch_one(&mut *self.tx())
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("do_transfer upsert failed: {e}"),
            source: None,
        })?;

        // Insert transfer record
        sqlx::query(
            "INSERT INTO sql_activity_test_transfers (from_entity, to_entity, amount, created_at)
             VALUES ($1, $2, $3, NOW())",
        )
        .bind(&entity_id)
        .bind(&to_entity)
        .bind(amount)
        .execute(self.tx())
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("do_transfer insert failed: {e}"),
            source: None,
        })?;

        Ok(result.transfer_count)
    }
}

// ============================================================================
// SqlFailingTransferWorkflow - tests rollback on activity failure
// ============================================================================

/// Request to make a transfer that will fail.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailingTransferRequest {
    /// Source entity ID.
    pub entity_id: String,
    /// Target entity ID.
    pub to_entity: String,
    /// Amount to transfer.
    pub amount: i64,
}

/// Workflow that writes to state + transfers then fails, testing rollback.
#[workflow]
#[derive(Clone)]
pub struct SqlFailingTransferWorkflow;

#[workflow_impl(key = |req: &FailingTransferRequest| format!("{}/failing/{}/{}", req.entity_id, req.to_entity, req.amount))]
impl SqlFailingTransferWorkflow {
    async fn execute(&self, request: FailingTransferRequest) -> Result<i64, ClusterError> {
        self.do_failing_transfer(request.entity_id, request.to_entity, request.amount)
            .await
    }

    /// Activity that updates state and SQL, then fails.
    /// This tests that both state AND SQL changes are rolled back.
    #[activity]
    async fn do_failing_transfer(
        &mut self,
        entity_id: String,
        to_entity: String,
        amount: i64,
    ) -> Result<i64, ClusterError> {
        // Upsert state (should be rolled back)
        sqlx::query(
            "INSERT INTO sql_activity_test_state (entity_id, transfer_count, total_transferred)
             VALUES ($1, 1, $2)
             ON CONFLICT (entity_id) DO UPDATE SET
               transfer_count = sql_activity_test_state.transfer_count + 1,
               total_transferred = sql_activity_test_state.total_transferred + $2",
        )
        .bind(&entity_id)
        .bind(amount)
        .execute(&mut *self.tx())
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("do_failing_transfer upsert failed: {e}"),
            source: None,
        })?;

        // Insert transfer record (should also be rolled back)
        sqlx::query(
            "INSERT INTO sql_activity_test_transfers (from_entity, to_entity, amount, created_at)
             VALUES ($1, $2, $3, NOW())",
        )
        .bind(&entity_id)
        .bind(&to_entity)
        .bind(amount)
        .execute(self.tx())
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("do_failing_transfer insert failed: {e}"),
            source: None,
        })?;

        // Now fail - both state and SQL should roll back
        Err(ClusterError::PersistenceError {
            reason: "intentional failure for testing rollback".to_string(),
            source: None,
        })
    }
}

// ============================================================================
// SqlCountWorkflow - queries transfers table via sql_transaction
// ============================================================================

/// Request to query SQL count.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetSqlCountRequest {
    /// Source entity ID.
    pub entity_id: String,
    /// Unique query ID to prevent caching.
    pub query_id: String,
}

/// Workflow that queries the transfers table using `self.tx()`.
#[workflow]
#[derive(Clone)]
pub struct SqlCountWorkflow;

#[workflow_impl(key = |req: &GetSqlCountRequest| format!("{}/count/{}", req.entity_id, req.query_id), hash = false)]
impl SqlCountWorkflow {
    async fn execute(&self, request: GetSqlCountRequest) -> Result<i64, ClusterError> {
        self.do_get_transfer_count(request.entity_id).await
    }

    /// Activity that queries the transfers table within a SQL transaction.
    #[activity]
    async fn do_get_transfer_count(&mut self, entity_id: String) -> Result<i64, ClusterError> {
        #[derive(sqlx::FromRow)]
        struct CountResult {
            count: i64,
        }

        let result: CountResult = sqlx::query_as(
            "SELECT COUNT(*) as count FROM sql_activity_test_transfers WHERE from_entity = $1",
        )
        .bind(&entity_id)
        .fetch_one(self.tx())
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("do_get_transfer_count failed: {e}"),
            source: None,
        })?;

        Ok(result.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_serialization() {
        let state = SqlActivityTestState {
            entity_id: "test-entity-1".to_string(),
            transfer_count: 5,
            total_transferred: 500,
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: SqlActivityTestState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "test-entity-1");
        assert_eq!(parsed.transfer_count, 5);
        assert_eq!(parsed.total_transferred, 500);
    }

    #[test]
    fn test_transfer_request_serialization() {
        let req = TransferRequest {
            entity_id: "src-1".to_string(),
            to_entity: "dst-1".to_string(),
            amount: 100,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: TransferRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "src-1");
        assert_eq!(parsed.to_entity, "dst-1");
        assert_eq!(parsed.amount, 100);
    }

    #[test]
    fn test_failing_transfer_request_serialization() {
        let req = FailingTransferRequest {
            entity_id: "src-1".to_string(),
            to_entity: "dst-1".to_string(),
            amount: 999,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: FailingTransferRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entity_id, "src-1");
        assert_eq!(parsed.amount, 999);
    }
}
