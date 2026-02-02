//! SqlActivityTest entity - tests executing arbitrary SQL within activity transactions.
//!
//! This entity demonstrates and tests the ability to execute custom SQL statements
//! within the same transaction as entity state changes, ensuring atomicity.

use cruster::__internal::ActivityScope;
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

/// State for SqlActivityTest entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SqlActivityTestState {
    /// The entity ID (stored for SQL queries).
    pub entity_id: String,
    /// Number of transfers made by this entity.
    pub transfer_count: i64,
    /// Total amount transferred.
    pub total_transferred: i64,
}

/// SqlActivityTest entity for testing SQL execution within activities.
///
/// This entity tests:
/// - Executing arbitrary SQL in the same transaction as state changes
/// - SQL operations are committed/rolled back with state changes
/// - Querying custom tables within activities
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct SqlActivityTest;

/// Request to make a transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferRequest {
    /// Target entity ID.
    pub to_entity: String,
    /// Amount to transfer.
    pub amount: i64,
}

/// Request to make a transfer that will fail.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailingTransferRequest {
    /// Target entity ID.
    pub to_entity: String,
    /// Amount to transfer.
    pub amount: i64,
}

/// Request to query SQL count.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetSqlCountRequest {
    /// Unique query ID to prevent caching.
    pub query_id: String,
}

#[entity_impl]
#[state(SqlActivityTestState)]
impl SqlActivityTest {
    fn init(&self, ctx: &EntityContext) -> Result<SqlActivityTestState, ClusterError> {
        Ok(SqlActivityTestState {
            entity_id: ctx.address.entity_id.to_string(),
            transfer_count: 0,
            total_transferred: 0,
        })
    }

    /// Activity that records a transfer in both entity state AND a custom SQL table.
    ///
    /// Both operations happen in the same transaction:
    /// - State update (transfer_count, total_transferred)
    /// - SQL INSERT into sql_activity_test_transfers table
    #[activity]
    async fn do_transfer(&mut self, to_entity: String, amount: i64) -> Result<i64, ClusterError> {
        // Update entity state
        self.state.transfer_count += 1;
        self.state.total_transferred += amount;

        // Execute SQL in the same transaction
        if let Some(tx) = ActivityScope::sql_transaction().await {
            tx.execute(
                sqlx::query(
                    "INSERT INTO sql_activity_test_transfers (from_entity, to_entity, amount, created_at) 
                     VALUES ($1, $2, $3, NOW())"
                )
                .bind(&self.state.entity_id)
                .bind(&to_entity)
                .bind(amount)
            ).await?;
        }

        Ok(self.state.transfer_count)
    }

    /// Activity that updates state and SQL, then fails.
    /// This tests that both state AND SQL changes are rolled back.
    #[activity]
    async fn do_failing_transfer(
        &mut self,
        to_entity: String,
        amount: i64,
    ) -> Result<i64, ClusterError> {
        // Update entity state (should be rolled back)
        self.state.transfer_count += 1;
        self.state.total_transferred += amount;

        // Execute SQL in the same transaction (should also be rolled back)
        if let Some(tx) = ActivityScope::sql_transaction().await {
            tx.execute(
                sqlx::query(
                    "INSERT INTO sql_activity_test_transfers (from_entity, to_entity, amount, created_at) 
                     VALUES ($1, $2, $3, NOW())"
                )
                .bind(&self.state.entity_id)
                .bind(&to_entity)
                .bind(amount)
            ).await?;
        }

        // Now fail - both state and SQL should roll back
        Err(ClusterError::PersistenceError {
            reason: "intentional failure for testing rollback".to_string(),
            source: None,
        })
    }

    /// Activity that queries the transfers table.
    /// Takes a query_id parameter to ensure each call is unique (not replayed from journal).
    #[activity]
    async fn do_get_transfer_count_from_sql(
        &mut self,
        _query_id: String,
    ) -> Result<i64, ClusterError> {
        if let Some(tx) = ActivityScope::sql_transaction().await {
            #[derive(sqlx::FromRow)]
            struct CountResult {
                count: i64,
            }

            let result: CountResult = tx
                .fetch_one(
                    sqlx::query_as(
                        "SELECT COUNT(*) as count FROM sql_activity_test_transfers WHERE from_entity = $1"
                    )
                    .bind(&self.state.entity_id)
                )
                .await?;

            Ok(result.count)
        } else {
            // No SQL transaction available (shouldn't happen in production with SQL storage)
            Ok(-1)
        }
    }

    /// Make a transfer (workflow that calls the activity).
    #[workflow]
    pub async fn transfer(&self, request: TransferRequest) -> Result<i64, ClusterError> {
        self.do_transfer(request.to_entity, request.amount).await
    }

    /// Make a transfer that will fail (tests rollback).
    #[workflow]
    pub async fn failing_transfer(
        &self,
        request: FailingTransferRequest,
    ) -> Result<i64, ClusterError> {
        self.do_failing_transfer(request.to_entity, request.amount)
            .await
    }

    /// Get current state.
    #[rpc]
    pub async fn get_state(&self) -> Result<SqlActivityTestState, ClusterError> {
        Ok(self.state.clone())
    }

    /// Get transfer count from SQL table (verifies SQL writes).
    #[workflow]
    pub async fn get_transfer_count_from_sql(
        &self,
        request: GetSqlCountRequest,
    ) -> Result<i64, ClusterError> {
        self.do_get_transfer_count_from_sql(request.query_id).await
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
}
