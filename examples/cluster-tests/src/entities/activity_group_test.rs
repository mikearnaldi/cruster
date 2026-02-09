//! ActivityGroupTest - tests activity group composition in workflows.
//!
//! ## Architecture
//! - `Inventory` activity group: reusable activities for inventory operations
//! - `Payments` activity group: reusable activities for payment operations
//! - `OrderWorkflow`: standalone workflow composing both activity groups + local activities
//! - `ActivityGroupTest` entity: pure-RPC for reading order results from PG
//!
//! All state is stored in PostgreSQL `activity_group_test_orders` table.

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

// ============================================================================
// Inventory Activity Group
// ============================================================================

/// Reusable activity group for inventory operations.
#[activity_group]
#[derive(Clone)]
pub struct Inventory {
    /// Database pool for inventory queries.
    pub pool: PgPool,
}

/// Request to reserve inventory items.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReserveItemsRequest {
    /// Order ID for tracking.
    pub order_id: String,
    /// Number of items to reserve.
    pub item_count: i32,
}

/// Request to confirm a reservation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfirmReservationRequest {
    /// Order ID.
    pub order_id: String,
    /// Reservation ID to confirm.
    pub reservation_id: String,
}

#[activity_group_impl]
impl Inventory {
    /// Reserve inventory items. Returns a reservation ID.
    #[activity]
    async fn reserve_items(
        &self,
        request: ReserveItemsRequest,
    ) -> Result<String, ClusterError> {
        let reservation_id = format!("res-{}-{}", request.order_id, request.item_count);

        // Record the reservation in the database
        sqlx::query(
            "INSERT INTO activity_group_test_orders (order_id, step, detail, created_at)
             VALUES ($1, 'reserve', $2, NOW())
             ON CONFLICT (order_id, step) DO UPDATE SET detail = $2",
        )
        .bind(&request.order_id)
        .bind(&reservation_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("reserve_items failed: {e}"),
            source: None,
        })?;

        Ok(reservation_id)
    }

    /// Confirm a previously made reservation.
    #[activity]
    async fn confirm_reservation(
        &self,
        request: ConfirmReservationRequest,
    ) -> Result<String, ClusterError> {
        let confirmation = format!("confirmed-{}", request.reservation_id);

        sqlx::query(
            "INSERT INTO activity_group_test_orders (order_id, step, detail, created_at)
             VALUES ($1, 'confirm', $2, NOW())
             ON CONFLICT (order_id, step) DO UPDATE SET detail = $2",
        )
        .bind(&request.order_id)
        .bind(&confirmation)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("confirm_reservation failed: {e}"),
            source: None,
        })?;

        Ok(confirmation)
    }
}

// ============================================================================
// Payments Activity Group
// ============================================================================

/// Reusable activity group for payment operations.
#[activity_group]
#[derive(Clone)]
pub struct Payments {
    /// Database pool for payment records.
    pub pool: PgPool,
}

/// Request to charge a payment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChargePaymentRequest {
    /// Order ID.
    pub order_id: String,
    /// Amount to charge (in cents).
    pub amount: i64,
}

#[activity_group_impl]
impl Payments {
    /// Charge a payment. Returns a transaction ID.
    #[activity]
    async fn charge_payment(
        &self,
        request: ChargePaymentRequest,
    ) -> Result<String, ClusterError> {
        let tx_id = format!("tx-{}-{}", request.order_id, request.amount);

        sqlx::query(
            "INSERT INTO activity_group_test_orders (order_id, step, detail, created_at)
             VALUES ($1, 'charge', $2, NOW())
             ON CONFLICT (order_id, step) DO UPDATE SET detail = $2",
        )
        .bind(&request.order_id)
        .bind(&tx_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("charge_payment failed: {e}"),
            source: None,
        })?;

        Ok(tx_id)
    }
}

// ============================================================================
// OrderWorkflow - composes Inventory + Payments activity groups + local activity
// ============================================================================

/// Request to process an order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessOrderRequest {
    /// Order ID (used as idempotency key).
    pub order_id: String,
    /// Number of items.
    pub item_count: i32,
    /// Payment amount (in cents).
    pub amount: i64,
}

/// Result of processing an order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderResult {
    /// Order ID.
    pub order_id: String,
    /// Reservation ID from inventory.
    pub reservation_id: String,
    /// Transaction ID from payment.
    pub transaction_id: String,
    /// Confirmation from inventory.
    pub confirmation: String,
    /// Summary from local activity.
    pub summary: String,
}

/// Workflow that processes orders using composed activity groups.
///
/// Demonstrates:
/// - Two activity groups (`Inventory` and `Payments`) composed into one workflow
/// - Local `#[activity]` methods alongside group activities
/// - Sequential orchestration of group and local activities
#[workflow]
#[derive(Clone)]
pub struct OrderWorkflow {
    /// Database pool for the local summarize activity.
    pub pool: PgPool,
}

#[workflow_impl(
    key = |req: &ProcessOrderRequest| req.order_id.clone(),
    hash = false,
    activity_groups(Inventory, Payments)
)]
impl OrderWorkflow {
    async fn execute(&self, request: ProcessOrderRequest) -> Result<OrderResult, ClusterError> {
        let order_id = request.order_id.clone();

        // Step 1: Reserve inventory (from Inventory activity group)
        let reservation_id = self
            .reserve_items(ReserveItemsRequest {
                order_id: order_id.clone(),
                item_count: request.item_count,
            })
            .await?;

        // Step 2: Charge payment (from Payments activity group)
        let transaction_id = self
            .charge_payment(ChargePaymentRequest {
                order_id: order_id.clone(),
                amount: request.amount,
            })
            .await?;

        // Step 3: Confirm reservation (from Inventory activity group)
        let confirmation = self
            .confirm_reservation(ConfirmReservationRequest {
                order_id: order_id.clone(),
                reservation_id: reservation_id.clone(),
            })
            .await?;

        // Step 4: Summarize (local activity on this workflow)
        let summary = self
            .summarize(SummarizeRequest {
                order_id: order_id.clone(),
                reservation_id: reservation_id.clone(),
                transaction_id: transaction_id.clone(),
            })
            .await?;

        Ok(OrderResult {
            order_id,
            reservation_id,
            transaction_id,
            confirmation,
            summary,
        })
    }

    /// Local activity: summarize the order processing and write to DB.
    #[activity]
    async fn summarize(&self, request: SummarizeRequest) -> Result<String, ClusterError> {
        let summary = format!(
            "order={},res={},tx={}",
            request.order_id, request.reservation_id, request.transaction_id
        );

        sqlx::query(
            "INSERT INTO activity_group_test_orders (order_id, step, detail, created_at)
             VALUES ($1, 'summary', $2, NOW())
             ON CONFLICT (order_id, step) DO UPDATE SET detail = $2",
        )
        .bind(&request.order_id)
        .bind(&summary)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("summarize failed: {e}"),
            source: None,
        })?;

        Ok(summary)
    }
}

/// Internal request for the summarize activity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SummarizeRequest {
    /// Order ID.
    pub order_id: String,
    /// Reservation ID.
    pub reservation_id: String,
    /// Transaction ID.
    pub transaction_id: String,
}

// ============================================================================
// ActivityGroupTest entity - pure-RPC for reading order data
// ============================================================================

/// Pure-RPC entity for querying activity group test results.
///
/// Reads data written by the `OrderWorkflow` and its composed activity groups
/// from the `activity_group_test_orders` table.
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct ActivityGroupTest {
    /// Database pool for queries.
    pub pool: PgPool,
}

/// Request to get order steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetOrderStepsRequest {
    /// Order ID to query.
    pub order_id: String,
}

/// A single step in order processing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderStep {
    /// Processing step name (reserve, charge, confirm, summary).
    pub step: String,
    /// Step detail/result.
    pub detail: String,
    /// When the step was recorded.
    pub created_at: DateTime<Utc>,
}

#[entity_impl]
impl ActivityGroupTest {
    /// Get all steps for an order.
    #[rpc]
    pub async fn get_order_steps(
        &self,
        request: GetOrderStepsRequest,
    ) -> Result<Vec<OrderStep>, ClusterError> {
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT step, detail, created_at FROM activity_group_test_orders
             WHERE order_id = $1
             ORDER BY created_at ASC",
        )
        .bind(&request.order_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_order_steps failed: {e}"),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(|(step, detail, created_at)| OrderStep {
                step,
                detail,
                created_at,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_order_request_serialization() {
        let req = ProcessOrderRequest {
            order_id: "order-1".to_string(),
            item_count: 3,
            amount: 9999,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ProcessOrderRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.order_id, "order-1");
        assert_eq!(parsed.item_count, 3);
        assert_eq!(parsed.amount, 9999);
    }

    #[test]
    fn test_order_result_serialization() {
        let result = OrderResult {
            order_id: "order-1".to_string(),
            reservation_id: "res-1".to_string(),
            transaction_id: "tx-1".to_string(),
            confirmation: "confirmed-res-1".to_string(),
            summary: "summary".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let parsed: OrderResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.order_id, "order-1");
        assert_eq!(parsed.transaction_id, "tx-1");
    }

    #[test]
    fn test_order_step_serialization() {
        let step = OrderStep {
            step: "reserve".to_string(),
            detail: "res-1".to_string(),
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&step).unwrap();
        let parsed: OrderStep = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.step, "reserve");
    }
}
