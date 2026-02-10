//! TraitTest entity - entity for testing RPC group composition.
//!
//! This entity demonstrates and tests the RPC group system (replacement for entity traits):
//! - Uses `Auditable` RPC group for audit logging
//! - Uses `Versioned` RPC group for optimistic concurrency control
//! - Multiple RPC groups compose correctly
//! - State is managed directly in PostgreSQL (no framework-managed state)

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

// ================== Auditable RPC Group ==================

/// An entry in the audit log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Unique identifier for this entry.
    pub id: String,
    /// The action that was performed.
    pub action: String,
    /// The user or entity that performed the action.
    pub actor: String,
    /// When the action occurred.
    pub timestamp: DateTime<Utc>,
    /// Additional details about the action.
    pub details: Option<String>,
}

/// Request to log an audit action.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogActionRequest {
    /// Entity ID for the audit log.
    pub entity_id: String,
    /// The action to log.
    pub action: String,
    /// The actor performing the action.
    pub actor: String,
    /// Optional details about the action.
    pub details: Option<String>,
}

/// Request to get the audit log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetAuditLogRequest {
    /// Entity ID to get the audit log for.
    pub entity_id: String,
}

/// Auditable RPC group - provides audit logging for entities.
///
/// This RPC group adds audit logging capability to any entity.
/// All actions can be logged with actor, timestamp, and details.
/// State is stored directly in PostgreSQL (`trait_test_audit_log` table).
#[rpc_group]
#[derive(Clone)]
pub struct Auditable {
    /// Database pool for direct state management.
    pub pool: PgPool,
}

#[rpc_group_impl]
impl Auditable {
    /// Log an action to the audit log.
    #[rpc(persisted)]
    pub async fn log_action(&self, request: LogActionRequest) -> Result<AuditEntry, ClusterError> {
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO trait_test_audit_log (entity_id, action, actor, timestamp, details)
             VALUES ($1, $2, $3, NOW(), $4)
             RETURNING id",
        )
        .bind(&request.entity_id)
        .bind(&request.action)
        .bind(&request.actor)
        .bind(&request.details)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("log_action failed: {e}"),
            source: None,
        })?;

        Ok(AuditEntry {
            id: format!("audit-{}", row.0),
            action: request.action,
            actor: request.actor,
            timestamp: Utc::now(),
            details: request.details,
        })
    }

    /// Get all audit log entries.
    #[rpc]
    pub async fn get_audit_log(
        &self,
        request: GetAuditLogRequest,
    ) -> Result<Vec<AuditEntry>, ClusterError> {
        let rows = sqlx::query_as::<_, (i64, String, String, DateTime<Utc>, Option<String>)>(
            "SELECT id, action, actor, timestamp, details
             FROM trait_test_audit_log
             WHERE entity_id = $1
             ORDER BY id ASC",
        )
        .bind(&request.entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("get_audit_log failed: {e}"),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(|(id, action, actor, timestamp, details)| AuditEntry {
                id: format!("audit-{id}"),
                action,
                actor,
                timestamp,
                details,
            })
            .collect())
    }
}

// ================== Versioned RPC Group ==================

/// Request to get the current version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetVersionRequest {
    /// Entity ID to get the version for.
    pub entity_id: String,
}

/// Request to bump the version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BumpVersionRequest {
    /// Entity ID to bump the version for.
    pub entity_id: String,
}

/// Request to check a version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckVersionRequest {
    /// Entity ID to check the version for.
    pub entity_id: String,
    /// The expected version.
    pub expected: u64,
}

/// Versioned RPC group - provides optimistic concurrency control.
///
/// This RPC group adds versioning to entities. Each mutation bumps
/// the version, enabling optimistic concurrency checks.
/// State is stored directly in PostgreSQL (`trait_test_versions` table).
#[rpc_group]
#[derive(Clone)]
pub struct Versioned {
    /// Database pool for direct state management.
    pub pool: PgPool,
}

#[rpc_group_impl]
impl Versioned {
    /// Get the current version.
    #[rpc]
    pub async fn get_version(&self, request: GetVersionRequest) -> Result<u64, ClusterError> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT version FROM trait_test_versions WHERE entity_id = $1")
                .bind(&request.entity_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("get_version failed: {e}"),
                    source: None,
                })?;

        Ok(row.map(|r| r.0 as u64).unwrap_or(0))
    }

    /// Bump the version and return the new version.
    #[rpc(persisted)]
    pub async fn bump_version(&self, request: BumpVersionRequest) -> Result<u64, ClusterError> {
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO trait_test_versions (entity_id, version)
             VALUES ($1, 1)
             ON CONFLICT (entity_id)
             DO UPDATE SET version = trait_test_versions.version + 1
             RETURNING version",
        )
        .bind(&request.entity_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("bump_version failed: {e}"),
            source: None,
        })?;

        Ok(row.0 as u64)
    }

    /// Check if the provided version matches the current version.
    #[rpc]
    pub async fn check_version(&self, request: CheckVersionRequest) -> Result<bool, ClusterError> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT version FROM trait_test_versions WHERE entity_id = $1")
                .bind(&request.entity_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("check_version failed: {e}"),
                    source: None,
                })?;

        let version = row.map(|r| r.0 as u64).unwrap_or(0);
        Ok(version == request.expected)
    }
}

// ================== TraitTest Entity ==================

/// TraitTest entity for testing RPC group composition.
///
/// Uses the new pure-RPC API — state is managed directly in PostgreSQL
/// via the `trait_test_data`, `trait_test_audit_log`, and `trait_test_versions` tables.
///
/// ## RPC Groups
/// - `Auditable` - provides audit logging via `log_action` and `get_audit_log`
/// - `Versioned` - provides version tracking via `get_version`, `bump_version`, and `check_version`
///
/// ## RPCs
/// - `update(data)` - Update data, log audit entry, and bump version (persisted)
/// - `get()` - Get current data (non-persisted, read-only)
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct TraitTest {
    /// Database pool for direct state management.
    pub pool: PgPool,
}

/// Request to update the data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateRequest {
    /// Entity ID (used as the DB key).
    pub entity_id: String,
    /// The new data value.
    pub data: String,
}

/// Request to get the current data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetTraitDataRequest {
    /// Entity ID (used as the DB key).
    pub entity_id: String,
}

#[entity_impl(rpc_groups(Auditable, Versioned))]
impl TraitTest {
    /// Update the data value.
    ///
    /// This operation:
    /// 1. Updates the data in PostgreSQL
    /// 2. Logs the update via the Auditable RPC group
    /// 3. Bumps the version via the Versioned RPC group
    ///
    /// All three operations happen in the same persisted RPC call.
    #[rpc(persisted)]
    pub async fn update(&self, request: UpdateRequest) -> Result<(), ClusterError> {
        // Update the entity's own data
        sqlx::query(
            "INSERT INTO trait_test_data (entity_id, data)
             VALUES ($1, $2)
             ON CONFLICT (entity_id)
             DO UPDATE SET data = $2",
        )
        .bind(&request.entity_id)
        .bind(&request.data)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("update data failed: {e}"),
            source: None,
        })?;

        // Log via Auditable RPC group (direct SQL in same handler)
        sqlx::query(
            "INSERT INTO trait_test_audit_log (entity_id, action, actor, timestamp, details)
             VALUES ($1, 'update', 'system', NOW(), $2)",
        )
        .bind(&request.entity_id)
        .bind(format!("Updated data to: {}", request.data))
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("log audit failed: {e}"),
            source: None,
        })?;

        // Bump version via Versioned RPC group (direct SQL in same handler)
        sqlx::query(
            "INSERT INTO trait_test_versions (entity_id, version)
             VALUES ($1, 1)
             ON CONFLICT (entity_id)
             DO UPDATE SET version = trait_test_versions.version + 1",
        )
        .bind(&request.entity_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("bump version failed: {e}"),
            source: None,
        })?;

        Ok(())
    }

    /// Get the current data value.
    #[rpc]
    pub async fn get(&self, request: GetTraitDataRequest) -> Result<String, ClusterError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT data FROM trait_test_data WHERE entity_id = $1")
                .bind(&request.entity_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("get data failed: {e}"),
                    source: None,
                })?;

        Ok(row.map(|r| r.0).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_serialization() {
        let entry = AuditEntry {
            id: "audit-1".to_string(),
            action: "update".to_string(),
            actor: "user1".to_string(),
            timestamp: Utc::now(),
            details: Some("Updated value".to_string()),
        };

        let json = serde_json::to_string(&entry).unwrap();
        let parsed: AuditEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, "audit-1");
        assert_eq!(parsed.action, "update");
        assert_eq!(parsed.actor, "user1");
        assert_eq!(parsed.details, Some("Updated value".to_string()));
    }

    #[test]
    fn test_update_request_serialization() {
        let req = UpdateRequest {
            entity_id: "e1".to_string(),
            data: "hello".to_string(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: UpdateRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_id, "e1");
        assert_eq!(parsed.data, "hello");
    }
}
