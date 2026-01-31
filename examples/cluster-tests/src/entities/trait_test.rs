//! TraitTest entity - entity for testing entity trait composition.
//!
//! This entity demonstrates and tests the entity trait system:
//! - Uses `Auditable` trait for audit logging
//! - Uses `Versioned` trait for optimistic concurrency control
//! - Multiple traits compose correctly
//! - Trait state persists alongside entity state

use chrono::{DateTime, Utc};
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

// ================== Auditable Trait ==================

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

/// State for the Auditable trait.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuditableState {
    /// The audit log entries.
    pub entries: Vec<AuditEntry>,
    /// Counter for generating unique entry IDs.
    pub next_id: u64,
}

/// Auditable trait - provides audit logging for entities.
///
/// This trait adds audit logging capability to any entity.
/// All actions can be logged with actor, timestamp, and details.
#[entity_trait]
#[derive(Clone)]
pub struct Auditable;

#[entity_trait_impl]
#[state(AuditableState)]
impl Auditable {
    fn init(&self) -> Result<AuditableState, ClusterError> {
        Ok(AuditableState::default())
    }

    /// Log an action to the audit log.
    #[activity]
    #[protected]
    pub async fn log_action(
        &mut self,
        action: String,
        actor: String,
        details: Option<String>,
    ) -> Result<AuditEntry, ClusterError> {
        let entry = AuditEntry {
            id: format!("audit-{}", self.state.next_id),
            action,
            actor,
            timestamp: Utc::now(),
            details,
        };
        self.state.next_id += 1;
        self.state.entries.push(entry.clone());
        Ok(entry)
    }

    /// Get all audit log entries.
    #[rpc]
    pub async fn get_audit_log(&self) -> Result<Vec<AuditEntry>, ClusterError> {
        Ok(self.state.entries.clone())
    }

    /// Clear the audit log.
    #[allow(dead_code)]
    #[activity]
    #[protected]
    pub async fn clear_audit_log(&mut self) -> Result<(), ClusterError> {
        self.state.entries.clear();
        Ok(())
    }
}

// ================== Versioned Trait ==================

/// State for the Versioned trait.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VersionedState {
    /// The current version number.
    pub version: u64,
}

/// Versioned trait - provides optimistic concurrency control.
///
/// This trait adds versioning to entities. Each mutation bumps
/// the version, enabling optimistic concurrency checks.
#[entity_trait]
#[derive(Clone)]
pub struct Versioned;

#[entity_trait_impl]
#[state(VersionedState)]
impl Versioned {
    fn init(&self) -> Result<VersionedState, ClusterError> {
        Ok(VersionedState::default())
    }

    /// Get the current version.
    #[rpc]
    pub async fn get_version(&self) -> Result<u64, ClusterError> {
        Ok(self.state.version)
    }

    /// Bump the version and return the new version.
    #[activity]
    #[protected]
    pub async fn bump_version(&mut self) -> Result<u64, ClusterError> {
        self.state.version += 1;
        Ok(self.state.version)
    }

    /// Check if the provided version matches the current version.
    #[rpc]
    pub async fn check_version(&self, expected: u64) -> Result<bool, ClusterError> {
        Ok(self.state.version == expected)
    }
}

// ================== TraitTest Entity ==================

/// State for the TraitTest entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TraitTestState {
    /// The data stored in this entity.
    pub data: String,
}

/// TraitTest entity for testing entity trait composition.
///
/// ## State (Persisted)
/// - data: String - the current data
///
/// ## Traits
/// - `Auditable` - provides audit logging via `log_action` and `get_audit_log`
/// - `Versioned` - provides version tracking via `get_version` and `bump_version`
///
/// ## RPCs
/// - `update(data)` - Update data (logs via Auditable, bumps Versioned)
/// - `get()` - Get current data
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct TraitTest;

/// Request to update the data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateRequest {
    /// The new data value.
    pub data: String,
}

#[entity_impl(traits(Auditable, Versioned))]
#[state(TraitTestState)]
impl TraitTest {
    fn init(&self, _ctx: &EntityContext) -> Result<TraitTestState, ClusterError> {
        Ok(TraitTestState::default())
    }

    /// Update the data value.
    ///
    /// This operation:
    /// 1. Updates the data in entity state
    /// 2. Logs the update via the Auditable trait
    /// 3. Bumps the version via the Versioned trait
    #[workflow]
    pub async fn update(&self, request: UpdateRequest) -> Result<(), ClusterError> {
        // Update the entity's own state via activity
        self.update_data(request.data.clone()).await?;

        // Log via Auditable trait
        self.log_action(
            "update".to_string(),
            "system".to_string(),
            Some(format!("Updated data to: {}", request.data)),
        )
        .await?;

        // Bump version via Versioned trait
        self.bump_version().await?;

        Ok(())
    }

    /// Activity to update the data.
    #[activity]
    async fn update_data(&mut self, data: String) -> Result<(), ClusterError> {
        self.state.data = data;
        Ok(())
    }

    /// Get the current data value.
    #[rpc]
    pub async fn get(&self) -> Result<String, ClusterError> {
        Ok(self.state.data.clone())
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
    fn test_auditable_state_default() {
        let state = AuditableState::default();
        assert!(state.entries.is_empty());
        assert_eq!(state.next_id, 0);
    }

    #[test]
    fn test_versioned_state_default() {
        let state = VersionedState::default();
        assert_eq!(state.version, 0);
    }

    #[test]
    fn test_trait_test_state_serialization() {
        let state = TraitTestState {
            data: "hello world".to_string(),
        };

        let json = serde_json::to_string(&state).unwrap();
        let parsed: TraitTestState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.data, "hello world");
    }

    #[test]
    fn test_trait_test_state_default() {
        let state = TraitTestState::default();
        assert!(state.data.is_empty());
    }
}
