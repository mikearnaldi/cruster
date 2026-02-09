//! Auditable RPC group - shared audit logging capability for entities.
//!
//! This RPC group provides a composable audit logging capability that can be
//! added to entities that need audit trails. When an entity uses this group
//! via `#[entity_impl(rpc_groups(Auditable))]`, it gains audit logging RPCs
//! like `log_player_action` that can be called on the entity's client.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::types::player::PlayerId;

/// A single audit log entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    /// When this action occurred.
    pub timestamp: DateTime<Utc>,
    /// The action that was performed (e.g., "move", "resign", "draw_offer").
    pub action: String,
    /// The player who performed the action (None for system actions).
    pub actor: Option<PlayerId>,
    /// Additional details about the action as JSON.
    pub details: JsonValue,
}

impl AuditEntry {
    /// Create a new audit entry.
    #[must_use]
    pub fn new(action: impl Into<String>, actor: Option<PlayerId>, details: JsonValue) -> Self {
        Self {
            timestamp: Utc::now(),
            action: action.into(),
            actor,
            details,
        }
    }

    /// Create a new audit entry for a player action.
    #[must_use]
    pub fn player_action(action: impl Into<String>, actor: PlayerId, details: JsonValue) -> Self {
        Self::new(action, Some(actor), details)
    }

    /// Create a new audit entry for a system action.
    #[must_use]
    pub fn system_action(action: impl Into<String>, details: JsonValue) -> Self {
        Self::new(action, None, details)
    }
}

/// The audit log state for the Auditable RPC group.
///
/// This is a bounded deque of audit entries that automatically
/// trims old entries when the max size is exceeded.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditLog {
    /// The audit entries (most recent last).
    entries: VecDeque<AuditEntry>,
    /// Maximum number of entries to retain.
    max_entries: usize,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries: Self::DEFAULT_MAX_ENTRIES,
        }
    }
}

impl AuditLog {
    /// Default maximum number of audit entries.
    pub const DEFAULT_MAX_ENTRIES: usize = 100;

    /// Create a new audit log with the default max entries.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new audit log with a custom max entries limit.
    #[must_use]
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    /// Log an audit entry.
    ///
    /// If the log is at capacity, the oldest entry will be removed.
    pub fn log(&mut self, entry: AuditEntry) {
        // If at capacity, remove the oldest entry
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// Get the most recent audit entries.
    ///
    /// Returns up to `limit` entries, most recent first.
    #[must_use]
    pub fn get_recent(&self, limit: usize) -> Vec<AuditEntry> {
        self.entries.iter().rev().take(limit).cloned().collect()
    }

    /// Get all audit entries.
    ///
    /// Returns entries in chronological order (oldest first).
    #[must_use]
    pub fn get_all(&self) -> Vec<AuditEntry> {
        self.entries.iter().cloned().collect()
    }

    /// Get the total number of entries in the log.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries from the log.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Get entries for a specific actor.
    #[must_use]
    pub fn entries_by_actor(&self, actor: PlayerId) -> Vec<AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.actor == Some(actor))
            .cloned()
            .collect()
    }

    /// Get entries for a specific action type.
    #[must_use]
    pub fn entries_by_action(&self, action: &str) -> Vec<AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.action == action)
            .cloned()
            .collect()
    }
}

/// Request to log a player action.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogPlayerActionRequest {
    /// The action that was performed.
    pub action: String,
    /// The player who performed the action.
    pub actor: PlayerId,
    /// Additional details about the action.
    pub details: JsonValue,
}

/// Request to log a system action.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogSystemActionRequest {
    /// The action that was performed.
    pub action: String,
    /// Additional details about the action.
    pub details: JsonValue,
}

/// Request to get audit log entries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetAuditLogRequest {
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
}

impl Default for GetAuditLogRequest {
    fn default() -> Self {
        Self { limit: Some(50) }
    }
}

/// Response containing audit log entries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetAuditLogResponse {
    /// The audit entries (most recent first).
    pub entries: Vec<AuditEntry>,
    /// Total number of entries in the log.
    pub total_entries: usize,
}

/// Auditable RPC group - provides audit logging capability.
///
/// This group can be composed with entities using `#[entity_impl(rpc_groups(Auditable))]`.
/// The entity's client then gains audit logging RPCs: `log_player_action`,
/// `log_system_action`, `get_audit_log`, `get_audit_log_by_actor`, and
/// `get_audit_log_by_action`.
///
/// State is ephemeral, held in an `Arc<Mutex<AuditLog>>` — same pattern as
/// other chess-cluster entities.
#[rpc_group]
#[derive(Clone)]
pub struct Auditable {
    /// Maximum number of entries to retain (configurable per-entity).
    pub max_entries: usize,
    /// Ephemeral audit log state.
    pub log: Arc<Mutex<AuditLog>>,
}

impl Default for Auditable {
    fn default() -> Self {
        let max_entries = AuditLog::DEFAULT_MAX_ENTRIES;
        Self {
            max_entries,
            log: Arc::new(Mutex::new(AuditLog::with_max_entries(max_entries))),
        }
    }
}

impl Auditable {
    /// Create a new Auditable group with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new Auditable group with a custom max entries limit.
    #[must_use]
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            max_entries,
            log: Arc::new(Mutex::new(AuditLog::with_max_entries(max_entries))),
        }
    }
}

#[rpc_group_impl]
impl Auditable {
    /// Log a player action to the audit log.
    #[rpc(persisted)]
    pub async fn log_player_action(
        &self,
        request: LogPlayerActionRequest,
    ) -> Result<(), ClusterError> {
        let entry = AuditEntry::player_action(request.action, request.actor, request.details);
        self.log.lock().unwrap().log(entry);
        Ok(())
    }

    /// Log a system action to the audit log.
    #[rpc(persisted)]
    pub async fn log_system_action(
        &self,
        request: LogSystemActionRequest,
    ) -> Result<(), ClusterError> {
        let entry = AuditEntry::system_action(request.action, request.details);
        self.log.lock().unwrap().log(entry);
        Ok(())
    }

    /// Get recent audit log entries.
    #[rpc]
    pub async fn get_audit_log(
        &self,
        request: GetAuditLogRequest,
    ) -> Result<GetAuditLogResponse, ClusterError> {
        let log = self.log.lock().unwrap();
        let limit = request.limit.unwrap_or(50);
        Ok(GetAuditLogResponse {
            entries: log.get_recent(limit),
            total_entries: log.len(),
        })
    }

    /// Get all audit entries for a specific player.
    #[rpc]
    pub async fn get_audit_log_by_actor(
        &self,
        actor: PlayerId,
    ) -> Result<Vec<AuditEntry>, ClusterError> {
        let log = self.log.lock().unwrap();
        Ok(log.entries_by_actor(actor))
    }

    /// Get all audit entries for a specific action type.
    #[rpc]
    pub async fn get_audit_log_by_action(
        &self,
        action: String,
    ) -> Result<Vec<AuditEntry>, ClusterError> {
        let log = self.log.lock().unwrap();
        Ok(log.entries_by_action(&action))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_audit_entry_creation() {
        let player = PlayerId::new();
        let entry = AuditEntry::player_action("move", player, json!({"uci": "e2e4"}));

        assert_eq!(entry.action, "move");
        assert_eq!(entry.actor, Some(player));
        assert_eq!(entry.details["uci"], "e2e4");
    }

    #[test]
    fn test_audit_entry_system_action() {
        let entry = AuditEntry::system_action("timeout", json!({"color": "white"}));

        assert_eq!(entry.action, "timeout");
        assert_eq!(entry.actor, None);
        assert_eq!(entry.details["color"], "white");
    }

    #[test]
    fn test_audit_log_basic_operations() {
        let mut log = AuditLog::new();
        let player = PlayerId::new();

        assert!(log.is_empty());
        assert_eq!(log.len(), 0);

        log.log(AuditEntry::player_action("action1", player, json!({})));
        log.log(AuditEntry::player_action("action2", player, json!({})));
        log.log(AuditEntry::system_action("action3", json!({})));

        assert!(!log.is_empty());
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn test_audit_log_get_recent() {
        let mut log = AuditLog::new();
        let player = PlayerId::new();

        log.log(AuditEntry::player_action("first", player, json!({})));
        log.log(AuditEntry::player_action("second", player, json!({})));
        log.log(AuditEntry::player_action("third", player, json!({})));

        let recent = log.get_recent(2);
        assert_eq!(recent.len(), 2);
        // Most recent first
        assert_eq!(recent[0].action, "third");
        assert_eq!(recent[1].action, "second");
    }

    #[test]
    fn test_audit_log_bounded_size() {
        let mut log = AuditLog::with_max_entries(3);
        let player = PlayerId::new();

        log.log(AuditEntry::player_action("a1", player, json!({})));
        log.log(AuditEntry::player_action("a2", player, json!({})));
        log.log(AuditEntry::player_action("a3", player, json!({})));
        assert_eq!(log.len(), 3);

        // Adding a fourth should evict the first
        log.log(AuditEntry::player_action("a4", player, json!({})));
        assert_eq!(log.len(), 3);

        let all = log.get_all();
        assert_eq!(all[0].action, "a2"); // a1 was evicted
        assert_eq!(all[1].action, "a3");
        assert_eq!(all[2].action, "a4");
    }

    #[test]
    fn test_audit_log_entries_by_actor() {
        let mut log = AuditLog::new();
        let player1 = PlayerId::new();
        let player2 = PlayerId::new();

        log.log(AuditEntry::player_action("action1", player1, json!({})));
        log.log(AuditEntry::player_action("action2", player2, json!({})));
        log.log(AuditEntry::player_action("action3", player1, json!({})));
        log.log(AuditEntry::system_action("system", json!({})));

        let p1_entries = log.entries_by_actor(player1);
        assert_eq!(p1_entries.len(), 2);
        assert!(p1_entries.iter().all(|e| e.actor == Some(player1)));

        let p2_entries = log.entries_by_actor(player2);
        assert_eq!(p2_entries.len(), 1);
    }

    #[test]
    fn test_audit_log_entries_by_action() {
        let mut log = AuditLog::new();
        let player = PlayerId::new();

        log.log(AuditEntry::player_action("move", player, json!({})));
        log.log(AuditEntry::player_action("resign", player, json!({})));
        log.log(AuditEntry::player_action("move", player, json!({})));

        let move_entries = log.entries_by_action("move");
        assert_eq!(move_entries.len(), 2);

        let resign_entries = log.entries_by_action("resign");
        assert_eq!(resign_entries.len(), 1);
    }

    #[test]
    fn test_audit_log_serialization() {
        let mut log = AuditLog::with_max_entries(10);
        let player = PlayerId::new();

        log.log(AuditEntry::player_action(
            "test",
            player,
            json!({"key": "value"}),
        ));

        let json = serde_json::to_string(&log).unwrap();
        let parsed: AuditLog = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed.max_entries, 10);
        assert_eq!(parsed.get_recent(1)[0].action, "test");
    }

    #[test]
    fn test_audit_entry_serialization() {
        let player = PlayerId::new();
        let entry = AuditEntry::player_action("move", player, json!({"uci": "e2e4", "san": "e4"}));

        let json = serde_json::to_string(&entry).unwrap();
        let parsed: AuditEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.action, "move");
        assert_eq!(parsed.actor, Some(player));
        assert_eq!(parsed.details["uci"], "e2e4");
    }

    #[test]
    fn test_audit_log_clear() {
        let mut log = AuditLog::new();
        let player = PlayerId::new();

        log.log(AuditEntry::player_action("action1", player, json!({})));
        log.log(AuditEntry::player_action("action2", player, json!({})));
        assert_eq!(log.len(), 2);

        log.clear();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_get_audit_log_request_default() {
        let req = GetAuditLogRequest::default();
        assert_eq!(req.limit, Some(50));
    }

    #[test]
    fn test_get_audit_log_response_serialization() {
        let player = PlayerId::new();
        let entries = vec![AuditEntry::player_action("test", player, json!({}))];

        let response = GetAuditLogResponse {
            entries,
            total_entries: 1,
        };

        let json = serde_json::to_string(&response).unwrap();
        let parsed: GetAuditLogResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.total_entries, 1);
    }

    #[test]
    fn test_auditable_default() {
        let auditable = Auditable::default();
        assert_eq!(auditable.max_entries, AuditLog::DEFAULT_MAX_ENTRIES);
    }

    #[test]
    fn test_auditable_with_max_entries() {
        let auditable = Auditable::with_max_entries(50);
        assert_eq!(auditable.max_entries, 50);
    }
}
