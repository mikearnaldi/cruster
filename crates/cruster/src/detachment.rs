//! Detachment state management for shard ownership.
//!
//! When storage connectivity is lost or uncertain, the runner must stop executing
//! shard entities immediately to prevent split-brain (multiple runners executing
//! the same shard concurrently). This module provides the [`DetachedState`] type
//! that tracks whether the runner is currently detached from the cluster.
//!
//! See `specs/shard-assignment.md` for the full design.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Reason for detachment from the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetachmentReason {
    /// Storage connectivity error during shard operations.
    StorageError(String),
    /// Keep-alive failure streak exceeded threshold.
    KeepAliveFailure { consecutive_failures: u32 },
    /// Lease expired or could not be confirmed healthy.
    LeaseExpired,
    /// Manual detachment (e.g., for testing or maintenance).
    Manual,
}

impl fmt::Display for DetachmentReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageError(msg) => write!(f, "storage error: {}", msg),
            Self::KeepAliveFailure {
                consecutive_failures,
            } => write!(
                f,
                "keep-alive failure streak: {} consecutive failures",
                consecutive_failures
            ),
            Self::LeaseExpired => write!(f, "lease expired"),
            Self::Manual => write!(f, "manual detachment"),
        }
    }
}

/// Shared state tracking whether the runner is detached from the cluster.
///
/// When detached:
/// - `owned_shards` should be cleared
/// - All shard entities should be interrupted
/// - Shard acquisition and refresh loops should pause
/// - The runner is treated as having zero shards until re-attached
///
/// This struct is designed to be shared across multiple tokio tasks via `Arc`.
/// All operations are thread-safe and lock-free for the hot path (`is_detached`).
pub struct DetachedState {
    /// Whether the runner is currently detached.
    detached: AtomicBool,

    /// Unix timestamp (millis) when detachment occurred.
    detached_at_ms: AtomicU64,

    /// Unix timestamp (millis) when healthy status was first observed after detachment.
    /// Used to implement the recovery window (must be healthy for N ms before re-attaching).
    healthy_since_ms: AtomicU64,

    /// Current detachment reason (protected by RwLock for rare writes).
    reason: RwLock<Option<DetachmentReason>>,

    /// Monotonic instant when detachment occurred, for duration calculations.
    /// Protected by RwLock since it's only accessed during state transitions.
    detached_instant: RwLock<Option<Instant>>,

    /// Duration of sustained healthy status required before re-attaching.
    recover_window: Duration,
}

impl DetachedState {
    /// Create a new detached state tracker.
    ///
    /// # Arguments
    ///
    /// * `recover_window` - Duration of sustained healthy status required before
    ///   the runner can re-attach to the cluster.
    pub fn new(recover_window: Duration) -> Self {
        Self {
            detached: AtomicBool::new(false),
            detached_at_ms: AtomicU64::new(0),
            healthy_since_ms: AtomicU64::new(0),
            reason: RwLock::new(None),
            detached_instant: RwLock::new(None),
            recover_window,
        }
    }

    /// Check if the runner is currently detached.
    ///
    /// This is the hot path - called on every storage poll and message dispatch.
    /// Uses relaxed ordering since exact timing isn't critical; we just need
    /// eventual consistency within a few microseconds.
    #[inline]
    pub fn is_detached(&self) -> bool {
        self.detached.load(Ordering::Relaxed)
    }

    /// Mark the runner as detached with the given reason.
    ///
    /// This clears any healthy-since timestamp and records the detachment time.
    /// If already detached, this updates the reason but doesn't change timestamps.
    ///
    /// Returns `true` if this call caused the transition from attached to detached.
    pub fn detach(&self, reason: DetachmentReason) -> bool {
        let was_detached = self.detached.swap(true, Ordering::SeqCst);

        if !was_detached {
            // First detachment - record timestamps
            let now_ms = current_time_ms();
            self.detached_at_ms.store(now_ms, Ordering::Release);
            self.healthy_since_ms.store(0, Ordering::Release);

            if let Ok(mut instant) = self.detached_instant.write() {
                *instant = Some(Instant::now());
            }

            tracing::warn!(
                reason = %reason,
                "runner detached from cluster"
            );
        } else {
            tracing::debug!(
                reason = %reason,
                "detachment reason updated (already detached)"
            );
        }

        // Always update reason
        if let Ok(mut r) = self.reason.write() {
            *r = Some(reason);
        }

        !was_detached
    }

    /// Attempt to re-attach to the cluster.
    ///
    /// This should be called when a healthy signal is observed (e.g., successful
    /// storage call or keep-alive). Re-attachment only occurs if the runner has
    /// been continuously healthy for the `recover_window` duration.
    ///
    /// Returns `true` if this call caused the transition from detached to attached.
    pub fn maybe_reattach(&self) -> bool {
        if !self.detached.load(Ordering::SeqCst) {
            // Not detached, nothing to do
            return false;
        }

        let now_ms = current_time_ms();
        let healthy_since = self.healthy_since_ms.load(Ordering::Acquire);

        if healthy_since == 0 {
            // First healthy signal since detachment - start the recovery window
            self.healthy_since_ms.store(now_ms, Ordering::Release);
            tracing::info!(
                recover_window_ms = self.recover_window.as_millis() as u64,
                "healthy signal observed while detached, starting recovery window"
            );
            return false;
        }

        // Check if we've been healthy long enough
        let healthy_duration_ms = now_ms.saturating_sub(healthy_since);
        let recover_window_ms = self.recover_window.as_millis() as u64;

        if healthy_duration_ms >= recover_window_ms {
            // Recovery window elapsed - re-attach
            self.detached.store(false, Ordering::SeqCst);
            self.healthy_since_ms.store(0, Ordering::Release);

            if let Ok(mut instant) = self.detached_instant.write() {
                *instant = None;
            }
            if let Ok(mut r) = self.reason.write() {
                let old_reason = r.take();
                tracing::info!(
                    previous_reason = ?old_reason,
                    healthy_duration_ms,
                    "runner re-attached to cluster"
                );
            }

            return true;
        }

        tracing::debug!(
            healthy_duration_ms,
            recover_window_ms,
            remaining_ms = recover_window_ms.saturating_sub(healthy_duration_ms),
            "healthy but recovery window not elapsed yet"
        );
        false
    }

    /// Reset the healthy-since timestamp.
    ///
    /// Call this when an unhealthy signal is observed while detached. This resets
    /// the recovery window, requiring another full `recover_window` of healthy
    /// signals before re-attachment can occur.
    pub fn reset_healthy_since(&self) {
        if self.healthy_since_ms.swap(0, Ordering::Release) != 0 {
            tracing::debug!("recovery window reset due to unhealthy signal");
        }
    }

    /// Get the current detachment reason, if detached.
    pub fn reason(&self) -> Option<DetachmentReason> {
        if !self.is_detached() {
            return None;
        }
        self.reason.read().ok().and_then(|r| r.clone())
    }

    /// Get the duration since detachment, if currently detached.
    pub fn detached_duration(&self) -> Option<Duration> {
        if !self.is_detached() {
            return None;
        }
        self.detached_instant
            .read()
            .ok()
            .and_then(|opt| opt.map(|instant| instant.elapsed()))
    }
}

impl Default for DetachedState {
    fn default() -> Self {
        // Default to 500ms recovery window
        Self::new(Duration::from_millis(500))
    }
}

/// Get current Unix time in milliseconds.
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initially_not_detached() {
        let state = DetachedState::new(Duration::from_millis(100));
        assert!(!state.is_detached());
        assert!(state.reason().is_none());
        assert!(state.detached_duration().is_none());
    }

    #[test]
    fn detach_sets_state() {
        let state = DetachedState::new(Duration::from_millis(100));

        let transitioned = state.detach(DetachmentReason::Manual);
        assert!(transitioned);
        assert!(state.is_detached());
        assert_eq!(state.reason(), Some(DetachmentReason::Manual));
        assert!(state.detached_duration().is_some());
    }

    #[test]
    fn double_detach_returns_false() {
        let state = DetachedState::new(Duration::from_millis(100));

        assert!(state.detach(DetachmentReason::Manual));
        assert!(!state.detach(DetachmentReason::LeaseExpired));
        // Reason should be updated
        assert_eq!(state.reason(), Some(DetachmentReason::LeaseExpired));
    }

    #[test]
    fn reattach_requires_recovery_window() {
        let state = DetachedState::new(Duration::from_millis(50));
        state.detach(DetachmentReason::Manual);

        // First healthy signal starts the window
        assert!(!state.maybe_reattach());
        assert!(state.is_detached());

        // Too soon - should still be detached
        assert!(!state.maybe_reattach());
        assert!(state.is_detached());

        // Wait for recovery window
        std::thread::sleep(Duration::from_millis(60));

        // Now should re-attach
        assert!(state.maybe_reattach());
        assert!(!state.is_detached());
    }

    #[test]
    fn unhealthy_signal_resets_recovery() {
        let state = DetachedState::new(Duration::from_millis(100));
        state.detach(DetachmentReason::Manual);

        // Start recovery
        state.maybe_reattach();

        // Unhealthy signal resets it
        state.reset_healthy_since();

        // Wait should not help since we reset
        std::thread::sleep(Duration::from_millis(50));

        // Still detached because recovery was reset
        assert!(!state.maybe_reattach());
        assert!(state.is_detached());
    }

    #[test]
    fn storage_error_reason_display() {
        let reason = DetachmentReason::StorageError("connection timeout".to_string());
        assert_eq!(reason.to_string(), "storage error: connection timeout");
    }

    #[test]
    fn keepalive_failure_reason_display() {
        let reason = DetachmentReason::KeepAliveFailure {
            consecutive_failures: 5,
        };
        assert_eq!(
            reason.to_string(),
            "keep-alive failure streak: 5 consecutive failures"
        );
    }
}
