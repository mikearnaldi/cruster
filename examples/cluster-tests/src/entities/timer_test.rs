//! TimerTest entity - entity for testing timers and scheduled execution.
//!
//! This entity provides timer operations to test:
//! - Timer fires after delay
//! - Timer can be cancelled
//! - Timer survives runner restart
//! - Multiple timers handled correctly

use chrono::{DateTime, Utc};
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Record of a timer that has fired.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerFire {
    /// Unique identifier for the timer.
    pub timer_id: String,
    /// When the timer was scheduled.
    pub scheduled_at: DateTime<Utc>,
    /// When the timer actually fired.
    pub fired_at: DateTime<Utc>,
}

/// Record of a pending timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingTimer {
    /// Unique identifier for the timer.
    pub timer_id: String,
    /// When the timer was scheduled.
    pub scheduled_at: DateTime<Utc>,
    /// Delay in milliseconds.
    pub delay_ms: u64,
    /// Whether the timer was cancelled.
    pub cancelled: bool,
}

/// State for a TimerTest entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TimerTestState {
    /// History of fired timers.
    pub timer_fires: Vec<TimerFire>,
    /// Currently pending timers.
    pub pending_timers: Vec<PendingTimer>,
}

/// TimerTest entity for testing timer/scheduling functionality.
///
/// ## State (Persisted)
/// - timer_fires: `Vec<TimerFire>` - history of fired timers
/// - pending_timers: `Vec<PendingTimer>` - currently scheduled timers
///
/// ## RPCs
/// - `schedule_timer(timer_id, delay_ms)` - Schedule a timer
/// - `cancel_timer(timer_id)` - Cancel a pending timer
/// - `get_timer_fires()` - Get fired timer history
/// - `clear_fires()` - Clear fire history
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct TimerTest;

/// Request to schedule a timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduleTimerRequest {
    /// Unique timer identifier.
    pub timer_id: String,
    /// Delay in milliseconds.
    pub delay_ms: u64,
}

/// Request to cancel a timer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelTimerRequest {
    /// Timer ID to cancel.
    pub timer_id: String,
}

#[entity_impl]
#[state(TimerTestState)]
impl TimerTest {
    fn init(&self, _ctx: &EntityContext) -> Result<TimerTestState, ClusterError> {
        Ok(TimerTestState::default())
    }

    #[activity]
    async fn add_pending_timer(
        &mut self,
        timer_id: String,
        scheduled_at: DateTime<Utc>,
        delay_ms: u64,
    ) -> Result<(), ClusterError> {
        self.state.pending_timers.push(PendingTimer {
            timer_id,
            scheduled_at,
            delay_ms,
            cancelled: false,
        });
        Ok(())
    }

    #[activity]
    async fn record_timer_fire(
        &mut self,
        timer_id: String,
        scheduled_at: DateTime<Utc>,
        fired_at: DateTime<Utc>,
    ) -> Result<(), ClusterError> {
        self.state.timer_fires.push(TimerFire {
            timer_id: timer_id.clone(),
            scheduled_at,
            fired_at,
        });
        self.state.pending_timers.retain(|t| t.timer_id != timer_id);
        Ok(())
    }

    #[activity]
    async fn remove_pending_timer(&mut self, timer_id: String) -> Result<(), ClusterError> {
        self.state.pending_timers.retain(|t| t.timer_id != timer_id);
        Ok(())
    }

    #[activity]
    async fn do_cancel_timer(&mut self, timer_id: String) -> Result<bool, ClusterError> {
        if let Some(timer) = self
            .state
            .pending_timers
            .iter_mut()
            .find(|t| t.timer_id == timer_id && !t.cancelled)
        {
            timer.cancelled = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[activity]
    async fn do_clear_fires(&mut self) -> Result<(), ClusterError> {
        self.state.timer_fires.clear();
        Ok(())
    }

    /// Schedule a timer that will fire after the specified delay.
    ///
    /// This is implemented as a workflow that sleeps for the delay duration
    /// and then records the timer fire. The workflow is spawned and returns
    /// immediately.
    #[workflow]
    pub async fn schedule_timer(&self, request: ScheduleTimerRequest) -> Result<(), ClusterError> {
        let timer_id = request.timer_id;
        let delay_ms = request.delay_ms;
        let scheduled_at = Utc::now();

        // Record the pending timer
        self.add_pending_timer(timer_id.clone(), scheduled_at, delay_ms)
            .await?;

        // Use durable sleep with the timer_id as the unique name
        self.sleep(&timer_id, Duration::from_millis(delay_ms))
            .await?;

        // After sleep completes, check if timer was cancelled
        let was_cancelled = self
            .state
            .pending_timers
            .iter()
            .find(|t| t.timer_id == timer_id)
            .is_some_and(|t| t.cancelled);

        // If not cancelled, record the fire
        if !was_cancelled {
            let fired_at = Utc::now();
            self.record_timer_fire(timer_id, scheduled_at, fired_at)
                .await?;
        } else {
            // Remove cancelled timer from pending
            self.remove_pending_timer(timer_id).await?;
        }

        Ok(())
    }

    /// Cancel a pending timer.
    ///
    /// Returns true if the timer was found and cancelled, false if not found
    /// or already fired.
    #[workflow]
    pub async fn cancel_timer(&self, request: CancelTimerRequest) -> Result<bool, ClusterError> {
        self.do_cancel_timer(request.timer_id).await
    }

    /// Get the list of all fired timers.
    #[rpc]
    pub async fn get_timer_fires(&self) -> Result<Vec<TimerFire>, ClusterError> {
        Ok(self.state.timer_fires.clone())
    }

    /// Clear the timer fire history.
    #[workflow]
    pub async fn clear_fires(&self) -> Result<(), ClusterError> {
        self.do_clear_fires().await
    }

    /// Get the list of pending timers.
    #[rpc]
    pub async fn get_pending_timers(&self) -> Result<Vec<PendingTimer>, ClusterError> {
        Ok(self.state.pending_timers.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer_fire_serialization() {
        let fire = TimerFire {
            timer_id: "timer-1".to_string(),
            scheduled_at: Utc::now(),
            fired_at: Utc::now(),
        };

        let json = serde_json::to_string(&fire).unwrap();
        let parsed: TimerFire = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.timer_id, "timer-1");
    }

    #[test]
    fn test_pending_timer_serialization() {
        let pending = PendingTimer {
            timer_id: "timer-1".to_string(),
            scheduled_at: Utc::now(),
            delay_ms: 5000,
            cancelled: false,
        };

        let json = serde_json::to_string(&pending).unwrap();
        let parsed: PendingTimer = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.timer_id, "timer-1");
        assert_eq!(parsed.delay_ms, 5000);
        assert!(!parsed.cancelled);
    }

    #[test]
    fn test_timer_test_state_default() {
        let state = TimerTestState::default();
        assert!(state.timer_fires.is_empty());
        assert!(state.pending_timers.is_empty());
    }

    #[test]
    fn test_timer_test_state_serialization() {
        let mut state = TimerTestState::default();
        state.timer_fires.push(TimerFire {
            timer_id: "t1".to_string(),
            scheduled_at: Utc::now(),
            fired_at: Utc::now(),
        });
        state.pending_timers.push(PendingTimer {
            timer_id: "t2".to_string(),
            scheduled_at: Utc::now(),
            delay_ms: 1000,
            cancelled: false,
        });

        let json = serde_json::to_string(&state).unwrap();
        let parsed: TimerTestState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.timer_fires.len(), 1);
        assert_eq!(parsed.timer_fires[0].timer_id, "t1");
        assert_eq!(parsed.pending_timers.len(), 1);
        assert_eq!(parsed.pending_timers[0].timer_id, "t2");
    }
}
