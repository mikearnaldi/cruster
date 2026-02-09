//! MatchmakingService entity - pure-RPC service for pairing players into games.
//!
//! This entity maintains an ephemeral in-memory queue of players waiting for matches.
//! When two compatible players are found, it returns a match result. The queue is not
//! persisted — on restart, players need to re-queue.
//!
//! Uses the new pure-RPC entity pattern: no framework-managed state, no workflows,
//! no activities. State is held directly via `Arc<Mutex<MatchmakingState>>`.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

use crate::types::game::{GameId, TimeControl};
use crate::types::player::PlayerId;

/// Matchmaking preferences for a player.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MatchPreferences {
    /// Desired time control.
    pub time_control: TimeControl,
    /// Minimum opponent rating (if any).
    pub min_rating: Option<i32>,
    /// Maximum opponent rating (if any).
    pub max_rating: Option<i32>,
}

impl Default for MatchPreferences {
    fn default() -> Self {
        Self {
            time_control: TimeControl::BLITZ,
            min_rating: None,
            max_rating: None,
        }
    }
}

/// A player waiting in the matchmaking queue.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuedPlayer {
    /// Player ID.
    pub player_id: PlayerId,
    /// Match preferences.
    pub preferences: MatchPreferences,
    /// When the player joined the queue.
    pub queued_at: DateTime<Utc>,
    /// Player's current rating (for matchmaking).
    pub rating: i32,
}

impl QueuedPlayer {
    /// Create a new queued player entry.
    #[must_use]
    pub fn new(player_id: PlayerId, preferences: MatchPreferences, rating: i32) -> Self {
        Self {
            player_id,
            preferences,
            queued_at: Utc::now(),
            rating,
        }
    }

    /// Check if this player is compatible with another for matchmaking.
    #[must_use]
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        // Must have matching time control
        if self.preferences.time_control != other.preferences.time_control {
            return false;
        }

        // Check rating constraints (each player's constraints on their opponent)
        if !self.rating_acceptable_to(other) || !other.rating_acceptable_to(self) {
            return false;
        }

        true
    }

    /// Check if this player's rating is acceptable to the other player.
    fn rating_acceptable_to(&self, other: &Self) -> bool {
        if let Some(min) = other.preferences.min_rating {
            if self.rating < min {
                return false;
            }
        }
        if let Some(max) = other.preferences.max_rating {
            if self.rating > max {
                return false;
            }
        }
        true
    }
}

/// State for the matchmaking service.
///
/// This is ephemeral state - not persisted. On restart, players need to re-queue.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MatchmakingState {
    /// Players waiting to be matched.
    pub queue: Vec<QueuedPlayer>,
    /// Total matches made by this service instance.
    pub matches_made: u64,
}

/// MatchmakingService pairs players into games.
///
/// ## Pure-RPC Design
/// This entity maintains ephemeral queue state via `Arc<Mutex<MatchmakingState>>`.
/// State is not persisted by the framework — on restart, players need to re-enter
/// the queue via their PlayerSession.
///
/// ## RPCs
/// - `find_match(request)` — Add to queue, try to find match (persisted for at-least-once)
/// - `cancel_search(request)` — Remove from queue (persisted for at-least-once)
/// - `get_queue_status()` — Return queue statistics
/// - `is_in_queue(player_id)` — Check if player is queued
/// - `get_queue_position(player_id)` — Get player's queue position
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct MatchmakingService {
    /// In-memory ephemeral state, shared across all RPC calls for this entity instance.
    state: Arc<Mutex<MatchmakingState>>,
}

impl MatchmakingService {
    /// Create a new MatchmakingService with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MatchmakingState::default())),
        }
    }
}

impl Default for MatchmakingService {
    fn default() -> Self {
        Self::new()
    }
}

/// Request to find a match.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FindMatchRequest {
    /// Player seeking a match.
    pub player_id: PlayerId,
    /// Match preferences.
    pub preferences: MatchPreferences,
    /// Player's current rating.
    pub rating: i32,
}

/// Response from find_match.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FindMatchResponse {
    /// Immediately matched with an opponent.
    Matched {
        /// The created game ID.
        game_id: GameId,
        /// The opponent's player ID.
        opponent_id: PlayerId,
        /// Whether this player is white (true) or black (false).
        is_white: bool,
    },
    /// Added to queue, waiting for opponent.
    Queued {
        /// Position in queue.
        position: u32,
        /// Estimated wait time in seconds (rough estimate).
        estimated_wait_secs: Option<u32>,
    },
}

/// Request to cancel matchmaking search.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelSearchRequest {
    /// Player canceling the search.
    pub player_id: PlayerId,
}

/// Queue statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueStatus {
    /// Number of players currently in queue.
    pub players_in_queue: u32,
    /// Number of matches made (since service start).
    pub total_matches_made: u64,
    /// Queue breakdown by time control.
    pub by_time_control: Vec<TimeControlQueueInfo>,
}

/// Queue info for a specific time control.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeControlQueueInfo {
    /// The time control.
    pub time_control: TimeControl,
    /// Number of players waiting for this time control.
    pub count: u32,
}

/// Error types specific to matchmaking operations.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum MatchmakingError {
    /// Player is already in the queue.
    #[error("player is already in the matchmaking queue")]
    AlreadyInQueue,
    /// Player is not in the queue.
    #[error("player is not in the matchmaking queue")]
    NotInQueue,
    /// Cannot match player with themselves.
    #[error("cannot match player with themselves")]
    SelfMatch,
}

impl From<MatchmakingError> for ClusterError {
    fn from(err: MatchmakingError) -> Self {
        ClusterError::MalformedMessage {
            reason: err.to_string(),
            source: None,
        }
    }
}

#[entity_impl]
impl MatchmakingService {
    /// Find a match for a player.
    ///
    /// If a compatible opponent is already in the queue, returns a match immediately.
    /// Otherwise, adds the player to the queue and returns a queued status.
    #[rpc(persisted)]
    pub async fn find_match(
        &self,
        request: FindMatchRequest,
    ) -> Result<FindMatchResponse, ClusterError> {
        let mut state = self.state.lock().unwrap();

        // Validation: check if player is already in queue
        if state.queue.iter().any(|p| p.player_id == request.player_id) {
            return Err(MatchmakingError::AlreadyInQueue.into());
        }

        let new_player = QueuedPlayer::new(request.player_id, request.preferences, request.rating);

        // Check for compatible opponent
        let opponent_idx = find_compatible_opponent(&state.queue, &new_player);

        if let Some(idx) = opponent_idx {
            // Match found — remove opponent from queue and return match result
            let opponent = state.queue.remove(idx);
            let game_id = GameId::new();
            let new_player_is_white = new_player.queued_at > opponent.queued_at;
            state.matches_made += 1;

            Ok(FindMatchResponse::Matched {
                game_id,
                opponent_id: opponent.player_id,
                is_white: new_player_is_white,
            })
        } else {
            // No match — add to queue
            let position = state.queue.len() as u32 + 1;
            state.queue.push(new_player);
            let estimated_wait_secs = Some(position * 30);

            Ok(FindMatchResponse::Queued {
                position,
                estimated_wait_secs,
            })
        }
    }

    /// Cancel a matchmaking search.
    #[rpc(persisted)]
    pub async fn cancel_search(&self, request: CancelSearchRequest) -> Result<(), ClusterError> {
        let mut state = self.state.lock().unwrap();

        let in_queue = state.queue.iter().any(|p| p.player_id == request.player_id);
        if !in_queue {
            return Err(MatchmakingError::NotInQueue.into());
        }

        state.queue.retain(|p| p.player_id != request.player_id);
        Ok(())
    }

    /// Get the current queue status.
    #[rpc]
    pub async fn get_queue_status(&self) -> Result<QueueStatus, ClusterError> {
        let state = self.state.lock().unwrap();

        // Count players by time control
        let mut by_time_control: Vec<TimeControlQueueInfo> = Vec::new();

        for player in &state.queue {
            if let Some(info) = by_time_control
                .iter_mut()
                .find(|i| i.time_control == player.preferences.time_control)
            {
                info.count += 1;
            } else {
                by_time_control.push(TimeControlQueueInfo {
                    time_control: player.preferences.time_control,
                    count: 1,
                });
            }
        }

        Ok(QueueStatus {
            players_in_queue: state.queue.len() as u32,
            total_matches_made: state.matches_made,
            by_time_control,
        })
    }

    /// Check if a player is currently in the queue.
    #[rpc]
    pub async fn is_in_queue(&self, player_id: PlayerId) -> Result<bool, ClusterError> {
        let state = self.state.lock().unwrap();
        Ok(state.queue.iter().any(|p| p.player_id == player_id))
    }

    /// Get a player's position in the queue.
    #[rpc]
    pub async fn get_queue_position(
        &self,
        player_id: PlayerId,
    ) -> Result<Option<u32>, ClusterError> {
        let state = self.state.lock().unwrap();
        Ok(state
            .queue
            .iter()
            .position(|p| p.player_id == player_id)
            .map(|pos| pos as u32 + 1))
    }
}

/// Find a compatible opponent for the given player in the queue.
fn find_compatible_opponent(queue: &[QueuedPlayer], player: &QueuedPlayer) -> Option<usize> {
    queue
        .iter()
        .position(|opponent| player.is_compatible_with(opponent))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queued_player_compatibility() {
        let p1 = QueuedPlayer::new(
            PlayerId::new(),
            MatchPreferences {
                time_control: TimeControl::BLITZ,
                min_rating: Some(1100),
                max_rating: Some(1300),
            },
            1200,
        );

        let p2 = QueuedPlayer::new(
            PlayerId::new(),
            MatchPreferences {
                time_control: TimeControl::BLITZ,
                min_rating: None,
                max_rating: None,
            },
            1200,
        );

        // Compatible - same time control, p2 has no rating constraints
        assert!(p1.is_compatible_with(&p2));

        // Different time control
        let p3 = QueuedPlayer::new(
            PlayerId::new(),
            MatchPreferences {
                time_control: TimeControl::RAPID,
                min_rating: None,
                max_rating: None,
            },
            1200,
        );
        assert!(!p1.is_compatible_with(&p3));

        // Rating out of range
        let p4 = QueuedPlayer::new(
            PlayerId::new(),
            MatchPreferences {
                time_control: TimeControl::BLITZ,
                min_rating: None,
                max_rating: None,
            },
            1400, // Outside p1's max_rating
        );
        assert!(!p1.is_compatible_with(&p4));
    }

    #[test]
    fn test_match_preferences_default() {
        let prefs = MatchPreferences::default();
        assert_eq!(prefs.time_control, TimeControl::BLITZ);
        assert!(prefs.min_rating.is_none());
        assert!(prefs.max_rating.is_none());
    }

    #[test]
    fn test_queue_status_serialization() {
        let status = QueueStatus {
            players_in_queue: 5,
            total_matches_made: 42,
            by_time_control: vec![
                TimeControlQueueInfo {
                    time_control: TimeControl::BLITZ,
                    count: 3,
                },
                TimeControlQueueInfo {
                    time_control: TimeControl::RAPID,
                    count: 2,
                },
            ],
        };

        let json = serde_json::to_string(&status).unwrap();
        let parsed: QueueStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.players_in_queue, 5);
        assert_eq!(parsed.total_matches_made, 42);
        assert_eq!(parsed.by_time_control.len(), 2);
    }

    #[test]
    fn test_matchmaking_error_display() {
        let err = MatchmakingError::AlreadyInQueue;
        assert!(err.to_string().contains("already in"));

        let err = MatchmakingError::NotInQueue;
        assert!(err.to_string().contains("not in"));
    }

    #[test]
    fn test_find_match_response_serialization() {
        let response = FindMatchResponse::Matched {
            game_id: GameId::new(),
            opponent_id: PlayerId::new(),
            is_white: true,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("Matched"));

        let response = FindMatchResponse::Queued {
            position: 3,
            estimated_wait_secs: Some(90),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("Queued"));
    }
}
