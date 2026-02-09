//! Leaderboard entity - singleton for global player rankings.
//!
//! This entity maintains global ELO ratings and leaderboard rankings.
//! It is a singleton - exactly one instance exists across the cluster.
//!
//! Uses the pure-RPC entity pattern: no framework-managed state, no workflows,
//! no activities. State is held directly via `Arc<Mutex<LeaderboardState>>`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

use crate::types::game::{GameResultInfo, GameStatus};
use crate::types::player::{PlayerId, PlayerRating};

/// A ranked player entry for the leaderboard.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RankedPlayer {
    /// Player ID.
    pub player_id: PlayerId,
    /// Player's rating.
    pub rating: PlayerRating,
    /// Current rank (1-indexed).
    pub rank: u32,
}

/// State for the leaderboard singleton.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderboardState {
    /// Ratings for all players.
    pub ratings: HashMap<PlayerId, PlayerRating>,
    /// Cached top 100 players (sorted by ELO descending).
    pub top_players: Vec<RankedPlayer>,
    /// Total games played.
    pub total_games_played: u64,
    /// When the leaderboard was last updated.
    pub last_updated: DateTime<Utc>,
}

impl Default for LeaderboardState {
    fn default() -> Self {
        Self {
            ratings: HashMap::new(),
            top_players: Vec::new(),
            total_games_played: 0,
            last_updated: Utc::now(),
        }
    }
}

impl LeaderboardState {
    /// Maximum number of players to cache in top_players.
    const TOP_PLAYERS_CACHE_SIZE: usize = 100;

    /// Refresh the top_players cache from ratings.
    fn refresh_top_players_cache(&mut self) {
        let mut players: Vec<_> = self
            .ratings
            .iter()
            .map(|(id, rating)| (*id, rating.clone()))
            .collect();

        // Sort by ELO descending
        players.sort_by(|a, b| b.1.elo.cmp(&a.1.elo));

        // Take top N and assign ranks
        self.top_players = players
            .into_iter()
            .take(Self::TOP_PLAYERS_CACHE_SIZE)
            .enumerate()
            .map(|(idx, (player_id, rating))| RankedPlayer {
                player_id,
                rating,
                rank: idx as u32 + 1,
            })
            .collect();
    }

    /// Get a player's rank (1-indexed, None if not ranked).
    fn get_player_rank(&self, player_id: PlayerId) -> Option<u32> {
        // First check the cache
        if let Some(entry) = self.top_players.iter().find(|p| p.player_id == player_id) {
            return Some(entry.rank);
        }

        // If not in top N, calculate rank by counting players with higher ELO
        let player_elo = self.ratings.get(&player_id)?;
        let rank = self
            .ratings
            .values()
            .filter(|r| r.elo > player_elo.elo)
            .count() as u32
            + 1;
        Some(rank)
    }
}

/// Leaderboard singleton entity.
///
/// This is a singleton - exactly one instance exists across the cluster.
/// It maintains global ELO ratings and tracks all player rankings.
///
/// ## Pure-RPC Design
/// State is held directly via `Arc<Mutex<LeaderboardState>>`.
/// State is not persisted by the framework — it is ephemeral in-memory state.
///
/// ## RPCs (persisted, at-least-once)
/// - `record_game_result(request)` — Calculate ELO, update ratings
/// - `ensure_player_rating(player_id)` — Initialize or get a player's rating
///
/// ## RPCs (non-persisted)
/// - `get_player_rating(player_id)` — Get a player's rating
/// - `get_top_players(request)` — Get top N players
/// - `get_rankings_around(request)` — Get players near a given rank
/// - `get_total_games()` — Get total games played
/// - `get_total_players()` — Get total rated players
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct Leaderboard {
    /// In-memory ephemeral state, shared across all RPC calls for this entity instance.
    state: Arc<Mutex<LeaderboardState>>,
}

impl Leaderboard {
    /// Create a new Leaderboard with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(LeaderboardState::default())),
        }
    }
}

impl Default for Leaderboard {
    fn default() -> Self {
        Self::new()
    }
}

/// Request to record a game result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordGameResultRequest {
    /// The game result information.
    pub result: GameResultInfo,
}

/// Response from recording a game result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordGameResultResponse {
    /// White player's new rating.
    pub white_rating: PlayerRating,
    /// Black player's new rating.
    pub black_rating: PlayerRating,
    /// White player's ELO change.
    pub white_elo_change: i32,
    /// Black player's ELO change.
    pub black_elo_change: i32,
}

/// Request to get top players.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetTopPlayersRequest {
    /// Maximum number of players to return.
    pub limit: u32,
}

/// Response containing top players.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetTopPlayersResponse {
    /// List of top players.
    pub players: Vec<RankedPlayer>,
}

/// Request to get rankings around a player.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetRankingsAroundRequest {
    /// Player to center the rankings on.
    pub player_id: PlayerId,
    /// Number of players to return on each side.
    pub range: u32,
}

/// Response containing rankings around a player.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetRankingsAroundResponse {
    /// The target player's ranking.
    pub target: RankedPlayer,
    /// Players ranked higher (closer to #1).
    pub above: Vec<RankedPlayer>,
    /// Players ranked lower.
    pub below: Vec<RankedPlayer>,
}

/// Error types specific to leaderboard operations.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum LeaderboardError {
    /// Player not found in the leaderboard.
    #[error("player not found: {player_id}")]
    PlayerNotFound {
        /// The player ID that was not found.
        player_id: PlayerId,
    },
    /// Invalid game result (e.g., game was aborted).
    #[error("invalid game result: {reason}")]
    InvalidGameResult {
        /// Reason the result is invalid.
        reason: String,
    },
}

impl From<LeaderboardError> for ClusterError {
    fn from(err: LeaderboardError) -> Self {
        ClusterError::MalformedMessage {
            reason: err.to_string(),
            source: None,
        }
    }
}

/// ELO calculation constants.
mod elo {
    /// K-factor for ELO calculations.
    /// Higher values mean more volatile ratings.
    pub const K_FACTOR: f64 = 32.0;

    /// Calculate expected score for player A against player B.
    pub fn expected_score(rating_a: i32, rating_b: i32) -> f64 {
        1.0 / (1.0 + 10_f64.powf((rating_b - rating_a) as f64 / 400.0))
    }

    /// Calculate ELO change for a player.
    ///
    /// `actual_score` is 1.0 for win, 0.5 for draw, 0.0 for loss.
    pub fn rating_change(rating: i32, opponent_rating: i32, actual_score: f64) -> i32 {
        let expected = expected_score(rating, opponent_rating);
        (K_FACTOR * (actual_score - expected)).round() as i32
    }
}

#[entity_impl]
impl Leaderboard {
    /// Record a game result and update ELO ratings.
    #[rpc(persisted)]
    pub async fn record_game_result(
        &self,
        request: RecordGameResultRequest,
    ) -> Result<RecordGameResultResponse, ClusterError> {
        let result = &request.result;

        // Validation (deterministic)
        if result.status == GameStatus::Aborted {
            return Err(LeaderboardError::InvalidGameResult {
                reason: "game was aborted".to_string(),
            }
            .into());
        }

        let (white_score, black_score) = match result.status {
            GameStatus::WhiteWins => (1.0, 0.0),
            GameStatus::BlackWins => (0.0, 1.0),
            GameStatus::Draw => (0.5, 0.5),
            _ => {
                return Err(LeaderboardError::InvalidGameResult {
                    reason: format!("unexpected status: {:?}", result.status),
                }
                .into())
            }
        };

        let mut state = self.state.lock().unwrap();

        // Get or create ratings for both players
        let white_rating = state
            .ratings
            .entry(result.white_player)
            .or_default()
            .clone();
        let black_rating = state
            .ratings
            .entry(result.black_player)
            .or_default()
            .clone();

        // Calculate ELO changes
        let white_elo_change = elo::rating_change(white_rating.elo, black_rating.elo, white_score);
        let black_elo_change = elo::rating_change(black_rating.elo, white_rating.elo, black_score);

        // Update white player's rating
        let white_entry = state
            .ratings
            .get_mut(&result.white_player)
            .expect("white player entry exists");
        white_entry.elo += white_elo_change;
        if white_entry.elo > white_entry.peak_elo {
            white_entry.peak_elo = white_entry.elo;
        }
        match result.status {
            GameStatus::WhiteWins => white_entry.wins += 1,
            GameStatus::BlackWins => white_entry.losses += 1,
            GameStatus::Draw => white_entry.draws += 1,
            _ => {}
        }
        let new_white_rating = white_entry.clone();

        // Update black player's rating
        let black_entry = state
            .ratings
            .get_mut(&result.black_player)
            .expect("black player entry exists");
        black_entry.elo += black_elo_change;
        if black_entry.elo > black_entry.peak_elo {
            black_entry.peak_elo = black_entry.elo;
        }
        match result.status {
            GameStatus::BlackWins => black_entry.wins += 1,
            GameStatus::WhiteWins => black_entry.losses += 1,
            GameStatus::Draw => black_entry.draws += 1,
            _ => {}
        }
        let new_black_rating = black_entry.clone();

        // Update totals and refresh cache
        state.total_games_played += 1;
        state.last_updated = Utc::now();
        state.refresh_top_players_cache();

        Ok(RecordGameResultResponse {
            white_rating: new_white_rating,
            black_rating: new_black_rating,
            white_elo_change,
            black_elo_change,
        })
    }

    /// Get a player's rating.
    #[rpc]
    pub async fn get_player_rating(
        &self,
        player_id: PlayerId,
    ) -> Result<PlayerRating, ClusterError> {
        let state = self.state.lock().unwrap();
        state
            .ratings
            .get(&player_id)
            .cloned()
            .ok_or_else(|| LeaderboardError::PlayerNotFound { player_id }.into())
    }

    /// Get the top N players.
    #[rpc]
    pub async fn get_top_players(
        &self,
        request: GetTopPlayersRequest,
    ) -> Result<GetTopPlayersResponse, ClusterError> {
        let state = self.state.lock().unwrap();
        let players: Vec<RankedPlayer> = state
            .top_players
            .iter()
            .take(request.limit as usize)
            .cloned()
            .collect();

        Ok(GetTopPlayersResponse { players })
    }

    /// Get rankings around a specific player.
    ///
    /// Returns the target player plus `range` players above and below.
    #[rpc]
    pub async fn get_rankings_around(
        &self,
        request: GetRankingsAroundRequest,
    ) -> Result<GetRankingsAroundResponse, ClusterError> {
        let state = self.state.lock().unwrap();
        let player_id = request.player_id;

        // Get the player's rating
        let rating = state
            .ratings
            .get(&player_id)
            .cloned()
            .ok_or(LeaderboardError::PlayerNotFound { player_id })?;

        let rank = state
            .get_player_rank(player_id)
            .ok_or(LeaderboardError::PlayerNotFound { player_id })?;

        let target = RankedPlayer {
            player_id,
            rating,
            rank,
        };

        // Build a sorted list of all players
        let mut all_players: Vec<_> = state
            .ratings
            .iter()
            .map(|(id, r)| (*id, r.clone()))
            .collect();
        all_players.sort_by(|a, b| b.1.elo.cmp(&a.1.elo));

        // Find players above and below
        let target_idx = all_players
            .iter()
            .position(|(id, _)| *id == player_id)
            .unwrap_or(0);

        let above: Vec<RankedPlayer> = all_players
            .iter()
            .take(target_idx)
            .rev()
            .take(request.range as usize)
            .enumerate()
            .map(|(idx, (id, r))| RankedPlayer {
                player_id: *id,
                rating: r.clone(),
                rank: rank - idx as u32 - 1,
            })
            .collect();

        let below: Vec<RankedPlayer> = all_players
            .iter()
            .skip(target_idx + 1)
            .take(request.range as usize)
            .enumerate()
            .map(|(idx, (id, r))| RankedPlayer {
                player_id: *id,
                rating: r.clone(),
                rank: rank + idx as u32 + 1,
            })
            .collect();

        Ok(GetRankingsAroundResponse {
            target,
            above,
            below,
        })
    }

    /// Get the total number of games played.
    #[rpc]
    pub async fn get_total_games(&self) -> Result<u64, ClusterError> {
        let state = self.state.lock().unwrap();
        Ok(state.total_games_played)
    }

    /// Get the total number of players with ratings.
    #[rpc]
    pub async fn get_total_players(&self) -> Result<u32, ClusterError> {
        let state = self.state.lock().unwrap();
        Ok(state.ratings.len() as u32)
    }

    /// Initialize or get a player's rating.
    #[rpc(persisted)]
    pub async fn ensure_player_rating(
        &self,
        player_id: PlayerId,
    ) -> Result<PlayerRating, ClusterError> {
        let mut state = self.state.lock().unwrap();
        let rating = state.ratings.entry(player_id).or_default().clone();
        Ok(rating)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::game::{GameId, GameResult};

    #[test]
    fn test_elo_expected_score() {
        // Equal ratings should give 0.5 expected score
        let score = elo::expected_score(1200, 1200);
        assert!((score - 0.5).abs() < 0.001);

        // Higher rated player should have higher expected score
        let higher = elo::expected_score(1400, 1200);
        let lower = elo::expected_score(1200, 1400);
        assert!(higher > 0.5);
        assert!(lower < 0.5);
        assert!((higher + lower - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_elo_rating_change() {
        // Upset win (lower rated beats higher rated)
        let change = elo::rating_change(1200, 1400, 1.0);
        assert!(change > 20); // Should gain significant ELO

        // Expected win (higher rated beats lower rated)
        let change = elo::rating_change(1400, 1200, 1.0);
        assert!(change > 0);
        assert!(change < 20); // Should gain less ELO

        // Draw between equal players
        let change = elo::rating_change(1200, 1200, 0.5);
        assert_eq!(change, 0);
    }

    #[test]
    fn test_leaderboard_state_default() {
        let state = LeaderboardState::default();
        assert!(state.ratings.is_empty());
        assert!(state.top_players.is_empty());
        assert_eq!(state.total_games_played, 0);
    }

    #[test]
    fn test_ranked_player_serialization() {
        let player = RankedPlayer {
            player_id: PlayerId::new(),
            rating: PlayerRating::new(),
            rank: 1,
        };

        let json = serde_json::to_string(&player).unwrap();
        let parsed: RankedPlayer = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.rank, 1);
        assert_eq!(parsed.rating.elo, PlayerRating::STARTING_ELO);
    }

    #[test]
    fn test_refresh_top_players_cache() {
        let mut state = LeaderboardState::default();

        // Add some players
        let p1 = PlayerId::new();
        let p2 = PlayerId::new();
        let p3 = PlayerId::new();

        state.ratings.insert(
            p1,
            PlayerRating {
                elo: 1500,
                wins: 10,
                losses: 5,
                draws: 2,
                peak_elo: 1550,
            },
        );
        state.ratings.insert(
            p2,
            PlayerRating {
                elo: 1300,
                wins: 5,
                losses: 5,
                draws: 0,
                peak_elo: 1300,
            },
        );
        state.ratings.insert(
            p3,
            PlayerRating {
                elo: 1400,
                wins: 8,
                losses: 4,
                draws: 1,
                peak_elo: 1420,
            },
        );

        state.refresh_top_players_cache();

        assert_eq!(state.top_players.len(), 3);
        assert_eq!(state.top_players[0].player_id, p1); // Highest ELO
        assert_eq!(state.top_players[0].rank, 1);
        assert_eq!(state.top_players[1].player_id, p3); // Second highest
        assert_eq!(state.top_players[1].rank, 2);
        assert_eq!(state.top_players[2].player_id, p2); // Lowest
        assert_eq!(state.top_players[2].rank, 3);
    }

    #[test]
    fn test_get_player_rank() {
        let mut state = LeaderboardState::default();

        let p1 = PlayerId::new();
        let p2 = PlayerId::new();

        state.ratings.insert(
            p1,
            PlayerRating {
                elo: 1500,
                ..Default::default()
            },
        );
        state.ratings.insert(
            p2,
            PlayerRating {
                elo: 1300,
                ..Default::default()
            },
        );

        state.refresh_top_players_cache();

        assert_eq!(state.get_player_rank(p1), Some(1));
        assert_eq!(state.get_player_rank(p2), Some(2));
        assert_eq!(state.get_player_rank(PlayerId::new()), None);
    }

    #[test]
    fn test_leaderboard_error_display() {
        let err = LeaderboardError::PlayerNotFound {
            player_id: PlayerId::new(),
        };
        assert!(err.to_string().contains("not found"));

        let err = LeaderboardError::InvalidGameResult {
            reason: "test".to_string(),
        };
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn test_game_result_info_serialization() {
        let result = GameResultInfo {
            game_id: GameId::new(),
            white_player: PlayerId::new(),
            black_player: PlayerId::new(),
            status: GameStatus::WhiteWins,
            result_reason: GameResult::Checkmate,
            total_moves: 42,
        };

        let json = serde_json::to_string(&result).unwrap();
        let parsed: GameResultInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.status, GameStatus::WhiteWins);
        assert_eq!(parsed.total_moves, 42);
    }

    #[test]
    fn test_record_game_result_request_serialization() {
        let request = RecordGameResultRequest {
            result: GameResultInfo {
                game_id: GameId::new(),
                white_player: PlayerId::new(),
                black_player: PlayerId::new(),
                status: GameStatus::Draw,
                result_reason: GameResult::DrawAgreement,
                total_moves: 50,
            },
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("draw"));
    }
}
