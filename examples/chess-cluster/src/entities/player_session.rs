//! PlayerSession entity - tracks a player's active connection and current game.
//!
//! This is an in-memory entity that maintains state only while the player is connected.
//! State is lost on restart, which is appropriate since it tracks ephemeral connection state.
//!
//! Uses the pure-RPC entity pattern: no framework-managed state, no workflows,
//! no activities. State is held directly via `Arc<Mutex<HashMap<...>>>`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

use crate::types::chess::Color;
use crate::types::game::GameId;
use crate::types::player::{PlayerId, PlayerInfo, PlayerStatus};

/// State for a player session entity.
///
/// This is in-memory only - when the server restarts, sessions are lost.
/// Players need to reconnect to establish a new session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlayerSessionState {
    /// The player ID (derived from entity ID at init time).
    pub player_id: PlayerId,
    /// Player information (set after connect).
    pub info: Option<PlayerInfo>,
}

/// PlayerSession entity tracks a connected player's state.
///
/// ## Pure-RPC Design
/// This entity maintains ephemeral session state via `Arc<Mutex<HashMap<String, PlayerSessionState>>>`.
/// Each player is keyed by their player_id string. State is not persisted by the framework
/// — on restart, players need to reconnect.
///
/// ## RPCs (persisted, at-least-once)
/// - `connect(request)` - Initialize session
/// - `disconnect(player_id)` - End session
/// - `join_matchmaking_queue(player_id)` - Enter queue
/// - `leave_queue(player_id)` - Exit queue
/// - `resign_game(player_id)` - Resign current game
/// - `notify_game_event(event)` - Called by ChessGame to push events
/// - `notify_match_found(notification)` - Called by Matchmaking when paired
/// - `heartbeat(player_id)` - Update last activity
///
/// ## RPCs (non-persisted)
/// - `get_status(player_id)` - Return current state
/// - `get_current_game(player_id)` - Return current game ID
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct PlayerSession {
    /// In-memory ephemeral state, shared across all entity instances.
    /// Maps player_id string to session state.
    sessions: Arc<Mutex<HashMap<String, PlayerSessionState>>>,
}

impl PlayerSession {
    /// Create a new PlayerSession entity with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for PlayerSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Request to connect a player session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectRequest {
    /// The player ID for this session.
    pub player_id: PlayerId,
    /// Display username for the player.
    pub username: String,
}

/// Response from connecting a session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectResponse {
    /// The player ID (derived from entity ID).
    pub player_id: PlayerId,
    /// The assigned username.
    pub username: String,
    /// Current status after connecting.
    pub status: PlayerStatus,
}

/// Response for getting player status.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Whether the session is active.
    pub connected: bool,
    /// Player info if connected.
    pub info: Option<PlayerInfo>,
}

/// A game event notification sent to the player.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GameEvent {
    /// The game has started.
    GameStarted {
        /// ID of the game.
        game_id: GameId,
        /// Opponent's player ID.
        opponent: PlayerId,
        /// Your color in the game.
        your_color: Color,
    },
    /// Opponent made a move.
    OpponentMoved {
        /// ID of the game.
        game_id: GameId,
        /// The move in UCI notation.
        uci_move: String,
        /// The move in SAN notation.
        san_move: String,
    },
    /// Opponent offered a draw.
    DrawOffered {
        /// ID of the game.
        game_id: GameId,
    },
    /// The game has ended.
    GameEnded {
        /// ID of the game.
        game_id: GameId,
        /// Whether you won, lost, or drew.
        result: GameEventResult,
        /// Reason the game ended.
        reason: String,
    },
    /// It's your turn to move.
    YourTurn {
        /// ID of the game.
        game_id: GameId,
    },
}

/// Result of a game from the player's perspective.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GameEventResult {
    /// You won the game.
    Won,
    /// You lost the game.
    Lost,
    /// The game was a draw.
    Draw,
}

/// Notification that a match was found.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MatchFoundNotification {
    /// The player ID for this notification.
    pub player_id: PlayerId,
    /// The game ID for the new game.
    pub game_id: GameId,
    /// Your assigned color.
    pub color: Color,
    /// The opponent's player ID.
    pub opponent_id: PlayerId,
}

/// Request to notify a player of a game event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotifyGameEventRequest {
    /// The player ID.
    pub player_id: PlayerId,
    /// The game event.
    pub event: GameEvent,
}

/// Error types specific to player session operations.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum PlayerSessionError {
    /// Player is not connected.
    #[error("player not connected")]
    NotConnected,
    /// Player is already connected.
    #[error("player already connected")]
    AlreadyConnected,
    /// Player cannot join queue in current state.
    #[error("cannot join queue: player status is {status}")]
    CannotJoinQueue {
        /// Current status preventing queue join.
        status: PlayerStatus,
    },
    /// Player is not in the queue.
    #[error("player not in queue")]
    NotInQueue,
    /// Player is not in a game.
    #[error("player not in game")]
    NotInGame,
    /// Invalid game reference.
    #[error("not in the specified game")]
    WrongGame,
}

impl From<PlayerSessionError> for ClusterError {
    fn from(err: PlayerSessionError) -> Self {
        ClusterError::MalformedMessage {
            reason: err.to_string(),
            source: None,
        }
    }
}

#[entity_impl]
impl PlayerSession {
    /// Connect a player session with the given username.
    #[rpc(persisted)]
    pub async fn connect(&self, request: ConnectRequest) -> Result<ConnectResponse, ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let player_id = request.player_id;
        let key = player_id.to_string();

        // Check if already connected
        if let Some(state) = sessions.get(&key) {
            if state.info.is_some() {
                return Err(PlayerSessionError::AlreadyConnected.into());
            }
        }

        // Create session with connected info
        let info = PlayerInfo::new(player_id, request.username.clone());
        sessions.insert(
            key,
            PlayerSessionState {
                player_id,
                info: Some(info),
            },
        );

        Ok(ConnectResponse {
            player_id,
            username: request.username,
            status: PlayerStatus::Online,
        })
    }

    /// Disconnect the player session.
    #[rpc(persisted)]
    pub async fn disconnect(&self, player_id: PlayerId) -> Result<(), ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let key = player_id.to_string();

        if let Some(state) = sessions.get_mut(&key) {
            if let Some(info) = &mut state.info {
                info.status = PlayerStatus::Offline;
            }
        }
        Ok(())
    }

    /// Get the current status of the player session.
    #[rpc]
    pub async fn get_status(&self, player_id: PlayerId) -> Result<StatusResponse, ClusterError> {
        let sessions = self.sessions.lock().unwrap();
        let key = player_id.to_string();

        match sessions.get(&key) {
            Some(state) => Ok(StatusResponse {
                connected: state.info.as_ref().is_some_and(|i| i.status.is_connected()),
                info: state.info.clone(),
            }),
            None => Ok(StatusResponse {
                connected: false,
                info: None,
            }),
        }
    }

    /// Join the matchmaking queue.
    #[rpc(persisted)]
    pub async fn join_matchmaking_queue(&self, player_id: PlayerId) -> Result<u32, ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let key = player_id.to_string();

        let state = sessions
            .get_mut(&key)
            .ok_or(PlayerSessionError::NotConnected)?;
        let info = state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;

        if !info.status.can_join_queue() {
            return Err(PlayerSessionError::CannotJoinQueue {
                status: info.status,
            }
            .into());
        }

        info.status = PlayerStatus::InQueue;
        info.touch();
        Ok(1) // Placeholder queue position
    }

    /// Leave the matchmaking queue.
    #[rpc(persisted)]
    pub async fn leave_queue(&self, player_id: PlayerId) -> Result<(), ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let key = player_id.to_string();

        let state = sessions
            .get_mut(&key)
            .ok_or(PlayerSessionError::NotConnected)?;
        let info = state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;

        if info.status != PlayerStatus::InQueue {
            return Err(PlayerSessionError::NotInQueue.into());
        }

        info.status = PlayerStatus::Online;
        info.touch();
        Ok(())
    }

    /// Resign from the current game.
    #[rpc(persisted)]
    pub async fn resign_game(&self, player_id: PlayerId) -> Result<GameId, ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let key = player_id.to_string();

        let state = sessions
            .get_mut(&key)
            .ok_or(PlayerSessionError::NotConnected)?;
        let info = state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;

        let game_id = info.current_game_id.ok_or(PlayerSessionError::NotInGame)?;
        info.current_game_id = None;
        info.status = PlayerStatus::Online;
        info.touch();
        Ok(game_id)
    }

    /// Notify this player of a game event.
    #[rpc(persisted)]
    pub async fn notify_game_event(
        &self,
        request: NotifyGameEventRequest,
    ) -> Result<(), ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let key = request.player_id.to_string();

        let state = sessions
            .get_mut(&key)
            .ok_or(PlayerSessionError::NotConnected)?;
        let info = state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;

        match &request.event {
            GameEvent::GameStarted { game_id, .. } => {
                info.current_game_id = Some(*game_id);
                info.status = PlayerStatus::InGame;
            }
            GameEvent::GameEnded { .. } => {
                info.current_game_id = None;
                info.status = PlayerStatus::Online;
            }
            _ => {}
        }
        info.touch();
        Ok(())
    }

    /// Notify this player that a match was found.
    #[rpc(persisted)]
    pub async fn notify_match_found(
        &self,
        notification: MatchFoundNotification,
    ) -> Result<(), ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let key = notification.player_id.to_string();

        let state = sessions
            .get_mut(&key)
            .ok_or(PlayerSessionError::NotConnected)?;
        let info = state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;

        info.current_game_id = Some(notification.game_id);
        info.status = PlayerStatus::InGame;
        info.touch();
        Ok(())
    }

    /// Get the player's current game ID, if any.
    #[rpc]
    pub async fn get_current_game(
        &self,
        player_id: PlayerId,
    ) -> Result<Option<GameId>, ClusterError> {
        let sessions = self.sessions.lock().unwrap();
        let key = player_id.to_string();

        Ok(sessions
            .get(&key)
            .and_then(|s| s.info.as_ref())
            .and_then(|i| i.current_game_id))
    }

    /// Update the last activity timestamp.
    #[rpc(persisted)]
    pub async fn heartbeat(&self, player_id: PlayerId) -> Result<(), ClusterError> {
        let mut sessions = self.sessions.lock().unwrap();
        let key = player_id.to_string();

        if let Some(state) = sessions.get_mut(&key) {
            if let Some(info) = &mut state.info {
                info.touch();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_player_session_state_serialization() {
        let state = PlayerSessionState {
            player_id: PlayerId::new(),
            info: None,
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: PlayerSessionState = serde_json::from_str(&json).unwrap();
        assert!(parsed.info.is_none());
    }

    #[test]
    fn test_game_event_serialization() {
        let event = GameEvent::GameStarted {
            game_id: GameId::new(),
            opponent: PlayerId::new(),
            your_color: Color::White,
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: GameEvent = serde_json::from_str(&json).unwrap();

        match parsed {
            GameEvent::GameStarted { your_color, .. } => {
                assert_eq!(your_color, Color::White);
            }
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn test_player_session_error_display() {
        let err = PlayerSessionError::CannotJoinQueue {
            status: PlayerStatus::InGame,
        };
        assert!(err.to_string().contains("in_game"));
    }
}
