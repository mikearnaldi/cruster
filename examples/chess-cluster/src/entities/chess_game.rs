//! ChessGame entity - manages chess games with ephemeral in-memory state.
//!
//! This entity manages a chess game between two players. Move validation is done
//! via shakmaty.
//!
//! Uses the pure-RPC entity pattern: no framework-managed state, no workflows,
//! no activities. State is held directly via `Arc<Mutex<HashMap<...>>>`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

use crate::chess::engine::{ChessError, ChessPosition};

use crate::types::chess::{Color, LegalMove, UciMove};
use crate::types::game::{
    DrawOffer, GameId, GameResult, GameState, GameStatus, MoveRecord, TimeControl,
};
use crate::types::player::PlayerId;

/// State for a chess game entity.
///
/// This is ephemeral in-memory state. On restart, games need to be recreated.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChessGameState {
    /// The full game state.
    pub game: GameState,
}

impl ChessGameState {
    /// Create a new game between two players.
    #[must_use]
    pub fn new(
        game_id: GameId,
        white_player: PlayerId,
        black_player: PlayerId,
        time_control: TimeControl,
    ) -> Self {
        Self {
            game: GameState::new(game_id, white_player, black_player, time_control),
        }
    }

    /// Ensure the game has been initialized with players.
    fn ensure_initialized(&self) -> Result<(), ChessGameError> {
        if self.game.white_player.is_nil() {
            return Err(ChessGameError::NotInitialized);
        }
        Ok(())
    }

    /// Ensure the game is still in progress.
    fn ensure_game_in_progress(&self) -> Result<(), ChessGameError> {
        if !self.game.status.is_ongoing() {
            return Err(ChessGameError::GameOver {
                status: self.game.status,
            });
        }
        Ok(())
    }

    /// Get the color for a player in this game.
    fn player_color(&self, player_id: PlayerId) -> Result<Color, ChessGameError> {
        self.game
            .player_color(player_id)
            .ok_or(ChessGameError::NotInGame)
    }
}

/// ChessGame entity manages a game between two players.
///
/// ## Pure-RPC Design
/// This entity maintains ephemeral game state via `Arc<Mutex<HashMap<String, ChessGameState>>>`.
/// Each game is keyed by its game_id string. All RPCs include the `game_id` in their
/// request to identify which game to operate on. State is not persisted by the framework
/// — on restart, games need to be recreated.
///
/// ## RPCs (persisted, at-least-once)
/// - `create(request)` — Initialize game with players
/// - `make_move(request)` — Validate and apply a move
/// - `offer_draw(request)` — Record draw offer
/// - `accept_draw(request)` — Accept draw if offer exists
/// - `resign(request)` — End game with resignation
/// - `handle_timeout(request)` — Handle move timeout
/// - `abort(request)` — Abort game early
///
/// ## RPCs (non-persisted)
/// - `get_state(game_id)` — Return full game state
/// - `get_legal_moves(request)` — Return legal moves for current position
/// - `get_move_history(game_id)` — Return move list
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct ChessGame {
    /// In-memory ephemeral state, shared across all entity instances.
    /// Maps game_id string to game state.
    games: Arc<Mutex<HashMap<String, ChessGameState>>>,
}

impl ChessGame {
    /// Create a new ChessGame entity with empty state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            games: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get game state by game_id, returning NotInitialized if the game doesn't exist.
    fn get_game<'a>(
        games: &'a HashMap<String, ChessGameState>,
        game_id: &str,
    ) -> Result<&'a ChessGameState, ChessGameError> {
        games.get(game_id).ok_or(ChessGameError::NotInitialized)
    }
}

impl Default for ChessGame {
    fn default() -> Self {
        Self::new()
    }
}

/// Request to create a new game.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateGameRequest {
    /// The game ID.
    pub game_id: GameId,
    /// White player ID.
    pub white_player: PlayerId,
    /// Black player ID.
    pub black_player: PlayerId,
    /// Time control settings.
    pub time_control: TimeControl,
}

/// Response from creating a game.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateGameResponse {
    /// The game ID.
    pub game_id: GameId,
    /// Initial game state.
    pub state: GameState,
}

/// Request to make a move.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MakeMoveRequest {
    /// The game ID.
    pub game_id: GameId,
    /// Player making the move.
    pub player_id: PlayerId,
    /// Move in UCI notation (e.g., "e2e4").
    pub uci_move: String,
}

/// Response from making a move.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MakeMoveResponse {
    /// The move in SAN notation (e.g., "e4").
    pub san: String,
    /// The move in UCI notation.
    pub uci: String,
    /// FEN after the move.
    pub fen_after: String,
    /// Whether the game is now over.
    pub game_over: bool,
    /// Final status if game ended.
    pub status: GameStatus,
    /// Result reason if game ended.
    pub result_reason: Option<GameResult>,
}

/// Request to offer a draw.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DrawOfferRequest {
    /// The game ID.
    pub game_id: GameId,
    /// Player offering the draw.
    pub player_id: PlayerId,
}

/// Request to accept a draw.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DrawAcceptRequest {
    /// The game ID.
    pub game_id: GameId,
    /// Player accepting the draw.
    pub player_id: PlayerId,
}

/// Request to resign.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResignRequest {
    /// The game ID.
    pub game_id: GameId,
    /// Player resigning.
    pub player_id: PlayerId,
}

/// Request to handle a timeout.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandleTimeoutRequest {
    /// The game ID.
    pub game_id: GameId,
    /// The color that timed out.
    pub color: Color,
}

/// Request to abort a game.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AbortRequest {
    /// The game ID.
    pub game_id: GameId,
    /// Player requesting the abort.
    pub player_id: PlayerId,
}

/// Request for legal moves.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetLegalMovesRequest {
    /// The game ID.
    pub game_id: GameId,
    /// Player requesting legal moves.
    pub player_id: PlayerId,
}

/// Error types specific to chess game operations.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum ChessGameError {
    /// Game has not been initialized.
    #[error("game not initialized - call create() first")]
    NotInitialized,
    /// Game is already over.
    #[error("game is already over: {status}")]
    GameOver {
        /// Final game status.
        status: GameStatus,
    },
    /// It's not this player's turn.
    #[error("not your turn - it is {expected}'s turn")]
    NotYourTurn {
        /// Whose turn it is.
        expected: Color,
    },
    /// Player is not in this game.
    #[error("player is not in this game")]
    NotInGame,
    /// Invalid move.
    #[error("invalid move: {reason}")]
    InvalidMove {
        /// Reason the move is invalid.
        reason: String,
    },
    /// No draw offer to accept.
    #[error("no draw offer to accept")]
    NoDrawOffer,
    /// Cannot accept own draw offer.
    #[error("cannot accept your own draw offer")]
    CannotAcceptOwnOffer,
    /// Game already has players.
    #[error("game already has players assigned")]
    AlreadyCreated,
}

impl From<ChessGameError> for ClusterError {
    fn from(err: ChessGameError) -> Self {
        ClusterError::MalformedMessage {
            reason: err.to_string(),
            source: None,
        }
    }
}

impl From<ChessError> for ChessGameError {
    fn from(err: ChessError) -> Self {
        ChessGameError::InvalidMove {
            reason: err.to_string(),
        }
    }
}

#[entity_impl]
impl ChessGame {
    /// Create a new game with the specified players and time control.
    #[rpc(persisted)]
    pub async fn create(
        &self,
        request: CreateGameRequest,
    ) -> Result<CreateGameResponse, ClusterError> {
        let mut games = self.games.lock().unwrap();
        let key = request.game_id.to_string();

        if let Some(existing) = games.get(&key) {
            if !existing.game.white_player.is_nil() {
                return Err(ChessGameError::AlreadyCreated.into());
            }
        }

        let game_id = request.game_id;
        let state = ChessGameState::new(
            game_id,
            request.white_player,
            request.black_player,
            request.time_control,
        );
        let game_state = state.game.clone();
        games.insert(key, state);

        Ok(CreateGameResponse {
            game_id,
            state: game_state,
        })
    }

    /// Get the current game state.
    #[rpc]
    pub async fn get_state(&self, game_id: GameId) -> Result<GameState, ClusterError> {
        let games = self.games.lock().unwrap();
        let key = game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;
        Ok(state.game.clone())
    }

    /// Get legal moves for the current position.
    ///
    /// Returns moves only if it's the given player's turn.
    #[rpc]
    pub async fn get_legal_moves(
        &self,
        request: GetLegalMovesRequest,
    ) -> Result<Vec<LegalMove>, ClusterError> {
        let games = self.games.lock().unwrap();
        let key = request.game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;
        state.ensure_game_in_progress()?;

        let color = state.player_color(request.player_id)?;
        let turn = state.game.turn();

        if color != turn {
            return Err(ChessGameError::NotYourTurn { expected: turn }.into());
        }

        let position = ChessPosition::from_fen(&state.game.board_fen).map_err(|e| {
            ChessGameError::InvalidMove {
                reason: format!("invalid position: {e}"),
            }
        })?;

        Ok(position.legal_moves())
    }

    /// Get the move history.
    #[rpc]
    pub async fn get_move_history(&self, game_id: GameId) -> Result<Vec<MoveRecord>, ClusterError> {
        let games = self.games.lock().unwrap();
        let key = game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;
        Ok(state.game.moves.clone())
    }

    /// Make a move in the game.
    ///
    /// Validates the move and applies it atomically.
    #[rpc(persisted)]
    pub async fn make_move(
        &self,
        request: MakeMoveRequest,
    ) -> Result<MakeMoveResponse, ClusterError> {
        let mut games = self.games.lock().unwrap();
        let key = request.game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;
        state.ensure_game_in_progress()?;

        let color = state.player_color(request.player_id)?;
        let turn = state.game.turn();

        if color != turn {
            return Err(ChessGameError::NotYourTurn { expected: turn }.into());
        }

        // Parse and validate the move
        let uci_move =
            UciMove::new(&request.uci_move).map_err(|e| ChessGameError::InvalidMove {
                reason: e.to_string(),
            })?;

        let mut position = ChessPosition::from_fen(&state.game.board_fen).map_err(|e| {
            ChessGameError::InvalidMove {
                reason: format!("invalid position: {e}"),
            }
        })?;

        // Apply the move to validate legality
        let san = position
            .make_move(&uci_move)
            .map_err(ChessGameError::from)?;
        let fen_after = position.to_fen();
        let outcome = position.outcome();

        // Apply state mutation
        let state = games.get_mut(&key).unwrap();
        let now = Utc::now();
        let time_taken = state
            .game
            .last_move_at
            .map(|last| (now - last).to_std().unwrap_or_default())
            .unwrap_or_default();

        let move_number = state.game.current_move_number();
        let move_record = MoveRecord::new(
            move_number,
            color,
            san.clone(),
            request.uci_move.clone(),
            fen_after.clone(),
            time_taken,
        );

        state.game.moves.push(move_record);
        state.game.board_fen = fen_after.clone();
        state.game.last_move_at = Some(now);
        state.game.draw_offer = None;

        let (game_over, status, result_reason) = if let Some(outcome) = outcome {
            let (status, result) = outcome.to_status_and_result();
            state.game.status = status;
            state.game.result_reason = Some(result);
            (true, status, Some(result))
        } else {
            (false, GameStatus::InProgress, None)
        };

        Ok(MakeMoveResponse {
            san,
            uci: request.uci_move,
            fen_after,
            game_over,
            status,
            result_reason,
        })
    }

    /// Offer a draw to the opponent.
    #[rpc(persisted)]
    pub async fn offer_draw(&self, request: DrawOfferRequest) -> Result<(), ClusterError> {
        let mut games = self.games.lock().unwrap();
        let key = request.game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;
        state.ensure_game_in_progress()?;
        let _color = state.player_color(request.player_id)?;

        let state = games.get_mut(&key).unwrap();
        state.game.draw_offer = Some(DrawOffer::new(request.player_id));
        Ok(())
    }

    /// Accept a draw offer.
    #[rpc(persisted)]
    pub async fn accept_draw(&self, request: DrawAcceptRequest) -> Result<GameState, ClusterError> {
        let mut games = self.games.lock().unwrap();
        let key = request.game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;
        state.ensure_game_in_progress()?;
        let _color = state.player_color(request.player_id)?;

        let offer = state
            .game
            .draw_offer
            .as_ref()
            .ok_or(ChessGameError::NoDrawOffer)?;

        if offer.offered_by == request.player_id {
            return Err(ChessGameError::CannotAcceptOwnOffer.into());
        }

        let state = games.get_mut(&key).unwrap();
        state.game.status = GameStatus::Draw;
        state.game.result_reason = Some(GameResult::DrawAgreement);
        state.game.draw_offer = None;

        Ok(state.game.clone())
    }

    /// Resign from the game.
    #[rpc(persisted)]
    pub async fn resign(&self, request: ResignRequest) -> Result<GameState, ClusterError> {
        let mut games = self.games.lock().unwrap();
        let key = request.game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;
        state.ensure_game_in_progress()?;
        let color = state.player_color(request.player_id)?;

        let state = games.get_mut(&key).unwrap();
        state.game.status = match color {
            Color::White => GameStatus::BlackWins,
            Color::Black => GameStatus::WhiteWins,
        };
        state.game.result_reason = Some(GameResult::Resignation);
        state.game.draw_offer = None;

        Ok(state.game.clone())
    }

    /// Handle a move timeout (called externally when time runs out).
    #[rpc(persisted)]
    pub async fn handle_timeout(
        &self,
        request: HandleTimeoutRequest,
    ) -> Result<GameState, ClusterError> {
        let mut games = self.games.lock().unwrap();
        let key = request.game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;

        // Early return if game already ended or not this player's turn
        if !state.game.status.is_ongoing() || state.game.turn() != request.color {
            return Ok(state.game.clone());
        }

        let state = games.get_mut(&key).unwrap();
        state.game.status = match request.color {
            Color::White => GameStatus::BlackWins,
            Color::Black => GameStatus::WhiteWins,
        };
        state.game.result_reason = Some(GameResult::Timeout);

        Ok(state.game.clone())
    }

    /// Abort the game (only allowed before enough moves are made).
    #[rpc(persisted)]
    pub async fn abort(&self, request: AbortRequest) -> Result<GameState, ClusterError> {
        let mut games = self.games.lock().unwrap();
        let key = request.game_id.to_string();
        let state = Self::get_game(&games, &key)?;
        state.ensure_initialized()?;

        if state.game.moves.len() >= 2 {
            return Err(ChessGameError::GameOver {
                status: state.game.status,
            }
            .into());
        }
        let _color = state.player_color(request.player_id)?;

        let state = games.get_mut(&key).unwrap();
        state.game.status = GameStatus::Aborted;
        state.game.result_reason = Some(GameResult::Aborted);

        Ok(state.game.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chess_game_state_serialization() {
        let white = PlayerId::new();
        let black = PlayerId::new();
        let state = ChessGameState::new(GameId::new(), white, black, TimeControl::BLITZ);

        let json = serde_json::to_string(&state).unwrap();
        let parsed: ChessGameState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.game.white_player, white);
        assert_eq!(parsed.game.black_player, black);
        assert_eq!(parsed.game.status, GameStatus::InProgress);
    }

    #[test]
    fn test_chess_game_error_display() {
        let err = ChessGameError::NotYourTurn {
            expected: Color::Black,
        };
        assert!(err.to_string().contains("black"));

        let err = ChessGameError::GameOver {
            status: GameStatus::WhiteWins,
        };
        assert!(err.to_string().contains("white_wins"));
    }

    #[test]
    fn test_make_move_request_serialization() {
        let req = MakeMoveRequest {
            game_id: GameId::new(),
            player_id: PlayerId::new(),
            uci_move: "e2e4".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: MakeMoveRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.uci_move, "e2e4");
    }
}
