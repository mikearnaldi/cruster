//! ChessGame entity - persisted game state with durable workflows.
//!
//! This entity manages a chess game between two players, with persisted state
//! that survives server restarts. Move validation is done via shakmaty.
//!
//! The entity uses the `Auditable` trait for comprehensive audit logging of all
//! game actions.

use chrono::{Duration as ChronoDuration, Utc};
use cruster::entity::EntityContext;
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
/// This is persisted to storage so games survive server restarts.
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
/// ## State (Persisted)
/// - game_id, white_player, black_player, board_fen
/// - moves (list of MoveRecord)
/// - status (InProgress/WhiteWins/BlackWins/Draw/Aborted)
/// - result_reason
/// - time_control, white_time_remaining, black_time_remaining
/// - last_move_at, created_at, draw_offer
///
/// ## Workflows (durable, at-least-once)
/// - `create(request)` - Initialize game with players
/// - `make_move(request)` - Validate and apply a move
/// - `offer_draw(request)` - Record draw offer
/// - `accept_draw(request)` - Accept draw if offer exists
/// - `resign(request)` - End game with resignation
/// - `abort(player_id)` - Abort game early
///
/// ## RPCs
/// - `get_state()` - Return full game state
/// - `get_legal_moves(player)` - Return legal moves for current position
/// - `get_move_history()` - Return move list
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct ChessGame;

/// Request to create a new game.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateGameRequest {
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
    /// Player offering the draw.
    pub player_id: PlayerId,
}

/// Request to accept a draw.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DrawAcceptRequest {
    /// Player accepting the draw.
    pub player_id: PlayerId,
}

/// Request to resign.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResignRequest {
    /// Player resigning.
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
#[state(ChessGameState)]
impl ChessGame {
    fn init(&self, ctx: &EntityContext) -> Result<ChessGameState, ClusterError> {
        // Parse the entity ID as a game ID
        let game_id: GameId =
            ctx.address
                .entity_id
                .as_ref()
                .parse()
                .map_err(|e| ClusterError::MalformedMessage {
                    reason: format!("invalid game ID: {e}"),
                    source: None,
                })?;

        // Initialize with placeholder players - they will be set by create()
        Ok(ChessGameState {
            game: GameState::new(
                game_id,
                PlayerId::nil(),
                PlayerId::nil(),
                TimeControl::BLITZ,
            ),
        })
    }

    /// Create a new game with the specified players and time control.
    ///
    /// This must be called before any moves can be made.
    #[workflow]
    pub async fn create(
        &self,
        request: CreateGameRequest,
    ) -> Result<CreateGameResponse, ClusterError> {
        // Validation (deterministic)
        if !self.state.game.white_player.is_nil() {
            return Err(ChessGameError::AlreadyCreated.into());
        }

        // State mutation via activity
        self.do_create(request).await
    }

    #[activity]
    async fn do_create(
        &mut self,
        request: CreateGameRequest,
    ) -> Result<CreateGameResponse, ClusterError> {
        let game_id = self.state.game.game_id;
        self.state.game = GameState::new(
            game_id,
            request.white_player,
            request.black_player,
            request.time_control,
        );

        Ok(CreateGameResponse {
            game_id,
            state: self.state.game.clone(),
        })
    }

    /// Get the current game state.
    #[rpc]
    pub async fn get_state(&self) -> Result<GameState, ClusterError> {
        self.state.ensure_initialized()?;
        Ok(self.state.game.clone())
    }

    /// Get legal moves for the current position.
    ///
    /// Returns moves only if it's the given player's turn.
    #[rpc]
    pub async fn get_legal_moves(
        &self,
        player_id: PlayerId,
    ) -> Result<Vec<LegalMove>, ClusterError> {
        self.state.ensure_initialized()?;
        self.state.ensure_game_in_progress()?;

        let color = self.state.player_color(player_id)?;
        let turn = self.state.game.turn();

        if color != turn {
            return Err(ChessGameError::NotYourTurn { expected: turn }.into());
        }

        let position = ChessPosition::from_fen(&self.state.game.board_fen).map_err(|e| {
            ChessGameError::InvalidMove {
                reason: format!("invalid position: {e}"),
            }
        })?;

        Ok(position.legal_moves())
    }

    /// Get the move history.
    #[rpc]
    pub async fn get_move_history(&self) -> Result<Vec<MoveRecord>, ClusterError> {
        self.state.ensure_initialized()?;
        Ok(self.state.game.moves.clone())
    }

    /// Make a move in the game.
    ///
    /// This is a durable workflow that validates the move and applies it via activity.
    #[workflow]
    pub async fn make_move(
        &self,
        request: MakeMoveRequest,
    ) -> Result<MakeMoveResponse, ClusterError> {
        // Validation (deterministic)
        self.state.ensure_initialized()?;
        self.state.ensure_game_in_progress()?;

        let color = self.state.player_color(request.player_id)?;
        let turn = self.state.game.turn();

        if color != turn {
            return Err(ChessGameError::NotYourTurn { expected: turn }.into());
        }

        // Parse and validate the move (deterministic)
        let uci_move =
            UciMove::new(&request.uci_move).map_err(|e| ChessGameError::InvalidMove {
                reason: e.to_string(),
            })?;

        let mut position = ChessPosition::from_fen(&self.state.game.board_fen).map_err(|e| {
            ChessGameError::InvalidMove {
                reason: format!("invalid position: {e}"),
            }
        })?;

        // Apply the move to validate legality (deterministic - no state mutation yet)
        let san = position
            .make_move(&uci_move)
            .map_err(ChessGameError::from)?;
        let fen_after = position.to_fen();
        let outcome = position.outcome();

        // Apply via activity (state mutation)
        let response = self
            .do_make_move(
                request.player_id,
                request.uci_move.clone(),
                color,
                san,
                fen_after,
                outcome,
            )
            .await?;

        // Schedule timeout (external side effect) - fire and forget
        if !response.game_over {
            if let Some(client) = self.self_client() {
                let opp_color = if color == Color::White {
                    Color::Black
                } else {
                    Color::White
                };
                let opp_time = self.state.game.time_remaining(opp_color);
                let timeout_at =
                    Utc::now() + ChronoDuration::from_std(opp_time).unwrap_or_default();
                let entity_id = self.entity_id().clone();
                let _ = client
                    .notify_at(&entity_id, "handle_timeout", &opp_color, timeout_at)
                    .await;
            }
        }

        Ok(response)
    }

    #[activity]
    async fn do_make_move(
        &mut self,
        _player_id: PlayerId,
        uci: String,
        color: Color,
        san: String,
        fen_after: String,
        outcome: Option<crate::chess::engine::Outcome>,
    ) -> Result<MakeMoveResponse, ClusterError> {
        let now = Utc::now();
        let time_taken = self
            .state
            .game
            .last_move_at
            .map(|last| (now - last).to_std().unwrap_or_default())
            .unwrap_or_default();

        let move_number = self.state.game.current_move_number();
        let move_record = MoveRecord::new(
            move_number,
            color,
            san.clone(),
            uci.clone(),
            fen_after.clone(),
            time_taken,
        );

        self.state.game.moves.push(move_record);
        self.state.game.board_fen = fen_after.clone();
        self.state.game.last_move_at = Some(now);
        self.state.game.draw_offer = None;

        let (game_over, status, result_reason) = if let Some(outcome) = outcome {
            let (status, result) = outcome.to_status_and_result();
            self.state.game.status = status;
            self.state.game.result_reason = Some(result);
            (true, status, Some(result))
        } else {
            (false, GameStatus::InProgress, None)
        };

        Ok(MakeMoveResponse {
            san,
            uci,
            fen_after,
            game_over,
            status,
            result_reason,
        })
    }

    /// Offer a draw to the opponent.
    #[workflow]
    pub async fn offer_draw(&self, request: DrawOfferRequest) -> Result<(), ClusterError> {
        // Validation (deterministic)
        self.state.ensure_initialized()?;
        self.state.ensure_game_in_progress()?;
        let _color = self.state.player_color(request.player_id)?;

        // State mutation via activity
        self.do_offer_draw(request.player_id).await
    }

    #[activity]
    async fn do_offer_draw(&mut self, player_id: PlayerId) -> Result<(), ClusterError> {
        self.state.game.draw_offer = Some(DrawOffer::new(player_id));

        Ok(())
    }

    /// Accept a draw offer.
    #[workflow]
    pub async fn accept_draw(&self, request: DrawAcceptRequest) -> Result<GameState, ClusterError> {
        // Validation (deterministic)
        self.state.ensure_initialized()?;
        self.state.ensure_game_in_progress()?;
        let _color = self.state.player_color(request.player_id)?;

        let offer = self
            .state
            .game
            .draw_offer
            .as_ref()
            .ok_or(ChessGameError::NoDrawOffer)?;

        if offer.offered_by == request.player_id {
            return Err(ChessGameError::CannotAcceptOwnOffer.into());
        }

        // State mutation via activity
        self.do_accept_draw(request.player_id).await
    }

    #[activity]
    async fn do_accept_draw(
        &mut self,
        _accepting_player: PlayerId,
    ) -> Result<GameState, ClusterError> {
        self.state.game.status = GameStatus::Draw;
        self.state.game.result_reason = Some(GameResult::DrawAgreement);
        self.state.game.draw_offer = None;

        Ok(self.state.game.clone())
    }

    /// Resign from the game.
    #[workflow]
    pub async fn resign(&self, request: ResignRequest) -> Result<GameState, ClusterError> {
        // Validation (deterministic)
        self.state.ensure_initialized()?;
        self.state.ensure_game_in_progress()?;
        let color = self.state.player_color(request.player_id)?;

        // State mutation via activity
        self.do_resign(color, request.player_id).await
    }

    #[activity]
    async fn do_resign(
        &mut self,
        color: Color,
        _player_id: PlayerId,
    ) -> Result<GameState, ClusterError> {
        self.state.game.status = match color {
            Color::White => GameStatus::BlackWins,
            Color::Black => GameStatus::WhiteWins,
        };
        self.state.game.result_reason = Some(GameResult::Resignation);
        self.state.game.draw_offer = None;

        Ok(self.state.game.clone())
    }

    /// Handle a move timeout (called by scheduled message).
    #[workflow]
    pub async fn handle_timeout(&self, color: Color) -> Result<GameState, ClusterError> {
        // Validation (deterministic)
        self.state.ensure_initialized()?;

        // Early return if game already ended or not this player's turn
        if !self.state.game.status.is_ongoing() || self.state.game.turn() != color {
            return Ok(self.state.game.clone());
        }

        // State mutation via activity
        self.do_timeout(color).await
    }

    #[activity]
    async fn do_timeout(&mut self, color: Color) -> Result<GameState, ClusterError> {
        self.state.game.status = match color {
            Color::White => GameStatus::BlackWins,
            Color::Black => GameStatus::WhiteWins,
        };
        self.state.game.result_reason = Some(GameResult::Timeout);

        Ok(self.state.game.clone())
    }

    /// Abort the game (only allowed before enough moves are made).
    #[workflow]
    pub async fn abort(&self, player_id: PlayerId) -> Result<GameState, ClusterError> {
        // Validation (deterministic)
        self.state.ensure_initialized()?;

        if self.state.game.moves.len() >= 2 {
            return Err(ChessGameError::GameOver {
                status: self.state.game.status,
            }
            .into());
        }
        let _color = self.state.player_color(player_id)?;

        // State mutation via activity
        self.do_abort(player_id).await
    }

    #[activity]
    async fn do_abort(&mut self, _player_id: PlayerId) -> Result<GameState, ClusterError> {
        self.state.game.status = GameStatus::Aborted;
        self.state.game.result_reason = Some(GameResult::Aborted);

        Ok(self.state.game.clone())
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
            player_id: PlayerId::new(),
            uci_move: "e2e4".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: MakeMoveRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.uci_move, "e2e4");
    }
}
