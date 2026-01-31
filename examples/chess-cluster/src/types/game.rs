//! Game-related types.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::chess::Color;
use super::player::PlayerId;

/// Unique identifier for a chess game.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GameId(Uuid);

impl GameId {
    /// Create a new random game ID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a game ID from an existing UUID.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for GameId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for GameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for GameId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// Current status of a chess game.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GameStatus {
    /// Game is waiting for players to join.
    Pending,
    /// Game is in progress.
    InProgress,
    /// White player won.
    WhiteWins,
    /// Black player won.
    BlackWins,
    /// Game ended in a draw.
    Draw,
    /// Game was aborted (not enough moves).
    Aborted,
}

impl GameStatus {
    /// Returns true if the game is still ongoing.
    #[must_use]
    pub const fn is_ongoing(&self) -> bool {
        matches!(self, Self::Pending | Self::InProgress)
    }

    /// Returns true if the game has ended.
    #[must_use]
    pub const fn is_finished(&self) -> bool {
        !self.is_ongoing()
    }

    /// Returns the winning color if any.
    #[must_use]
    pub const fn winner(&self) -> Option<Color> {
        match self {
            Self::WhiteWins => Some(Color::White),
            Self::BlackWins => Some(Color::Black),
            _ => None,
        }
    }
}

impl std::fmt::Display for GameStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::InProgress => write!(f, "in_progress"),
            Self::WhiteWins => write!(f, "white_wins"),
            Self::BlackWins => write!(f, "black_wins"),
            Self::Draw => write!(f, "draw"),
            Self::Aborted => write!(f, "aborted"),
        }
    }
}

/// Reason why a game ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GameResult {
    /// Checkmate.
    Checkmate,
    /// Resignation.
    Resignation,
    /// Timeout (ran out of time).
    Timeout,
    /// Draw by agreement.
    DrawAgreement,
    /// Stalemate.
    Stalemate,
    /// Threefold repetition.
    ThreefoldRepetition,
    /// Fifty-move rule.
    FiftyMoveRule,
    /// Insufficient material.
    InsufficientMaterial,
    /// Game was aborted.
    Aborted,
}

impl std::fmt::Display for GameResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checkmate => write!(f, "checkmate"),
            Self::Resignation => write!(f, "resignation"),
            Self::Timeout => write!(f, "timeout"),
            Self::DrawAgreement => write!(f, "draw_agreement"),
            Self::Stalemate => write!(f, "stalemate"),
            Self::ThreefoldRepetition => write!(f, "threefold_repetition"),
            Self::FiftyMoveRule => write!(f, "fifty_move_rule"),
            Self::InsufficientMaterial => write!(f, "insufficient_material"),
            Self::Aborted => write!(f, "aborted"),
        }
    }
}

/// Time control settings for a game.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeControl {
    /// Initial time in seconds.
    pub initial_seconds: u32,
    /// Increment per move in seconds.
    pub increment_seconds: u32,
}

impl TimeControl {
    /// Bullet time control: 1 minute, no increment.
    pub const BULLET: Self = Self {
        initial_seconds: 60,
        increment_seconds: 0,
    };

    /// Blitz time control: 5 minutes + 3 second increment.
    pub const BLITZ: Self = Self {
        initial_seconds: 300,
        increment_seconds: 3,
    };

    /// Rapid time control: 15 minutes + 10 second increment.
    pub const RAPID: Self = Self {
        initial_seconds: 900,
        increment_seconds: 10,
    };

    /// Create a custom time control.
    #[must_use]
    pub const fn new(initial_seconds: u32, increment_seconds: u32) -> Self {
        Self {
            initial_seconds,
            increment_seconds,
        }
    }

    /// Get the initial time as a Duration.
    #[must_use]
    pub const fn initial_time(&self) -> Duration {
        Duration::from_secs(self.initial_seconds as u64)
    }

    /// Get the increment as a Duration.
    #[must_use]
    pub const fn increment(&self) -> Duration {
        Duration::from_secs(self.increment_seconds as u64)
    }

    /// Parse time control from string like "5+3" (5 min + 3 sec increment).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('+').collect();
        if parts.len() != 2 {
            return None;
        }
        let initial: u32 = parts[0].trim().parse().ok()?;
        let increment: u32 = parts[1].trim().parse().ok()?;
        Some(Self::new(initial * 60, increment))
    }
}

impl std::fmt::Display for TimeControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}+{}",
            self.initial_seconds / 60,
            self.increment_seconds
        )
    }
}

/// Record of a single move in a game.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveRecord {
    /// Move number (1-indexed, same for both colors).
    pub move_number: u16,
    /// Color that made the move.
    pub color: Color,
    /// Standard Algebraic Notation (e.g., "Nf3", "O-O").
    pub san: String,
    /// Universal Chess Interface notation (e.g., "g1f3", "e1g1").
    pub uci: String,
    /// FEN string after the move.
    pub fen_after: String,
    /// When the move was made.
    pub timestamp: DateTime<Utc>,
    /// Time taken to make the move.
    pub time_taken: Duration,
}

impl MoveRecord {
    /// Create a new move record.
    #[must_use]
    pub fn new(
        move_number: u16,
        color: Color,
        san: String,
        uci: String,
        fen_after: String,
        time_taken: Duration,
    ) -> Self {
        Self {
            move_number,
            color,
            san,
            uci,
            fen_after,
            timestamp: Utc::now(),
            time_taken,
        }
    }
}

/// Draw offer state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrawOffer {
    /// Player who offered the draw.
    pub offered_by: PlayerId,
    /// When the offer was made.
    pub offered_at: DateTime<Utc>,
}

impl DrawOffer {
    /// Create a new draw offer.
    #[must_use]
    pub fn new(offered_by: PlayerId) -> Self {
        Self {
            offered_by,
            offered_at: Utc::now(),
        }
    }
}

/// Complete game state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameState {
    /// Unique game identifier.
    pub game_id: GameId,
    /// White player.
    pub white_player: PlayerId,
    /// Black player.
    pub black_player: PlayerId,
    /// Current board position in FEN notation.
    pub board_fen: String,
    /// List of moves played.
    pub moves: Vec<MoveRecord>,
    /// Current game status.
    pub status: GameStatus,
    /// How the game ended (if finished).
    pub result_reason: Option<GameResult>,
    /// Time control settings.
    pub time_control: TimeControl,
    /// White's remaining time.
    pub white_time_remaining: Duration,
    /// Black's remaining time.
    pub black_time_remaining: Duration,
    /// When the last move was made.
    pub last_move_at: Option<DateTime<Utc>>,
    /// When the game was created.
    pub created_at: DateTime<Utc>,
    /// Active draw offer if any.
    pub draw_offer: Option<DrawOffer>,
}

impl GameState {
    /// Standard starting FEN.
    pub const STARTING_FEN: &'static str =
        "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";

    /// Create a new game between two players.
    #[must_use]
    pub fn new(
        game_id: GameId,
        white_player: PlayerId,
        black_player: PlayerId,
        time_control: TimeControl,
    ) -> Self {
        let initial_time = time_control.initial_time();
        Self {
            game_id,
            white_player,
            black_player,
            board_fen: Self::STARTING_FEN.to_string(),
            moves: Vec::new(),
            status: GameStatus::InProgress,
            result_reason: None,
            time_control,
            white_time_remaining: initial_time,
            black_time_remaining: initial_time,
            last_move_at: None,
            created_at: Utc::now(),
            draw_offer: None,
        }
    }

    /// Get the color for a player, if they are in this game.
    #[must_use]
    pub fn player_color(&self, player_id: PlayerId) -> Option<Color> {
        if player_id == self.white_player {
            Some(Color::White)
        } else if player_id == self.black_player {
            Some(Color::Black)
        } else {
            None
        }
    }

    /// Get the player ID for a given color.
    #[must_use]
    pub const fn player_for_color(&self, color: Color) -> PlayerId {
        match color {
            Color::White => self.white_player,
            Color::Black => self.black_player,
        }
    }

    /// Whose turn is it?
    #[must_use]
    pub fn turn(&self) -> Color {
        // In a standard game, white moves on even indices, black on odd
        if self.moves.len() % 2 == 0 {
            Color::White
        } else {
            Color::Black
        }
    }

    /// Get the current move number.
    #[must_use]
    pub fn current_move_number(&self) -> u16 {
        // Move number increments after black's move
        (self.moves.len() / 2 + 1) as u16
    }

    /// Get remaining time for the given color.
    #[must_use]
    pub const fn time_remaining(&self, color: Color) -> Duration {
        match color {
            Color::White => self.white_time_remaining,
            Color::Black => self.black_time_remaining,
        }
    }
}

/// Result sent when a game ends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameResultInfo {
    /// The game that ended.
    pub game_id: GameId,
    /// White player.
    pub white_player: PlayerId,
    /// Black player.
    pub black_player: PlayerId,
    /// Final status.
    pub status: GameStatus,
    /// Reason for the result.
    pub result_reason: GameResult,
    /// Number of moves played.
    pub total_moves: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_game_id_roundtrip() {
        let id = GameId::new();
        let s = id.to_string();
        let parsed: GameId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_game_status_helpers() {
        assert!(GameStatus::InProgress.is_ongoing());
        assert!(!GameStatus::InProgress.is_finished());
        assert!(GameStatus::WhiteWins.is_finished());
        assert_eq!(GameStatus::WhiteWins.winner(), Some(Color::White));
        assert_eq!(GameStatus::BlackWins.winner(), Some(Color::Black));
        assert_eq!(GameStatus::Draw.winner(), None);
    }

    #[test]
    fn test_time_control_parse() {
        let tc = TimeControl::parse("5+3").unwrap();
        assert_eq!(tc.initial_seconds, 300);
        assert_eq!(tc.increment_seconds, 3);

        let tc = TimeControl::parse("15+10").unwrap();
        assert_eq!(tc.initial_seconds, 900);
        assert_eq!(tc.increment_seconds, 10);
    }

    #[test]
    fn test_time_control_display() {
        assert_eq!(TimeControl::BLITZ.to_string(), "5+3");
        assert_eq!(TimeControl::RAPID.to_string(), "15+10");
    }

    #[test]
    fn test_game_state_turn() {
        let white = PlayerId::new();
        let black = PlayerId::new();
        let mut game = GameState::new(GameId::new(), white, black, TimeControl::BLITZ);

        assert_eq!(game.turn(), Color::White);
        assert_eq!(game.current_move_number(), 1);

        // Simulate white's first move
        game.moves.push(MoveRecord::new(
            1,
            Color::White,
            "e4".to_string(),
            "e2e4".to_string(),
            "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1".to_string(),
            Duration::from_secs(5),
        ));
        assert_eq!(game.turn(), Color::Black);
        assert_eq!(game.current_move_number(), 1);

        // Simulate black's first move
        game.moves.push(MoveRecord::new(
            1,
            Color::Black,
            "e5".to_string(),
            "e7e5".to_string(),
            "rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2".to_string(),
            Duration::from_secs(3),
        ));
        assert_eq!(game.turn(), Color::White);
        assert_eq!(game.current_move_number(), 2);
    }

    #[test]
    fn test_player_color() {
        let white = PlayerId::new();
        let black = PlayerId::new();
        let other = PlayerId::new();
        let game = GameState::new(GameId::new(), white, black, TimeControl::BLITZ);

        assert_eq!(game.player_color(white), Some(Color::White));
        assert_eq!(game.player_color(black), Some(Color::Black));
        assert_eq!(game.player_color(other), None);
    }
}
