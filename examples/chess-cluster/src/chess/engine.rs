//! Chess engine integration with shakmaty.
//!
//! This module provides a high-level interface to shakmaty for:
//! - Position management and FEN parsing
//! - Move validation and application
//! - Legal move generation
//! - Game end detection (checkmate, stalemate, draw conditions)

use shakmaty::{
    fen::Fen, san::San, uci::UciMove as ShakmatyUciMove, CastlingMode, Chess, Move, Position,
};
use thiserror::Error;

use crate::types::chess::{Color, LegalMove, UciMove};
use crate::types::game::{GameResult, GameStatus};

/// Errors that can occur during chess operations.
#[derive(Debug, Error)]
pub enum ChessError {
    /// Invalid FEN string.
    #[error("invalid FEN: {0}")]
    InvalidFen(String),

    /// Invalid UCI move format.
    #[error("invalid UCI move: {0}")]
    InvalidUciMove(String),

    /// Move is not legal in the current position.
    #[error("illegal move: {0}")]
    IllegalMove(String),

    /// Not the player's turn.
    #[error("not your turn")]
    NotYourTurn,

    /// Game is already over.
    #[error("game is already over")]
    GameOver,
}

/// The outcome of a chess game.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Outcome {
    /// Checkmate - the given color won.
    Checkmate(Color),
    /// Stalemate - draw.
    Stalemate,
    /// Insufficient material - draw.
    InsufficientMaterial,
}

impl Outcome {
    /// Convert outcome to game status and result.
    #[must_use]
    pub fn to_status_and_result(self) -> (GameStatus, GameResult) {
        match self {
            Self::Checkmate(Color::White) => (GameStatus::WhiteWins, GameResult::Checkmate),
            Self::Checkmate(Color::Black) => (GameStatus::BlackWins, GameResult::Checkmate),
            Self::Stalemate => (GameStatus::Draw, GameResult::Stalemate),
            Self::InsufficientMaterial => (GameStatus::Draw, GameResult::InsufficientMaterial),
        }
    }
}

/// A chess position with move validation and game state tracking.
///
/// This is the main interface to shakmaty, providing a clean API for
/// the chess-cluster game entity.
#[derive(Debug, Clone)]
pub struct ChessPosition {
    position: Chess,
}

impl ChessPosition {
    /// Create a new position from the standard starting position.
    #[must_use]
    pub fn new() -> Self {
        Self {
            position: Chess::default(),
        }
    }

    /// Create a position from a FEN string.
    pub fn from_fen(fen: &str) -> Result<Self, ChessError> {
        let fen: Fen = fen
            .parse()
            .map_err(|e| ChessError::InvalidFen(format!("{e}")))?;
        let position: Chess = fen
            .into_position(CastlingMode::Standard)
            .map_err(|e| ChessError::InvalidFen(format!("{e}")))?;
        Ok(Self { position })
    }

    /// Get the FEN string for the current position.
    #[must_use]
    pub fn to_fen(&self) -> String {
        Fen::from_position(self.position.clone(), shakmaty::EnPassantMode::Legal).to_string()
    }

    /// Get whose turn it is to move.
    #[must_use]
    pub fn turn(&self) -> Color {
        self.position.turn().into()
    }

    /// Check if the current player is in check.
    #[must_use]
    pub fn is_check(&self) -> bool {
        self.position.is_check()
    }

    /// Check if the game is over (checkmate, stalemate, or insufficient material).
    #[must_use]
    pub fn outcome(&self) -> Option<Outcome> {
        if self.position.is_checkmate() {
            // The player to move is checkmated, so the other color wins
            Some(Outcome::Checkmate(self.turn().opposite()))
        } else if self.position.is_stalemate() {
            Some(Outcome::Stalemate)
        } else if self.position.is_insufficient_material() {
            Some(Outcome::InsufficientMaterial)
        } else {
            None
        }
    }

    /// Check if the game is over.
    #[must_use]
    pub fn is_game_over(&self) -> bool {
        self.outcome().is_some()
    }

    /// Get all legal moves in the current position.
    #[must_use]
    pub fn legal_moves(&self) -> Vec<LegalMove> {
        let legals = self.position.legal_moves();
        legals.iter().map(|m| self.move_to_legal_move(m)).collect()
    }

    /// Validate and apply a move given in UCI notation.
    ///
    /// Returns the move in SAN notation if successful.
    pub fn make_move(&mut self, uci_move: &UciMove) -> Result<String, ChessError> {
        if self.is_game_over() {
            return Err(ChessError::GameOver);
        }

        let m = self.parse_uci_move(uci_move)?;

        // Get SAN before making the move (SAN depends on position)
        let san = San::from_move(&self.position, &m);

        // Verify the move is legal
        if !self.position.is_legal(&m) {
            return Err(ChessError::IllegalMove(uci_move.to_string()));
        }

        // Apply the move
        self.position = self.position.clone().play(&m).map_err(|_| {
            // This shouldn't happen since we checked legality, but handle it
            ChessError::IllegalMove(uci_move.to_string())
        })?;

        Ok(san.to_string())
    }

    /// Validate a move without applying it.
    ///
    /// Returns the move in SAN notation if the move would be legal.
    pub fn validate_move(&self, uci_move: &UciMove) -> Result<String, ChessError> {
        if self.is_game_over() {
            return Err(ChessError::GameOver);
        }

        let m = self.parse_uci_move(uci_move)?;

        if !self.position.is_legal(&m) {
            return Err(ChessError::IllegalMove(uci_move.to_string()));
        }

        let san = San::from_move(&self.position, &m);
        Ok(san.to_string())
    }

    /// Check if a move is legal without applying it.
    #[must_use]
    pub fn is_legal_move(&self, uci_move: &UciMove) -> bool {
        self.validate_move(uci_move).is_ok()
    }

    /// Get the halfmove clock (for 50-move rule).
    #[must_use]
    pub fn halfmove_clock(&self) -> u32 {
        self.position.halfmoves()
    }

    /// Check if the 50-move rule draw can be claimed.
    #[must_use]
    pub fn can_claim_fifty_move_draw(&self) -> bool {
        self.halfmove_clock() >= 100
    }

    /// Get the fullmove number.
    #[must_use]
    pub fn fullmove_number(&self) -> u32 {
        self.position.fullmoves().get()
    }

    // --- Private helper methods ---

    /// Parse a UCI move string into a shakmaty Move.
    fn parse_uci_move(&self, uci_move: &UciMove) -> Result<Move, ChessError> {
        let shakmaty_uci: ShakmatyUciMove = uci_move
            .as_str()
            .parse()
            .map_err(|_| ChessError::InvalidUciMove(uci_move.to_string()))?;

        shakmaty_uci
            .to_move(&self.position)
            .map_err(|_| ChessError::IllegalMove(uci_move.to_string()))
    }

    /// Convert a shakmaty Move to our LegalMove type.
    fn move_to_legal_move(&self, m: &Move) -> LegalMove {
        let san = San::from_move(&self.position, m);
        let uci = ShakmatyUciMove::from_move(m, CastlingMode::Standard);

        // Check if this move would give check
        let mut pos_after = self.position.clone();
        pos_after.play_unchecked(m);
        let is_check = pos_after.is_check();

        LegalMove {
            uci: uci.to_string(),
            san: san.to_string(),
            from: m.from().map_or_else(String::new, |sq| sq.to_string()),
            to: m.to().to_string(),
            promotion: m.promotion().map(|r| r.into()),
            is_capture: m.is_capture(),
            is_check,
        }
    }
}

impl Default for ChessPosition {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_starting_position() {
        let pos = ChessPosition::new();
        assert_eq!(pos.turn(), Color::White);
        assert!(!pos.is_check());
        assert!(!pos.is_game_over());
        assert_eq!(pos.halfmove_clock(), 0);
        assert_eq!(pos.fullmove_number(), 1);
    }

    #[test]
    fn test_fen_roundtrip() {
        let original_fen = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1";
        let pos = ChessPosition::from_fen(original_fen).unwrap();
        assert_eq!(pos.turn(), Color::Black);
        // FEN may have minor formatting differences, so just verify it parses back
        let roundtrip = ChessPosition::from_fen(&pos.to_fen()).unwrap();
        assert_eq!(roundtrip.turn(), pos.turn());
    }

    #[test]
    fn test_invalid_fen() {
        let result = ChessPosition::from_fen("not a valid fen");
        assert!(result.is_err());
    }

    #[test]
    fn test_make_move() {
        let mut pos = ChessPosition::new();

        // 1. e4
        let san = pos.make_move(&UciMove::new("e2e4").unwrap()).unwrap();
        assert_eq!(san, "e4");
        assert_eq!(pos.turn(), Color::Black);

        // 1... e5
        let san = pos.make_move(&UciMove::new("e7e5").unwrap()).unwrap();
        assert_eq!(san, "e5");
        assert_eq!(pos.turn(), Color::White);

        // 2. Nf3
        let san = pos.make_move(&UciMove::new("g1f3").unwrap()).unwrap();
        assert_eq!(san, "Nf3");
    }

    #[test]
    fn test_illegal_move() {
        let mut pos = ChessPosition::new();
        // Can't move pawn 3 squares
        let result = pos.make_move(&UciMove::new("e2e5").unwrap());
        assert!(result.is_err());
        assert!(matches!(result, Err(ChessError::IllegalMove(_))));
    }

    #[test]
    fn test_validate_move() {
        let pos = ChessPosition::new();
        // e4 is legal
        assert!(pos.validate_move(&UciMove::new("e2e4").unwrap()).is_ok());
        // e5 is illegal (wrong color's pawn)
        assert!(pos.validate_move(&UciMove::new("e7e5").unwrap()).is_err());
    }

    #[test]
    fn test_legal_moves_count() {
        let pos = ChessPosition::new();
        let moves = pos.legal_moves();
        // Starting position has 20 legal moves
        assert_eq!(moves.len(), 20);
    }

    #[test]
    fn test_legal_move_info() {
        let pos = ChessPosition::new();
        let moves = pos.legal_moves();

        // Find e4
        let e4 = moves.iter().find(|m| m.uci == "e2e4").unwrap();
        assert_eq!(e4.san, "e4");
        assert_eq!(e4.from, "e2");
        assert_eq!(e4.to, "e4");
        assert!(!e4.is_capture);
        assert!(e4.promotion.is_none());
    }

    #[test]
    fn test_checkmate_fools_mate() {
        // Fool's mate position
        let mut pos = ChessPosition::new();
        pos.make_move(&UciMove::new("f2f3").unwrap()).unwrap(); // 1. f3
        pos.make_move(&UciMove::new("e7e5").unwrap()).unwrap(); // 1... e5
        pos.make_move(&UciMove::new("g2g4").unwrap()).unwrap(); // 2. g4
        pos.make_move(&UciMove::new("d8h4").unwrap()).unwrap(); // 2... Qh4#

        assert!(pos.is_game_over());
        assert_eq!(pos.outcome(), Some(Outcome::Checkmate(Color::Black)));
    }

    #[test]
    fn test_stalemate() {
        // A known stalemate position: white king on h1, black king on f2, black queen on g3
        // White to move - no legal moves but not in check
        let fen = "8/8/8/8/8/6q1/5k2/7K w - - 0 1";
        let pos = ChessPosition::from_fen(fen).unwrap();

        assert!(pos.is_game_over());
        assert_eq!(pos.outcome(), Some(Outcome::Stalemate));
        assert!(pos.legal_moves().is_empty());
    }

    #[test]
    fn test_insufficient_material() {
        // King vs King
        let fen = "8/8/8/4k3/8/8/8/4K3 w - - 0 1";
        let pos = ChessPosition::from_fen(fen).unwrap();

        assert!(pos.is_game_over());
        assert_eq!(pos.outcome(), Some(Outcome::InsufficientMaterial));
    }

    #[test]
    fn test_check_detection() {
        // Position where black is in check
        let fen = "rnbqkbnr/ppppp1pp/8/5p1Q/4P3/8/PPPP1PPP/RNB1KBNR b KQkq - 1 2";
        let pos = ChessPosition::from_fen(fen).unwrap();
        assert!(pos.is_check());
        assert!(!pos.is_game_over()); // Not checkmate, can block
    }

    #[test]
    fn test_promotion() {
        // Position with a pawn about to promote
        let fen = "8/P7/8/8/8/8/8/4K2k w - - 0 1";
        let mut pos = ChessPosition::from_fen(fen).unwrap();

        // Promote to queen
        let san = pos.make_move(&UciMove::new("a7a8q").unwrap()).unwrap();
        assert_eq!(san, "a8=Q");
    }

    #[test]
    fn test_capture_move() {
        let fen = "rnbqkbnr/ppp1pppp/8/3p4/4P3/8/PPPP1PPP/RNBQKBNR w KQkq d6 0 2";
        let pos = ChessPosition::from_fen(fen).unwrap();
        let moves = pos.legal_moves();

        // exd5 should be a capture
        let exd5 = moves.iter().find(|m| m.uci == "e4d5").unwrap();
        assert!(exd5.is_capture);
        assert_eq!(exd5.san, "exd5");
    }

    #[test]
    fn test_castling() {
        // Position where castling is legal
        let fen = "r3k2r/pppppppp/8/8/8/8/PPPPPPPP/R3K2R w KQkq - 0 1";
        let mut pos = ChessPosition::from_fen(fen).unwrap();

        // Kingside castle
        let san = pos.make_move(&UciMove::new("e1g1").unwrap()).unwrap();
        assert_eq!(san, "O-O");
    }

    #[test]
    fn test_en_passant() {
        // Position where en passant is available
        let fen = "rnbqkbnr/pppp1ppp/8/4pP2/8/8/PPPPP1PP/RNBQKBNR w KQkq e6 0 3";
        let pos = ChessPosition::from_fen(fen).unwrap();
        let moves = pos.legal_moves();

        // f5xe6 e.p. should be available
        let ep = moves.iter().find(|m| m.uci == "f5e6");
        assert!(ep.is_some());
        assert!(ep.unwrap().is_capture);
    }

    #[test]
    fn test_fifty_move_rule() {
        // Position with high halfmove clock
        let fen = "8/8/8/4k3/8/8/8/4K3 w - - 100 50";
        let pos = ChessPosition::from_fen(fen).unwrap();
        assert!(pos.can_claim_fifty_move_draw());
        assert_eq!(pos.halfmove_clock(), 100);
    }

    #[test]
    fn test_game_over_prevents_moves() {
        // Checkmate position
        let fen = "rnb1kbnr/pppp1ppp/8/4p3/6Pq/5P2/PPPPP2P/RNBQKBNR w KQkq - 1 3";
        let mut pos = ChessPosition::from_fen(fen).unwrap();

        assert!(pos.is_game_over());
        let result = pos.make_move(&UciMove::new("e2e4").unwrap());
        assert!(matches!(result, Err(ChessError::GameOver)));
    }

    #[test]
    fn test_outcome_to_status_and_result() {
        let (status, result) = Outcome::Checkmate(Color::White).to_status_and_result();
        assert_eq!(status, GameStatus::WhiteWins);
        assert_eq!(result, GameResult::Checkmate);

        let (status, result) = Outcome::Checkmate(Color::Black).to_status_and_result();
        assert_eq!(status, GameStatus::BlackWins);
        assert_eq!(result, GameResult::Checkmate);

        let (status, result) = Outcome::Stalemate.to_status_and_result();
        assert_eq!(status, GameStatus::Draw);
        assert_eq!(result, GameResult::Stalemate);

        let (status, result) = Outcome::InsufficientMaterial.to_status_and_result();
        assert_eq!(status, GameStatus::Draw);
        assert_eq!(result, GameResult::InsufficientMaterial);
    }
}
