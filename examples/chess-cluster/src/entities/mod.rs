//! Entity definitions for the chess cluster.
//!
//! ## Entities
//!
//! - `PlayerSession` - In-memory state tracking connected players
//! - `ChessGame` - Persisted game state with durable workflows
//! - `MatchmakingService` - Stateless service for pairing players
//! - `Leaderboard` - Singleton for global rankings
//!
//! ## Traits
//!
//! - `Auditable` - Shared audit logging capability

pub mod chess_game;
pub mod leaderboard;
pub mod matchmaking;
pub mod player_session;
pub mod traits;

pub use chess_game::{
    AbortRequest, ChessGame, ChessGameClient, ChessGameError, ChessGameState, CreateGameRequest,
    CreateGameResponse, DrawAcceptRequest, DrawOfferRequest, GetLegalMovesRequest,
    HandleTimeoutRequest, MakeMoveRequest, MakeMoveResponse, ResignRequest,
};
pub use leaderboard::{
    GetRankingsAroundRequest, GetRankingsAroundResponse, GetTopPlayersRequest,
    GetTopPlayersResponse, Leaderboard, LeaderboardClient, LeaderboardError, LeaderboardState,
    RankedPlayer, RecordGameResultRequest, RecordGameResultResponse,
};
pub use matchmaking::{
    CancelSearchRequest, FindMatchRequest, FindMatchResponse, MatchPreferences, MatchmakingError,
    MatchmakingService, MatchmakingServiceClient, MatchmakingState, QueueStatus, QueuedPlayer,
    TimeControlQueueInfo,
};
pub use player_session::{
    ConnectRequest, ConnectResponse, GameEvent, GameEventResult, MatchFoundNotification,
    PlayerSession, PlayerSessionClient, PlayerSessionError, PlayerSessionState, StatusResponse,
};
pub use traits::{AuditEntry, AuditLog, Auditable, GetAuditLogRequest, GetAuditLogResponse};
