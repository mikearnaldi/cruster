//! Entity definitions for the chess cluster.
//!
//! All entities use the pure-RPC pattern: no framework-managed state, no workflows,
//! no activities. State is held directly via `Arc<Mutex<...>>`.
//!
//! ## Entities
//!
//! - `PlayerSession` - Ephemeral session state tracking connected players
//! - `ChessGame` - Ephemeral game state with move validation
//! - `MatchmakingService` - Ephemeral queue for pairing players
//! - `Leaderboard` - Singleton for global rankings
//!
//! ## RPC Groups
//!
//! - `Auditable` - Shared audit logging capability (`#[rpc_group]`, composable into entities)

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
    NotifyGameEventRequest, PlayerSession, PlayerSessionClient, PlayerSessionError,
    PlayerSessionState, StatusResponse,
};
pub use traits::{AuditEntry, AuditLog, Auditable, GetAuditLogRequest, GetAuditLogResponse};
