//! # Chess Cluster
//!
//! A distributed chess server demonstrating all features of `cruster`.
//!
//! ## Features
//!
//! - **Pure-RPC Entity**: `MatchmakingService` - pairs players into games
//! - **Pure-RPC Entity**: `PlayerSession` - tracks connected player state (ephemeral)
//! - **Pure-RPC Entity**: `ChessGame` - board state with move validation
//! - **Singletons**: `Leaderboard` - single instance for global rankings
//! - **RPC Groups**: `Auditable` - composable audit logging capability

pub mod chess;
pub mod entities;
pub mod types;

// API and CLI modules will be added in later milestones
// pub mod api;
// pub mod cli;
