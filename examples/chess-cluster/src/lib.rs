//! # Chess Cluster
//!
//! A distributed chess server demonstrating all features of `cruster`.
//!
//! ## Features
//!
//! - **Stateless Entity**: `MatchmakingService` - pairs players into games
//! - **Stateful Entity (in-memory)**: `PlayerSession` - tracks connected player state
//! - **Persisted State**: `ChessGame` - board state survives crashes
//! - **Workflows (durable)**: `ChessGame::make_move` - move with validation, persisted
//! - **Singletons**: `Leaderboard` - single instance for global rankings
//! - **Streaming**: `ChessGame::watch` - live game event stream
//! - **Scheduled Messages**: Move timeout - auto-forfeit after configurable delay

pub mod chess;
pub mod entities;
pub mod types;

// API and CLI modules will be added in later milestones
// pub mod api;
// pub mod cli;
