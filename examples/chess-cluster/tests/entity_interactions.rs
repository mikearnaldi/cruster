//! Entity interaction tests for the chess cluster.
//!
//! These tests verify that entities interact correctly with each other:
//! - Matchmaking creates games and notifies players
//! - Game completion updates the leaderboard
//! - Full end-to-end flows work correctly

use std::sync::Arc;

use cruster::sharding::Sharding;
use cruster::testing::TestCluster;
use cruster::types::EntityId;

use chess_cluster::entities::{
    CancelSearchRequest, ChessGame, ChessGameClient, ConnectRequest, CreateGameRequest,
    FindMatchRequest, FindMatchResponse, GameEvent, GameEventResult, GetTopPlayersRequest,
    Leaderboard, LeaderboardClient, MakeMoveRequest, MatchFoundNotification, MatchPreferences,
    MatchmakingService, PlayerSession, RecordGameResultRequest, ResignRequest,
};
use chess_cluster::types::{
    Color, GameId, GameResult, GameResultInfo, GameStatus, PlayerId, PlayerStatus, TimeControl,
};

// =============================================================================
// Matchmaking Service Tests
// =============================================================================

#[tokio::test]
async fn test_matchmaking_queue_and_match() {
    let cluster = TestCluster::with_workflow_support().await;
    let mm_client = MatchmakingService::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let mm_entity = EntityId::new("matchmaking");

    let player1 = PlayerId::new();
    let player2 = PlayerId::new();

    // First player joins queue
    let response = mm_client
        .find_match(
            &mm_entity,
            &FindMatchRequest {
                player_id: player1,
                preferences: MatchPreferences::default(),
                rating: 1200,
            },
        )
        .await
        .unwrap();

    // First player should be queued (no opponent yet)
    match response {
        FindMatchResponse::Queued { position, .. } => {
            assert_eq!(position, 1);
        }
        FindMatchResponse::Matched { .. } => {
            panic!("expected Queued, got Matched");
        }
    }

    // Verify queue status
    let status = mm_client.get_queue_status(&mm_entity).await.unwrap();
    assert_eq!(status.players_in_queue, 1);

    // Second player joins - should immediately match
    let response = mm_client
        .find_match(
            &mm_entity,
            &FindMatchRequest {
                player_id: player2,
                preferences: MatchPreferences::default(),
                rating: 1250,
            },
        )
        .await
        .unwrap();

    match response {
        FindMatchResponse::Matched {
            opponent_id,
            is_white,
            ..
        } => {
            assert_eq!(opponent_id, player1);
            // Note: Current implementation gives white to the player who joined later
            // (player2 in this case), so is_white = true
            assert!(is_white);
        }
        FindMatchResponse::Queued { .. } => {
            panic!("expected Matched, got Queued");
        }
    }

    // Queue should now be empty
    let status = mm_client.get_queue_status(&mm_entity).await.unwrap();
    assert_eq!(status.players_in_queue, 0);
    assert_eq!(status.total_matches_made, 1);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_matchmaking_cancel_search() {
    let cluster = TestCluster::with_workflow_support().await;
    let mm_client = MatchmakingService::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let mm_entity = EntityId::new("matchmaking");
    let player = PlayerId::new();

    // Join queue
    mm_client
        .find_match(
            &mm_entity,
            &FindMatchRequest {
                player_id: player,
                preferences: MatchPreferences::default(),
                rating: 1200,
            },
        )
        .await
        .unwrap();

    // Cancel search
    mm_client
        .cancel_search(&mm_entity, &CancelSearchRequest { player_id: player })
        .await
        .unwrap();

    // Verify player is no longer in queue
    let in_queue = mm_client.is_in_queue(&mm_entity, &player).await.unwrap();
    assert!(!in_queue);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_matchmaking_different_time_controls_dont_match() {
    let cluster = TestCluster::with_workflow_support().await;
    let mm_client = MatchmakingService::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let mm_entity = EntityId::new("matchmaking");
    let player1 = PlayerId::new();
    let player2 = PlayerId::new();

    // Player 1 wants blitz
    mm_client
        .find_match(
            &mm_entity,
            &FindMatchRequest {
                player_id: player1,
                preferences: MatchPreferences {
                    time_control: TimeControl::BLITZ,
                    min_rating: None,
                    max_rating: None,
                },
                rating: 1200,
            },
        )
        .await
        .unwrap();

    // Player 2 wants rapid - should not match
    let response = mm_client
        .find_match(
            &mm_entity,
            &FindMatchRequest {
                player_id: player2,
                preferences: MatchPreferences {
                    time_control: TimeControl::RAPID,
                    min_rating: None,
                    max_rating: None,
                },
                rating: 1200,
            },
        )
        .await
        .unwrap();

    // Should be queued, not matched (different time controls)
    match response {
        FindMatchResponse::Queued { position, .. } => {
            // Position is 2 because blitz player is also in queue
            // (but they have different time controls)
            assert!(position >= 1);
        }
        FindMatchResponse::Matched { .. } => {
            panic!("should not match players with different time controls");
        }
    }

    // Queue should have both players
    let status = mm_client.get_queue_status(&mm_entity).await.unwrap();
    assert_eq!(status.players_in_queue, 2);

    cluster.shutdown().await.unwrap();
}

// =============================================================================
// Leaderboard Tests
// =============================================================================

#[tokio::test]
async fn test_leaderboard_record_game_result() {
    let cluster = TestCluster::with_workflow_support().await;
    let lb_client = Leaderboard
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let lb_entity = EntityId::new("leaderboard");
    let white = PlayerId::new();
    let black = PlayerId::new();
    let game_id = GameId::new();

    // Record a white win
    let response = lb_client
        .record_game_result(
            &lb_entity,
            &RecordGameResultRequest {
                result: GameResultInfo {
                    game_id,
                    white_player: white,
                    black_player: black,
                    status: GameStatus::WhiteWins,
                    result_reason: GameResult::Checkmate,
                    total_moves: 30,
                },
            },
        )
        .await
        .unwrap();

    // White should have gained ELO, black lost ELO
    assert!(response.white_elo_change > 0);
    assert!(response.black_elo_change < 0);
    assert_eq!(response.white_rating.wins, 1);
    assert_eq!(response.white_rating.losses, 0);
    assert_eq!(response.black_rating.wins, 0);
    assert_eq!(response.black_rating.losses, 1);

    // Verify total games
    let total = lb_client.get_total_games(&lb_entity).await.unwrap();
    assert_eq!(total, 1);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_leaderboard_rankings() {
    let cluster = TestCluster::with_workflow_support().await;
    let lb_client = Leaderboard
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let lb_entity = EntityId::new("leaderboard");

    // Create some players with varying records
    let player1 = PlayerId::new();
    let player2 = PlayerId::new();
    let player3 = PlayerId::new();

    // Player 1 beats player 2
    lb_client
        .record_game_result(
            &lb_entity,
            &RecordGameResultRequest {
                result: GameResultInfo {
                    game_id: GameId::new(),
                    white_player: player1,
                    black_player: player2,
                    status: GameStatus::WhiteWins,
                    result_reason: GameResult::Checkmate,
                    total_moves: 25,
                },
            },
        )
        .await
        .unwrap();

    // Player 1 beats player 3
    lb_client
        .record_game_result(
            &lb_entity,
            &RecordGameResultRequest {
                result: GameResultInfo {
                    game_id: GameId::new(),
                    white_player: player1,
                    black_player: player3,
                    status: GameStatus::WhiteWins,
                    result_reason: GameResult::Resignation,
                    total_moves: 15,
                },
            },
        )
        .await
        .unwrap();

    // Get top players - player1 should be #1
    let response = lb_client
        .get_top_players(&lb_entity, &GetTopPlayersRequest { limit: 10 })
        .await
        .unwrap();

    assert!(!response.players.is_empty());
    assert_eq!(response.players[0].player_id, player1);
    assert_eq!(response.players[0].rank, 1);
    assert_eq!(response.players[0].rating.wins, 2);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_leaderboard_draw_updates_both() {
    let cluster = TestCluster::with_workflow_support().await;
    let lb_client = Leaderboard
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let lb_entity = EntityId::new("leaderboard");
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Record a draw
    let response = lb_client
        .record_game_result(
            &lb_entity,
            &RecordGameResultRequest {
                result: GameResultInfo {
                    game_id: GameId::new(),
                    white_player: white,
                    black_player: black,
                    status: GameStatus::Draw,
                    result_reason: GameResult::DrawAgreement,
                    total_moves: 40,
                },
            },
        )
        .await
        .unwrap();

    // Both should have a draw recorded
    assert_eq!(response.white_rating.draws, 1);
    assert_eq!(response.black_rating.draws, 1);
    assert_eq!(response.white_rating.wins, 0);
    assert_eq!(response.black_rating.wins, 0);

    // Equal ELO players drawing should have no change
    assert_eq!(response.white_elo_change, 0);
    assert_eq!(response.black_elo_change, 0);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_leaderboard_aborted_game_rejected() {
    let cluster = TestCluster::with_workflow_support().await;
    let lb_client = Leaderboard
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let lb_entity = EntityId::new("leaderboard");

    // Try to record an aborted game - should fail
    let result = lb_client
        .record_game_result(
            &lb_entity,
            &RecordGameResultRequest {
                result: GameResultInfo {
                    game_id: GameId::new(),
                    white_player: PlayerId::new(),
                    black_player: PlayerId::new(),
                    status: GameStatus::Aborted,
                    result_reason: GameResult::Aborted,
                    total_moves: 1,
                },
            },
        )
        .await;

    assert!(result.is_err());

    cluster.shutdown().await.unwrap();
}

// =============================================================================
// Full Flow Integration Tests
// =============================================================================

#[tokio::test]
async fn test_full_matchmaking_to_game_flow() {
    let cluster = TestCluster::with_workflow_support().await;
    let sharding: Arc<dyn Sharding> = cluster.sharding().clone();
    let session_client = PlayerSession.register(Arc::clone(&sharding)).await.unwrap();
    let mm_client = MatchmakingService::new()
        .register(Arc::clone(&sharding))
        .await
        .unwrap();
    let game_client = ChessGame.register(sharding).await.unwrap();

    let mm_entity = EntityId::new("matchmaking");

    // Create two players
    let player1_id = PlayerId::new();
    let player2_id = PlayerId::new();
    let player1_session = EntityId::new(player1_id.to_string());
    let player2_session = EntityId::new(player2_id.to_string());

    // Connect both players
    session_client
        .connect(
            &player1_session,
            &ConnectRequest {
                username: "player1".to_string(),
            },
        )
        .await
        .unwrap();

    session_client
        .connect(
            &player2_session,
            &ConnectRequest {
                username: "player2".to_string(),
            },
        )
        .await
        .unwrap();

    // Player 1 joins queue
    mm_client
        .find_match(
            &mm_entity,
            &FindMatchRequest {
                player_id: player1_id,
                preferences: MatchPreferences::default(),
                rating: 1200,
            },
        )
        .await
        .unwrap();

    // Player 2 joins - should match immediately
    let match_response = mm_client
        .find_match(
            &mm_entity,
            &FindMatchRequest {
                player_id: player2_id,
                preferences: MatchPreferences::default(),
                rating: 1200,
            },
        )
        .await
        .unwrap();

    // Extract game info from match
    let (game_id, white_id, black_id) = match match_response {
        FindMatchResponse::Matched {
            game_id,
            opponent_id,
            is_white,
        } => {
            if is_white {
                (game_id, player2_id, opponent_id)
            } else {
                (game_id, opponent_id, player2_id)
            }
        }
        FindMatchResponse::Queued { .. } => panic!("expected match"),
    };

    // Create the game entity
    let game_entity = EntityId::new(game_id.to_string());
    game_client
        .create(
            &game_entity,
            &CreateGameRequest {
                white_player: white_id,
                black_player: black_id,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Notify both players about the game
    session_client
        .notify_match_found(
            &EntityId::new(white_id.to_string()),
            &MatchFoundNotification {
                game_id,
                color: Color::White,
                opponent_id: black_id,
            },
        )
        .await
        .unwrap();

    session_client
        .notify_match_found(
            &EntityId::new(black_id.to_string()),
            &MatchFoundNotification {
                game_id,
                color: Color::Black,
                opponent_id: white_id,
            },
        )
        .await
        .unwrap();

    // Verify both players are now InGame
    let white_status = session_client
        .get_status(&EntityId::new(white_id.to_string()))
        .await
        .unwrap();
    assert_eq!(
        white_status.info.as_ref().unwrap().status,
        PlayerStatus::InGame
    );
    assert_eq!(
        white_status.info.as_ref().unwrap().current_game_id,
        Some(game_id)
    );

    let black_status = session_client
        .get_status(&EntityId::new(black_id.to_string()))
        .await
        .unwrap();
    assert_eq!(
        black_status.info.as_ref().unwrap().status,
        PlayerStatus::InGame
    );

    // Play a few moves
    game_client
        .make_move(
            &game_entity,
            &MakeMoveRequest {
                player_id: white_id,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    game_client
        .make_move(
            &game_entity,
            &MakeMoveRequest {
                player_id: black_id,
                uci_move: "e7e5".to_string(),
            },
        )
        .await
        .unwrap();

    // Verify game state
    let state = game_client.get_state(&game_entity).await.unwrap();
    assert_eq!(state.moves.len(), 2);
    assert_eq!(state.status, GameStatus::InProgress);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_game_to_leaderboard_flow() {
    let cluster = TestCluster::with_workflow_support().await;
    let sharding: Arc<dyn Sharding> = cluster.sharding().clone();
    let game_client = ChessGame.register(Arc::clone(&sharding)).await.unwrap();
    let lb_client = Leaderboard.register(sharding).await.unwrap();

    let game_id = GameId::new();
    let game_entity = EntityId::new(game_id.to_string());
    let lb_entity = EntityId::new("leaderboard");

    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    game_client
        .create(
            &game_entity,
            &CreateGameRequest {
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Play a couple moves then resign
    game_client
        .make_move(
            &game_entity,
            &MakeMoveRequest {
                player_id: white,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    game_client
        .make_move(
            &game_entity,
            &MakeMoveRequest {
                player_id: black,
                uci_move: "e7e5".to_string(),
            },
        )
        .await
        .unwrap();

    // Black resigns
    let final_state = game_client
        .resign(&game_entity, &ResignRequest { player_id: black })
        .await
        .unwrap();

    assert_eq!(final_state.status, GameStatus::WhiteWins);
    assert_eq!(final_state.result_reason, Some(GameResult::Resignation));

    // Now update leaderboard with this result
    let lb_response = lb_client
        .record_game_result(
            &lb_entity,
            &RecordGameResultRequest {
                result: GameResultInfo {
                    game_id,
                    white_player: white,
                    black_player: black,
                    status: final_state.status,
                    result_reason: final_state.result_reason.unwrap(),
                    total_moves: final_state.moves.len(),
                },
            },
        )
        .await
        .unwrap();

    // White won, should have gained rating
    assert!(lb_response.white_elo_change > 0);
    assert_eq!(lb_response.white_rating.wins, 1);

    // Black lost, should have lost rating
    assert!(lb_response.black_elo_change < 0);
    assert_eq!(lb_response.black_rating.losses, 1);

    // Verify leaderboard reflects this
    let top = lb_client
        .get_top_players(&lb_entity, &GetTopPlayersRequest { limit: 10 })
        .await
        .unwrap();

    // White should be ranked higher
    assert_eq!(top.players.len(), 2);
    assert_eq!(top.players[0].player_id, white);
    assert_eq!(top.players[0].rank, 1);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multiple_games_update_rankings() {
    let cluster = TestCluster::with_workflow_support().await;
    let sharding: Arc<dyn Sharding> = cluster.sharding().clone();
    let game_client = ChessGame.register(Arc::clone(&sharding)).await.unwrap();
    let lb_client = Leaderboard.register(sharding).await.unwrap();

    let lb_entity = EntityId::new("leaderboard");

    let player_a = PlayerId::new();
    let player_b = PlayerId::new();
    let player_c = PlayerId::new();

    // Helper function to play and record a game
    async fn play_game(
        game_client: &ChessGameClient,
        lb_client: &LeaderboardClient,
        lb_entity: &EntityId,
        white: PlayerId,
        black: PlayerId,
        white_wins: bool,
    ) {
        let game_id = GameId::new();
        let game_entity = EntityId::new(game_id.to_string());

        game_client
            .create(
                &game_entity,
                &CreateGameRequest {
                    white_player: white,
                    black_player: black,
                    time_control: TimeControl::BLITZ,
                },
            )
            .await
            .unwrap();

        // Make minimum moves
        game_client
            .make_move(
                &game_entity,
                &MakeMoveRequest {
                    player_id: white,
                    uci_move: "e2e4".to_string(),
                },
            )
            .await
            .unwrap();

        game_client
            .make_move(
                &game_entity,
                &MakeMoveRequest {
                    player_id: black,
                    uci_move: "e7e5".to_string(),
                },
            )
            .await
            .unwrap();

        // Determine winner by resignation
        let resigning = if white_wins { black } else { white };
        let final_state = game_client
            .resign(
                &game_entity,
                &ResignRequest {
                    player_id: resigning,
                },
            )
            .await
            .unwrap();

        // Record result
        lb_client
            .record_game_result(
                lb_entity,
                &RecordGameResultRequest {
                    result: GameResultInfo {
                        game_id,
                        white_player: white,
                        black_player: black,
                        status: final_state.status,
                        result_reason: final_state.result_reason.unwrap(),
                        total_moves: final_state.moves.len(),
                    },
                },
            )
            .await
            .unwrap();
    }

    // A beats B
    play_game(
        &game_client,
        &lb_client,
        &lb_entity,
        player_a,
        player_b,
        true,
    )
    .await;

    // A beats C
    play_game(
        &game_client,
        &lb_client,
        &lb_entity,
        player_a,
        player_c,
        true,
    )
    .await;

    // B beats C
    play_game(
        &game_client,
        &lb_client,
        &lb_entity,
        player_b,
        player_c,
        true,
    )
    .await;

    // Check final rankings
    let top = lb_client
        .get_top_players(&lb_entity, &GetTopPlayersRequest { limit: 10 })
        .await
        .unwrap();

    assert_eq!(top.players.len(), 3);

    // A should be #1 (2 wins, 0 losses)
    assert_eq!(top.players[0].player_id, player_a);
    assert_eq!(top.players[0].rating.wins, 2);
    assert_eq!(top.players[0].rating.losses, 0);

    // B should be #2 (1 win, 1 loss)
    assert_eq!(top.players[1].player_id, player_b);
    assert_eq!(top.players[1].rating.wins, 1);
    assert_eq!(top.players[1].rating.losses, 1);

    // C should be #3 (0 wins, 2 losses)
    assert_eq!(top.players[2].player_id, player_c);
    assert_eq!(top.players[2].rating.wins, 0);
    assert_eq!(top.players[2].rating.losses, 2);

    // Verify total games
    let total = lb_client.get_total_games(&lb_entity).await.unwrap();
    assert_eq!(total, 3);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_player_session_game_lifecycle() {
    let cluster = TestCluster::with_workflow_support().await;
    let sharding: Arc<dyn Sharding> = cluster.sharding().clone();
    let session_client = PlayerSession.register(Arc::clone(&sharding)).await.unwrap();
    let game_client = ChessGame.register(sharding).await.unwrap();

    let white_id = PlayerId::new();
    let black_id = PlayerId::new();
    let game_id = GameId::new();

    let white_session = EntityId::new(white_id.to_string());
    let black_session = EntityId::new(black_id.to_string());
    let game_entity = EntityId::new(game_id.to_string());

    // Connect both players
    session_client
        .connect(
            &white_session,
            &ConnectRequest {
                username: "white_player".to_string(),
            },
        )
        .await
        .unwrap();

    session_client
        .connect(
            &black_session,
            &ConnectRequest {
                username: "black_player".to_string(),
            },
        )
        .await
        .unwrap();

    // Verify initial status
    let status = session_client.get_status(&white_session).await.unwrap();
    assert_eq!(status.info.as_ref().unwrap().status, PlayerStatus::Online);

    // Create game
    game_client
        .create(
            &game_entity,
            &CreateGameRequest {
                white_player: white_id,
                black_player: black_id,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Notify players of game start
    session_client
        .notify_game_event(
            &white_session,
            &GameEvent::GameStarted {
                game_id,
                opponent: black_id,
                your_color: Color::White,
            },
        )
        .await
        .unwrap();

    session_client
        .notify_game_event(
            &black_session,
            &GameEvent::GameStarted {
                game_id,
                opponent: white_id,
                your_color: Color::Black,
            },
        )
        .await
        .unwrap();

    // Verify both are in game
    let status = session_client.get_status(&white_session).await.unwrap();
    assert_eq!(status.info.as_ref().unwrap().status, PlayerStatus::InGame);
    assert_eq!(status.info.as_ref().unwrap().current_game_id, Some(game_id));

    // Play the game
    game_client
        .make_move(
            &game_entity,
            &MakeMoveRequest {
                player_id: white_id,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    game_client
        .make_move(
            &game_entity,
            &MakeMoveRequest {
                player_id: black_id,
                uci_move: "e7e5".to_string(),
            },
        )
        .await
        .unwrap();

    // Black resigns
    let final_state = game_client
        .resign(
            &game_entity,
            &ResignRequest {
                player_id: black_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(final_state.status, GameStatus::WhiteWins);

    // Notify players of game end
    session_client
        .notify_game_event(
            &white_session,
            &GameEvent::GameEnded {
                game_id,
                result: GameEventResult::Won,
                reason: "resignation".to_string(),
            },
        )
        .await
        .unwrap();

    session_client
        .notify_game_event(
            &black_session,
            &GameEvent::GameEnded {
                game_id,
                result: GameEventResult::Lost,
                reason: "resignation".to_string(),
            },
        )
        .await
        .unwrap();

    // Verify both are back online (not in game)
    let status = session_client.get_status(&white_session).await.unwrap();
    assert_eq!(status.info.as_ref().unwrap().status, PlayerStatus::Online);
    assert_eq!(status.info.as_ref().unwrap().current_game_id, None);

    let status = session_client.get_status(&black_session).await.unwrap();
    assert_eq!(status.info.as_ref().unwrap().status, PlayerStatus::Online);

    cluster.shutdown().await.unwrap();
}
