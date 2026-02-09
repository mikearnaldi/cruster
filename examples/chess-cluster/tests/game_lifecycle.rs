//! Integration tests for ChessGame and PlayerSession entities using TestCluster.
//!
//! These tests verify the core game lifecycle functionality:
//! - Creating games
//! - Making moves
//! - Resignation
//! - Draw offers and acceptance
//! - Player session management

use std::sync::Arc;

use cruster::sharding::Sharding;
use cruster::testing::TestCluster;
use cruster::types::EntityId;

use chess_cluster::entities::{
    AbortRequest, ChessGame, ConnectRequest, CreateGameRequest, DrawAcceptRequest,
    DrawOfferRequest, MakeMoveRequest, MatchFoundNotification, PlayerSession, ResignRequest,
};
use chess_cluster::types::{
    Color, GameId, GameResult, GameStatus, PlayerId, PlayerStatus, TimeControl,
};

// =============================================================================
// ChessGame Entity Tests
// =============================================================================

#[tokio::test]
async fn test_create_game() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let white = PlayerId::new();
    let black = PlayerId::new();

    let response = client
        .create(
            &EntityId::new(game_id.to_string()),
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    assert_eq!(response.game_id, game_id);
    assert_eq!(response.state.white_player, white);
    assert_eq!(response.state.black_player, black);
    assert_eq!(response.state.status, GameStatus::InProgress);
    assert!(response.state.moves.is_empty());

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_get_game_state() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game first
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Get state
    let state = client.get_state(&entity_id, &game_id).await.unwrap();

    assert_eq!(state.white_player, white);
    assert_eq!(state.black_player, black);
    assert_eq!(state.status, GameStatus::InProgress);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_make_move() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // White plays e4
    let response = client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: white,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    assert_eq!(response.san, "e4");
    assert_eq!(response.uci, "e2e4");
    assert!(!response.game_over);
    assert_eq!(response.status, GameStatus::InProgress);

    // Verify state updated
    let state = client.get_state(&entity_id, &game_id).await.unwrap();
    assert_eq!(state.moves.len(), 1);
    assert_eq!(state.moves[0].san, "e4");
    assert_eq!(state.moves[0].color, Color::White);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multiple_moves() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // 1. e4
    client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: white,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    // 1... e5
    let response = client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: black,
                uci_move: "e7e5".to_string(),
            },
        )
        .await
        .unwrap();

    assert_eq!(response.san, "e5");
    assert!(!response.game_over);

    // 2. Nf3
    let response = client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: white,
                uci_move: "g1f3".to_string(),
            },
        )
        .await
        .unwrap();

    assert_eq!(response.san, "Nf3");

    // Verify state
    let state = client.get_state(&entity_id, &game_id).await.unwrap();
    assert_eq!(state.moves.len(), 3);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_wrong_turn_rejected() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Black tries to move first (should fail)
    let result = client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: black,
                uci_move: "e7e5".to_string(),
            },
        )
        .await;

    assert!(result.is_err());

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_illegal_move_rejected() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Try illegal move (pawn 3 squares)
    let result = client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: white,
                uci_move: "e2e5".to_string(),
            },
        )
        .await;

    assert!(result.is_err());

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_resignation() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Make at least 2 moves so game can't be aborted
    client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: white,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: black,
                uci_move: "e7e5".to_string(),
            },
        )
        .await
        .unwrap();

    // White resigns
    let state = client
        .resign(
            &entity_id,
            &ResignRequest {
                game_id,
                player_id: white,
            },
        )
        .await
        .unwrap();

    assert_eq!(state.status, GameStatus::BlackWins);
    assert_eq!(state.result_reason, Some(GameResult::Resignation));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_draw_offer_and_accept() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Make a move
    client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: white,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    // White offers draw
    client
        .offer_draw(
            &entity_id,
            &DrawOfferRequest {
                game_id,
                player_id: white,
            },
        )
        .await
        .unwrap();

    // Verify draw offer is recorded
    let state = client.get_state(&entity_id, &game_id).await.unwrap();
    assert!(state.draw_offer.is_some());
    assert_eq!(state.draw_offer.unwrap().offered_by, white);

    // Black accepts draw
    let state = client
        .accept_draw(
            &entity_id,
            &DrawAcceptRequest {
                game_id,
                player_id: black,
            },
        )
        .await
        .unwrap();

    assert_eq!(state.status, GameStatus::Draw);
    assert_eq!(state.result_reason, Some(GameResult::DrawAgreement));

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_cannot_accept_own_draw_offer() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // White offers draw
    client
        .offer_draw(
            &entity_id,
            &DrawOfferRequest {
                game_id,
                player_id: white,
            },
        )
        .await
        .unwrap();

    // White tries to accept their own offer (should fail)
    let result = client
        .accept_draw(
            &entity_id,
            &DrawAcceptRequest {
                game_id,
                player_id: white,
            },
        )
        .await;

    assert!(result.is_err());

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_move_clears_draw_offer() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Make first move
    client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: white,
                uci_move: "e2e4".to_string(),
            },
        )
        .await
        .unwrap();

    // Black offers draw
    client
        .offer_draw(
            &entity_id,
            &DrawOfferRequest {
                game_id,
                player_id: black,
            },
        )
        .await
        .unwrap();

    // Black makes a move (clears draw offer)
    client
        .make_move(
            &entity_id,
            &MakeMoveRequest {
                game_id,
                player_id: black,
                uci_move: "e7e5".to_string(),
            },
        )
        .await
        .unwrap();

    // Draw offer should be cleared
    let state = client.get_state(&entity_id, &game_id).await.unwrap();
    assert!(state.draw_offer.is_none());

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_game_abort_early() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = ChessGame::new()
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let game_id = GameId::new();
    let entity_id = EntityId::new(game_id.to_string());
    let white = PlayerId::new();
    let black = PlayerId::new();

    // Create game
    client
        .create(
            &entity_id,
            &CreateGameRequest {
                game_id,
                white_player: white,
                black_player: black,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    // Abort before 2 moves (should succeed)
    let state = client
        .abort(
            &entity_id,
            &AbortRequest {
                game_id,
                player_id: white,
            },
        )
        .await
        .unwrap();

    assert_eq!(state.status, GameStatus::Aborted);
    assert_eq!(state.result_reason, Some(GameResult::Aborted));

    cluster.shutdown().await.unwrap();
}

// =============================================================================
// PlayerSession Entity Tests
// =============================================================================

#[tokio::test]
async fn test_player_connect() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = PlayerSession
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let player_id = PlayerId::new();
    let entity_id = EntityId::new(player_id.to_string());

    let response = client
        .connect(
            &entity_id,
            &ConnectRequest {
                username: "alice".to_string(),
            },
        )
        .await
        .unwrap();

    assert_eq!(response.player_id, player_id);
    assert_eq!(response.username, "alice");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_player_get_status() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = PlayerSession
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let player_id = PlayerId::new();
    let entity_id = EntityId::new(player_id.to_string());

    client
        .connect(
            &entity_id,
            &ConnectRequest {
                username: "bob".to_string(),
            },
        )
        .await
        .unwrap();

    let status = client.get_status(&entity_id).await.unwrap();

    assert!(status.connected);
    assert!(status.info.is_some());
    assert_eq!(status.info.unwrap().username, "bob");

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_player_join_and_leave_queue() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = PlayerSession
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let player_id = PlayerId::new();
    let entity_id = EntityId::new(player_id.to_string());

    client
        .connect(
            &entity_id,
            &ConnectRequest {
                username: "charlie".to_string(),
            },
        )
        .await
        .unwrap();

    let position = client.join_matchmaking_queue(&entity_id).await.unwrap();
    assert_eq!(position, 1);

    let status = client.get_status(&entity_id).await.unwrap();
    assert!(status.info.is_some());
    assert_eq!(status.info.as_ref().unwrap().status, PlayerStatus::InQueue);

    client.leave_queue(&entity_id).await.unwrap();

    let status = client.get_status(&entity_id).await.unwrap();
    assert_eq!(status.info.as_ref().unwrap().status, PlayerStatus::Online);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_player_cannot_join_queue_while_in_game() {
    let cluster = TestCluster::with_workflow_support().await;
    let sharding: Arc<dyn Sharding> = cluster.sharding().clone();
    let session_client = PlayerSession.register(Arc::clone(&sharding)).await.unwrap();
    let game_client = ChessGame::new().register(sharding).await.unwrap();

    let player_id = PlayerId::new();
    let opponent_id = PlayerId::new();
    let session_entity_id = EntityId::new(player_id.to_string());
    let game_id = GameId::new();
    let game_entity_id = EntityId::new(game_id.to_string());

    session_client
        .connect(
            &session_entity_id,
            &ConnectRequest {
                username: "dave".to_string(),
            },
        )
        .await
        .unwrap();

    game_client
        .create(
            &game_entity_id,
            &CreateGameRequest {
                game_id,
                white_player: player_id,
                black_player: opponent_id,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    session_client
        .notify_match_found(
            &session_entity_id,
            &MatchFoundNotification {
                game_id,
                color: Color::White,
                opponent_id,
            },
        )
        .await
        .unwrap();

    let result = session_client
        .join_matchmaking_queue(&session_entity_id)
        .await;

    assert!(result.is_err());

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_player_disconnect() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = PlayerSession
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let player_id = PlayerId::new();
    let entity_id = EntityId::new(player_id.to_string());

    client
        .connect(
            &entity_id,
            &ConnectRequest {
                username: "eve".to_string(),
            },
        )
        .await
        .unwrap();

    client.disconnect(&entity_id).await.unwrap();

    let status = client.get_status(&entity_id).await.unwrap();
    assert!(!status.connected);

    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_player_heartbeat() {
    let cluster = TestCluster::with_workflow_support().await;
    let client = PlayerSession
        .register(cluster.sharding().clone())
        .await
        .unwrap();

    let player_id = PlayerId::new();
    let entity_id = EntityId::new(player_id.to_string());

    client
        .connect(
            &entity_id,
            &ConnectRequest {
                username: "frank".to_string(),
            },
        )
        .await
        .unwrap();

    client.heartbeat(&entity_id).await.unwrap();

    let status = client.get_status(&entity_id).await.unwrap();
    assert!(status.connected);

    cluster.shutdown().await.unwrap();
}

// =============================================================================
// Multi-Entity Interaction Tests
// =============================================================================

#[tokio::test]
async fn test_game_notifies_player_session() {
    let cluster = TestCluster::with_workflow_support().await;
    let sharding: Arc<dyn Sharding> = cluster.sharding().clone();
    let session_client = PlayerSession.register(Arc::clone(&sharding)).await.unwrap();
    let game_client = ChessGame::new().register(sharding).await.unwrap();

    let white_id = PlayerId::new();
    let black_id = PlayerId::new();
    let game_id = GameId::new();

    let white_session = EntityId::new(white_id.to_string());
    let black_session = EntityId::new(black_id.to_string());
    let game_entity = EntityId::new(game_id.to_string());

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

    game_client
        .create(
            &game_entity,
            &CreateGameRequest {
                game_id,
                white_player: white_id,
                black_player: black_id,
                time_control: TimeControl::BLITZ,
            },
        )
        .await
        .unwrap();

    use chess_cluster::entities::GameEvent;

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

    let white_status = session_client.get_status(&white_session).await.unwrap();
    assert_eq!(
        white_status.info.as_ref().unwrap().status,
        PlayerStatus::InGame
    );
    assert_eq!(
        white_status.info.as_ref().unwrap().current_game_id,
        Some(game_id)
    );

    let black_status = session_client.get_status(&black_session).await.unwrap();
    assert_eq!(
        black_status.info.as_ref().unwrap().status,
        PlayerStatus::InGame
    );

    cluster.shutdown().await.unwrap();
}
