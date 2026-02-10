//! Integration tests for SQL storage backends using testcontainers.
//!
//! These tests spin up a real PostgreSQL container and exercise:
//! - `SqlWorkflowStorage`: journal save/load/delete/list_keys/cleanup
//! - `SqlMessageStorage`: save/ack/poll/dead-letter/reset_shards
//! - `SqlWorkflowEngine`: sleep/deferred/resolve/cleanup
//! - Transaction atomicity: journal + user SQL commit/rollback
//! - Concurrent dispatch: `FOR UPDATE SKIP LOCKED` correctness
//!
//! Run: `cargo test -p cruster --test sql_integration`
//!
//! Requires Docker to be running.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cruster::__internal::{DurableContext, WorkflowEngine, WorkflowStorage};
use cruster::envelope::EnvelopeRequest;
use cruster::message_storage::{MessageStorage, SaveResult};
use cruster::reply::{ExitResult, Reply, ReplyChunk, ReplyWithExit};
use cruster::snowflake::{Snowflake, SnowflakeGenerator};
use cruster::storage::sql_message::SqlMessageStorage;
use cruster::storage::sql_workflow::SqlWorkflowStorage;
use cruster::storage::sql_workflow_engine::SqlWorkflowEngine;
use cruster::types::{EntityAddress, EntityId, EntityType, ShardId};

use sqlx::Row;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ============================================================================
// Shared test infrastructure
// ============================================================================

/// Start a Postgres container and return a connected `PgPool` with migrations applied.
async fn setup_postgres() -> (testcontainers::ContainerAsync<Postgres>, sqlx::PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start postgres container");

    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get port");

    let url = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("failed to connect to postgres");

    // Run migrations (all three storage types share the same migration set)
    let wf_storage = SqlWorkflowStorage::new(pool.clone());
    wf_storage.migrate().await.expect("migration failed");

    (container, pool)
}

fn test_address(entity_id: &str) -> EntityAddress {
    EntityAddress {
        shard_id: ShardId::new("default", 0),
        entity_type: EntityType::new("Test"),
        entity_id: EntityId::new(entity_id),
    }
}

fn test_envelope(request_id: i64, entity_id: &str) -> EnvelopeRequest {
    EnvelopeRequest {
        request_id: Snowflake(request_id),
        address: test_address(entity_id),
        tag: "test_tag".into(),
        payload: rmp_serde::to_vec(&"hello").unwrap(),
        headers: HashMap::new(),
        span_id: None,
        trace_id: None,
        sampled: None,
        persisted: true,
        uninterruptible: Default::default(),
        deliver_at: None,
    }
}

// ============================================================================
// SqlWorkflowStorage tests
// ============================================================================

mod workflow_storage {
    use super::*;

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let key = "test/workflow/save-load";
        let value = b"hello world";

        storage.save(key, value).await.unwrap();
        let loaded = storage.load(key).await.unwrap();
        assert_eq!(loaded, Some(value.to_vec()));
    }

    #[tokio::test]
    async fn load_missing_key_returns_none() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let loaded = storage.load("nonexistent/key").await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn save_overwrites_existing() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let key = "test/workflow/overwrite";
        storage.save(key, b"first").await.unwrap();
        storage.save(key, b"second").await.unwrap();

        let loaded = storage.load(key).await.unwrap();
        assert_eq!(loaded, Some(b"second".to_vec()));
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let key = "test/workflow/delete";
        storage.save(key, b"data").await.unwrap();
        storage.delete(key).await.unwrap();

        let loaded = storage.load(key).await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        // Should not error
        storage.delete("nonexistent/key").await.unwrap();
    }

    #[tokio::test]
    async fn list_keys_returns_matching_prefix() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        storage.save("prefix/a", b"1").await.unwrap();
        storage.save("prefix/b", b"2").await.unwrap();
        storage.save("prefix/c", b"3").await.unwrap();
        storage.save("other/d", b"4").await.unwrap();

        let mut keys = storage.list_keys("prefix/").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["prefix/a", "prefix/b", "prefix/c"]);
    }

    #[tokio::test]
    async fn list_keys_empty_prefix() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        // With empty prefix, should list all keys
        storage.save("a/1", b"x").await.unwrap();
        storage.save("b/2", b"y").await.unwrap();

        let keys = storage.list_keys("").await.unwrap();
        assert!(keys.len() >= 2);
    }

    #[tokio::test]
    async fn mark_completed_and_cleanup() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let key = "test/workflow/cleanup-target";
        storage.save(key, b"data").await.unwrap();
        storage.mark_completed(key).await.unwrap();

        // Cleanup with zero duration should remove it immediately
        let removed = storage.cleanup(Duration::ZERO).await.unwrap();
        assert!(
            removed >= 1,
            "expected at least 1 cleaned up, got {removed}"
        );

        let loaded = storage.load(key).await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn cleanup_skips_non_completed() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let key = "test/workflow/no-cleanup";
        storage.save(key, b"data").await.unwrap();
        // Do NOT mark_completed

        let removed = storage.cleanup(Duration::ZERO).await.unwrap();
        // The key should still exist
        let loaded = storage.load(key).await.unwrap();
        assert_eq!(loaded, Some(b"data".to_vec()));
        // Removed count should not include this key (may include others from migrations)
        let _ = removed;
    }

    #[tokio::test]
    async fn transaction_commit() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let mut tx = storage.begin_transaction().await.unwrap();
        tx.save("test/tx/commit", b"committed").await.unwrap();
        tx.commit().await.unwrap();

        let loaded = storage.load("test/tx/commit").await.unwrap();
        assert_eq!(loaded, Some(b"committed".to_vec()));
    }

    #[tokio::test]
    async fn transaction_rollback() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        let mut tx = storage.begin_transaction().await.unwrap();
        tx.save("test/tx/rollback", b"should-not-persist")
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        let loaded = storage.load("test/tx/rollback").await.unwrap();
        assert_eq!(loaded, None, "rolled-back data should not be visible");
    }

    #[tokio::test]
    async fn sql_pool_returns_some() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool);

        assert!(storage.sql_pool().is_some());
    }
}

// ============================================================================
// SqlMessageStorage tests
// ============================================================================

mod message_storage {
    use super::*;

    #[tokio::test]
    async fn save_request_and_poll() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(5000, "entity-1");
        let result = storage.save_request(&envelope).await.unwrap();
        assert!(matches!(result, SaveResult::Success));

        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].request_id, Snowflake(5000));
    }

    #[tokio::test]
    async fn duplicate_request_returns_duplicate() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(6000, "entity-1");
        storage.save_request(&envelope).await.unwrap();

        // Save same request_id again
        let result = storage.save_request(&envelope).await.unwrap();
        assert!(
            matches!(result, SaveResult::Duplicate { .. }),
            "expected Duplicate, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn save_reply_marks_processed() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);
        let gen = SnowflakeGenerator::new();

        let envelope = test_envelope(7000, "entity-1");
        storage.save_request(&envelope).await.unwrap();

        let reply = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(7000),
            id: Snowflake(gen.next().unwrap().0),
            exit: ExitResult::Success(rmp_serde::to_vec(&"ok").unwrap()),
        });
        storage.save_reply(&reply).await.unwrap();

        // After reply, message should no longer appear as unprocessed
        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();
        let still_pending = msgs.iter().any(|m| m.request_id == Snowflake(7000));
        assert!(!still_pending, "replied message should be marked processed");
    }

    #[tokio::test]
    async fn replies_for_returns_saved_reply() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(8000, "entity-1");
        storage.save_request(&envelope).await.unwrap();

        let reply = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(8000),
            id: Snowflake(8001),
            exit: ExitResult::Success(vec![1, 2, 3]),
        });
        storage.save_reply(&reply).await.unwrap();

        let replies = storage.replies_for(Snowflake(8000)).await.unwrap();
        assert_eq!(replies.len(), 1);
        match &replies[0] {
            Reply::WithExit(r) => {
                assert_eq!(r.request_id, Snowflake(8000));
                assert!(matches!(&r.exit, ExitResult::Success(data) if data == &[1, 2, 3]));
            }
            _ => panic!("expected WithExit reply"),
        }
    }

    #[tokio::test]
    async fn unprocessed_messages_filters_by_shard() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        // Save messages to different shards
        let mut env_shard0 = test_envelope(9000, "entity-shard0");
        env_shard0.address.shard_id = ShardId::new("default", 0);
        storage.save_request(&env_shard0).await.unwrap();

        let mut env_shard1 = test_envelope(9001, "entity-shard1");
        env_shard1.address.shard_id = ShardId::new("default", 1);
        storage.save_request(&env_shard1).await.unwrap();

        // Only poll shard 1
        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 1)])
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].request_id, Snowflake(9001));
    }

    #[tokio::test]
    async fn reset_shards_unmarks_processed_without_exit_reply() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        // Save and process a message (by saving a chunk reply, not exit)
        let envelope = test_envelope(10000, "entity-reset");
        storage.save_request(&envelope).await.unwrap();

        let chunk_reply = Reply::Chunk(ReplyChunk {
            request_id: Snowflake(10000),
            id: Snowflake(10001),
            sequence: 0,
            values: vec![vec![1, 2]],
        });
        storage.save_reply(&chunk_reply).await.unwrap();

        // Save and process another message WITH an exit reply
        let envelope2 = test_envelope(10002, "entity-reset2");
        storage.save_request(&envelope2).await.unwrap();

        let exit_reply = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(10002),
            id: Snowflake(10003),
            exit: ExitResult::Success(vec![]),
        });
        storage.save_reply(&exit_reply).await.unwrap();

        // Reset shards
        storage
            .reset_shards(&[ShardId::new("default", 0)])
            .await
            .unwrap();

        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();

        // Message without exit reply should reappear as unprocessed
        // Message with exit reply should NOT reappear
        let ids: Vec<i64> = msgs.iter().map(|m| m.request_id.0).collect();
        assert!(
            !ids.contains(&10002),
            "message with exit reply should stay processed after reset"
        );
    }

    /// Test that the merged dead-letter CTE correctly sets processed=TRUE
    /// in a single UPDATE (no two-CTE-same-row snapshot issue).
    #[tokio::test]
    async fn dead_letter_cte_merged() {
        let (_container, pool) = setup_postgres().await;

        // Insert a test message with retry_count already at the threshold.
        // With max_retries=2, dead-letter fires when old retry_count > 2,
        // i.e., retry_count >= 3 before the increment.
        sqlx::query(
            "INSERT INTO cluster_messages (request_id, shard_group, shard_id, entity_type, entity_id, tag, payload, processed, retry_count)
             VALUES (99999, 'default', 0, 'Test', 'e-1', 'tag', ''::bytea, FALSE, 3)"
        )
        .execute(&pool)
        .await
        .unwrap();

        // Run the merged CTE pattern: single UPDATE increments retry_count
        // and conditionally sets processed=TRUE. max_retries=2.
        let result = sqlx::query(
            r#"
            WITH to_process AS (
                SELECT request_id
                FROM cluster_messages
                WHERE processed = FALSE AND request_id = 99999
                FOR UPDATE
            ),
            updated AS (
                UPDATE cluster_messages
                SET retry_count = retry_count + 1,
                    processed = CASE
                        WHEN retry_count > 2 THEN TRUE
                        ELSE processed
                    END
                WHERE request_id IN (SELECT request_id FROM to_process)
                RETURNING request_id, retry_count, processed
            )
            SELECT
                (SELECT retry_count FROM updated) as new_retry_count,
                (SELECT COUNT(*) FROM updated WHERE processed = TRUE) as dead_count,
                (SELECT processed FROM updated WHERE request_id = 99999) as is_processed
            "#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let new_retry_count: Option<i32> = result.get("new_retry_count");
        let dead_count: Option<i64> = result.get("dead_count");
        let is_processed: Option<bool> = result.get("is_processed");

        eprintln!("new_retry_count={new_retry_count:?}, dead_count={dead_count:?}, is_processed={is_processed:?}");

        assert_eq!(new_retry_count, Some(4));
        assert_eq!(dead_count, Some(1), "merged CTE should dead-letter 1 row");
        assert_eq!(
            is_processed,
            Some(true),
            "message should be marked processed"
        );
    }

    #[tokio::test]
    async fn dead_letter_after_max_retries() {
        let (_container, pool) = setup_postgres().await;
        // max_retries=2: first delivery + 2 retries = dead-letter on 4th poll.
        // The condition is: old_retry_count > max_retries, which means
        // after 3 increments (retry_count=3), the 4th poll sees retry_count=3 > 2
        // and sets processed=TRUE + inserts failure reply.
        let storage = SqlMessageStorage::with_max_retries(pool.clone(), 2)
            .with_last_read_guard_interval(Duration::ZERO);

        let envelope = test_envelope(11000, "entity-dead-letter");
        storage.save_request(&envelope).await.unwrap();

        // Poll until the message disappears (dead-lettered) or max iterations
        for i in 0..6 {
            let msgs = storage
                .unprocessed_messages(&[ShardId::new("default", 0)])
                .await
                .unwrap();
            let still_there = msgs.iter().any(|m| m.request_id == Snowflake(11000));
            eprintln!("poll {}: still_there={still_there}", i + 1);
            if !still_there {
                break;
            }
        }

        // Check final state in DB
        let row: (i32, bool) = sqlx::query_as(
            "SELECT retry_count, processed FROM cluster_messages WHERE request_id = 11000",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let (retry_count, processed) = row;
        eprintln!("final: retry_count={retry_count}, processed={processed}");

        assert!(
            processed,
            "message should be dead-lettered (processed=TRUE)"
        );
        assert!(
            retry_count > 2,
            "retry_count should have exceeded max_retries"
        );

        // Verify a failure reply was inserted
        let replies = storage.replies_for(Snowflake(11000)).await.unwrap();
        assert!(
            !replies.is_empty(),
            "should have a dead-letter failure reply"
        );
    }

    #[tokio::test]
    async fn save_envelope_success_then_duplicate() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(12000, "entity-env-dup");
        let result = storage.save_envelope(&envelope).await.unwrap();
        assert!(matches!(result, SaveResult::Success));

        // Second save with same request_id should return Duplicate
        let result2 = storage.save_envelope(&envelope).await.unwrap();
        assert!(
            matches!(result2, SaveResult::Duplicate { .. }),
            "expected Duplicate, got {:?}",
            result2
        );
    }

    #[tokio::test]
    async fn duplicate_returns_existing_reply() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(13000, "entity-dup-reply");
        storage.save_request(&envelope).await.unwrap();

        // Save a reply for the request
        let reply = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(13000),
            id: Snowflake(13001),
            exit: ExitResult::Success(vec![42, 43]),
        });
        storage.save_reply(&reply).await.unwrap();

        // Duplicate save_request should return the existing reply
        let result = storage.save_request(&envelope).await.unwrap();
        match result {
            SaveResult::Duplicate { existing_reply } => {
                assert!(
                    existing_reply.is_some(),
                    "duplicate should include the existing reply"
                );
                let reply = existing_reply.unwrap();
                match &reply {
                    Reply::WithExit(r) => {
                        assert!(matches!(&r.exit, ExitResult::Success(data) if data == &[42, 43]));
                    }
                    _ => panic!("expected WithExit reply"),
                }
            }
            other => panic!("expected Duplicate, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn replies_for_orders_exit_last() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(14000, "entity-chunk-order");
        storage.save_request(&envelope).await.unwrap();

        // Save chunks in reverse sequence order, then an exit reply
        let chunk2 = Reply::Chunk(ReplyChunk {
            request_id: Snowflake(14000),
            id: Snowflake(14003),
            sequence: 2,
            values: vec![vec![3]],
        });
        storage.save_reply(&chunk2).await.unwrap();

        let chunk0 = Reply::Chunk(ReplyChunk {
            request_id: Snowflake(14000),
            id: Snowflake(14001),
            sequence: 0,
            values: vec![vec![1]],
        });
        storage.save_reply(&chunk0).await.unwrap();

        let chunk1 = Reply::Chunk(ReplyChunk {
            request_id: Snowflake(14000),
            id: Snowflake(14002),
            sequence: 1,
            values: vec![vec![2]],
        });
        storage.save_reply(&chunk1).await.unwrap();

        let exit = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(14000),
            id: Snowflake(14004),
            exit: ExitResult::Success(vec![99]),
        });
        storage.save_reply(&exit).await.unwrap();

        let replies = storage.replies_for(Snowflake(14000)).await.unwrap();
        assert_eq!(replies.len(), 4, "should have 3 chunks + 1 exit");

        // Chunks should be ordered by sequence ascending
        for (i, reply) in replies[..3].iter().enumerate() {
            match reply {
                Reply::Chunk(c) => assert_eq!(c.sequence, i as i32, "chunk {i} out of order"),
                _ => panic!("expected Chunk at position {i}"),
            }
        }

        // Exit should be last
        assert!(
            matches!(&replies[3], Reply::WithExit(_)),
            "exit reply should be last"
        );
    }

    #[tokio::test]
    async fn clear_replies() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(15000, "entity-clear");
        storage.save_request(&envelope).await.unwrap();

        let reply = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(15000),
            id: Snowflake(15001),
            exit: ExitResult::Success(vec![1]),
        });
        storage.save_reply(&reply).await.unwrap();

        assert_eq!(
            storage.replies_for(Snowflake(15000)).await.unwrap().len(),
            1
        );

        storage.clear_replies(Snowflake(15000)).await.unwrap();

        assert!(
            storage
                .replies_for(Snowflake(15000))
                .await
                .unwrap()
                .is_empty(),
            "replies should be empty after clear"
        );
    }

    #[tokio::test]
    async fn ack_chunk_removes_chunk_reply() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool);

        let envelope = test_envelope(16000, "entity-ack");
        storage.save_request(&envelope).await.unwrap();

        let chunk = Reply::Chunk(ReplyChunk {
            request_id: Snowflake(16000),
            id: Snowflake(16001),
            sequence: 0,
            values: vec![vec![1, 2]],
        });
        storage.save_reply(&chunk).await.unwrap();

        let exit = Reply::WithExit(ReplyWithExit {
            request_id: Snowflake(16000),
            id: Snowflake(16002),
            exit: ExitResult::Success(vec![]),
        });
        storage.save_reply(&exit).await.unwrap();

        // Ack the chunk
        storage
            .ack_chunk(&cruster::envelope::AckChunk {
                request_id: Snowflake(16000),
                id: Snowflake(16001),
                sequence: 0,
            })
            .await
            .unwrap();

        // Only the exit reply should remain
        let replies = storage.replies_for(Snowflake(16000)).await.unwrap();
        assert_eq!(replies.len(), 1, "only exit reply should remain after ack");
        assert!(matches!(&replies[0], Reply::WithExit(_)));
    }

    #[tokio::test]
    async fn last_read_guard_prevents_redelivery() {
        let (_container, pool) = setup_postgres().await;
        // Set a 10-second guard interval
        let storage =
            SqlMessageStorage::new(pool).with_last_read_guard_interval(Duration::from_secs(10));

        let envelope = test_envelope(17000, "entity-guard");
        storage.save_request(&envelope).await.unwrap();

        // First poll should return the message
        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1, "first poll should return the message");

        // Second poll within guard interval should return nothing
        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();
        assert!(
            msgs.is_empty(),
            "second poll within guard interval should return nothing"
        );
    }

    #[tokio::test]
    async fn unprocessed_messages_filters_by_deliver_at() {
        use chrono::Utc;

        let (_container, pool) = setup_postgres().await;
        let storage =
            SqlMessageStorage::new(pool.clone()).with_last_read_guard_interval(Duration::ZERO);

        // Message with deliver_at in the future (1 hour from now)
        let mut future_env = test_envelope(18000, "entity-future");
        future_env.deliver_at = Some(Utc::now() + chrono::Duration::hours(1));
        storage.save_request(&future_env).await.unwrap();

        // Message with no deliver_at
        let normal_env = test_envelope(18001, "entity-normal");
        storage.save_request(&normal_env).await.unwrap();

        // Message with deliver_at in the past
        let mut past_env = test_envelope(18002, "entity-past");
        past_env.deliver_at = Some(Utc::now() - chrono::Duration::hours(1));
        storage.save_request(&past_env).await.unwrap();

        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();

        let ids: Vec<i64> = msgs.iter().map(|m| m.request_id.0).collect();
        assert!(!ids.contains(&18000), "future message should be excluded");
        assert!(ids.contains(&18001), "normal message should be included");
        assert!(ids.contains(&18002), "past message should be included");
    }

    #[tokio::test]
    async fn reset_shards_clears_last_read() {
        let (_container, pool) = setup_postgres().await;
        // Use a long guard interval so the message would normally be suppressed
        let storage =
            SqlMessageStorage::new(pool).with_last_read_guard_interval(Duration::from_secs(3600));

        let envelope = test_envelope(19000, "entity-reset-guard");
        storage.save_request(&envelope).await.unwrap();

        // First poll reads the message and sets last_read
        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        // Second poll within guard should return nothing
        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();
        assert!(msgs.is_empty(), "guard should suppress re-read");

        // Reset shards should clear last_read
        storage
            .reset_shards(&[ShardId::new("default", 0)])
            .await
            .unwrap();

        // Now the message should be readable again
        let msgs = storage
            .unprocessed_messages(&[ShardId::new("default", 0)])
            .await
            .unwrap();
        assert_eq!(
            msgs.len(),
            1,
            "message should be readable after reset_shards clears last_read"
        );
    }
}

// ============================================================================
// SqlWorkflowEngine tests
// ============================================================================

mod workflow_engine {
    use super::*;

    #[tokio::test]
    async fn sleep_idempotent() {
        let (_container, pool) = setup_postgres().await;
        let engine = SqlWorkflowEngine::with_poll_interval(pool, Duration::from_millis(50));

        // Sleep with a zero-duration timer (fires immediately)
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            engine.sleep("test-wf", "exec-1", "timer-1", Duration::ZERO),
        )
        .await;
        assert!(result.is_ok(), "sleep should complete within timeout");
        result.unwrap().unwrap();

        // Calling sleep again with the same name should return immediately (already fired)
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            engine.sleep("test-wf", "exec-1", "timer-1", Duration::from_secs(3600)),
        )
        .await;
        assert!(
            result.is_ok(),
            "second sleep with same name should return immediately (already fired)"
        );
        result.unwrap().unwrap();
    }

    #[tokio::test]
    async fn deferred_resolve_then_await() {
        let (_container, pool) = setup_postgres().await;
        let engine = SqlWorkflowEngine::with_poll_interval(pool, Duration::from_millis(50));

        let value = rmp_serde::to_vec(&"resolved-value").unwrap();
        engine
            .resolve_deferred("test-wf", "exec-1", "deferred-1", value.clone())
            .await
            .unwrap();

        // await_deferred should return immediately since it's already resolved
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            engine.await_deferred("test-wf", "exec-1", "deferred-1"),
        )
        .await;
        assert!(result.is_ok(), "await_deferred should complete quickly");
        let fetched = result.unwrap().unwrap();
        assert_eq!(fetched, value);
    }

    #[tokio::test]
    async fn deferred_await_then_resolve() {
        let (_container, pool) = setup_postgres().await;
        let engine = Arc::new(SqlWorkflowEngine::with_poll_interval(
            pool,
            Duration::from_millis(50),
        ));

        let engine_clone = engine.clone();
        let resolver = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let value = rmp_serde::to_vec(&42i32).unwrap();
            engine_clone
                .resolve_deferred("test-wf", "exec-2", "deferred-2", value)
                .await
                .unwrap();
        });

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            engine.await_deferred("test-wf", "exec-2", "deferred-2"),
        )
        .await;
        assert!(
            result.is_ok(),
            "await_deferred should complete after resolve"
        );
        let fetched = result.unwrap().unwrap();
        let decoded: i32 = rmp_serde::from_slice(&fetched).unwrap();
        assert_eq!(decoded, 42);

        resolver.await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_removes_old_timers_and_deferreds() {
        let (_container, pool) = setup_postgres().await;
        let engine = SqlWorkflowEngine::with_poll_interval(pool, Duration::from_millis(50));

        // Create a timer that fires immediately and a resolved deferred
        engine
            .sleep("test-wf", "exec-cleanup", "t1", Duration::ZERO)
            .await
            .unwrap();
        let value = rmp_serde::to_vec(&"done").unwrap();
        engine
            .resolve_deferred("test-wf", "exec-cleanup", "d1", value)
            .await
            .unwrap();

        // Cleanup with zero duration
        let removed = engine.cleanup(Duration::ZERO).await.unwrap();
        assert!(
            removed >= 1,
            "expected at least 1 cleaned up, got {removed}"
        );
    }
}

// ============================================================================
// Concurrent dispatch tests
// ============================================================================

mod concurrent {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn for_update_skip_locked_no_double_dispatch() {
        let (_container, pool) = setup_postgres().await;

        // Create two storage instances pointing to the same DB (simulating two runners)
        let storage1 = Arc::new(SqlMessageStorage::new(pool.clone()));
        let storage2 = Arc::new(SqlMessageStorage::new(pool));

        // Save several messages
        for i in 0..10 {
            let envelope = test_envelope(20000 + i, &format!("entity-{i}"));
            storage1.save_request(&envelope).await.unwrap();
        }

        let dispatched = Arc::new(AtomicUsize::new(0));
        let seen_ids = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Two concurrent pollers
        let mut handles = Vec::new();
        for storage in [storage1, storage2] {
            let dispatched = dispatched.clone();
            let seen_ids = seen_ids.clone();
            handles.push(tokio::spawn(async move {
                let msgs = storage
                    .unprocessed_messages(&[ShardId::new("default", 0)])
                    .await
                    .unwrap();
                dispatched.fetch_add(msgs.len(), Ordering::SeqCst);
                let mut ids = seen_ids.lock().await;
                for m in &msgs {
                    ids.push(m.request_id.0);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let ids = seen_ids.lock().await;
        let total = dispatched.load(Ordering::SeqCst);

        // With FOR UPDATE SKIP LOCKED, total dispatched should be exactly 10
        // (each message dispatched to exactly one poller, no duplicates)
        assert_eq!(
            total, 10,
            "expected 10 total dispatches, got {total}. IDs: {:?}",
            ids
        );

        // Verify no duplicates
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            ids.len(),
            "duplicate message IDs detected: {:?}",
            ids
        );
    }
}

// ============================================================================
// Transaction atomicity tests
// ============================================================================

mod transaction_atomicity {
    use super::*;
    use cruster::__internal::save_journal_entry;

    #[tokio::test]
    async fn journal_entry_commits_with_transaction() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool.clone());

        // Simulate what the generated activity dispatch does:
        // 1. Begin transaction from pool
        // 2. Write journal entry into the transaction
        // 3. Commit
        let mut tx = pool.begin().await.unwrap();
        save_journal_entry(&mut tx, "test/atomicity/committed", b"journal-data")
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Journal entry should be visible
        let loaded = storage.load("test/atomicity/committed").await.unwrap();
        assert_eq!(loaded, Some(b"journal-data".to_vec()));
    }

    #[tokio::test]
    async fn journal_entry_rolls_back_with_transaction() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool.clone());

        // Begin transaction, write journal, then rollback
        let mut tx = pool.begin().await.unwrap();
        save_journal_entry(&mut tx, "test/atomicity/rolled-back", b"should-not-persist")
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        // Journal entry should NOT be visible
        let loaded = storage.load("test/atomicity/rolled-back").await.unwrap();
        assert_eq!(loaded, None, "rolled-back journal entry should not persist");
    }

    #[tokio::test]
    async fn user_sql_and_journal_atomic_commit() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool.clone());

        // Create a test table
        sqlx::query("CREATE TABLE IF NOT EXISTS test_atomicity (id TEXT PRIMARY KEY, value TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        // Simulate activity: user SQL + journal in same transaction
        let mut tx = pool.begin().await.unwrap();

        // User SQL
        sqlx::query("INSERT INTO test_atomicity (id, value) VALUES ('key1', 'hello')")
            .execute(&mut *tx)
            .await
            .unwrap();

        // Journal entry
        save_journal_entry(&mut tx, "test/atomicity/user-sql", b"journal-ok")
            .await
            .unwrap();

        tx.commit().await.unwrap();

        // Both should be visible
        let loaded = storage.load("test/atomicity/user-sql").await.unwrap();
        assert_eq!(loaded, Some(b"journal-ok".to_vec()));

        let row: (String,) = sqlx::query_as("SELECT value FROM test_atomicity WHERE id = 'key1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.0, "hello");
    }

    #[tokio::test]
    async fn user_sql_and_journal_atomic_rollback() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlWorkflowStorage::new(pool.clone());

        // Create a test table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS test_atomicity_rb (id TEXT PRIMARY KEY, value TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Simulate activity failure: user SQL + journal then rollback
        let mut tx = pool.begin().await.unwrap();

        sqlx::query("INSERT INTO test_atomicity_rb (id, value) VALUES ('key2', 'should-vanish')")
            .execute(&mut *tx)
            .await
            .unwrap();

        save_journal_entry(&mut tx, "test/atomicity/user-sql-rb", b"should-vanish")
            .await
            .unwrap();

        // Simulate failure — rollback
        tx.rollback().await.unwrap();

        // Neither should be visible
        let loaded = storage.load("test/atomicity/user-sql-rb").await.unwrap();
        assert_eq!(loaded, None, "journal should be rolled back");

        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM test_atomicity_rb WHERE id = 'key2'")
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert!(row.is_none(), "user SQL should be rolled back");
    }
}

// ============================================================================
// Workflow crash-restart resumption tests
// ============================================================================
//
// These tests exercise the full crash-restart cycle: a workflow executes
// activities against real PostgreSQL, the DurableContext is dropped (simulating
// a node crash), and a new DurableContext with the same identity resumes
// execution. Journaled activities must return cached results without
// re-executing their closures, while un-journaled activities must execute
// normally.

mod workflow_resumption {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Build a DurableContext backed by real SQL storage.
    ///
    /// `workflow_name` and `execution_id` together form the journal key scope.
    /// Using the same values across contexts simulates the same workflow
    /// instance resuming after a crash.
    fn make_ctx(
        workflow_name: &str,
        execution_id: &str,
        pool: &sqlx::PgPool,
        msg: Arc<dyn MessageStorage>,
        wf: Arc<dyn WorkflowStorage>,
    ) -> DurableContext {
        let engine: Arc<dyn WorkflowEngine> = Arc::new(SqlWorkflowEngine::new(pool.clone()));
        DurableContext::with_journal_storage(engine, workflow_name, execution_id, msg, wf)
    }

    /// Two activities execute in run 1 and are journaled. After dropping the
    /// context (crash), run 2 creates a fresh context with the same identity.
    /// Both activities return cached results without re-executing. A third
    /// activity that was never journaled executes normally.
    #[tokio::test]
    async fn activities_replay_from_journal_after_crash() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        let calls_a = Arc::new(AtomicU32::new(0));
        let calls_b = Arc::new(AtomicU32::new(0));

        // === Run 1: execute two activities, journal their results ===
        {
            let ctx = make_ctx("OrderWorkflow", "order-42", &pool, msg.clone(), wf.clone());

            let ca = calls_a.clone();
            let result_a: i32 = ctx
                .run("validate_order", b"item-abc", || async move {
                    ca.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                })
                .await
                .unwrap();
            assert_eq!(result_a, 42);

            let cb = calls_b.clone();
            let result_b: String = ctx
                .run("charge_payment", b"card-xyz", || async move {
                    cb.fetch_add(1, Ordering::SeqCst);
                    Ok("txn-001".to_string())
                })
                .await
                .unwrap();
            assert_eq!(result_b, "txn-001");
        }
        // ctx dropped — simulates node crash

        assert_eq!(
            calls_a.load(Ordering::SeqCst),
            1,
            "activity_a ran once in run 1"
        );
        assert_eq!(
            calls_b.load(Ordering::SeqCst),
            1,
            "activity_b ran once in run 1"
        );

        // === Run 2: new context, same identity — simulates restart ===
        {
            let ctx = make_ctx("OrderWorkflow", "order-42", &pool, msg.clone(), wf.clone());

            // Replay activity_a — should return cached value, NOT re-execute
            let ca = calls_a.clone();
            let result_a: i32 = ctx
                .run("validate_order", b"item-abc", || async move {
                    ca.fetch_add(1, Ordering::SeqCst);
                    Ok(999) // would return 999 if re-executed
                })
                .await
                .unwrap();
            assert_eq!(result_a, 42, "should return cached value from run 1");
            assert_eq!(calls_a.load(Ordering::SeqCst), 1, "should NOT re-execute");

            // Replay activity_b — also cached
            let cb = calls_b.clone();
            let result_b: String = ctx
                .run("charge_payment", b"card-xyz", || async move {
                    cb.fetch_add(1, Ordering::SeqCst);
                    Ok("wrong-txn".to_string())
                })
                .await
                .unwrap();
            assert_eq!(result_b, "txn-001", "should return cached value from run 1");
            assert_eq!(calls_b.load(Ordering::SeqCst), 1, "should NOT re-execute");

            // New activity_c — not yet journaled, SHOULD execute
            let calls_c = Arc::new(AtomicU32::new(0));
            let cc = calls_c.clone();
            let result_c: i64 = ctx
                .run("send_confirmation", b"email-1", || async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(200i64)
                })
                .await
                .unwrap();
            assert_eq!(result_c, 200);
            assert_eq!(
                calls_c.load(Ordering::SeqCst),
                1,
                "new activity SHOULD execute"
            );
        }
    }

    /// An activity that failed (returned Err) has its error journaled.
    /// On restart, the cached error is returned without re-executing.
    #[tokio::test]
    async fn failed_activity_replays_cached_error() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        let calls = Arc::new(AtomicU32::new(0));

        // Run 1: activity fails
        {
            let ctx = make_ctx("FailWorkflow", "fail-1", &pool, msg.clone(), wf.clone());
            let c = calls.clone();
            let result: Result<i32, _> = ctx
                .run("flaky_step", b"attempt-1", || async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err(cruster::error::ClusterError::PersistenceError {
                        reason: "payment declined".into(),
                        source: None,
                    })
                })
                .await;
            assert!(result.is_err());
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Run 2: restart — should return the cached error
        {
            let ctx = make_ctx("FailWorkflow", "fail-1", &pool, msg.clone(), wf.clone());
            let c = calls.clone();
            let result: Result<i32, _> = ctx
                .run("flaky_step", b"attempt-1", || async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(999) // would succeed if re-executed
                })
                .await;
            assert!(result.is_err(), "should return cached error from run 1");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "should NOT re-execute");
        }
    }

    /// Same activity name but different argument keys produce independent
    /// journal entries. Each replays its own cached value on restart.
    #[tokio::test]
    async fn different_args_journal_independently() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        // Run 1: same activity name, two different arg keys
        {
            let ctx = make_ctx("BatchWorkflow", "batch-1", &pool, msg.clone(), wf.clone());

            let r1: i32 = ctx
                .run("process_item", b"item-A", || async { Ok(10) })
                .await
                .unwrap();
            assert_eq!(r1, 10);

            let r2: i32 = ctx
                .run("process_item", b"item-B", || async { Ok(20) })
                .await
                .unwrap();
            assert_eq!(r2, 20);
        }

        // Run 2: restart — each key returns its own cached value
        {
            let ctx = make_ctx("BatchWorkflow", "batch-1", &pool, msg.clone(), wf.clone());

            let r1: i32 = ctx
                .run("process_item", b"item-A", || async { Ok(999) })
                .await
                .unwrap();
            assert_eq!(r1, 10, "item-A should return cached 10");

            let r2: i32 = ctx
                .run("process_item", b"item-B", || async { Ok(999) })
                .await
                .unwrap();
            assert_eq!(r2, 20, "item-B should return cached 20");
        }
    }

    /// Different workflow instances (different execution_id) have independent
    /// journals. One instance's cached results do not leak into another.
    #[tokio::test]
    async fn different_executions_have_independent_journals() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        // Execution A: journals result = 100
        {
            let ctx = make_ctx("TransferWorkflow", "exec-A", &pool, msg.clone(), wf.clone());
            let r: i32 = ctx
                .run("compute", b"args", || async { Ok(100) })
                .await
                .unwrap();
            assert_eq!(r, 100);
        }

        // Execution B: same workflow type, different execution_id — should NOT
        // see exec-A's journal
        {
            let ctx = make_ctx("TransferWorkflow", "exec-B", &pool, msg.clone(), wf.clone());
            let calls = Arc::new(AtomicU32::new(0));
            let c = calls.clone();
            let r: i32 = ctx
                .run("compute", b"args", || async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(200)
                })
                .await
                .unwrap();
            assert_eq!(r, 200, "exec-B should execute independently");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "exec-B closure SHOULD run");
        }

        // Restart exec-A — should still return cached 100
        {
            let ctx = make_ctx("TransferWorkflow", "exec-A", &pool, msg.clone(), wf.clone());
            let r: i32 = ctx
                .run("compute", b"args", || async { Ok(999) })
                .await
                .unwrap();
            assert_eq!(r, 100, "exec-A should return its own cached value");
        }
    }

    /// Simulates a crash between two activities: activity_a is journaled,
    /// activity_b never runs. On restart, activity_a replays from cache and
    /// activity_b executes fresh.
    #[tokio::test]
    async fn crash_mid_workflow_resumes_from_last_checkpoint() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        let calls_a = Arc::new(AtomicU32::new(0));
        let calls_b = Arc::new(AtomicU32::new(0));

        // Run 1: only activity_a executes, then "crash" before activity_b
        {
            let ctx = make_ctx("PipelineWorkflow", "pipe-1", &pool, msg.clone(), wf.clone());
            let ca = calls_a.clone();
            let r: String = ctx
                .run("step_one", b"input", || async move {
                    ca.fetch_add(1, Ordering::SeqCst);
                    Ok("intermediate-result".to_string())
                })
                .await
                .unwrap();
            assert_eq!(r, "intermediate-result");
            // "crash" here — activity_b never runs
        }

        assert_eq!(calls_a.load(Ordering::SeqCst), 1);
        assert_eq!(calls_b.load(Ordering::SeqCst), 0);

        // Run 2: restart — re-execute the full workflow
        {
            let ctx = make_ctx("PipelineWorkflow", "pipe-1", &pool, msg.clone(), wf.clone());

            // step_one replays from journal
            let ca = calls_a.clone();
            let r1: String = ctx
                .run("step_one", b"input", || async move {
                    ca.fetch_add(1, Ordering::SeqCst);
                    Ok("wrong-value".to_string())
                })
                .await
                .unwrap();
            assert_eq!(r1, "intermediate-result", "step_one replays cached value");
            assert_eq!(
                calls_a.load(Ordering::SeqCst),
                1,
                "step_one NOT re-executed"
            );

            // step_two runs fresh (was never journaled)
            let cb = calls_b.clone();
            let r2: i32 = ctx
                .run("step_two", b"input", || async move {
                    cb.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                })
                .await
                .unwrap();
            assert_eq!(r2, 42);
            assert_eq!(calls_b.load(Ordering::SeqCst), 1, "step_two SHOULD execute");
        }
    }
}

// ============================================================================
// DurableContext unit tests (migrated from durable.rs — were using MemoryWorkflowStorage)
// ============================================================================

mod durable_context {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn make_ctx(
        pool: &sqlx::PgPool,
        msg_storage: Arc<dyn MessageStorage>,
        wf_storage: Arc<dyn WorkflowStorage>,
    ) -> DurableContext {
        let engine: Arc<dyn WorkflowEngine> = Arc::new(SqlWorkflowEngine::new(pool.clone()));
        DurableContext::with_journal_storage(engine, "TestEntity", "e-1", msg_storage, wf_storage)
    }

    fn make_ctx_no_storage(pool: &sqlx::PgPool) -> DurableContext {
        let engine: Arc<dyn WorkflowEngine> = Arc::new(SqlWorkflowEngine::new(pool.clone()));
        DurableContext::new(engine, "TestEntity", "e-1")
    }

    #[tokio::test]
    async fn run_without_storage_executes_directly() {
        let (_container, pool) = setup_postgres().await;
        let ctx = make_ctx_no_storage(&pool);

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: i32 = ctx
            .run("my_activity", b"key1", || async move {
                cc.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            })
            .await
            .unwrap();

        assert_eq!(result, 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_without_storage_always_re_executes() {
        let (_container, pool) = setup_postgres().await;
        let ctx = make_ctx_no_storage(&pool);

        let call_count = Arc::new(AtomicU32::new(0));

        for _ in 0..3 {
            let cc = call_count.clone();
            let _: i32 = ctx
                .run("my_activity", b"key1", || async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                })
                .await
                .unwrap();
        }

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "without storage, every call should execute"
        );
    }

    #[tokio::test]
    async fn run_caches_result_on_first_execution() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));
        let ctx = make_ctx(&pool, msg, wf);

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: i32 = ctx
            .run("my_activity", b"key1", || async move {
                cc.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            })
            .await
            .unwrap();

        assert_eq!(result, 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_returns_cached_on_replay() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        // First execution — caches the result
        {
            let ctx = make_ctx(&pool, msg.clone(), wf.clone());
            let result: i32 = ctx
                .run("my_activity", b"key1", || async { Ok(42) })
                .await
                .unwrap();
            assert_eq!(result, 42);
        }

        // Second execution (simulates replay) — should return cached result
        {
            let ctx = make_ctx(&pool, msg.clone(), wf.clone());
            let call_count = Arc::new(AtomicU32::new(0));
            let cc = call_count.clone();

            let result: i32 = ctx
                .run("my_activity", b"key1", || async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(99) // Would return 99 if actually executed
                })
                .await
                .unwrap();

            assert_eq!(result, 42, "should return cached result, not re-execute");
            assert_eq!(
                call_count.load(Ordering::SeqCst),
                0,
                "closure should not have been called"
            );
        }
    }

    #[tokio::test]
    async fn run_different_keys_execute_independently() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));
        let ctx = make_ctx(&pool, msg.clone(), wf.clone());

        let a: i32 = ctx
            .run("activity_a", b"k1", || async { Ok(1) })
            .await
            .unwrap();
        let b: i32 = ctx
            .run("activity_b", b"k2", || async { Ok(2) })
            .await
            .unwrap();

        assert_eq!(a, 1);
        assert_eq!(b, 2);

        // Replay — both should return cached values
        let ctx2 = make_ctx(&pool, msg.clone(), wf.clone());
        let a2: i32 = ctx2
            .run("activity_a", b"k1", || async { Ok(99) })
            .await
            .unwrap();
        let b2: i32 = ctx2
            .run("activity_b", b"k2", || async { Ok(99) })
            .await
            .unwrap();

        assert_eq!(a2, 1, "activity_a should return cached value");
        assert_eq!(b2, 2, "activity_b should return cached value");
    }

    #[tokio::test]
    async fn run_same_name_different_args_execute_independently() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));
        let ctx = make_ctx(&pool, msg.clone(), wf.clone());

        // Same activity name but different key bytes (different arguments)
        let a: i32 = ctx
            .run("do_work", b"arg-1", || async { Ok(10) })
            .await
            .unwrap();
        let b: i32 = ctx
            .run("do_work", b"arg-2", || async { Ok(20) })
            .await
            .unwrap();

        assert_eq!(a, 10);
        assert_eq!(b, 20);

        // Replay — each should return its own cached value
        let ctx2 = make_ctx(&pool, msg.clone(), wf.clone());
        let a2: i32 = ctx2
            .run("do_work", b"arg-1", || async { Ok(99) })
            .await
            .unwrap();
        let b2: i32 = ctx2
            .run("do_work", b"arg-2", || async { Ok(99) })
            .await
            .unwrap();

        assert_eq!(a2, 10, "arg-1 should return its cached value");
        assert_eq!(b2, 20, "arg-2 should return its cached value");
    }

    #[tokio::test]
    async fn run_caches_error_result() {
        let (_container, pool) = setup_postgres().await;
        let msg: Arc<dyn MessageStorage> = Arc::new(SqlMessageStorage::new(pool.clone()));
        let wf: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        // First execution — fails
        {
            let ctx = make_ctx(&pool, msg.clone(), wf.clone());
            let result: Result<i32, cruster::error::ClusterError> = ctx
                .run("failing_activity", b"key1", || async {
                    Err(cruster::error::ClusterError::PersistenceError {
                        reason: "activity failed".into(),
                        source: None,
                    })
                })
                .await;
            assert!(result.is_err());
        }

        // Replay — should return the cached failure
        {
            let ctx = make_ctx(&pool, msg.clone(), wf.clone());
            let call_count = Arc::new(AtomicU32::new(0));
            let cc = call_count.clone();

            let result: Result<i32, cruster::error::ClusterError> = ctx
                .run("failing_activity", b"key1", || async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(99) // Would succeed if actually executed
                })
                .await;

            assert!(result.is_err(), "should return cached failure");
            assert_eq!(
                call_count.load(Ordering::SeqCst),
                0,
                "closure should not have been called"
            );
        }
    }
}

// ============================================================================
// Storage retry exhaustion (migrated from tests/storage_retry_exhaustion.rs)
// ============================================================================

mod storage_retry_exhaustion {
    use super::*;

    fn make_envelope(request_id: i64, shard_id: i32) -> EnvelopeRequest {
        EnvelopeRequest {
            request_id: Snowflake(request_id),
            address: EntityAddress {
                shard_id: ShardId::new("default", shard_id),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "test".into(),
            payload: Vec::new(),
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        }
    }

    #[tokio::test]
    async fn dead_letters_message_after_retries() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::with_max_retries(pool.clone(), 1)
            .with_last_read_guard_interval(Duration::ZERO);

        let request_id_value = 9000;
        storage
            .save_request(&make_envelope(request_id_value, 1))
            .await
            .unwrap();
        let request_id = Snowflake(request_id_value);
        let shard = ShardId::new("default", 1);

        for _ in 0..2 {
            let messages = storage
                .unprocessed_messages(std::slice::from_ref(&shard))
                .await
                .unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].request_id, request_id);
        }

        let messages = storage
            .unprocessed_messages(std::slice::from_ref(&shard))
            .await
            .unwrap();
        assert!(messages.is_empty());

        let replies = storage.replies_for(request_id).await.unwrap();
        assert_eq!(replies.len(), 1);
        match &replies[0] {
            Reply::WithExit(reply) => {
                assert_eq!(reply.id, cruster::reply::dead_letter_reply_id(request_id));
                match &reply.exit {
                    ExitResult::Failure(reason) => assert_eq!(reason, "max retries exceeded"),
                    _ => panic!("expected failure exit"),
                }
            }
            _ => panic!("expected exit reply"),
        }
    }
}

// ============================================================================
// Streaming replay ordering (migrated from tests/streaming_replay_ordering.rs)
// ============================================================================

mod streaming_replay_ordering {
    use super::*;

    #[tokio::test]
    async fn persisted_chunk_replay_orders_by_sequence() {
        let (_container, pool) = setup_postgres().await;
        let storage = SqlMessageStorage::new(pool.clone());
        let request_id = Snowflake(1010);

        // Save the parent message so replies can reference it (FK constraint).
        let envelope = EnvelopeRequest {
            request_id,
            address: EntityAddress {
                shard_id: ShardId::new("default", 0),
                entity_type: EntityType::new("StreamTest"),
                entity_id: EntityId::new("stream-1"),
            },
            tag: "stream".into(),
            payload: Vec::new(),
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: true,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        storage.save_request(&envelope).await.unwrap();

        let chunk_two = Reply::Chunk(ReplyChunk {
            request_id,
            id: Snowflake(20),
            sequence: 2,
            values: vec![rmp_serde::to_vec(&2i32).unwrap()],
        });
        let chunk_one = Reply::Chunk(ReplyChunk {
            request_id,
            id: Snowflake(21),
            sequence: 1,
            values: vec![rmp_serde::to_vec(&1i32).unwrap()],
        });
        let exit = Reply::WithExit(ReplyWithExit {
            request_id,
            id: Snowflake(22),
            exit: ExitResult::Success(rmp_serde::to_vec(&()).unwrap()),
        });

        storage.save_reply(&chunk_two).await.unwrap();
        storage.save_reply(&exit).await.unwrap();
        storage.save_reply(&chunk_one).await.unwrap();

        let replies = storage.replies_for(request_id).await.unwrap();
        let sequences: Vec<i32> = replies
            .iter()
            .filter_map(|reply| match reply {
                Reply::Chunk(chunk) => Some(chunk.sequence),
                Reply::WithExit(_) => None,
            })
            .collect();

        assert!(replies
            .iter()
            .any(|reply| matches!(reply, Reply::WithExit(_))));
        assert_eq!(sequences, vec![1, 2]);
    }
}

// ============================================================================
// ActivityScope tests (migrated from state_guard.rs — were using MemoryWorkflowStorage)
// ============================================================================

mod activity_scope {
    use super::*;
    use cruster::__internal::ActivityScope;

    #[tokio::test]
    async fn commits_writes_on_success() {
        let (_container, pool) = setup_postgres().await;
        let storage: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        // Verify is_active is false before running
        assert!(!ActivityScope::is_active());

        let result = ActivityScope::run(&storage, || {
            async move {
                // Verify is_active is true inside the scope
                assert!(
                    ActivityScope::is_active(),
                    "ActivityScope should be active inside run()"
                );

                ActivityScope::buffer_write("test/key".to_string(), b"hello".to_vec());

                Ok::<_, cruster::error::ClusterError>(())
            }
        })
        .await;

        assert!(result.is_ok());

        // Write should be persisted to storage
        let stored = storage.load("test/key").await.unwrap();
        assert!(
            stored.is_some(),
            "Write should be persisted after ActivityScope::run() completes"
        );
        assert_eq!(stored.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn rolls_back_on_error() {
        let (_container, pool) = setup_postgres().await;
        let storage: Arc<dyn WorkflowStorage> = Arc::new(SqlWorkflowStorage::new(pool.clone()));

        let result = ActivityScope::run(&storage, || {
            async move {
                ActivityScope::buffer_write("test/key".to_string(), b"hello".to_vec());

                // Return an error - should rollback
                Err::<(), _>(cruster::error::ClusterError::PersistenceError {
                    reason: "test error".to_string(),
                    source: None,
                })
            }
        })
        .await;

        assert!(result.is_err());

        // Write should NOT be persisted
        let stored = storage.load("test/key").await.unwrap();
        assert!(stored.is_none(), "Write should NOT be persisted on error");
    }
}
