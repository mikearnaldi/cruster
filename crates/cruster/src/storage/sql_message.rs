//! SQL-backed message storage using PostgreSQL via sqlx.
//!
//! Tables:
//! - `cluster_messages` — persisted envelopes with shard routing and processing state
//! - `cluster_replies` — reply payloads linked to messages
//!
//! This module is only available when the `sql` feature is enabled.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::envelope::{AckChunk, EnvelopeRequest};
use crate::error::ClusterError;
use crate::message::ReplySender;
use crate::message_storage::{MessageStorage, SaveResult};
use crate::reply::{
    dead_letter_reply_id, ExitResult, Reply, ReplyChunk, ReplyWithExit, EXIT_SEQUENCE,
};
use crate::schema::Uninterruptible;
use crate::snowflake::Snowflake;
use crate::types::{EntityAddress, EntityId, EntityType, RunnerAddress, ShardId};

/// PostgreSQL-backed message storage.
pub struct SqlMessageStorage {
    pool: PgPool,
    /// In-memory reply handlers for real-time delivery (not persisted).
    reply_handlers: dashmap::DashMap<Snowflake, ReplySender>,
    /// Maximum delivery attempts before dead-lettering. 0 = unlimited.
    max_retries: AtomicU32,
    /// Maximum number of messages to fetch per poll. 0 = unlimited.
    batch_limit: u32,
    /// Guard interval for the `last_read` dedup mechanism. After a message is
    /// read by `unprocessed_messages`, it will not be re-read until this
    /// interval has elapsed, preventing duplicate dispatch of messages that
    /// are still being processed. 0 = disabled. Default: 10 minutes.
    last_read_guard_interval: std::time::Duration,
}

impl SqlMessageStorage {
    /// Create a new SQL message storage with the given connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            reply_handlers: dashmap::DashMap::new(),
            max_retries: AtomicU32::new(0),
            batch_limit: 0,
            last_read_guard_interval: std::time::Duration::from_secs(600),
        }
    }

    /// Create a new SQL message storage with a maximum retry count.
    /// Messages exceeding this count are dead-lettered (marked processed with a failure reply).
    pub fn with_max_retries(pool: PgPool, max_retries: u32) -> Self {
        Self {
            pool,
            reply_handlers: dashmap::DashMap::new(),
            max_retries: AtomicU32::new(max_retries),
            batch_limit: 0,
            last_read_guard_interval: std::time::Duration::from_secs(600),
        }
    }

    /// Set the maximum number of messages to fetch per poll cycle.
    /// 0 = unlimited (default). Recommended: use `config.storage_inbox_size`.
    pub fn with_batch_limit(mut self, limit: u32) -> Self {
        self.batch_limit = limit;
        self
    }

    /// Set the `last_read` guard interval. After a message is read by
    /// `unprocessed_messages`, it will not be re-read until this interval
    /// has elapsed. Set to `Duration::ZERO` to disable the guard.
    /// Default: 10 minutes (matches TS).
    pub fn with_last_read_guard_interval(mut self, interval: std::time::Duration) -> Self {
        self.last_read_guard_interval = interval;
        self
    }

    /// Run database migrations.
    pub async fn migrate(&self) -> Result<(), ClusterError> {
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("migration failed: {e}"),
                source: Some(Box::new(e)),
            })
    }

    /// Save an envelope (request or fire-and-forget) to the database.
    /// Returns `SaveResult::Duplicate` if the request_id already exists.
    #[tracing::instrument(level = "debug", skip(self, envelope), fields(
        request_id = %envelope.request_id,
        entity_type = %envelope.address.entity_type,
        entity_id = %envelope.address.entity_id,
    ))]
    async fn save_envelope_inner(
        &self,
        envelope: &EnvelopeRequest,
        is_request: bool,
    ) -> Result<SaveResult, ClusterError> {
        let headers_json = serde_json::to_value(&envelope.headers).map_err(|e| {
            ClusterError::PersistenceError {
                reason: format!("failed to serialize headers: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        // Use INSERT ... ON CONFLICT DO NOTHING to handle duplicates atomically.
        let uninterruptible_str = uninterruptible_to_str(envelope.uninterruptible);

        let result = sqlx::query(
            r#"
            INSERT INTO cluster_messages
                (request_id, shard_group, shard_id, entity_type, entity_id, tag,
                 payload, headers, is_request, processed, span_id, trace_id, sampled, deliver_at,
                 uninterruptible)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE, $10, $11, $12, $13, $14)
            ON CONFLICT (request_id) DO NOTHING
            "#,
        )
        .bind(envelope.request_id.0)
        .bind(&envelope.address.shard_id.group)
        .bind(envelope.address.shard_id.id)
        .bind(&envelope.address.entity_type.0)
        .bind(&envelope.address.entity_id.0)
        .bind(&envelope.tag)
        .bind(&envelope.payload)
        .bind(&headers_json)
        .bind(is_request)
        .bind(&envelope.span_id)
        .bind(&envelope.trace_id)
        .bind(envelope.sampled)
        .bind(envelope.deliver_at)
        .bind(uninterruptible_str)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("save envelope failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        if result.rows_affected() == 0 {
            // Duplicate — check if there's an existing reply.
            let existing_reply = if is_request {
                self.replies_for(envelope.request_id)
                    .await?
                    .into_iter()
                    .find(|r| matches!(r, Reply::WithExit(_)))
            } else {
                None
            };
            return Ok(SaveResult::Duplicate { existing_reply });
        }

        Ok(SaveResult::Success)
    }
}

#[async_trait]
impl MessageStorage for SqlMessageStorage {
    #[tracing::instrument(level = "debug", skip(self, envelope), fields(request_id = %envelope.request_id))]
    async fn save_request(&self, envelope: &EnvelopeRequest) -> Result<SaveResult, ClusterError> {
        self.save_envelope_inner(envelope, true).await
    }

    #[tracing::instrument(level = "debug", skip(self, envelope), fields(request_id = %envelope.request_id))]
    async fn save_envelope(&self, envelope: &EnvelopeRequest) -> Result<SaveResult, ClusterError> {
        self.save_envelope_inner(envelope, false).await
    }

    #[tracing::instrument(level = "debug", skip(self, reply), fields(request_id = %reply.request_id()))]
    async fn save_reply(&self, reply: &Reply) -> Result<(), ClusterError> {
        let (request_id, id, sequence, payload, is_exit) = match reply {
            Reply::WithExit(r) => {
                let payload =
                    rmp_serde::to_vec(&r.exit).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("failed to serialize exit result: {e}"),
                        source: Some(Box::new(e)),
                    })?;
                (r.request_id.0, r.id.0, EXIT_SEQUENCE, payload, true)
            }
            Reply::Chunk(r) => {
                let payload =
                    rmp_serde::to_vec(&r.values).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("failed to serialize chunk values: {e}"),
                        source: Some(Box::new(e)),
                    })?;
                (r.request_id.0, r.id.0, r.sequence, payload, false)
            }
        };

        // Insert the reply row and mark as processed atomically within a transaction.
        // This prevents a crash between reply INSERT and processed UPDATE from causing
        // the message to be redelivered and the handler re-executed.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("begin transaction failed: {e}"),
                source: Some(Box::new(e)),
            })?;

        sqlx::query(
            r#"
            INSERT INTO cluster_replies (id, request_id, sequence, payload, is_exit)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (request_id, sequence) DO NOTHING
            "#,
        )
        .bind(id)
        .bind(request_id)
        .bind(sequence)
        .bind(&payload)
        .bind(is_exit)
        .execute(&mut *tx)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("save reply failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        // If this is a final exit reply, mark the message as processed within the same transaction.
        if is_exit {
            sqlx::query("UPDATE cluster_messages SET processed = TRUE WHERE request_id = $1")
                .bind(request_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("mark processed failed: {e}"),
                    source: Some(Box::new(e)),
                })?;
        }

        tx.commit()
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("commit transaction failed: {e}"),
                source: Some(Box::new(e)),
            })?;

        // Deliver to live handler if registered.
        let handler = if is_exit {
            self.reply_handlers
                .remove(&Snowflake(request_id))
                .map(|(_key, sender)| sender)
        } else {
            self.reply_handlers
                .get(&Snowflake(request_id))
                .map(|entry| entry.value().clone())
        };
        if let Some(tx) = handler {
            if tx.try_send(reply.clone()).is_err() {
                tracing::warn!(
                    request_id = request_id,
                    "failed to notify live reply handler (channel full or closed)"
                );
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn clear_replies(&self, request_id: Snowflake) -> Result<(), ClusterError> {
        sqlx::query("DELETE FROM cluster_replies WHERE request_id = $1")
            .bind(request_id.0)
            .execute(&self.pool)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("clear replies failed: {e}"),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn replies_for(&self, request_id: Snowflake) -> Result<Vec<Reply>, ClusterError> {
        let rows = sqlx::query(
            r#"
            SELECT id, request_id, sequence, payload, is_exit
            FROM cluster_replies
            WHERE request_id = $1
            ORDER BY is_exit ASC, sequence ASC, id ASC
            "#,
        )
        .bind(request_id.0)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("fetch replies failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        let mut reply_rows = Vec::with_capacity(rows.len());
        for row in rows {
            reply_rows.push(ReplyRow::from_row(&row));
        }

        decode_reply_rows(reply_rows)
    }

    #[tracing::instrument(level = "debug", skip(self, shard_ids), fields(shard_count = shard_ids.len()))]
    async fn unprocessed_messages(
        &self,
        shard_ids: &[ShardId],
    ) -> Result<Vec<EnvelopeRequest>, ClusterError> {
        // Periodically clean up closed reply handlers to prevent unbounded growth.
        // This runs on every poll cycle, which is frequent enough to bound the leak.
        self.reply_handlers
            .retain(|_id, sender| !sender.is_closed());

        if shard_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Build arrays for the query parameters.
        let groups: Vec<&str> = shard_ids.iter().map(|s| s.group.as_str()).collect();
        let ids: Vec<i32> = shard_ids.iter().map(|s| s.id).collect();

        // Use a CTE to atomically increment retry_count and fetch messages.
        // Messages exceeding max_retries are dead-lettered (marked processed).
        // retry_count is incremented on every poll (including the first delivery),
        // so the dead-letter threshold is max_retries + 1 to allow the initial
        // delivery plus max_retries actual retries.
        // This prevents duplicate processing by concurrent runners.
        let max_retries = self.max_retries.load(Ordering::Relaxed);
        let max_retries_bound = i32::try_from(max_retries).unwrap_or(i32::MAX);
        let guard_interval_secs = self.last_read_guard_interval.as_secs_f64();
        let dead_letter_payload = rmp_serde::to_vec(&ExitResult::Failure(
            "max retries exceeded".to_string(),
        ))
        .map_err(|e| ClusterError::MalformedMessage {
            reason: format!("failed to serialize dead-letter reply: {e}"),
            source: Some(Box::new(e)),
        })?;
        let rows = sqlx::query(
            r#"
            WITH to_process AS (
                SELECT request_id
                FROM cluster_messages
                WHERE processed = FALSE
                  AND (deliver_at IS NULL OR deliver_at <= NOW())
                  AND ($6::double precision <= 0 OR last_read IS NULL OR last_read < NOW() - make_interval(secs => $6::double precision))
                  AND (shard_group, shard_id) IN (
                      SELECT * FROM UNNEST($1::text[], $2::int[])
                  )
                ORDER BY request_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT CASE WHEN $5::int > 0 THEN $5::int ELSE NULL END
            ),
            updated AS (
                UPDATE cluster_messages
                SET retry_count = retry_count + 1,
                    last_read = NOW(),
                    processed = CASE
                        WHEN $3::int > 0 AND retry_count > $3::int THEN TRUE
                        ELSE processed
                    END
                WHERE request_id IN (SELECT request_id FROM to_process)
                RETURNING request_id, retry_count, processed
            ),
            dead_lettered AS (
                SELECT request_id FROM updated WHERE processed = TRUE
            ),
            dead_letter_replies AS (
                INSERT INTO cluster_replies (id, request_id, sequence, payload, is_exit)
                SELECT -request_id, request_id, $7, $4::bytea, TRUE
                FROM dead_lettered
                WHERE NOT EXISTS (
                    SELECT 1 FROM cluster_replies cr
                    WHERE cr.request_id = dead_lettered.request_id AND cr.is_exit = TRUE
                )
                ON CONFLICT (request_id, sequence) DO NOTHING
            )
            SELECT m.request_id, m.shard_group, m.shard_id, m.entity_type, m.entity_id,
                   m.tag, m.payload, m.headers, m.span_id, m.trace_id, m.sampled, m.deliver_at,
                   m.uninterruptible
            FROM cluster_messages m
            INNER JOIN updated u ON m.request_id = u.request_id
            WHERE m.request_id NOT IN (SELECT request_id FROM dead_lettered)
            "#,
        )
        .bind(&groups)
        .bind(&ids)
        .bind(max_retries_bound)
        .bind(&dead_letter_payload)
        .bind(i32::try_from(self.batch_limit).unwrap_or(i32::MAX))
        .bind(guard_interval_secs)
        .bind(EXIT_SEQUENCE)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("fetch unprocessed messages failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            match row_to_envelope(&row) {
                Ok(message) => messages.push(message),
                Err(RowDecodeError::MalformedHeaders { request_id, source }) => {
                    tracing::warn!(
                        request_id = request_id.0,
                        error = %source,
                        "skipping message with malformed headers"
                    );
                }
                Err(RowDecodeError::Fatal(err)) => return Err(err),
            }
        }

        // Notify in-memory reply handlers for any dead-lettered messages.
        // Only check request IDs that have active reply handlers, avoiding
        // a full scan of all historical dead-lettered messages.
        if max_retries > 0 && !self.reply_handlers.is_empty() {
            let handler_ids: Vec<i64> = self
                .reply_handlers
                .iter()
                .map(|entry| entry.key().0)
                .collect();

            if !handler_ids.is_empty() {
                let dead_lettered_rows = sqlx::query(
                    r#"
                    SELECT cm.request_id
                    FROM cluster_messages cm
                    WHERE cm.request_id = ANY($1::bigint[])
                      AND cm.processed = TRUE
                      AND EXISTS (
                          SELECT 1 FROM cluster_replies cr
                          WHERE cr.request_id = cm.request_id
                            AND cr.is_exit = TRUE
                            AND cr.id < 0
                      )
                    "#,
                )
                .bind(&handler_ids)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("fetch dead-lettered messages failed: {e}"),
                    source: Some(Box::new(e)),
                })?;

                for row in dead_lettered_rows {
                    let request_id: i64 = row.get("request_id");
                    let snowflake = Snowflake(request_id);
                    if let Some((_key, handler)) = self.reply_handlers.remove(&snowflake) {
                        let failure_reply = Reply::WithExit(ReplyWithExit {
                            request_id: snowflake,
                            id: dead_letter_reply_id(snowflake),
                            exit: ExitResult::Failure("max retries exceeded".to_string()),
                        });
                        if handler.try_send(failure_reply).is_err() {
                            tracing::debug!(
                                request_id = request_id,
                                "failed to notify reply handler for dead-lettered message"
                            );
                        }
                    }
                }
            }
        }

        Ok(messages)
    }

    #[tracing::instrument(level = "debug", skip(self, shard_ids), fields(shard_count = shard_ids.len()))]
    async fn reset_shards(&self, shard_ids: &[ShardId]) -> Result<(), ClusterError> {
        if shard_ids.is_empty() {
            return Ok(());
        }

        let groups: Vec<&str> = shard_ids.iter().map(|s| s.group.as_str()).collect();
        let ids: Vec<i32> = shard_ids.iter().map(|s| s.id).collect();

        // Reset processed flag and retry_count for messages in reassigned shards.
        // retry_count is reset to 0 because shard reassignment is not a retry —
        // the new runner should get the full retry budget.
        //
        // Note: Messages currently in-flight on the old runner may be delivered
        // twice (once by the old runner completing, once by the new runner picking
        // up from storage). This is expected at-least-once delivery behavior during
        // shard transitions. Entity handlers must be idempotent for persisted messages.
        sqlx::query(
            r#"
            UPDATE cluster_messages
            SET processed = FALSE, retry_count = 0, last_read = NULL
            WHERE (shard_group, shard_id) IN (
                SELECT * FROM UNNEST($1::text[], $2::int[])
            )
            AND NOT EXISTS (
                SELECT 1 FROM cluster_replies cr
                WHERE cr.request_id = cluster_messages.request_id
                AND cr.is_exit = TRUE
            )
            "#,
        )
        .bind(&groups)
        .bind(&ids)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("reset shards failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        Ok(())
    }

    async fn clear_address(&self, address: &RunnerAddress) -> Result<(), ClusterError> {
        // Intentional no-op: SQL messages are keyed by shard, not runner address.
        // Cleanup is performed via `reset_shards()` when shards are reassigned.
        // The runner address parameter is part of the trait contract for backends
        // that track per-runner message ownership (e.g., in-memory storage).
        tracing::debug!(
            ?address,
            "clear_address is a no-op for SQL message storage; use reset_shards instead"
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, request_ids), fields(count = request_ids.len()))]
    async fn unprocessed_messages_by_id(
        &self,
        request_ids: &[Snowflake],
    ) -> Result<Vec<EnvelopeRequest>, ClusterError> {
        if request_ids.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<i64> = request_ids.iter().map(|s| s.0).collect();
        let rows = sqlx::query(
            r#"
            SELECT request_id, shard_group, shard_id, entity_type, entity_id,
                   tag, payload, headers, span_id, trace_id, sampled, deliver_at,
                   uninterruptible
            FROM cluster_messages
            WHERE processed = FALSE
              AND (deliver_at IS NULL OR deliver_at <= NOW())
              AND request_id = ANY($1::bigint[])
            "#,
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("fetch unprocessed messages by id failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            match row_to_envelope(&row) {
                Ok(message) => messages.push(message),
                Err(RowDecodeError::MalformedHeaders { request_id, source }) => {
                    tracing::warn!(
                        request_id = request_id.0,
                        error = %source,
                        "skipping message with malformed headers"
                    );
                }
                Err(RowDecodeError::Fatal(err)) => return Err(err),
            }
        }
        Ok(messages)
    }

    fn register_reply_handler(&self, request_id: Snowflake, sender: ReplySender) {
        self.reply_handlers.insert(request_id, sender);
    }

    fn unregister_reply_handler(&self, request_id: Snowflake) {
        self.reply_handlers.remove(&request_id);
    }

    #[tracing::instrument(level = "debug", skip(self), fields(request_id = %ack.request_id, sequence = ack.sequence))]
    async fn ack_chunk(&self, ack: &AckChunk) -> Result<(), ClusterError> {
        sqlx::query(
            "DELETE FROM cluster_replies WHERE request_id = $1 AND sequence = $2 AND id = $3 AND is_exit = FALSE",
        )
        .bind(ack.request_id.0)
        .bind(ack.sequence)
        .bind(ack.id.0)
        .execute(&self.pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("ack chunk failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        Ok(())
    }

    fn set_max_retries(&self, max_retries: u32) {
        self.max_retries.store(max_retries, Ordering::Relaxed);
    }
}

/// Convert a database row to an `EnvelopeRequest`.
fn try_get_column<'r, T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>>(
    row: &'r sqlx::postgres::PgRow,
    column: &str,
) -> Result<T, ClusterError> {
    row.try_get(column)
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to read '{column}' column: {e}"),
            source: Some(Box::new(e)),
        })
}

enum RowDecodeError {
    MalformedHeaders {
        request_id: Snowflake,
        source: serde_json::Error,
    },
    Fatal(ClusterError),
}

struct ReplyRow {
    id: i64,
    request_id: i64,
    sequence: i32,
    payload: Vec<u8>,
    is_exit: bool,
}

impl ReplyRow {
    fn from_row(row: &sqlx::postgres::PgRow) -> Self {
        Self {
            id: row.get("id"),
            request_id: row.get("request_id"),
            sequence: row.get("sequence"),
            payload: row.get("payload"),
            is_exit: row.get("is_exit"),
        }
    }
}

fn decode_reply_rows(mut rows: Vec<ReplyRow>) -> Result<Vec<Reply>, ClusterError> {
    rows.sort_by(|a, b| {
        a.is_exit
            .cmp(&b.is_exit)
            .then(a.sequence.cmp(&b.sequence))
            .then(a.id.cmp(&b.id))
    });
    let mut replies = Vec::with_capacity(rows.len());
    for row in rows {
        if row.is_exit {
            let exit: ExitResult = rmp_serde::from_slice(&row.payload).map_err(|e| {
                ClusterError::MalformedMessage {
                    reason: format!("failed to deserialize exit result: {e}"),
                    source: Some(Box::new(e)),
                }
            })?;
            replies.push(Reply::WithExit(ReplyWithExit {
                request_id: Snowflake(row.request_id),
                id: Snowflake(row.id),
                exit,
            }));
        } else {
            let values: Vec<Vec<u8>> = rmp_serde::from_slice(&row.payload).map_err(|e| {
                ClusterError::MalformedMessage {
                    reason: format!("failed to deserialize chunk values: {e}"),
                    source: Some(Box::new(e)),
                }
            })?;
            replies.push(Reply::Chunk(ReplyChunk {
                request_id: Snowflake(row.request_id),
                id: Snowflake(row.id),
                sequence: row.sequence,
                values,
            }));
        }
    }

    Ok(replies)
}

fn row_to_envelope(row: &sqlx::postgres::PgRow) -> Result<EnvelopeRequest, RowDecodeError> {
    let request_id: i64 = try_get_column(row, "request_id").map_err(RowDecodeError::Fatal)?;
    let shard_group: String = try_get_column(row, "shard_group").map_err(RowDecodeError::Fatal)?;
    let shard_id: i32 = try_get_column(row, "shard_id").map_err(RowDecodeError::Fatal)?;
    let entity_type: String = try_get_column(row, "entity_type").map_err(RowDecodeError::Fatal)?;
    let entity_id: String = try_get_column(row, "entity_id").map_err(RowDecodeError::Fatal)?;
    let tag: String = try_get_column(row, "tag").map_err(RowDecodeError::Fatal)?;
    let payload: Vec<u8> = try_get_column(row, "payload").map_err(RowDecodeError::Fatal)?;
    let headers_json: serde_json::Value =
        try_get_column(row, "headers").map_err(RowDecodeError::Fatal)?;
    let span_id: Option<String> = try_get_column(row, "span_id").map_err(RowDecodeError::Fatal)?;
    let trace_id: Option<String> =
        try_get_column(row, "trace_id").map_err(RowDecodeError::Fatal)?;
    let sampled: Option<bool> = try_get_column(row, "sampled").map_err(RowDecodeError::Fatal)?;
    let deliver_at: Option<DateTime<Utc>> =
        try_get_column(row, "deliver_at").map_err(RowDecodeError::Fatal)?;
    // Backward compatibility: databases created before migration 0003 may not
    // have the `uninterruptible` column. Fall back to "No" (the default) only
    // for missing-column errors; propagate type-conversion errors.
    let uninterruptible_raw: String = match row.try_get("uninterruptible") {
        Ok(val) => val,
        Err(sqlx::Error::ColumnNotFound(_)) => {
            tracing::debug!("uninterruptible column not found, using default 'No'");
            "No".to_string()
        }
        Err(e) => {
            return Err(RowDecodeError::Fatal(ClusterError::PersistenceError {
                reason: format!("failed to read 'uninterruptible' column: {e}"),
                source: Some(Box::new(e)),
            }));
        }
    };

    let headers: HashMap<String, String> =
        serde_json::from_value(headers_json).map_err(|e| RowDecodeError::MalformedHeaders {
            request_id: Snowflake(request_id),
            source: e,
        })?;

    Ok(EnvelopeRequest {
        request_id: Snowflake(request_id),
        address: EntityAddress {
            shard_id: ShardId::new(&shard_group, shard_id),
            entity_type: EntityType::new(&entity_type),
            entity_id: EntityId::new(&entity_id),
        },
        tag,
        payload,
        headers,
        span_id,
        trace_id,
        sampled,
        persisted: true, // Messages read from storage are persisted by definition
        uninterruptible: str_to_uninterruptible(&uninterruptible_raw),
        deliver_at,
    })
}

/// Serialize `Uninterruptible` to a database-friendly string.
fn uninterruptible_to_str(u: Uninterruptible) -> &'static str {
    match u {
        Uninterruptible::No => "No",
        Uninterruptible::Client => "Client",
        Uninterruptible::Server => "Server",
        Uninterruptible::Both => "Both",
    }
}

/// Deserialize an `Uninterruptible` value from a database string.
fn str_to_uninterruptible(s: &str) -> Uninterruptible {
    match s {
        "No" => Uninterruptible::No,
        "Client" => Uninterruptible::Client,
        "Server" => Uninterruptible::Server,
        "Both" => Uninterruptible::Both,
        other => {
            tracing::warn!(
                value = other,
                "unknown uninterruptible value in database, defaulting to No"
            );
            Uninterruptible::No
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_rows_sort_exit_last() {
        let request_id = 101;
        let first_payload = rmp_serde::to_vec(&vec![vec![1u8]]).unwrap();
        let second_payload = rmp_serde::to_vec(&vec![vec![2u8]]).unwrap();
        let exit_payload = rmp_serde::to_vec(&ExitResult::Success(vec![9u8])).unwrap();
        let rows = vec![
            ReplyRow {
                id: 200,
                request_id,
                sequence: 1,
                payload: second_payload,
                is_exit: false,
            },
            ReplyRow {
                id: 150,
                request_id,
                sequence: 0,
                payload: exit_payload,
                is_exit: true,
            },
            ReplyRow {
                id: 100,
                request_id,
                sequence: 0,
                payload: first_payload,
                is_exit: false,
            },
        ];

        let replies = decode_reply_rows(rows).expect("decode replies");
        assert!(matches!(replies.last(), Some(Reply::WithExit(_))));
        let sequences: Vec<i32> = replies
            .iter()
            .map(|reply| match reply {
                Reply::Chunk(chunk) => chunk.sequence,
                Reply::WithExit(_) => -1,
            })
            .collect();

        assert_eq!(sequences, vec![0, 1, -1]);
    }
}
