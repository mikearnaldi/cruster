use crate::envelope::EnvelopeRequest;
use crate::envelope::{STREAM_HEADER_KEY, STREAM_HEADER_VALUE};
use crate::error::ClusterError;
use crate::hash::{djb2_hash64, djb2_hash64_with_seed};
use crate::reply::{ExitResult, Reply};
use crate::sharding::Sharding;
use crate::snowflake::Snowflake;
use crate::types::{EntityId, EntityType};
use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tracing::instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Client for sending messages to a specific entity type.
///
/// Created via `Sharding::make_client`. Handles shard resolution,
/// envelope construction, and response deserialization.
///
/// Cloning is cheap — the inner sharding handle is an `Arc`.
#[derive(Clone)]
pub struct EntityClient {
    sharding: Arc<dyn Sharding>,
    entity_type: EntityType,
}

/// Access to an underlying [`EntityClient`].
///
/// Used by macro-generated trait client extensions.
pub trait EntityClientAccessor {
    fn entity_client(&self) -> &EntityClient;
}

impl EntityClientAccessor for EntityClient {
    fn entity_client(&self) -> &EntityClient {
        self
    }
}

/// Factory trait for creating typed workflow/entity clients.
///
/// Automatically implemented by `#[workflow]` macros.
/// Used by the `self.client::<T>()` method inside workflow execute bodies
/// to get a typed client for another workflow or entity.
pub trait WorkflowClientFactory {
    /// The typed client type produced by this factory.
    type Client;

    /// Create a new typed client from a sharding interface.
    fn workflow_client(sharding: Arc<dyn Sharding>) -> Self::Client;
}

pub(crate) fn persisted_request_id(
    entity_type: &EntityType,
    entity_id: &EntityId,
    tag: &str,
    key_bytes: &[u8],
) -> Snowflake {
    // Include entity address in the hash to ensure uniqueness across entity instances
    let mut hash = djb2_hash64(entity_type.0.as_bytes());
    hash = djb2_hash64_with_seed(hash, &[0]);
    hash = djb2_hash64_with_seed(hash, entity_id.0.as_bytes());
    hash = djb2_hash64_with_seed(hash, &[0]);
    hash = djb2_hash64_with_seed(hash, tag.as_bytes());
    hash = djb2_hash64_with_seed(hash, &[0]);
    hash = djb2_hash64_with_seed(hash, key_bytes);
    Snowflake((hash & i64::MAX as u64) as i64)
}

impl EntityClient {
    /// Create a new entity client for the given entity type.
    pub fn new(sharding: Arc<dyn Sharding>, entity_type: EntityType) -> Self {
        Self {
            sharding,
            entity_type,
        }
    }

    /// Send a request and await a deserialized response.
    #[instrument(skip(self, request), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn send<Req, Res>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
    ) -> Result<Res, ClusterError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let trace_ctx = Self::capture_trace_context();
        let envelope = self
            .build_envelope(entity_id, tag, request, trace_ctx)
            .await?;
        let mut reply_rx = self.sharding.send(envelope).await?;

        let reply = reply_rx
            .recv()
            .await
            .ok_or_else(|| ClusterError::MalformedMessage {
                reason: "reply channel closed without response".into(),
                source: None,
            })?;

        match reply {
            Reply::WithExit(r) => match r.exit {
                ExitResult::Success(bytes) => {
                    rmp_serde::from_slice(&bytes).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("failed to deserialize response: {e}"),
                        source: Some(Box::new(e)),
                    })
                }
                ExitResult::Failure(msg) => Err(ClusterError::MalformedMessage {
                    reason: msg,
                    source: None,
                }),
            },
            Reply::Chunk(_) => Err(ClusterError::MalformedMessage {
                reason: "expected WithExit reply, got Chunk".into(),
                source: None,
            }),
        }
    }

    /// Send a request and receive a stream of deserialized responses.
    #[allow(clippy::type_complexity)]
    #[instrument(skip(self, request), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn send_stream<Req, Res>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Res, ClusterError>> + Send>>, ClusterError>
    where
        Req: Serialize,
        Res: DeserializeOwned + Send + 'static,
    {
        fn push_values<Res: DeserializeOwned>(
            pending: &mut VecDeque<Result<Res, ClusterError>>,
            values: Vec<Vec<u8>>,
        ) {
            for bytes in values {
                let item =
                    rmp_serde::from_slice(&bytes).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("failed to deserialize chunk: {e}"),
                        source: Some(Box::new(e)),
                    });
                pending.push_back(item);
            }
        }

        let trace_ctx = Self::capture_trace_context();
        let mut envelope = self
            .build_envelope(entity_id, tag, request, trace_ctx)
            .await?;
        envelope.headers.insert(
            STREAM_HEADER_KEY.to_string(),
            STREAM_HEADER_VALUE.to_string(),
        );
        let reply_rx = self.sharding.send(envelope).await?;

        let stream = tokio_stream::wrappers::ReceiverStream::new(reply_rx);

        struct StreamState<Res> {
            stream: Pin<Box<dyn Stream<Item = Reply> + Send>>,
            next_sequence: i32,
            pending_chunks: BTreeMap<i32, Vec<Vec<u8>>>,
            pending_items: VecDeque<Result<Res, ClusterError>>,
            exit: Option<Result<Res, ClusterError>>,
            finished: bool,
        }

        let ordered = futures::stream::unfold(
            StreamState {
                stream: Box::pin(stream),
                next_sequence: 0,
                pending_chunks: BTreeMap::new(),
                pending_items: VecDeque::new(),
                exit: None,
                finished: false,
            },
            |mut state| async move {
                use tokio_stream::StreamExt;

                loop {
                    if let Some(item) = state.pending_items.pop_front() {
                        return Some((item, state));
                    }

                    if state.finished {
                        if !state.pending_chunks.is_empty() {
                            let pending_chunks = std::mem::take(&mut state.pending_chunks);
                            for (_, values) in pending_chunks {
                                push_values(&mut state.pending_items, values);
                            }
                            if let Some(item) = state.pending_items.pop_front() {
                                return Some((item, state));
                            }
                        }

                        if let Some(exit) = state.exit.take() {
                            return Some((exit, state));
                        }

                        return None;
                    }

                    match state.stream.next().await {
                        Some(Reply::Chunk(chunk)) => {
                            if chunk.values.is_empty() {
                                continue;
                            }
                            if chunk.sequence < state.next_sequence {
                                continue;
                            }
                            state.pending_chunks.insert(chunk.sequence, chunk.values);
                            while let Some(values) =
                                state.pending_chunks.remove(&state.next_sequence)
                            {
                                push_values(&mut state.pending_items, values);
                                state.next_sequence += 1;
                            }
                        }
                        Some(Reply::WithExit(r)) => {
                            let result = match r.exit {
                                ExitResult::Success(bytes) => {
                                    if bytes.is_empty() {
                                        None
                                    } else {
                                        Some(rmp_serde::from_slice(&bytes).map_err(|e| {
                                            ClusterError::MalformedMessage {
                                                reason: format!(
                                                    "failed to deserialize response: {e}"
                                                ),
                                                source: Some(Box::new(e)),
                                            }
                                        }))
                                    }
                                }
                                ExitResult::Failure(msg) => {
                                    Some(Err(ClusterError::MalformedMessage {
                                        reason: msg,
                                        source: None,
                                    }))
                                }
                            };
                            state.exit = result;
                        }
                        None => {
                            state.finished = true;
                        }
                    }
                }
            },
        );

        Ok(Box::pin(ordered))
    }

    /// Fire-and-forget notification to an entity.
    #[instrument(skip(self, request), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn notify<Req: Serialize>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
    ) -> Result<(), ClusterError> {
        let trace_ctx = Self::capture_trace_context();
        let envelope = self
            .build_envelope(entity_id, tag, request, trace_ctx)
            .await?;
        self.sharding.notify(envelope).await
    }

    /// Send a persisted request and await a deserialized response.
    ///
    /// Persisted messages are saved to `MessageStorage` before delivery, ensuring
    /// at-least-once delivery even if the target runner crashes.
    #[instrument(skip(self, request), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn send_persisted<Req, Res>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
        uninterruptible: crate::schema::Uninterruptible,
    ) -> Result<Res, ClusterError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        self.send_persisted_with_key(entity_id, tag, request, None, uninterruptible)
            .await
    }

    /// Send a persisted request with an explicit idempotency key.
    ///
    /// When `key_bytes` is provided, the request ID is derived from the
    /// tag + key bytes instead of the serialized request payload.
    #[instrument(skip(self, request, key_bytes), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn send_persisted_with_key<Req, Res>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
        key_bytes: Option<Vec<u8>>,
        uninterruptible: crate::schema::Uninterruptible,
    ) -> Result<Res, ClusterError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let trace_ctx = Self::capture_trace_context();
        let mut envelope = self
            .build_envelope(entity_id, tag, request, trace_ctx)
            .await?;
        envelope.persisted = true;
        envelope.uninterruptible = uninterruptible;
        let key_bytes = key_bytes.unwrap_or_else(|| envelope.payload.clone());
        envelope.request_id = persisted_request_id(&self.entity_type, entity_id, tag, &key_bytes);
        let mut reply_rx = self.sharding.send(envelope).await?;

        let reply = reply_rx
            .recv()
            .await
            .ok_or_else(|| ClusterError::MalformedMessage {
                reason: "reply channel closed without response".into(),
                source: None,
            })?;

        match reply {
            Reply::WithExit(r) => match r.exit {
                ExitResult::Success(bytes) => {
                    rmp_serde::from_slice(&bytes).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("failed to deserialize response: {e}"),
                        source: Some(Box::new(e)),
                    })
                }
                ExitResult::Failure(msg) => Err(ClusterError::MalformedMessage {
                    reason: msg,
                    source: None,
                }),
            },
            Reply::Chunk(_) => Err(ClusterError::MalformedMessage {
                reason: "expected WithExit reply, got Chunk".into(),
                source: None,
            }),
        }
    }

    /// Fire-and-forget persisted notification to an entity.
    ///
    /// Persisted notifications are saved to `MessageStorage` before delivery.
    #[instrument(skip(self, request), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn notify_persisted<Req: Serialize>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
    ) -> Result<(), ClusterError> {
        self.notify_persisted_with_key(entity_id, tag, request, None)
            .await
    }

    /// Fire-and-forget persisted notification with an explicit idempotency key.
    pub async fn notify_persisted_with_key<Req: Serialize>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
        key_bytes: Option<Vec<u8>>,
    ) -> Result<(), ClusterError> {
        let trace_ctx = Self::capture_trace_context();
        let mut envelope = self
            .build_envelope(entity_id, tag, request, trace_ctx)
            .await?;
        envelope.persisted = true;
        let key_bytes = key_bytes.unwrap_or_else(|| envelope.payload.clone());
        envelope.request_id = persisted_request_id(&self.entity_type, entity_id, tag, &key_bytes);
        self.sharding.notify(envelope).await
    }

    /// Send a request with scheduled delivery at a specific time.
    ///
    /// The message is persisted to `MessageStorage` but will not be delivered
    /// to the entity until `deliver_at` time is reached. The storage polling
    /// loop filters out messages where `deliver_at > now()`.
    #[instrument(skip(self, request), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn send_at<Req, Res>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
        deliver_at: DateTime<Utc>,
    ) -> Result<Res, ClusterError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let trace_ctx = Self::capture_trace_context();
        let mut envelope = self
            .build_envelope(entity_id, tag, request, trace_ctx)
            .await?;
        envelope.persisted = true;
        envelope.deliver_at = Some(deliver_at);
        let mut reply_rx = self.sharding.send(envelope).await?;

        let reply = reply_rx
            .recv()
            .await
            .ok_or_else(|| ClusterError::MalformedMessage {
                reason: "reply channel closed without response".into(),
                source: None,
            })?;

        match reply {
            Reply::WithExit(r) => match r.exit {
                ExitResult::Success(bytes) => {
                    rmp_serde::from_slice(&bytes).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("failed to deserialize response: {e}"),
                        source: Some(Box::new(e)),
                    })
                }
                ExitResult::Failure(msg) => Err(ClusterError::MalformedMessage {
                    reason: msg,
                    source: None,
                }),
            },
            Reply::Chunk(_) => Err(ClusterError::MalformedMessage {
                reason: "expected WithExit reply, got Chunk".into(),
                source: None,
            }),
        }
    }

    /// Fire-and-forget notification with scheduled delivery at a specific time.
    ///
    /// The message is persisted to `MessageStorage` but will not be delivered
    /// until `deliver_at` time is reached.
    #[instrument(skip(self, request), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn notify_at<Req: Serialize>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &Req,
        deliver_at: DateTime<Utc>,
    ) -> Result<(), ClusterError> {
        let trace_ctx = Self::capture_trace_context();
        let mut envelope = self
            .build_envelope(entity_id, tag, request, trace_ctx)
            .await?;
        envelope.persisted = true;
        envelope.deliver_at = Some(deliver_at);
        self.sharding.notify(envelope).await
    }

    /// Poll for a persisted reply without sending a new message.
    ///
    /// Computes the deterministic request ID from the entity address and tag/key,
    /// then queries `MessageStorage` for any stored exit reply. Returns `Ok(Some(result))`
    /// if the workflow has completed, `Ok(None)` if it is still running or no reply exists.
    ///
    /// The `key_bytes` are the same bytes used by `send_persisted`/`notify_persisted`
    /// for deterministic request ID derivation (typically the serialized request payload).
    #[instrument(skip(self, key_bytes), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn poll_reply<Res>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        key_bytes: &[u8],
    ) -> Result<Option<Res>, ClusterError>
    where
        Res: DeserializeOwned,
    {
        let request_id = persisted_request_id(&self.entity_type, entity_id, tag, key_bytes);
        let replies = self.sharding.replies_for(request_id).await?;

        // Look for an exit reply
        for reply in replies {
            if let Reply::WithExit(r) = reply {
                return match r.exit {
                    ExitResult::Success(bytes) => {
                        let result = rmp_serde::from_slice(&bytes).map_err(|e| {
                            ClusterError::MalformedMessage {
                                reason: format!("failed to deserialize poll response: {e}"),
                                source: Some(Box::new(e)),
                            }
                        })?;
                        Ok(Some(result))
                    }
                    ExitResult::Failure(msg) => Err(ClusterError::MalformedMessage {
                        reason: msg,
                        source: None,
                    }),
                };
            }
        }

        Ok(None)
    }

    /// Join (await) the result of a previously-started persisted request.
    ///
    /// Like [`poll_reply`](Self::poll_reply) but blocks until the result is
    /// available instead of returning `Option`. If the reply already exists in
    /// storage it is returned immediately; otherwise a live handler is
    /// registered and the call awaits the reply in real-time.
    ///
    /// The `key_bytes` are the same bytes used by `send_persisted`/`notify_persisted`
    /// for deterministic request ID derivation (typically the entity_id bytes for
    /// workflow executions).
    #[instrument(skip(self, key_bytes), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    pub async fn join_reply<Res>(
        &self,
        entity_id: &EntityId,
        tag: &str,
        key_bytes: &[u8],
    ) -> Result<Res, ClusterError>
    where
        Res: DeserializeOwned,
    {
        let request_id = persisted_request_id(&self.entity_type, entity_id, tag, key_bytes);
        let mut reply_rx = self.sharding.await_reply(request_id).await?;

        let reply = reply_rx
            .recv()
            .await
            .ok_or_else(|| ClusterError::MalformedMessage {
                reason: "reply channel closed without response".into(),
                source: None,
            })?;

        match reply {
            Reply::WithExit(r) => match r.exit {
                ExitResult::Success(bytes) => {
                    rmp_serde::from_slice(&bytes).map_err(|e| ClusterError::MalformedMessage {
                        reason: format!("failed to deserialize join response: {e}"),
                        source: Some(Box::new(e)),
                    })
                }
                ExitResult::Failure(msg) => Err(ClusterError::MalformedMessage {
                    reason: msg,
                    source: None,
                }),
            },
            Reply::Chunk(_) => Err(ClusterError::MalformedMessage {
                reason: "expected WithExit reply, got Chunk".into(),
                source: None,
            }),
        }
    }

    /// Get the entity type this client targets.
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Capture the current OTel trace context from the caller's span.
    ///
    /// This must be called **before** entering `build_envelope`'s debug-level
    /// `#[instrument]` span, because per-layer filters (e.g. `Targets` set to
    /// INFO) may reject the debug span, making it invisible to the OTel layer.
    /// The caller's span (at INFO level) is always visible.
    fn capture_trace_context() -> (Option<String>, Option<String>, Option<bool>) {
        let context = tracing::Span::current().context();
        let span_ref = context.span();
        let sc = span_ref.span_context();
        if sc.is_valid() {
            (
                Some(sc.trace_id().to_string()),
                Some(sc.span_id().to_string()),
                Some(sc.trace_flags().is_sampled()),
            )
        } else {
            (None, None, None)
        }
    }

    #[instrument(level = "debug", skip(self, request, trace_ctx), fields(entity_type = %self.entity_type, entity_id = %entity_id))]
    async fn build_envelope(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &impl Serialize,
        trace_ctx: (Option<String>, Option<String>, Option<bool>),
    ) -> Result<EnvelopeRequest, ClusterError> {
        let shard_id = self.sharding.get_shard_id(&self.entity_type, entity_id);

        let payload = rmp_serde::to_vec(request).map_err(|e| ClusterError::MalformedMessage {
            reason: format!("failed to serialize request: {e}"),
            source: Some(Box::new(e)),
        })?;

        let (trace_id, span_id, sampled) = trace_ctx;

        Ok(EnvelopeRequest {
            request_id: self.sharding.snowflake().next_async().await?,
            address: crate::types::EntityAddress {
                shard_id,
                entity_type: self.entity_type.clone(),
                entity_id: entity_id.clone(),
            },
            tag: tag.to_string(),
            payload,
            headers: HashMap::new(),
            span_id,
            trace_id,
            sampled,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::envelope::{AckChunk, Interrupt};
    use crate::hash::shard_for_entity;
    use crate::message::ReplyReceiver;
    use crate::reply::{ReplyChunk, ReplyWithExit};
    use crate::sharding::ShardingRegistrationEvent;
    use crate::singleton::SingletonContext;
    use crate::snowflake::{Snowflake, SnowflakeGenerator};
    use crate::types::ShardId;
    use async_trait::async_trait;
    use futures::future::BoxFuture;
    use std::sync::Mutex;

    /// Minimal mock Sharding implementation for testing EntityClient.
    struct MockSharding {
        snowflake: SnowflakeGenerator,
        shards_per_group: i32,
    }

    impl MockSharding {
        fn new() -> Self {
            Self {
                snowflake: SnowflakeGenerator::new(),
                shards_per_group: 300,
            }
        }
    }

    struct CapturingSharding {
        inner: MockSharding,
        captured: Arc<Mutex<Vec<EnvelopeRequest>>>,
    }

    struct OutOfOrderSharding {
        inner: MockSharding,
    }

    struct MissingSequenceSharding {
        inner: MockSharding,
    }

    #[async_trait]
    impl Sharding for CapturingSharding {
        fn get_shard_id(&self, et: &EntityType, eid: &EntityId) -> ShardId {
            self.inner.get_shard_id(et, eid)
        }

        fn has_shard_id(&self, shard_id: &ShardId) -> bool {
            self.inner.has_shard_id(shard_id)
        }

        fn snowflake(&self) -> &SnowflakeGenerator {
            self.inner.snowflake()
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        async fn register_entity(&self, _entity: Arc<dyn Entity>) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn register_singleton(
            &self,
            _name: &str,
            _shard_group: Option<&str>,
            _run: Arc<
                dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                    + Send
                    + Sync,
            >,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient {
            EntityClient::new(self, entity_type)
        }

        async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
            self.captured.lock().unwrap().push(envelope.clone());
            self.inner.send(envelope).await
        }

        async fn notify(&self, envelope: EnvelopeRequest) -> Result<(), ClusterError> {
            self.captured.lock().unwrap().push(envelope);
            Ok(())
        }

        async fn ack_chunk(&self, _ack: AckChunk) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn interrupt(&self, _interrupt: Interrupt) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn poll_storage(&self) -> Result<(), ClusterError> {
            Ok(())
        }

        fn active_entity_count(&self) -> usize {
            0
        }

        async fn registration_events(
            &self,
        ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
            Box::pin(tokio_stream::empty())
        }

        async fn shutdown(&self) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Sharding for MockSharding {
        fn get_shard_id(&self, _entity_type: &EntityType, entity_id: &EntityId) -> ShardId {
            let shard = shard_for_entity(entity_id.as_ref(), self.shards_per_group);
            ShardId::new("default", shard)
        }

        fn has_shard_id(&self, _shard_id: &ShardId) -> bool {
            true
        }

        fn snowflake(&self) -> &SnowflakeGenerator {
            &self.snowflake
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        async fn register_entity(&self, _entity: Arc<dyn Entity>) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn register_singleton(
            &self,
            _name: &str,
            _shard_group: Option<&str>,
            _run: Arc<
                dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                    + Send
                    + Sync,
            >,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient {
            EntityClient::new(self, entity_type)
        }

        async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
            // Echo back the payload as a success reply
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let reply = Reply::WithExit(ReplyWithExit {
                request_id: envelope.request_id,
                id: self.snowflake.next_async().await?,
                exit: ExitResult::Success(envelope.payload),
            });
            tx.send(reply).await.unwrap();
            Ok(rx)
        }

        async fn notify(&self, _envelope: EnvelopeRequest) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn ack_chunk(&self, _ack: AckChunk) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn interrupt(&self, _interrupt: Interrupt) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn poll_storage(&self) -> Result<(), ClusterError> {
            Ok(())
        }

        fn active_entity_count(&self) -> usize {
            0
        }

        async fn registration_events(
            &self,
        ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
            Box::pin(tokio_stream::empty())
        }

        async fn shutdown(&self) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Sharding for OutOfOrderSharding {
        fn get_shard_id(&self, et: &EntityType, eid: &EntityId) -> ShardId {
            self.inner.get_shard_id(et, eid)
        }

        fn has_shard_id(&self, shard_id: &ShardId) -> bool {
            self.inner.has_shard_id(shard_id)
        }

        fn snowflake(&self) -> &SnowflakeGenerator {
            self.inner.snowflake()
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        async fn register_entity(&self, _entity: Arc<dyn Entity>) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn register_singleton(
            &self,
            _name: &str,
            _shard_group: Option<&str>,
            _run: Arc<
                dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                    + Send
                    + Sync,
            >,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient {
            EntityClient::new(self, entity_type)
        }

        async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            let request_id = envelope.request_id;
            tokio::spawn(async move {
                let chunk_one = Reply::Chunk(ReplyChunk {
                    request_id,
                    id: Snowflake(1),
                    sequence: 1,
                    values: vec![rmp_serde::to_vec(&1i32).unwrap()],
                });
                let exit = Reply::WithExit(ReplyWithExit {
                    request_id,
                    id: Snowflake(2),
                    exit: ExitResult::Success(rmp_serde::to_vec(&2i32).unwrap()),
                });
                let chunk_zero = Reply::Chunk(ReplyChunk {
                    request_id,
                    id: Snowflake(3),
                    sequence: 0,
                    values: vec![rmp_serde::to_vec(&0i32).unwrap()],
                });
                tx.send(chunk_one).await.unwrap();
                tx.send(exit).await.unwrap();
                tx.send(chunk_zero).await.unwrap();
            });
            Ok(rx)
        }

        async fn notify(&self, _envelope: EnvelopeRequest) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn ack_chunk(&self, _ack: AckChunk) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn interrupt(&self, _interrupt: Interrupt) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn poll_storage(&self) -> Result<(), ClusterError> {
            Ok(())
        }

        fn active_entity_count(&self) -> usize {
            0
        }

        async fn registration_events(
            &self,
        ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
            Box::pin(tokio_stream::empty())
        }

        async fn shutdown(&self) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Sharding for MissingSequenceSharding {
        fn get_shard_id(&self, et: &EntityType, eid: &EntityId) -> ShardId {
            self.inner.get_shard_id(et, eid)
        }

        fn has_shard_id(&self, shard_id: &ShardId) -> bool {
            self.inner.has_shard_id(shard_id)
        }

        fn snowflake(&self) -> &SnowflakeGenerator {
            self.inner.snowflake()
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        async fn register_entity(&self, _entity: Arc<dyn Entity>) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn register_singleton(
            &self,
            _name: &str,
            _shard_group: Option<&str>,
            _run: Arc<
                dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                    + Send
                    + Sync,
            >,
        ) -> Result<(), ClusterError> {
            Ok(())
        }

        fn make_client(self: Arc<Self>, entity_type: EntityType) -> EntityClient {
            EntityClient::new(self, entity_type)
        }

        async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            let request_id = envelope.request_id;
            tokio::spawn(async move {
                let chunk_one = Reply::Chunk(ReplyChunk {
                    request_id,
                    id: Snowflake(10),
                    sequence: 1,
                    values: vec![rmp_serde::to_vec(&1i32).unwrap()],
                });
                let exit = Reply::WithExit(ReplyWithExit {
                    request_id,
                    id: Snowflake(11),
                    exit: ExitResult::Success(rmp_serde::to_vec(&2i32).unwrap()),
                });
                tx.send(chunk_one).await.unwrap();
                tx.send(exit).await.unwrap();
            });
            Ok(rx)
        }

        async fn notify(&self, _envelope: EnvelopeRequest) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn ack_chunk(&self, _ack: AckChunk) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn interrupt(&self, _interrupt: Interrupt) -> Result<(), ClusterError> {
            Ok(())
        }

        async fn poll_storage(&self) -> Result<(), ClusterError> {
            Ok(())
        }

        fn active_entity_count(&self) -> usize {
            0
        }

        async fn registration_events(
            &self,
        ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
            Box::pin(tokio_stream::empty())
        }

        async fn shutdown(&self) -> Result<(), ClusterError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn send_request_and_receive_response() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        // Send i32, expect same bytes back deserialized as i32
        let result: i32 = client.send(&entity_id, "increment", &42i32).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn notify_succeeds() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        client.notify(&entity_id, "ping", &()).await.unwrap();
    }

    #[tokio::test]
    async fn entity_type_accessor() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Order"));
        assert_eq!(client.entity_type(), &EntityType::new("Order"));
    }

    #[tokio::test]
    async fn build_envelope_uses_correct_shard() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("User"));

        let entity_id = EntityId::new("u-123");
        let envelope = client
            .build_envelope(&entity_id, "getProfile", &(), (None, None, None))
            .await
            .unwrap();

        // Verify the envelope fields
        assert_eq!(envelope.address.entity_type, EntityType::new("User"));
        assert_eq!(envelope.address.entity_id, EntityId::new("u-123"));
        assert_eq!(envelope.tag, "getProfile");
        assert_eq!(envelope.address.shard_id.group, "default");
        // Shard ID should be deterministic
        let expected_shard = shard_for_entity("u-123", 300);
        assert_eq!(envelope.address.shard_id.id, expected_shard);
    }

    #[tokio::test]
    async fn send_persisted_request() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        let result: i32 = client
            .send_persisted(
                &entity_id,
                "increment",
                &42i32,
                crate::schema::Uninterruptible::Server,
            )
            .await
            .unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn send_persisted_is_deterministic_for_same_payload() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        let _: i32 = client
            .send_persisted(
                &entity_id,
                "increment",
                &42i32,
                crate::schema::Uninterruptible::No,
            )
            .await
            .unwrap();
        let _: i32 = client
            .send_persisted(
                &entity_id,
                "increment",
                &42i32,
                crate::schema::Uninterruptible::No,
            )
            .await
            .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].request_id, captured[1].request_id);
    }

    #[tokio::test]
    async fn send_persisted_with_key_overrides_payload_hash() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: Arc::clone(&captured),
        });
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        let key_bytes = rmp_serde::to_vec(&"key").unwrap();
        let _: i32 = client
            .send_persisted_with_key(
                &entity_id,
                "increment",
                &42i32,
                Some(key_bytes.clone()),
                crate::schema::Uninterruptible::No,
            )
            .await
            .unwrap();
        let _: i32 = client
            .send_persisted_with_key(
                &entity_id,
                "increment",
                &43i32,
                Some(key_bytes),
                crate::schema::Uninterruptible::No,
            )
            .await
            .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].request_id, captured[1].request_id);
    }

    #[tokio::test]
    async fn notify_persisted_succeeds() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        client
            .notify_persisted(&entity_id, "ping", &())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn send_at_sets_deliver_at_and_persisted() {
        use std::sync::Mutex;

        let captured: Arc<Mutex<Vec<EnvelopeRequest>>> = Arc::new(Mutex::new(Vec::new()));

        struct CapturingSharding {
            inner: MockSharding,
            captured: Arc<Mutex<Vec<EnvelopeRequest>>>,
        }

        #[async_trait]
        impl Sharding for CapturingSharding {
            fn get_shard_id(&self, et: &EntityType, eid: &EntityId) -> crate::types::ShardId {
                self.inner.get_shard_id(et, eid)
            }
            fn has_shard_id(&self, sid: &crate::types::ShardId) -> bool {
                self.inner.has_shard_id(sid)
            }
            fn snowflake(&self) -> &crate::snowflake::SnowflakeGenerator {
                self.inner.snowflake()
            }
            fn is_shutdown(&self) -> bool {
                false
            }
            async fn register_entity(
                &self,
                _: Arc<dyn crate::entity::Entity>,
            ) -> Result<(), ClusterError> {
                Ok(())
            }
            async fn register_singleton(
                &self,
                _: &str,
                _: Option<&str>,
                _: Arc<
                    dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>>
                        + Send
                        + Sync,
                >,
            ) -> Result<(), ClusterError> {
                Ok(())
            }
            fn make_client(self: Arc<Self>, et: EntityType) -> EntityClient {
                EntityClient::new(self, et)
            }
            async fn send(&self, envelope: EnvelopeRequest) -> Result<ReplyReceiver, ClusterError> {
                self.captured.lock().unwrap().push(envelope.clone());
                self.inner.send(envelope).await
            }
            async fn notify(&self, envelope: EnvelopeRequest) -> Result<(), ClusterError> {
                self.captured.lock().unwrap().push(envelope);
                Ok(())
            }
            async fn ack_chunk(&self, _: AckChunk) -> Result<(), ClusterError> {
                Ok(())
            }
            async fn interrupt(&self, _: Interrupt) -> Result<(), ClusterError> {
                Ok(())
            }
            async fn poll_storage(&self) -> Result<(), ClusterError> {
                Ok(())
            }
            fn active_entity_count(&self) -> usize {
                0
            }
            async fn registration_events(
                &self,
            ) -> Pin<Box<dyn Stream<Item = ShardingRegistrationEvent> + Send>> {
                Box::pin(tokio_stream::empty())
            }
            async fn shutdown(&self) -> Result<(), ClusterError> {
                Ok(())
            }
        }

        let sharding: Arc<dyn Sharding> = Arc::new(CapturingSharding {
            inner: MockSharding::new(),
            captured: captured.clone(),
        });
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        let deliver_time = chrono::Utc::now() + chrono::Duration::hours(1);
        let _result: i32 = client
            .send_at(&entity_id, "increment", &42i32, deliver_time)
            .await
            .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].persisted);
        assert_eq!(captured[0].deliver_at, Some(deliver_time));
    }

    #[tokio::test]
    async fn notify_at_sets_deliver_at_and_persisted() {
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        let deliver_time = chrono::Utc::now() + chrono::Duration::hours(1);
        client
            .notify_at(&entity_id, "ping", &(), deliver_time)
            .await
            .unwrap();
        // notify_at doesn't capture, but we can verify it doesn't error
    }

    #[tokio::test]
    async fn send_stream_returns_stream() {
        use tokio_stream::StreamExt;

        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        // MockSharding.send returns a WithExit reply, which send_stream handles
        let mut stream = client
            .send_stream::<i32, i32>(&entity_id, "count", &99)
            .await
            .unwrap();

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first, 99);
    }

    #[tokio::test]
    async fn send_stream_orders_chunks_by_sequence() {
        use tokio_stream::StreamExt;

        let sharding: Arc<dyn Sharding> = Arc::new(OutOfOrderSharding {
            inner: MockSharding::new(),
        });
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        let mut stream = client
            .send_stream::<i32, i32>(&entity_id, "count", &99)
            .await
            .unwrap();

        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }

        assert_eq!(items, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn send_stream_flushes_missing_sequences_on_close() {
        use tokio_stream::StreamExt;

        let sharding: Arc<dyn Sharding> = Arc::new(MissingSequenceSharding {
            inner: MockSharding::new(),
        });
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("Counter"));

        let entity_id = EntityId::new("c-1");
        let mut stream = client
            .send_stream::<i32, i32>(&entity_id, "count", &99)
            .await
            .unwrap();

        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }

        assert_eq!(items, vec![1, 2]);
    }

    #[tokio::test]
    async fn build_envelope_without_otel_sets_none_trace_context() {
        // Without an OTel subscriber layer, trace context fields should be None.
        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("User"));

        let entity_id = EntityId::new("u-1");
        let envelope = client
            .build_envelope(&entity_id, "test", &(), (None, None, None))
            .await
            .unwrap();

        // No OTel subscriber configured, so trace context should be None
        assert_eq!(envelope.trace_id, None);
        assert_eq!(envelope.span_id, None);
        assert_eq!(envelope.sampled, None);
    }

    #[tokio::test]
    async fn build_envelope_with_otel_injects_valid_trace_context() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::trace::{InMemorySpanExporterBuilder, SdkTracerProvider};
        use tracing_opentelemetry::OpenTelemetryLayer;
        use tracing_subscriber::prelude::*;

        let exporter = InMemorySpanExporterBuilder::new().build();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let otel_layer = OpenTelemetryLayer::new(tracer);
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        // Use the Dispatch-based API so we can hold the guard across await points.
        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("User"));
        let entity_id = EntityId::new("u-1");

        // Create an active span so that build_envelope has OTel context.
        let span = tracing::info_span!("test_sender_span");
        let _span_guard = span.enter();

        let envelope = client
            .build_envelope(
                &entity_id,
                "test_tag",
                &42i32,
                EntityClient::capture_trace_context(),
            )
            .await
            .unwrap();

        // The envelope must carry valid OTel hex IDs.
        assert!(
            envelope.trace_id.is_some(),
            "trace_id should be set when OTel subscriber is active"
        );
        assert!(
            envelope.span_id.is_some(),
            "span_id should be set when OTel subscriber is active"
        );
        assert!(
            envelope.sampled.is_some(),
            "sampled should be set when OTel subscriber is active"
        );

        // Validate they are proper hex strings of the right length.
        let trace_id = envelope.trace_id.unwrap();
        let span_id = envelope.span_id.unwrap();
        assert_eq!(trace_id.len(), 32, "trace_id should be 32 hex chars");
        assert_eq!(span_id.len(), 16, "span_id should be 16 hex chars");
        assert!(
            trace_id != "00000000000000000000000000000000",
            "trace_id must not be all zeros"
        );
        assert!(
            span_id != "0000000000000000",
            "span_id must not be all zeros"
        );

        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn trace_context_propagation_links_sender_and_receiver() {
        use opentelemetry::trace::{
            SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider,
        };
        use opentelemetry_sdk::trace::{InMemorySpanExporterBuilder, SdkTracerProvider};
        use tracing_opentelemetry::OpenTelemetryLayer;
        use tracing_subscriber::prelude::*;

        let exporter = InMemorySpanExporterBuilder::new().build();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let otel_layer = OpenTelemetryLayer::new(tracer);
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("User"));
        let entity_id = EntityId::new("u-1");

        // Step 1: Inject — build an envelope inside an OTel-traced span.
        let envelope = {
            let span = tracing::info_span!("sender_workflow");
            let _span_guard = span.enter();
            client
                .build_envelope(
                    &entity_id,
                    "do_work",
                    &(),
                    EntityClient::capture_trace_context(),
                )
                .await
                .unwrap()
        };

        let injected_trace_id = envelope.trace_id.clone().unwrap();
        let injected_span_id = envelope.span_id.clone().unwrap();

        // Step 2: Simulate serialization roundtrip (as happens via gRPC or Postgres).
        let bytes = rmp_serde::to_vec(&envelope).unwrap();
        let deserialized: crate::envelope::EnvelopeRequest = rmp_serde::from_slice(&bytes).unwrap();

        // Verify context survived serialization.
        assert_eq!(
            deserialized.trace_id.as_deref(),
            Some(injected_trace_id.as_str())
        );
        assert_eq!(
            deserialized.span_id.as_deref(),
            Some(injected_span_id.as_str())
        );

        // Step 3: Extract — reconstruct the remote SpanContext (as handle_message_with_recovery does).
        let tid = TraceId::from_hex(&injected_trace_id).expect("valid trace_id");
        let sid = SpanId::from_hex(&injected_span_id).expect("valid span_id");
        let flags = if deserialized.sampled.unwrap_or(false) {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::default()
        };
        let remote_ctx = SpanContext::new(tid, sid, flags, true, TraceState::default());

        assert!(
            remote_ctx.is_valid(),
            "reconstructed SpanContext must be valid"
        );
        assert!(remote_ctx.is_remote(), "context must be marked remote");
        assert_eq!(
            remote_ctx.trace_id().to_string(),
            injected_trace_id,
            "trace_id must match the sender's"
        );
        assert_eq!(
            remote_ctx.span_id().to_string(),
            injected_span_id,
            "span_id must match the sender's"
        );

        // Step 4: Verify the parent link can be set (as handle_message_with_recovery does).
        let otel_context = opentelemetry::Context::current().with_remote_span_context(remote_ctx);
        let span_ref = otel_context.span();
        let parent_sc = span_ref.span_context();
        assert!(parent_sc.is_valid(), "parent context must be valid");
        assert_eq!(
            parent_sc.trace_id().to_string(),
            injected_trace_id,
            "parent trace_id must propagate"
        );

        provider.shutdown().unwrap();
    }

    /// Reproduces the exact autopilot-cruster scenario: the OTel layer has a
    /// per-layer `Targets` filter set to INFO for the `cruster` target.
    /// `build_envelope` is `#[instrument(level = "debug")]`, so its span is
    /// invisible to the OTel layer. Before the fix, `Span::current().context()`
    /// inside `build_envelope` returned an invalid context and trace_id/span_id
    /// were `None`, producing disconnected traces.
    ///
    /// After the fix, `capture_trace_context()` runs in the caller's INFO-level
    /// span (before entering `build_envelope`), so the context is always valid.
    #[tokio::test]
    async fn trace_context_injected_despite_info_level_otel_filter() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::trace::{InMemorySpanExporterBuilder, SdkTracerProvider};
        use tracing_opentelemetry::OpenTelemetryLayer;
        use tracing_subscriber::filter::Targets;
        use tracing_subscriber::prelude::*;

        let exporter = InMemorySpanExporterBuilder::new().build();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");

        // Replicate the production filter: only INFO+ spans from cruster
        // are visible to the OTel layer. This filters out build_envelope's
        // debug-level span.
        let otel_filter = Targets::new().with_target("cruster", tracing::Level::INFO);
        let otel_layer = OpenTelemetryLayer::new(tracer).with_filter(otel_filter);
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("User"));
        let entity_id = EntityId::new("u-1");

        // Simulate the caller: an INFO-level span (like send_persisted_with_key).
        let span = tracing::info_span!(target: "cruster", "send_persisted_with_key");
        let _span_guard = span.enter();

        // capture_trace_context runs HERE, in the INFO-level span (visible to OTel).
        // Then build_envelope enters its debug-level span (invisible to OTel),
        // but the context was already captured.
        let trace_ctx = EntityClient::capture_trace_context();
        let envelope = client
            .build_envelope(&entity_id, "scale_deployment", &(), trace_ctx)
            .await
            .unwrap();

        assert!(
            envelope.trace_id.is_some(),
            "trace_id must be set even when OTel filter is INFO-only"
        );
        assert!(
            envelope.span_id.is_some(),
            "span_id must be set even when OTel filter is INFO-only"
        );
        assert!(
            envelope.sampled.is_some(),
            "sampled must be set even when OTel filter is INFO-only"
        );

        let trace_id = envelope.trace_id.unwrap();
        let span_id = envelope.span_id.unwrap();
        assert_eq!(trace_id.len(), 32);
        assert_eq!(span_id.len(), 16);
        assert!(trace_id != "00000000000000000000000000000000");
        assert!(span_id != "0000000000000000");

        provider.shutdown().unwrap();
    }

    /// Verify that calling `set_parent` on an already-entered `#[instrument]`
    /// span causes the receiver's exported span to inherit the sender's
    /// trace_id. This reproduces the exact pattern used in
    /// `handle_message_with_recovery`: the span is created by `#[instrument]`
    /// (which eagerly assigns a new trace_id), then `set_parent()` is called
    /// inside the function body with the remote context from the envelope.
    ///
    /// The OTel SDK resolves this correctly because `build_with_context`
    /// (called at `on_close`) prefers the parent context's trace_id over
    /// the builder's eagerly-assigned one.
    #[tokio::test]
    async fn set_parent_on_existing_span_inherits_sender_trace_id() {
        use opentelemetry::trace::{
            SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider,
        };
        use opentelemetry_sdk::trace::{InMemorySpanExporterBuilder, SdkTracerProvider};
        use tracing_opentelemetry::OpenTelemetryLayer;
        use tracing_subscriber::prelude::*;

        let exporter = InMemorySpanExporterBuilder::new().build();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let otel_layer = OpenTelemetryLayer::new(tracer);
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let sharding: Arc<dyn Sharding> = Arc::new(MockSharding::new());
        let client = EntityClient::new(Arc::clone(&sharding), EntityType::new("User"));
        let entity_id = EntityId::new("u-1");

        // Step 1: Create an envelope with trace context (sender side).
        let envelope = {
            let span = tracing::info_span!("sender_workflow");
            let _guard = span.enter();
            let trace_ctx = EntityClient::capture_trace_context();
            client
                .build_envelope(&entity_id, "do_work", &(), trace_ctx)
                .await
                .unwrap()
        };

        let sender_trace_id = envelope.trace_id.clone().unwrap();
        let sender_span_id = envelope.span_id.clone().unwrap();

        // Step 2: Simulate the receiver — create a span via #[instrument]
        // (no parent), then call set_parent() with the envelope's context.
        // This is exactly what handle_message_with_recovery does.
        {
            let receiver_span = tracing::info_span!("handle_message_with_recovery");
            let _guard = receiver_span.enter();

            // At this point the span has an eagerly-assigned, DIFFERENT trace_id.
            // Now set_parent with the sender's context:
            if let (Ok(tid), Ok(sid)) = (
                TraceId::from_hex(&sender_trace_id),
                SpanId::from_hex(&sender_span_id),
            ) {
                let flags = if envelope.sampled.unwrap_or(false) {
                    TraceFlags::SAMPLED
                } else {
                    TraceFlags::default()
                };
                let remote_ctx = SpanContext::new(tid, sid, flags, true, TraceState::default());
                let otel_context =
                    opentelemetry::Context::current().with_remote_span_context(remote_ctx);
                tracing::Span::current().set_parent(otel_context);
            }
        }
        // Span closed here — on_close exports it.

        // Step 3: Flush and check the exported spans.
        let _ = provider.force_flush();
        let spans = exporter.get_finished_spans().unwrap();

        let receiver_span = spans
            .iter()
            .find(|s| s.name == std::borrow::Cow::Borrowed("handle_message_with_recovery"))
            .expect("receiver span should be exported");

        // The receiver span must share the sender's trace_id.
        assert_eq!(
            receiver_span.span_context.trace_id().to_string(),
            sender_trace_id,
            "receiver span must inherit the sender's trace_id via set_parent"
        );

        // The receiver's parent_span_id must be the sender's span_id.
        assert_eq!(
            receiver_span.parent_span_id.to_string(),
            sender_span_id,
            "receiver span's parent must be the sender's span"
        );

        provider.shutdown().unwrap();
    }
}
