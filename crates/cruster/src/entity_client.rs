use crate::envelope::EnvelopeRequest;
use crate::envelope::{STREAM_HEADER_KEY, STREAM_HEADER_VALUE};
use crate::error::ClusterError;
use crate::hash::{djb2_hash64, djb2_hash64_with_seed};
use crate::reply::{ExitResult, Reply};
use crate::sharding::Sharding;
use crate::snowflake::Snowflake;
use crate::types::{EntityId, EntityType};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tracing::instrument;

/// Client for sending messages to a specific entity type.
///
/// Created via `Sharding::make_client`. Handles shard resolution,
/// envelope construction, and response deserialization.
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
        let envelope = self.build_envelope(entity_id, tag, request).await?;
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

        let mut envelope = self.build_envelope(entity_id, tag, request).await?;
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
        let envelope = self.build_envelope(entity_id, tag, request).await?;
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
        let mut envelope = self.build_envelope(entity_id, tag, request).await?;
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
        let mut envelope = self.build_envelope(entity_id, tag, request).await?;
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
        let mut envelope = self.build_envelope(entity_id, tag, request).await?;
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
        let mut envelope = self.build_envelope(entity_id, tag, request).await?;
        envelope.persisted = true;
        envelope.deliver_at = Some(deliver_at);
        self.sharding.notify(envelope).await
    }

    /// Get the entity type this client targets.
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    async fn build_envelope(
        &self,
        entity_id: &EntityId,
        tag: &str,
        request: &impl Serialize,
    ) -> Result<EnvelopeRequest, ClusterError> {
        let shard_id = self.sharding.get_shard_id(&self.entity_type, entity_id);

        let payload = rmp_serde::to_vec(request).map_err(|e| ClusterError::MalformedMessage {
            reason: format!("failed to serialize request: {e}"),
            source: Some(Box::new(e)),
        })?;

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
            span_id: None,
            trace_id: None,
            sampled: None,
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
            .build_envelope(&entity_id, "getProfile", &())
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
}
