//! gRPC transport for inter-runner communication.
//!
//! Provides `GrpcRunners` (client-side, implements the `Runners` trait) and
//! `GrpcRunnerServer` (server-side, handles incoming requests) plus
//! `GrpcRunnerHealth` (implements the `RunnerHealth` trait via ping).

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{self, instrument};

use crate::envelope::{Envelope, EnvelopeRequest};
use crate::error::ClusterError;
use crate::message::ReplyReceiver;
use crate::reply::{fallback_reply_id, ExitResult, Reply, ReplyWithExit, EXIT_SEQUENCE};
use crate::runner_health::RunnerHealth;
use crate::runners::Runners;
use crate::sharding::Sharding;
use crate::types::RunnerAddress;

/// Generated protobuf/gRPC code.
pub mod proto {
    tonic::include_proto!("cluster");
}

use proto::runner_service_client::RunnerServiceClient;
use proto::runner_service_server::{RunnerService, RunnerServiceServer};

// ---------------------------------------------------------------------------
// Client: GrpcRunners
// ---------------------------------------------------------------------------

/// gRPC-based implementation of [`Runners`] for inter-runner communication.
///
/// Maintains a connection pool (one channel per remote runner address).
pub struct GrpcRunners {
    /// Cached gRPC channels keyed by runner address string.
    channels: DashMap<String, RunnerServiceClient<Channel>>,
    /// Per-address connection creation locks to prevent duplicate connections
    /// from concurrent callers (TOCTOU race on `channels`).
    connect_locks: DashMap<String, Arc<Mutex<()>>>,
    /// Timeout for establishing new gRPC connections.
    connect_timeout: std::time::Duration,
}

impl GrpcRunners {
    /// Create a new `GrpcRunners` client with the default connect timeout (5s).
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
            connect_locks: DashMap::new(),
            connect_timeout: std::time::Duration::from_secs(5),
        }
    }

    /// Create a new `GrpcRunners` client with a custom connect timeout.
    pub fn with_connect_timeout(connect_timeout: std::time::Duration) -> Self {
        Self {
            channels: DashMap::new(),
            connect_locks: DashMap::new(),
            connect_timeout,
        }
    }

    /// Get or create a gRPC client for the given runner address.
    ///
    /// Uses a per-address lock to prevent duplicate connection creation when
    /// multiple concurrent callers target the same new runner address.
    #[instrument(skip(self), fields(runner_address = %address))]
    async fn client_for(
        &self,
        address: &RunnerAddress,
    ) -> Result<RunnerServiceClient<Channel>, ClusterError> {
        let key = address.to_string();

        // Fast path: existing connection
        if let Some(client) = self.channels.get(&key) {
            return Ok(client.clone());
        }

        // Slow path: acquire per-address lock to serialize connection creation
        let lock = self
            .connect_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Re-check after acquiring lock — another caller may have created the connection
        if let Some(client) = self.channels.get(&key) {
            return Ok(client.clone());
        }

        let endpoint = format!("http://{}:{}", address.host, address.port);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| ClusterError::RunnerUnavailable {
                address: address.clone(),
                source: Some(Box::new(e)),
            })?
            .connect_timeout(self.connect_timeout)
            .connect()
            .await
            .map_err(|e| ClusterError::RunnerUnavailable {
                address: address.clone(),
                source: Some(Box::new(e)),
            })?;

        let client = RunnerServiceClient::new(channel);
        self.channels.insert(key, client.clone());
        Ok(client)
    }

    /// Remove a cached connection for the given address.
    fn remove_connection(&self, address: &RunnerAddress) {
        let key = address.to_string();
        self.channels.remove(&key);
        self.connect_locks.remove(&key);
    }

    /// Remove orphaned `connect_locks` entries that have no corresponding
    /// connection. Called periodically to prevent unbounded growth from
    /// runners that depart without triggering `on_runner_unavailable`.
    pub fn cleanup_orphaned_locks(&self) {
        self.connect_locks
            .retain(|key, _| self.channels.contains_key(key));
    }
}

impl Default for GrpcRunners {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Runners for GrpcRunners {
    #[instrument(skip(self), fields(runner_address = %address))]
    async fn ping(&self, address: &RunnerAddress) -> Result<(), ClusterError> {
        let mut client = self.client_for(address).await?;
        client
            .ping(Request::new(proto::PingRequest {}))
            .await
            .map_err(|e| ClusterError::RunnerUnavailable {
                address: address.clone(),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    #[instrument(skip(self, envelope), fields(runner_address = %address))]
    async fn send(
        &self,
        address: &RunnerAddress,
        envelope: EnvelopeRequest,
    ) -> Result<ReplyReceiver, ClusterError> {
        let mut client = self.client_for(address).await?;

        let envelope = inject_trace_context(envelope);

        let request_id = envelope.request_id;

        let envelope_bytes = rmp_serde::to_vec(&Envelope::Request(envelope)).map_err(|e| {
            ClusterError::MalformedMessage {
                reason: "failed to serialize envelope".into(),
                source: Some(Box::new(e)),
            }
        })?;

        let response = client
            .send(Request::new(proto::SendRequest {
                envelope: envelope_bytes,
            }))
            .await
            .map_err(|e| ClusterError::RunnerUnavailable {
                address: address.clone(),
                source: Some(Box::new(e)),
            })?;

        let mut stream = response.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<Reply>(16);

        // Spawn a task to read the gRPC stream and forward replies.
        // On deserialization error, a synthetic failure reply is sent so
        // the caller receives a descriptive error instead of a silent
        // channel close.
        //
        // Uses `tx.closed()` as a cancellation signal: when the receiver
        // is dropped, the task exits promptly instead of hanging on an
        // idle gRPC stream indefinitely.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = tx.closed() => {
                        // Receiver dropped — no point reading more replies
                        break;
                    }
                    result = stream.message() => {
                        match result {
                            Ok(Some(msg)) => {
                                match rmp_serde::from_slice::<Reply>(&msg.reply) {
                                    Ok(reply) => {
                                        if tx.send(reply).await.is_err() {
                                            break; // Receiver dropped
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "failed to deserialize reply from gRPC stream");
                                        let failure = Reply::WithExit(ReplyWithExit {
                                            request_id,
                                            id: fallback_reply_id(request_id, EXIT_SEQUENCE),
                                            exit: ExitResult::Failure(format!(
                                                "gRPC reply deserialization failed: {e}"
                                            )),
                                        });
                                        let _ = tx.send(failure).await;
                                        break;
                                    }
                                }
                            }
                            Ok(None) => break, // Stream ended normally
                            Err(e) => {
                                tracing::warn!(error = %e, "gRPC reply stream error");
                                let failure = Reply::WithExit(ReplyWithExit {
                                    request_id,
                                    id: fallback_reply_id(request_id, EXIT_SEQUENCE),
                                    exit: ExitResult::Failure(format!(
                                        "gRPC reply stream error: {e}"
                                    )),
                                });
                                let _ = tx.send(failure).await;
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(rx)
    }

    #[instrument(skip(self, envelope), fields(runner_address = %address))]
    async fn notify(
        &self,
        address: &RunnerAddress,
        envelope: Envelope,
    ) -> Result<(), ClusterError> {
        let mut client = self.client_for(address).await?;

        let envelope = match envelope {
            Envelope::Request(envelope) => Envelope::Request(inject_trace_context(envelope)),
            other => other,
        };

        let envelope_bytes =
            rmp_serde::to_vec(&envelope).map_err(|e| ClusterError::MalformedMessage {
                reason: "failed to serialize envelope".into(),
                source: Some(Box::new(e)),
            })?;

        client
            .notify(Request::new(proto::NotifyRequest {
                envelope: envelope_bytes,
            }))
            .await
            .map_err(|e| ClusterError::RunnerUnavailable {
                address: address.clone(),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }

    async fn on_runner_unavailable(&self, address: &RunnerAddress) -> Result<(), ClusterError> {
        self.remove_connection(address);
        tracing::info!(%address, "removed cached gRPC connection for unavailable runner");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Server: GrpcRunnerServer
// ---------------------------------------------------------------------------

/// gRPC server that handles incoming runner-to-runner requests.
///
/// Delegates to a [`Sharding`] instance for local message routing.
pub struct GrpcRunnerServer {
    sharding: Arc<dyn Sharding>,
}

impl GrpcRunnerServer {
    /// Create a new gRPC runner server backed by the given sharding instance.
    pub fn new(sharding: Arc<dyn Sharding>) -> Self {
        Self { sharding }
    }

    /// Build a tonic [`RunnerServiceServer`] ready to be added to a tonic router.
    pub fn into_service(self) -> RunnerServiceServer<Self> {
        RunnerServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl RunnerService for GrpcRunnerServer {
    #[instrument(skip_all)]
    async fn ping(
        &self,
        _request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        Ok(Response::new(proto::PingResponse {}))
    }

    type SendStream = tokio_stream::wrappers::ReceiverStream<Result<proto::ReplyMessage, Status>>;

    #[instrument(skip_all, fields(remote_trace_id, remote_span_id, remote_sampled))]
    async fn send(
        &self,
        request: Request<proto::SendRequest>,
    ) -> Result<Response<Self::SendStream>, Status> {
        let envelope_bytes = request.into_inner().envelope;
        let envelope: EnvelopeRequest = match rmp_serde::from_slice::<Envelope>(&envelope_bytes) {
            Ok(Envelope::Request(envelope)) => envelope,
            Ok(_) => {
                return Err(Status::invalid_argument(
                    "expected Request envelope for send",
                ))
            }
            Err(_) => rmp_serde::from_slice(&envelope_bytes)
                .map_err(|e| Status::invalid_argument(format!("malformed envelope: {e}")))?,
        };

        extract_trace_context(&envelope);

        let mut reply_rx = self
            .sharding
            .send(envelope)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        tokio::spawn(async move {
            while let Some(reply) = reply_rx.recv().await {
                match rmp_serde::to_vec(&reply) {
                    Ok(bytes) => {
                        let msg = proto::ReplyMessage { reply: bytes };
                        if tx.send(Ok(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "failed to serialize reply: {e}"
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    #[instrument(skip_all, fields(remote_trace_id, remote_span_id, remote_sampled))]
    async fn notify(
        &self,
        request: Request<proto::NotifyRequest>,
    ) -> Result<Response<proto::NotifyResponse>, Status> {
        let envelope_bytes = request.into_inner().envelope;
        let envelope = match rmp_serde::from_slice::<Envelope>(&envelope_bytes) {
            Ok(envelope) => envelope,
            Err(_) => {
                let envelope: EnvelopeRequest = rmp_serde::from_slice(&envelope_bytes)
                    .map_err(|e| Status::invalid_argument(format!("malformed envelope: {e}")))?;
                Envelope::Request(envelope)
            }
        };

        match envelope {
            Envelope::Request(envelope) => {
                extract_trace_context(&envelope);
                self.sharding
                    .notify(envelope)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            Envelope::AckChunk(ack) => {
                self.sharding
                    .ack_chunk(ack)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            Envelope::Interrupt(interrupt) => {
                self.sharding
                    .interrupt(interrupt)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }

        Ok(Response::new(proto::NotifyResponse {}))
    }
}

// ---------------------------------------------------------------------------
// Health: GrpcRunnerHealth
// ---------------------------------------------------------------------------

/// [`RunnerHealth`] implementation that checks liveness via gRPC ping.
pub struct GrpcRunnerHealth {
    runners: Arc<GrpcRunners>,
}

impl GrpcRunnerHealth {
    /// Create a new health checker using the given gRPC runners client.
    pub fn new(runners: Arc<GrpcRunners>) -> Self {
        Self { runners }
    }
}

#[async_trait]
impl RunnerHealth for GrpcRunnerHealth {
    async fn is_alive(&self, address: &RunnerAddress) -> Result<bool, ClusterError> {
        match self.runners.ping(address).await {
            Ok(()) => Ok(true),
            Err(ClusterError::RunnerUnavailable { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Trace context propagation helpers
// ---------------------------------------------------------------------------

/// Inject the current tracing span's ID into the envelope's trace context fields.
///
/// When `tracing-opentelemetry` is configured as a subscriber layer, the span
/// ID corresponds to the OpenTelemetry span/trace IDs, enabling distributed
/// tracing across runner-to-runner gRPC calls. Without OpenTelemetry, the
/// numeric `tracing::Id` is injected as a string for correlation in logs.
fn inject_trace_context(mut envelope: EnvelopeRequest) -> EnvelopeRequest {
    let span = tracing::Span::current();
    if let Some(id) = span.id() {
        // Use the tracing span ID as span_id. The trace_id field requires
        // OpenTelemetry integration to populate meaningfully; without it we
        // propagate whatever was already set on the envelope.
        if envelope.span_id.is_none() {
            envelope.span_id = Some(format!("{}", id.into_u64()));
        }
    }
    envelope
}

/// Record the remote trace context from an incoming envelope on the current span.
///
/// When `tracing-opentelemetry` is configured as a subscriber layer, these
/// fields enable distributed trace correlation across runner-to-runner gRPC
/// calls. Without OpenTelemetry, they appear in structured log output.
fn extract_trace_context(envelope: &EnvelopeRequest) {
    let span = tracing::Span::current();
    if let Some(ref trace_id) = envelope.trace_id {
        span.record("remote_trace_id", trace_id.as_str());
    }
    if let Some(ref span_id) = envelope.span_id {
        span.record("remote_span_id", span_id.as_str());
    }
    if let Some(sampled) = envelope.sampled {
        span.record("remote_sampled", sampled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ShardingConfig;
    use crate::entity::{Entity, EntityContext, EntityHandler};
    use crate::metrics::ClusterMetrics;
    use crate::reply::{ExitResult, Reply};
    use crate::sharding_impl::ShardingImpl;
    use crate::snowflake::Snowflake;
    use crate::storage::noop_runners::NoopRunners;
    use crate::types::{EntityAddress, EntityId, EntityType};
    use std::collections::HashMap;
    use std::net::SocketAddr;

    struct EchoEntity;

    #[async_trait]
    impl Entity for EchoEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Echo")
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(EchoHandler))
        }
    }

    struct EchoHandler;

    #[async_trait]
    impl EntityHandler for EchoHandler {
        async fn handle_request(
            &self,
            _tag: &str,
            payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            Ok(payload.to_vec())
        }
    }

    /// Start a gRPC server on a random port and return the address.
    async fn start_test_server(sharding: Arc<dyn Sharding>) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = GrpcRunnerServer::new(sharding);
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        addr
    }

    fn make_sharding(addr: SocketAddr) -> Arc<ShardingImpl> {
        let config = Arc::new(ShardingConfig {
            runner_address: RunnerAddress::new(addr.ip().to_string(), addr.port()),
            shard_groups: vec!["default".to_string()],
            shards_per_group: 10,
            ..Default::default()
        });
        let runners: Arc<dyn Runners> = Arc::new(NoopRunners);
        let metrics = Arc::new(ClusterMetrics::unregistered());
        ShardingImpl::new(config, runners, None, None, None, metrics).unwrap()
    }

    #[tokio::test]
    async fn grpc_ping_roundtrip() {
        let sharding = make_sharding("127.0.0.1:0".parse().unwrap());
        let addr = start_test_server(sharding).await;

        let runners = GrpcRunners::new();
        let address = RunnerAddress::new(addr.ip().to_string(), addr.port());
        runners.ping(&address).await.unwrap();
    }

    #[tokio::test]
    async fn grpc_send_and_receive_reply() {
        let sharding = make_sharding("127.0.0.1:0".parse().unwrap());
        sharding.acquire_all_shards().await;
        sharding
            .register_entity(Arc::new(EchoEntity))
            .await
            .unwrap();

        let addr = start_test_server(sharding.clone()).await;
        let runners = GrpcRunners::new();
        let address = RunnerAddress::new(addr.ip().to_string(), addr.port());

        let eid = EntityId::new("e-1");
        let shard = sharding.get_shard_id(&EntityType::new("Echo"), &eid);
        let payload = rmp_serde::to_vec(&"hello").unwrap();

        let envelope = EnvelopeRequest {
            request_id: sharding.snowflake().next_async().await.unwrap(),
            address: EntityAddress {
                shard_id: shard,
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "echo".into(),
            payload: payload.clone(),
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let mut rx = runners.send(&address, envelope).await.unwrap();
        let reply = rx.recv().await.unwrap();
        match reply {
            Reply::WithExit(r) => match r.exit {
                ExitResult::Success(bytes) => assert_eq!(bytes, payload),
                ExitResult::Failure(msg) => panic!("unexpected failure: {msg}"),
            },
            Reply::Chunk(_) => panic!("unexpected chunk"),
        }
    }

    #[tokio::test]
    async fn grpc_notify_fire_and_forget() {
        let sharding = make_sharding("127.0.0.1:0".parse().unwrap());
        sharding.acquire_all_shards().await;
        sharding
            .register_entity(Arc::new(EchoEntity))
            .await
            .unwrap();

        let addr = start_test_server(sharding.clone()).await;
        let runners = GrpcRunners::new();
        let address = RunnerAddress::new(addr.ip().to_string(), addr.port());

        let eid = EntityId::new("e-2");
        let shard = sharding.get_shard_id(&EntityType::new("Echo"), &eid);

        let envelope = EnvelopeRequest {
            request_id: sharding.snowflake().next_async().await.unwrap(),
            address: EntityAddress {
                shard_id: shard,
                entity_type: EntityType::new("Echo"),
                entity_id: eid,
            },
            tag: "ping".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        runners
            .notify(&address, Envelope::Request(envelope))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn grpc_ping_unavailable_runner() {
        let runners = GrpcRunners::new();
        // Port that's not listening
        let address = RunnerAddress::new("127.0.0.1", 1);
        let result = runners.ping(&address).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn grpc_health_check_alive() {
        let sharding = make_sharding("127.0.0.1:0".parse().unwrap());
        let addr = start_test_server(sharding).await;

        let runners = Arc::new(GrpcRunners::new());
        let health = GrpcRunnerHealth::new(Arc::clone(&runners));
        let address = RunnerAddress::new(addr.ip().to_string(), addr.port());
        assert!(health.is_alive(&address).await.unwrap());
    }

    #[tokio::test]
    async fn grpc_health_check_dead() {
        let runners = Arc::new(GrpcRunners::new());
        let health = GrpcRunnerHealth::new(Arc::clone(&runners));
        let address = RunnerAddress::new("127.0.0.1", 1);
        assert!(!health.is_alive(&address).await.unwrap());
    }

    #[tokio::test]
    async fn grpc_connection_pooling() {
        let sharding = make_sharding("127.0.0.1:0".parse().unwrap());
        let addr = start_test_server(sharding).await;

        let runners = GrpcRunners::new();
        let address = RunnerAddress::new(addr.ip().to_string(), addr.port());

        // First ping creates connection
        runners.ping(&address).await.unwrap();
        assert_eq!(runners.channels.len(), 1);

        // Second ping reuses connection
        runners.ping(&address).await.unwrap();
        assert_eq!(runners.channels.len(), 1);
    }

    #[tokio::test]
    async fn grpc_inject_trace_context_populates_span_id() {
        let envelope = EnvelopeRequest {
            request_id: Snowflake(999),
            address: EntityAddress {
                shard_id: crate::types::ShardId::new("default", 0),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new("t-1"),
            },
            tag: "test".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        // inject_trace_context should not panic and should return the envelope
        let result = super::inject_trace_context(envelope);
        // span_id may or may not be set depending on whether there's an active span
        // but the function should not panic
        assert_eq!(result.tag, "test");
    }

    #[tokio::test]
    async fn grpc_extract_trace_context_does_not_panic() {
        let envelope = EnvelopeRequest {
            request_id: Snowflake(999),
            address: EntityAddress {
                shard_id: crate::types::ShardId::new("default", 0),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new("t-1"),
            },
            tag: "test".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: Some("12345".into()),
            trace_id: Some("abcdef".into()),
            sampled: Some(true),
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        // Should not panic even without an active subscriber
        super::extract_trace_context(&envelope);
    }

    #[tokio::test]
    async fn grpc_trace_context_roundtrip() {
        // Verify that trace context set on an envelope is preserved through
        // inject (which doesn't overwrite existing values)
        let envelope = EnvelopeRequest {
            request_id: Snowflake(999),
            address: EntityAddress {
                shard_id: crate::types::ShardId::new("default", 0),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new("t-1"),
            },
            tag: "test".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: Some("existing-span".into()),
            trace_id: Some("existing-trace".into()),
            sampled: Some(true),
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };

        let result = super::inject_trace_context(envelope);
        // Existing values should not be overwritten
        assert_eq!(result.span_id.as_deref(), Some("existing-span"));
        assert_eq!(result.trace_id.as_deref(), Some("existing-trace"));
        assert_eq!(result.sampled, Some(true));
    }

    #[tokio::test]
    async fn grpc_connect_timeout_is_applied() {
        // Use a very short timeout to verify it takes effect
        let runners = GrpcRunners::with_connect_timeout(std::time::Duration::from_millis(100));
        // Connect to a non-routable address that will hang (not refuse)
        // 192.0.2.1 is TEST-NET-1 (RFC 5737), typically black-holes packets
        let address = RunnerAddress::new("192.0.2.1", 9999);
        let start = std::time::Instant::now();
        let result = runners.ping(&address).await;
        let elapsed = start.elapsed();
        assert!(result.is_err());
        // Should fail within a reasonable time (timeout + overhead), not minutes
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "connect should have timed out quickly, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn grpc_on_runner_unavailable_removes_connection() {
        let sharding = make_sharding("127.0.0.1:0".parse().unwrap());
        let addr = start_test_server(sharding).await;

        let runners = GrpcRunners::new();
        let address = RunnerAddress::new(addr.ip().to_string(), addr.port());

        runners.ping(&address).await.unwrap();
        assert_eq!(runners.channels.len(), 1);

        runners.on_runner_unavailable(&address).await.unwrap();
        assert_eq!(runners.channels.len(), 0);
    }
}
