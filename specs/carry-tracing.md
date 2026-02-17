# Carry Tracing Context Across Message Boundaries

## Problem

When a workflow like `scale_deployment` sends a persisted message that triggers `handle_message_with_recovery` on another entity, the two operations appear as **disconnected root traces** in Jaeger/any OTel collector. There is no parent-child or link relationship between the sender and receiver spans.

## Root Cause

The `EnvelopeRequest` struct already carries `span_id`, `trace_id`, and `sampled` fields (`envelope.rs:35-37`), but they are never populated with real OTel values:

1. `build_envelope` (`entity_client.rs:553`) always sets `span_id: None, trace_id: None, sampled: None`
2. `inject_trace_context` (`grpc.rs:452`) only runs on the gRPC path, and uses raw `tracing::Id` (opaque u64) instead of OTel-compatible IDs
3. `extract_trace_context` (`grpc.rs:470`) records remote IDs as span fields but does not establish a parent-child relationship
4. `handle_message_with_recovery` (`entity_manager.rs:983`) creates a fresh root span and ignores the envelope's trace fields entirely

## Message Delivery Paths

Both paths must propagate context:

```
Local (including persisted):
  build_envelope -> sharding.send -> storage.save -> poll_storage -> send_local -> handle_message_with_recovery

Remote (gRPC):
  build_envelope -> sharding.send -> grpc.send -> [network] -> grpc.receive -> sharding.send -> send_local -> handle_message_with_recovery
```

The envelope is the carrier in both cases. For persisted messages, the envelope is serialized to Postgres and deserialized later — the trace fields survive this roundtrip since they are part of the `EnvelopeRequest` struct.

## Dependencies

Add `opentelemetry` and `tracing-opentelemetry` as **required dependencies** of the `cruster` crate (not behind a feature flag).

Rationale:
- All `#[instrument]` spans are already unconditional
- The `EnvelopeRequest` already carries trace context fields unconditionally
- The previous `otel` feature flag was added (v0.0.20) then removed (v0.0.21) — a clean always-on approach is simpler
- Any production deployment will want connected traces

## Implementation Plan

### Phase 1 — Inject at the source (`build_envelope`)

**File:** `crates/cruster/src/entity_client.rs`

In `build_envelope` (~line 553), use `tracing-opentelemetry`'s `OpenTelemetrySpanExt` to read the current span's OTel `SpanContext` and populate the envelope:

```rust
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing::Span;

let span = Span::current();
let context = span.context();
let span_context = context.span().span_context().clone();

if span_context.is_valid() {
    envelope.trace_id = Some(span_context.trace_id().to_string());
    envelope.span_id = Some(span_context.span_id().to_string());
    envelope.sampled = Some(span_context.trace_flags().is_sampled());
}
```

This is the single injection point for **all** delivery paths (local, remote, persisted). Every envelope created by `EntityClient` will carry the sender's trace context.

### Phase 2 — Extract at the receiver (`handle_message_with_recovery`)

**File:** `crates/cruster/src/entity_manager.rs`

In `handle_message_with_recovery` (~line 983), before or just after creating the `#[instrument]` span, read the envelope's trace context and set it as a remote parent:

```rust
use opentelemetry::trace::{SpanContext, SpanId, TraceId, TraceFlags, TraceState};
use tracing_opentelemetry::OpenTelemetrySpanExt;

// Inside handle_message_with_recovery, after the span is entered:
if let (Some(trace_id), Some(span_id)) = (&envelope.trace_id, &envelope.span_id) {
    if let (Ok(tid), Ok(sid)) = (
        TraceId::from_hex(trace_id),
        SpanId::from_hex(span_id),
    ) {
        let flags = if envelope.sampled.unwrap_or(false) {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::default()
        };
        let remote_context = SpanContext::new(tid, sid, flags, true, TraceState::default());
        let otel_context = opentelemetry::Context::current()
            .with_remote_span_context(remote_context);
        Span::current().set_parent(otel_context);
    }
}
```

This establishes a proper parent-child link: the receiver's span becomes a child of the sender's span in the trace tree.

### Phase 3 — Clean up gRPC propagation

**File:** `crates/cruster/src/transport/grpc.rs`

Now that `build_envelope` handles injection for all paths:

1. **Remove `inject_trace_context`** (~line 452) and its calls in `GrpcRunners::send` (line 162) and `GrpcRunners::notify` (line 256) — the envelope already carries context before it reaches gRPC
2. **Remove `extract_trace_context`** (~line 470) and its calls in `GrpcRunnerServer::send` (line 338) and `GrpcRunnerServer::notify` (line 391) — extraction now happens in `handle_message_with_recovery`
3. Remove the `remote_trace_id`, `remote_span_id`, `remote_sampled` fields from the `#[instrument]` annotations on `GrpcRunnerServer::send` and `GrpcRunnerServer::notify`
4. Update or remove the existing tests at `grpc.rs:699-750` (`grpc_inject_trace_context_populates_span_id`, `grpc_extract_trace_context_does_not_panic`, `grpc_trace_context_roundtrip`)

### Phase 4 — Update dependencies and verify

**File:** `crates/cruster/Cargo.toml`

Add under `[dependencies]`:

```toml
opentelemetry = "0.28"
tracing-opentelemetry = "0.29"
```

**File:** `examples/cluster-tests/Cargo.toml`

Add OTel subscriber dependencies so the example can export traces:

```toml
opentelemetry = "0.28"
opentelemetry-otlp = "0.28"
opentelemetry_sdk = { version = "0.28", features = ["rt-tokio"] }
tracing-opentelemetry = "0.29"
```

Update the subscriber setup in `examples/cluster-tests/src/main.rs` (~line 205) to include an OTel layer:

```rust
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use tracing_opentelemetry::OpenTelemetryLayer;

let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(opentelemetry_otlp::new_exporter().tonic())
    .install_batch(opentelemetry_sdk::runtime::Tokio)?;

tracing_subscriber::registry()
    .with(fmt::layer())
    .with(OpenTelemetryLayer::new(tracer))
    .with(EnvFilter::from_default_env().add_directive("cluster_tests=info".parse()?))
    .init();
```

## Testing

1. **Unit tests:** Replace the removed gRPC trace context tests with tests for the new injection/extraction logic:
   - `build_envelope` populates trace fields when an OTel span is active
   - `build_envelope` leaves trace fields as `None` when no OTel span is active
   - `handle_message_with_recovery` sets parent from envelope trace context
   - Round-trip: inject into envelope, serialize/deserialize via MessagePack, extract and verify trace/span IDs match

2. **E2E verification:** Run the cluster-tests example with an OTel collector (e.g., Jaeger) and confirm that `scale_deployment` and the triggered `handle_message_with_recovery` appear as a single connected trace.

## Expected Result

After this change, the two traces from the screenshots will merge into one:

```
scale_deployment (15.49s)
  └── send_persisted_with_key (15.49s)
        └── send (1.16s)
              └── send (922ms)
                    └── handle_message_with_recovery (14.44s)   <-- now a child span
                          └── execute (11.95s)
                                ├── load_deployment_info (408ms)
                                ├── update_replicas (163ms)
                                ├── load_deploy_data (243ms)
                                ├── schedule (256ms)
                                ├── send_persisted_with_key (6.1s)
                                │     └── ...
                                ├── send_persisted_with_key (5.9s)
                                │     └── ...
                                ├── mark_status (161ms)
                                └── load_deployment_info (433ms)
```
