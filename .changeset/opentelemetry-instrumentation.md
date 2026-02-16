---
default: patch
---

feat: add OpenTelemetry instrumentation behind `otel` feature flag

Adds optional OpenTelemetry support with an `otel.rs` initialization module and `#[instrument]` spans across all hot paths (message routing, entity lifecycle, database operations, workflow execution, gRPC transport, shard management). Zero overhead when the feature is disabled.
