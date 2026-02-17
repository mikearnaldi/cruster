---
default: patch
---

Remove otel.rs init/setup module from the library crate

OpenTelemetry tracing pipeline initialization (`OtelConfig`, `OtelGuard`, `init_tracing`) is the consumer's responsibility. The library should only emit `tracing` spans (via `#[instrument]`), not dictate how they are collected. All `#[instrument]` annotations and trace-context propagation remain intact.
