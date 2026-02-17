---
default: patch
---

feat: propagate OTel trace context across message boundaries

Envelopes now carry the sender's OpenTelemetry trace/span IDs so that the
receiver's `handle_message_with_recovery` span is linked as a child of the
sender's span. This connects previously disconnected traces (e.g.
`scale_deployment` → `handle_message_with_recovery`) into a single
distributed trace in Jaeger or any OTel-compatible collector.

- Inject trace context in `build_envelope` (covers local, gRPC, and persisted paths)
- Extract and set remote parent in `handle_message_with_recovery`
- Remove redundant gRPC-only `inject_trace_context` / `extract_trace_context`
- Add `opentelemetry` and `tracing-opentelemetry` as required dependencies
- Example app conditionally enables OTLP export via `OTEL_EXPORTER_OTLP_ENDPOINT`
