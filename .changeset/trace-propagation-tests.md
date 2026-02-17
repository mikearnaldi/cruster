---
default: patch
---

test: add OTel trace context propagation tests

Add tests that verify trace context injection and extraction work end-to-end
with an active OpenTelemetry subscriber, using `InMemorySpanExporter` from
`opentelemetry_sdk`. These tests catch regressions where the envelope's
`trace_id`/`span_id` fields would silently remain `None` and traces would
appear disconnected in collectors.
