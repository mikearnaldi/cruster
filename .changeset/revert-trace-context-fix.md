---
default: patch
---

revert: remove unnecessary capture_trace_context refactoring

The PR #36 fix (moving trace context capture before the debug-level
`build_envelope` span) was based on a wrong diagnosis. OTel context
lookup already falls back to the thread-local `opentelemetry::Context`
when the current tracing span has no OTel data, so filtered-out spans
are not a problem.

The actual root cause of disconnected traces is an `opentelemetry`
version mismatch between cruster (0.28) and autopilot-cruster (0.27),
which produces two separate thread-local context stores in the same
binary.
