---
default: patch
---

fix: capture OTel trace context before debug-level span filtering

Move trace context extraction out of `build_envelope` (debug-level span) into
a `capture_trace_context()` call in each public caller (INFO-level span). This
fixes disconnected traces when per-layer `Targets` filters reject debug spans
from the OTel layer — the exact configuration used in autopilot-cruster.

Add regression tests with `InMemorySpanExporter` that verify trace context
injection works even under INFO-only OTel filters.
