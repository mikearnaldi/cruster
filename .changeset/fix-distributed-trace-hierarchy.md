---
default: patch
---

fix: correct distributed trace span hierarchy for cross-runner messages

Trace context is now propagated through each layer of the send path so that
spans form a proper causal chain instead of appearing as disconnected siblings.

- `route` (was `send` on ShardingImpl) stamps its span ID on the envelope at
  entry, making `handle_message_with_recovery` a child of the local `route`.
- `deliver` (was `send` on GrpcRunnerTransport) overwrites the envelope's trace
  context with its own span ID before serializing for gRPC.
- `receive` (was `send` on GrpcRunnerServer) reads the envelope's trace context
  and links itself as a child of `deliver`.

Resulting trace hierarchy:
- Local:  `send_persisted_with_key > route > handle_message_with_recovery`
- Remote: `send_persisted_with_key > route > deliver > receive > route > handle_message_with_recovery`
