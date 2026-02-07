---
default: patch
---

fix: prevent infinite gRPC self-loop in shard routing

When `shard_assignments` marked a shard as owned by the local runner but `owned_shards` had not yet been populated (during the acquire phase window), `send()`, `notify()`, and `interrupt()` would route the message as "remote" via gRPC to the local runner itself. The gRPC handler called back into `sharding.send()`, hitting the same code path, creating an infinite loop that grew memory at ~350MB/s.

Added self-send guards to all three routing methods: when `get_shard_owner_async()` returns the local runner's address, route locally instead of making a gRPC call. Also handles persisted messages correctly in the self-routed path.

Concurrent `acquire_batch`/`refresh_batch` using `FuturesUnordered` with a concurrency limit of 64, reducing 2048-shard acquisition from 30+ seconds to under 5 seconds.
