---
default: patch
---

fix: reduce OpenTelemetry span noise by tiering #[instrument] levels

Background loop drivers (shard acquisition, lock refresh, storage poll, runner health,
lease health) had their #[instrument] removed entirely -- these created infinite-lifetime
spans that never exported properly, appearing as "(missing)" parents in trace backends.

Internal mechanics (rebalancing, singleton sync, storage CRUD, workflow engine, entity
reaping, health checks) downgraded from INFO to DEBUG. User-facing operations (entity
client API, message routing, gRPC transport, activity execution, lifecycle events) remain
at INFO.
