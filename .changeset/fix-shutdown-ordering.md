---
default: patch
---

fix: reorder shutdown to release shard locks after background tasks stop

Moves `release_all()` and shard clearing to execute AFTER waiting for background tasks to complete. This prevents concurrent etcd operations from the rebalance/refresh loops while the lengthy shard release is in progress, reducing shutdown time from ~32 seconds to ~17 seconds.
