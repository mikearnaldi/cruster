---
default: patch
---

Fix shutdown not cancelling in-flight network calls

- Network calls in background loops (get_runners, release, acquire_batch, refresh_batch) now race against the cancellation token using `tokio::select!`
- Fixed runner_health_loop singleton to use its own cancellation token from SingletonContext instead of ignoring it
- Added cancellation checks throughout check_runner_health including during concurrent health checks

Previously, even with cancellation checks between operations, a slow or hanging network call could block shutdown for the full timeout duration. The runner health singleton was also ignoring its cancellation context entirely.
