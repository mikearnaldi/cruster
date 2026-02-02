---
default: patch
---

Fix shutdown hanging due to background tasks not respecting cancellation

Background loops (shard_acquisition_loop, lock_refresh_loop, storage_poll_loop) now check the cancellation token at key points during their work, not just at sleep boundaries. This ensures shutdown completes promptly instead of hanging while tasks continue making gRPC calls.
