---
default: patch
---

test: harden cluster-tests e2e suite and add shard topology coverage

Adds shard allocation/lease tests, per-node health gating, and more resilient
helpers for timing and transient HTTP failures.
