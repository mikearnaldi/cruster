---
default: patch
---

breaking: upgrade sqlx and move single runner construction to a builder

Upgrades `sqlx` to `0.9.0-alpha.1`, adds configurable migration tracking tables through the storage and single-runner builders, and removes the old `SingleRunner::new` and `SingleRunner::with_config` constructors.
