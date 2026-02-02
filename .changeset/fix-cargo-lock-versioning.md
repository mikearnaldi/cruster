---
default: patch
---

Fix Cargo.lock versioning for all workspace crates

Configure knope to update all workspace crate versions in Cargo.lock during releases, not just `cruster`. This ensures CI passes with `--locked` flag after version bumps.
