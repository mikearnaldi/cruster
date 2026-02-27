---
default: patch
---

fix: run cruster SQL migrations with a dedicated tracking table

Port the migration runner utility to store migration state in `_cruster_migrations` instead of SQLx's default tracking table.
