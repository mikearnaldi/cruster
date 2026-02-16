---
default: patch
---

feat: derive `Clone` on `EntityClient` and generated entity/workflow clients

All clients are now `Clone`. They are lightweight handles (`Arc` + `String`),
so cloning is cheap and `Arc` wrapping is no longer necessary.
