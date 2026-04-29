---
default: patch
---

refactor: rely on sqlx migration tracking

Removes the custom migration tracking implementation, uses sqlx's migrator directly for framework migrations, and fixes release PR creation to target the correct GitHub repository.
