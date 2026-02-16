---
default: patch
---

fix: single-param activity key closure passes reference and journal check uses WorkflowStorage directly

The activity key closure for the single-param case now receives `&param` (reference) instead of the owned value, consistent with the multi-param case and preventing move errors when the activity body also uses the parameter.

Journal `check_journal` now queries `WorkflowStorage` directly for cached results instead of inserting synthetic messages into `MessageStorage` for dedup. The old approach caused `__journal/*` messages to leak into the delivery loop, producing spurious failures.
