---
"cruster": patch
---

Remove all memory-based fakes (MemoryMessageStorage, MemoryRunnerStorage, MemoryWorkflowStorage, MemoryWorkflowEngine, TestCluster) and replace with PostgreSQL-backed integration tests. Entities are now fully stateless — state management is the application's responsibility via direct database access. Rewrite README to document the new API: stateless entities with `#[rpc]`/`#[rpc(persisted)]`, durable workflows with `self.tx` (ActivityTx), activity groups, RPC groups, singletons, and cron jobs.
