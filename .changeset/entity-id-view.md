---
default: patch
---

feat: provide `self.entity_id()` in entity and RPC group handlers

Entity and RPC group `#[rpc]` handlers can now access the routed entity ID
via `self.entity_id()` (returns `&str`) and the full address via
`self.entity_address()`. This eliminates the need to pass `entity_id` as a
field in every request struct. Workflow request structs with domain-specific
ID parameters have been renamed for clarity (`owner_id`, `account_id`).
