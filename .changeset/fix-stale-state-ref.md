---
default: patch
---

fix: resolve stale state in workflows after activity calls

`self.state` in `#[workflow]` and `#[rpc]` methods previously captured a
point-in-time snapshot at method entry via `ArcSwap::load()`. After calling
an activity that mutated state, subsequent reads of `self.state` within the
same workflow would still return pre-activity values.

Introduces `StateRef<S>`, a read-only proxy wrapping `Arc<S>` that implements
`Deref<Target=S>`. The macro-generated activity delegation methods now refresh
the proxy after each activity commits, so `self.state.field` always reflects
the latest committed state. User-facing syntax is unchanged.
