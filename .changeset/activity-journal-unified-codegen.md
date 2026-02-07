---
default: patch
---

feat: add activity journaling for crash-recovery replay safety

When a `#[workflow]` method calls a `#[activity]`, the activity result is now
cached via `DurableContext`. On crash-recovery replay, the cached result is
returned instead of re-executing the activity body, preventing duplicate
side-effects.

Also unifies stateless and stateful entity codegen into a single path.
Persistence infrastructure (workflow engine, message storage, sharding,
durable builtins, activity journal wrapping) is now always generated
regardless of whether the entity has `#[state]`. State is orthogonal to
persistence — stateless entities get the same journaling and durable
workflow guarantees as stateful ones.

Key changes:
- `DurableContext` extended with journal check/write/serialize logic
- `ActivityScope::buffer_write` used for atomic journal+state commits
- `generate_stateless_entity` removed (~530 lines); unified into `generate_entity`
- All `state_persisted` guards removed from codegen — persistence always enabled
- Stateless entities now use the View pattern with `Deref` to entity struct
