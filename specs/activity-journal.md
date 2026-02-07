# Activity Journaling

## Problem

When a `#[workflow]` method calls a `#[activity]` method, the activity is executed as a direct `self.method_name()` call. If the workflow fails or the entity crashes mid-execution, crash recovery re-delivers the same message and the **entire** workflow body re-executes from the top — including activities that already completed and committed side effects (state mutations, entity-to-entity messages, external I/O). There is no mechanism to skip already-completed activities on replay.

This violates a fundamental property of durable workflow systems: **activities must be journaled so that on replay, their cached result is returned instead of re-executing**.

The TypeScript reference implementation has this — each activity invocation is a persisted RPC with a deterministic `primaryKey` (`"activityName/attempt"`), and `MessageStorage` handles deduplication. Cruster has all the underlying infrastructure (`MessageStorage`, `SaveResult::Duplicate`, `persisted_request_id`) but does not route intra-entity activity calls through it.

### Consequences

1. **Duplicate side effects** — activities that send messages to other entities, perform SQL writes, or call external services will re-fire on every crash recovery replay.
2. **OOM on crash loops** — if an activity late in a workflow panics, the crash recovery loop re-executes all prior activities on each retry, spawning unbounded work.
3. **State corruption** — activities that already committed state mutations via `ActivityScope` will re-execute against the already-mutated state, potentially double-counting or corrupting data.

## Design

### Core Idea

Route activity calls within workflows through `MessageStorage` as self-addressed persisted messages with deterministic request IDs. On replay, `MessageStorage` returns the cached reply (`SaveResult::Duplicate`) and the activity body is never re-entered.

### How It Works

#### 1. Activity Execution Key

Each activity invocation needs a deterministic identity. Composed from:

```
(entity_type, entity_id, workflow_tag, activity_name, execution_key)
```

- `entity_type` + `entity_id` — the entity running the workflow
- `workflow_tag` — the `#[workflow]` method name (the tag of the outer persisted message)
- `activity_name` — the `#[activity]` method name
- `execution_key` — derived from the activity's input arguments (or from an explicit `key(...)` closure if provided)

This produces a deterministic `Snowflake` via `persisted_request_id()` which becomes the `request_id` for the journaled message.

#### 2. Execution Flow

**First execution:**

```
workflow body
  └─ calls self.activity(args)
       └─ macro-generated wrapper:
            1. Compute deterministic request_id from (entity, workflow, activity, args)
            2. Check MessageStorage: save_request(envelope) → SaveResult::Success
            3. Execute the activity body (acquire write lock, run in ActivityScope)
            4. Serialize the return value
            5. Save reply to MessageStorage: save_reply(request_id, result)
            6. Return deserialized result to workflow
```

**Replay (crash recovery re-delivery):**

```
workflow body (re-executing from the top)
  └─ calls self.activity(args)
       └─ macro-generated wrapper:
            1. Compute deterministic request_id (same as before)
            2. Check MessageStorage: save_request(envelope) → SaveResult::Duplicate { existing_reply }
            3. Return deserialized result from cached reply — activity body never runs
```

#### 3. What Changes

**`DurableContext`** — add a method for journaled execution:

```rust
impl DurableContext {
    /// Execute a closure with journaled result caching.
    ///
    /// On first execution, runs the closure and persists the serialized result
    /// to MessageStorage. On replay, returns the cached result without
    /// re-executing the closure.
    pub async fn run<T, F, Fut>(
        &self,
        name: &str,
        f: F,
    ) -> Result<T, ClusterError>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ClusterError>>,
    {
        // 1. Build deterministic request_id from (workflow_name, execution_id, name)
        // 2. Try save_request → if Duplicate with reply, deserialize and return
        // 3. Otherwise execute f(), serialize result, save_reply, return
    }
}
```

This parallels `ctx.sleep()` and `ctx.await_deferred()` — durable operations that are idempotent on re-entry.

**Macro codegen** — the `#[activity]` delegation generated on `ReadView` (currently a plain `self.__handler.method_name().await`) needs to wrap the call through `DurableContext::run()` when called from within a workflow:

Current generated code (`lib.rs:2522-2527`):
```rust
// ReadView activity delegation (used by #[workflow] methods)
async fn activity_method(&self, args: Args) -> Result<T, ClusterError> {
    self.__handler.activity_method(args).await
}
```

New generated code:
```rust
async fn activity_method(&self, args: Args) -> Result<T, ClusterError> {
    let ctx = self.__durable_context()?;
    let handler = self.__handler;
    ctx.run("activity_method", || async move {
        handler.activity_method(args).await
    }).await
}
```

The `name` parameter to `ctx.run()` is the activity method name, which combined with the `DurableContext`'s `(workflow_name, execution_id)` gives a unique journal key.

**`MessageStorage` usage** — `DurableContext::run()` needs access to `MessageStorage`. Either:
- (a) Add a `message_storage: Option<Arc<dyn MessageStorage>>` field to `DurableContext`
- (b) Route through `WorkflowEngine` with a new `journal_activity` / `load_activity_result` method

Option (a) is simpler and aligns with how the TS reference does it (activities go through the same `MessageStorage` as regular persisted messages).

**Handler struct** — the macro already stores `__state_storage` (WorkflowStorage) and `__workflow_engine` (WorkflowEngine) on the handler. It also needs to store `__message_storage: Option<Arc<dyn MessageStorage>>` so that `DurableContext` can access it.

### Handling Multiple Calls to the Same Activity

If a workflow calls the same activity method multiple times (e.g., in a loop), the `name` alone isn't sufficient to distinguish invocations. Two approaches:

**Option A: Caller-provided key (explicit)**

Require a unique key per invocation via `ctx.run()`:

```rust
#[workflow]
async fn schedule_containers(&self, replicas: u32) -> Result<(), ClusterError> {
    for i in 0..replicas {
        let key = format!("provision/{i}");
        ctx.run(&key, || self.provision_container(i)).await?;
    }
}
```

This is what the TS reference does — the `primaryKey` is `"activityName/attempt"` where `attempt` is managed by the caller.

**Option B: Auto-incrementing sequence (implicit)**

Track a step counter in `DurableContext` that auto-increments on each `run()` call:

```rust
ctx.run("provision_container", || ...).await?;  // → key: "provision_container/0"
ctx.run("provision_container", || ...).await?;  // → key: "provision_container/1"
```

This is simpler for the user but fragile — if the workflow body changes (adds/removes/reorders activity calls), the step numbers shift and the journal entries no longer align. This is a known problem in workflow systems (Temporal has this issue with non-deterministic replay).

**Recommendation: Option A** — explicit keys. It's more verbose but correct by construction. The `#[activity]` macro can sugar this by using the serialized arguments as the key by default (matching how `#[activity(key(...))]` already works for client-side calls).

### Interaction with ActivityScope

`ActivityScope::run()` handles transactional state persistence. Activity journaling is orthogonal — it caches the *result* so the activity isn't re-entered, while `ActivityScope` ensures state mutations are atomic.

The journal check happens **before** entering `ActivityScope`:

```
journal check (MessageStorage)
  └─ cache hit → return cached result (ActivityScope never entered)
  └─ cache miss → enter ActivityScope
       └─ execute activity body
       └─ commit state transaction
       └─ save result to journal (MessageStorage)
       └─ return result
```

State persistence and result journaling are committed as separate operations. This is safe because:
- If the activity completes and state commits but the journal write fails → on replay, the activity re-executes against already-mutated state. The activity must be designed to handle this (idempotent state mutations). This is the same contract as today.
- If the journal write succeeds but state commit failed → impossible, state commits first inside `ActivityScope`.

To make this fully atomic, the journal write could be included in the `ActivityScope` transaction. This requires `MessageStorage` to support participating in a `WorkflowStorage` transaction, which is possible since both use the same Postgres pool in the SQL implementations.

### Direct Activity Calls (Outside Workflows)

Activities can also be called directly by clients (not from within a workflow). In this case, the call arrives as a persisted message through `MessageStorage` and is already deduplicated at the message level. The journal wrapping only applies to the `ReadView` delegation path (workflow → activity), not the top-level dispatch path.

## Implementation Plan

### Phase 1: `DurableContext::run()`

1. Add `message_storage: Option<Arc<dyn MessageStorage>>` field to `DurableContext`
2. Implement `DurableContext::run()` using `MessageStorage::save_request` / `save_reply` for journal check
3. Add `MemoryMessageStorage` support (already exists, just needs to be wired in)
4. Unit tests: run() caches results, run() returns cached on replay, run() with different keys

### Phase 2: Macro codegen

1. Pass `message_storage` from `EntityContext` through to `DurableContext` construction
2. Update `ReadView` activity delegation to route through `ctx.run()` instead of direct call
3. Generate the journal key from `(workflow_tag, activity_method_name, serialized_args)` — or from the `key(...)` closure if provided
4. Handle the case where `DurableContext` is not available (no workflow engine configured) — fall back to direct execution (current behavior)

### Phase 3: Atomic journal + state commit

1. Extend `ActivityScope` to optionally include a journal write in the same transaction
2. `SqlMessageStorage::save_reply_in_transaction()` that accepts an existing `StorageTransaction`
3. This eliminates the window between state commit and journal write

### Phase 4: Workflow-calls-workflow

When a `#[workflow]` calls another `#[workflow]` via `self.other_workflow()`, the inner workflow should also be journaled (its entire result cached). The same `ctx.run()` mechanism applies — the inner workflow is wrapped just like an activity call.

## Migration

This is a backward-compatible addition. Existing code that doesn't use `DurableContext` continues to work as before (activities execute directly without journaling). The journaling only activates when:

1. `MessageStorage` is configured on the sharding instance
2. The activity is called from within a `#[workflow]` method (via the `ReadView` delegation)

Existing `#[activity(key(...))]` annotations on the *client* side are unaffected — those control the external call deduplication, not the intra-entity journal.
