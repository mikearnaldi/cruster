# Cruster API Redesign: Stateless Entities + Standalone Workflows

## Problem

The current design has several issues:

1. **Nesting anti-pattern** — nothing prevents embedding workflows inside other workflows or calling entity methods that spawn further workflows, breaking the concurrency model (entities process one message at a time via mailbox).
2. **State semantics are confusing** — workflows use `&self` with `StateRef`, activities use `&mut self` with `StateMutGuard`, but both live on the same struct. The "who owns state" question is muddy.
3. **Mixed concerns** — entities conflate domain state management with workflow orchestration, making it easy to accidentally couple the two.
4. **Framework-managed state is a liability** — `ArcSwap<S>`, `StateMutGuard`, composite state serialization, and the `cluster_entity_state` table add complexity. In practice, applications use a real database (Postgres) for state anyway.

## Architecture Overview

The redesign separates concerns into two distinct constructs:

| Concern | Owner |
|---|---|
| Addressability, routing, sharding | **Entities** — stateless RPC handlers |
| Durable orchestration | **Workflows** — stateless, single `execute`, activities for side effects |
| State | **Your database** — not the framework |

Both entities and workflows are stateless. The framework no longer manages application state.

---

## Part 1: Simplified Entities

Entities become **addressable stateless RPC handlers**. Their value is sharding, routing, mailbox concurrency control, and optional message persistence.

### Entity Definition

```rust
use cruster::prelude::*;

#[entity(max_idle_time_secs = 120, mailbox_capacity = 50, concurrency = 4)]
#[derive(Clone)]
pub struct UserGateway {
    db: PgPool,
    http: HttpClient,
}

#[entity_impl]
impl UserGateway {
    #[rpc]
    async fn get_profile(&self, user_id: String) -> Result<Profile, ClusterError> {
        let row = sqlx::query_as("SELECT * FROM profiles WHERE id = $1")
            .bind(&user_id)
            .fetch_one(&self.db)
            .await?;
        Ok(row)
    }

    #[rpc(persisted)]
    async fn update_profile(&self, request: UpdateProfileRequest) -> Result<(), ClusterError> {
        sqlx::query("UPDATE profiles SET name = $1 WHERE id = $2")
            .bind(&request.name)
            .bind(&request.user_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }
}
```

No `init`, no `#[state]`, no `#[workflow]`, no `#[activity]`, no `&mut self`. Just `&self` RPCs.

### Entity Attributes

```rust
#[entity]                                              // all defaults
#[entity(max_idle_time_secs = 120)]                    // custom idle timeout
#[entity(mailbox_capacity = 50, concurrency = 4)]      // custom mailbox + concurrency
#[entity(name = "CustomName")]                         // override entity type name
#[entity(shard_group = "premium")]                     // custom shard group
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `name` | string | struct name | Override entity type name |
| `shard_group` | string | `"default"` | Shard group for routing |
| `max_idle_time_secs` | u64 | framework default | Idle timeout before eviction |
| `mailbox_capacity` | usize | framework default | Max queued messages |
| `concurrency` | usize | framework default | Max concurrent request processing |

### RPC Variants

| Variant | Delivery | Idempotency | Use case |
|---|---|---|---|
| `#[rpc]` | Best-effort | None | Reads, queries |
| `#[rpc(persisted)]` | At-least-once | Via `MessageStorage` dedup | Writes, mutations |

`persisted` means the request goes through `MessageStorage` (saved before dispatch, deduplicated on replay). This replaces the old distinction between `#[rpc]` (non-persisted) and `#[workflow]` (persisted).

### RPC Groups (Shared RPCs)

RPC groups replace entity traits. They bundle reusable RPCs composable into multiple entities — without state.

```rust
#[rpc_group]
#[derive(Clone)]
pub struct HealthCheck;

#[rpc_group_impl]
impl HealthCheck {
    #[rpc]
    async fn health(&self) -> Result<String, ClusterError> {
        Ok("ok".to_string())
    }
}

#[rpc_group]
#[derive(Clone)]
pub struct Metrics {
    collector: MetricsCollector,
}

#[rpc_group_impl]
impl Metrics {
    #[rpc]
    async fn get_metrics(&self) -> Result<MetricsSnapshot, ClusterError> {
        Ok(self.collector.snapshot())
    }
}
```

Composed into entities:

```rust
#[entity_impl(rpc_groups(HealthCheck, Metrics))]
impl UserGateway {
    // ... own RPCs
    // health() and get_metrics() are also callable on this entity's client
}
```

### Entity Registration (Type-Safe)

The macro generates a `register` function with explicit typed parameters for every declared RPC group. Forget a group — compile error. Wrong type — compile error.

```rust
// #[entity_impl] — no groups
// Generated: fn register(cluster: &Cluster, entity: Self) -> Result<Client, ClusterError>
let client = UserGateway::register(
    &cluster,
    UserGateway { db: pool.clone(), http: http_client.clone() },
).await?;

// #[entity_impl(rpc_groups(HealthCheck, Metrics))]
// Generated: fn register(cluster: &Cluster, entity: Self, g1: HealthCheck, g2: Metrics) -> Result<Client, ClusterError>
let client = UserGateway::register(
    &cluster,
    UserGateway { db: pool.clone(), http: http_client.clone() },
    HealthCheck,
    Metrics { collector: collector.clone() },
).await?;
```

`register` returns a typed client for immediate use.

### Entity Client

```rust
// From register
let client = UserGateway::register(&cluster, entity).await?;

// Or construct separately
let client = UserGateway::client(&cluster);

// Non-persisted RPC
let profile = client.get_profile(&entity_id, "user-123".into()).await?;

// Persisted RPC (at-least-once)
client.update_profile(&entity_id, UpdateProfileRequest { ... }).await?;
```

### What Gets Removed from Entities

| Removed | Replacement |
|---|---|
| `#[state(...)]` | Use your database directly |
| `StateRef<S>`, `StateMutGuard<S>`, `ArcSwap` | Gone — no framework-managed state |
| `#[workflow]` on entities | Use standalone `#[workflow]` |
| `#[activity]` on entities | Use workflow `#[activity]` |
| `init()` | Gone — no state to initialize |
| `&mut self` | Compile error — everything is `&self` |
| `#[entity_trait]` / `#[entity_trait_impl]` | Replaced by `#[rpc_group]` / `#[rpc_group_impl]` |
| `WorkflowStorage` in entities | Only workflows use it |
| `cluster_entity_state` table | No longer needed |
| `DurableContext` in entities | Only workflows have durable primitives |
| `#[protected]` / `#[public]` / `#[private]` on activities | No activities on entities |

### Validation Rules (Compile Errors)

- `#[state(...)]` on `#[entity_impl]` → error: "entities are stateless; use a database for state management"
- `#[workflow]` on entity method → error: "use standalone #[workflow] for durable orchestration"
- `#[activity]` on entity method → error: "activities belong on workflows, not entities"
- `&mut self` on any entity method → error: "entity methods must use &self"
- Method without `#[rpc]` (unless plain helper) → warning or error depending on visibility

---

## Part 2: Standalone Workflows

Workflows are a standalone top-level construct, **stateless by design**, separate from entities. Under the hood, each workflow is backed by an entity (entity type `Workflow/{name}`, entity ID = idempotency key), but users never see this.

### Core Principles

- **No state** — workflows and activity groups have no `#[state(...)]`. There is no `StateMutGuard`, no `StateRef`, no `ArcSwap`.
- **Single entry point** — each workflow has exactly one `execute` method.
- **`&self` everywhere** — both `execute` and activities take `&self`. Activities are side-effecting but stateless.
- **Durable primitives on `self`** — sleep, deferred, execution metadata are all accessible via `self` (no separate `WorkflowContext` parameter).
- **Cross-workflow calls go through clients** — workflows call other workflows via generated clients, never inline. This prevents nesting.
- **Activities run in DB transactions** — arbitrary SQL is safe inside activities; the journal entry commits in the same transaction.

### Workflow Definition

```rust
use cruster::prelude::*;

#[workflow]
#[derive(Clone)]
pub struct ProcessOrder {
    http: HttpClient,
}

#[workflow_impl(key = |req: &OrderRequest| req.order_id.clone(), hash = false)]
impl ProcessOrder {
    /// The single entry point.
    async fn execute(&self, request: OrderRequest) -> Result<OrderResult, ClusterError> {
        // Pure orchestration — no state access
        if request.items.is_empty() {
            return Err(ClusterError::invalid_input("empty order"));
        }

        // Call activities (durable, journaled, transactional)
        let reserved = self.reserve_inventory(request.items.clone()).await?;
        let charge = self.charge_payment(request.payment_info, reserved.total).await?;

        // Durable sleep — workflow suspends, underlying entity idles, resumes on wake
        self.sleep(Duration::from_secs(60 * 60)).await?;

        self.confirm_shipment(reserved.reservation_id).await?;

        // Call another workflow via its client
        let notifier = NotifyCustomer::client(&self);
        notifier.execute(NotifyRequest { email: request.email }).await?;

        Ok(OrderResult { order_id: charge.transaction_id })
    }

    #[activity(retries = 3, backoff = "exponential")]
    async fn reserve_inventory(&self, items: Vec<Item>) -> Result<Reservation, ClusterError> {
        // Access struct fields via self
        let response = self.http.post("/inventory/reserve").json(&items).send().await?;
        // ...
        todo!()
    }

    #[activity(retries = 2)]
    async fn charge_payment(&self, info: PaymentInfo, amount: u64) -> Result<Charge, ClusterError> {
        todo!()
    }

    #[activity]
    async fn confirm_shipment(&self, reservation_id: String) -> Result<(), ClusterError> {
        // SQL within the activity transaction
        let tx = self.transaction();
        sqlx::query("UPDATE shipments SET status = 'confirmed' WHERE reservation_id = $1")
            .bind(&reservation_id)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}
```

### Workflow Key Configuration

The `key` and `hash` attributes are specified on `#[workflow_impl]` (where the client is generated), not on the struct-level `#[workflow]`:

```rust
#[workflow_impl]                                                          // default: key = hash(serialize(request))
#[workflow_impl(key = |req: &OrderRequest| req.order_id.clone())]         // custom key, hashed (SHA-256)
#[workflow_impl(key = |req: &OrderRequest| req.order_id.clone(), hash = false)]  // custom key, used as-is
```

> **Note:** The `key`/`hash` are on `#[workflow_impl]` because proc macros cannot share data between separate attribute macro invocations (`#[workflow]` on the struct and `#[workflow_impl]` on the impl block).

| Attribute | Type | Default | Description |
|---|---|---|---|
| `key` | closure | `\|req\| req` (whole request) | Extracts value used to derive idempotency key. Takes `&RequestType` as input. |
| `hash` | bool | `true` | Whether to SHA-256 hash the key. When true, serializes the extracted value and hashes it. When false, uses the extracted value directly (must implement `ToString`). |

### Activity Attributes

```rust
#[activity]                                            // no retries
#[activity(retries = 3)]                               // 3 retries, default backoff
#[activity(retries = 3, backoff = "exponential")]      // 3 retries, exponential backoff
#[activity(retries = 5, backoff = "constant")]         // 5 retries, constant backoff
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `retries` | u32 | 0 | Max retry attempts on failure |
| `backoff` | string | `"exponential"` | Backoff strategy: `"exponential"`, `"constant"` |

### Activity Groups (Shared Activities)

Activity groups bundle reusable activities that can be composed into multiple workflows.

```rust
#[activity_group]
#[derive(Clone)]
pub struct Payments {
    stripe: StripeClient,
}

#[activity_group_impl]
impl Payments {
    #[activity(retries = 3, backoff = "exponential")]
    async fn charge(&self, info: PaymentInfo, amount: u64) -> Result<Charge, ClusterError> {
        self.stripe.charge(info, amount).await
    }

    #[activity]
    async fn refund(&self, transaction_id: String) -> Result<(), ClusterError> {
        self.stripe.refund(transaction_id).await
    }
}
```

Composed into workflows via `activity_groups(...)`:

```rust
#[workflow_impl(activity_groups(Payments, Inventory))]
impl ProcessOrder {
    async fn execute(&self, request: OrderRequest) -> Result<OrderResult, ClusterError> {
        // Group activities callable on self
        let charge = self.charge(request.payment_info, request.total).await?;
        let reserved = self.reserve(request.items).await?;

        // Local activities also work
        self.send_confirmation_email(request.email.clone()).await?;

        Ok(OrderResult { .. })
    }

    #[activity]
    async fn send_confirmation_email(&self, email: String) -> Result<(), ClusterError> {
        todo!()
    }
}
```

Activity group fields (`self.stripe`) are accessible via `self` inside group activity methods — the macro composes them the same way entity traits do today, minus all state machinery.

### Methods Available on `self`

The macro generates a view struct that provides durable primitives and struct field access via `Deref`.

#### In `execute` (workflow body)

| Method | Signature | Description |
|---|---|---|
| `sleep` | `async fn sleep(&self, duration: Duration) -> Result<(), ClusterError>` | Durable sleep. Workflow suspends, underlying entity goes idle, resumes on wake. |
| `await_deferred` | `async fn await_deferred<T: DeserializeOwned>(&self, key: impl DeferredKeyLike<T>) -> Result<T, ClusterError>` | Await a typed deferred signal. |
| `resolve_deferred` | `async fn resolve_deferred<T: Serialize>(&self, key: impl DeferredKeyLike<T>, value: T) -> Result<(), ClusterError>` | Resolve a deferred signal. |
| `on_interrupt` | `async fn on_interrupt(&self) -> Result<(), ClusterError>` | Await workflow interruption signal. |
| `execution_id` | `fn execution_id(&self) -> &str` | The current execution's idempotency key (= entity ID). |
| Struct fields | via `Deref` | `self.http`, `self.stripe`, etc. |
| Activities | generated delegation | `self.activity_name(args).await` |
| Group activities | generated delegation | `self.charge(args).await` (from composed activity groups) |
| Workflow clients | `T::client(&self)` | Get a client for another workflow. |
| Entity clients | `T::client(&self)` | Get a client for an entity. |

#### In `#[activity]` methods

| Method | Signature | Description |
|---|---|---|
| `transaction` | `fn transaction(&self) -> SqlTransactionHandle` | Access the current activity's DB transaction for arbitrary SQL. Only available with `sql` feature. |
| Struct fields | via `Deref` | `self.http`, etc. |

Activities do **not** have access to `sleep`, `await_deferred`, or other durable primitives. Those are orchestration concerns that belong in `execute`.

### Workflow Registration (Type-Safe)

Same pattern as entities — the macro generates a `register` function with explicit typed parameters for every declared activity group.

```rust
// #[workflow_impl] — no groups
// Generated: fn register(cluster: &Cluster, workflow: Self) -> Result<Client, ClusterError>
let client = ProcessOrder::register(
    &cluster,
    ProcessOrder { http: http_client.clone() },
).await?;

// #[workflow_impl(activity_groups(Payments, Inventory))]
// Generated: fn register(cluster: &Cluster, workflow: Self, g1: Payments, g2: Inventory) -> Result<Client, ClusterError>
let client = ProcessOrder::register(
    &cluster,
    ProcessOrder { http: http_client.clone() },
    Payments { stripe: stripe_client.clone() },
    Inventory { db: db_pool.clone() },
).await?;
```

`register` returns a typed client. Forget a group — compile error. Wrong type — compile error.

### Workflow Client API

#### Execution

```rust
// From register
let client = ProcessOrder::register(&cluster, workflow, payments, inventory).await?;

// Or construct separately
let client = ProcessOrder::client(&cluster);

// Blocking — waits for workflow completion
let result: OrderResult = client.execute(order_request).await?;

// Fire-and-forget — returns execution ID immediately
let execution_id: String = client.start(order_request).await?;

// Poll for result
let maybe: Option<Result<OrderResult, ClusterError>> = client.poll(execution_id).await?;
```

#### Idempotency Key Override

```rust
// Override key, hashed (SHA-256)
let result = client.with_key("order-42").execute(order_request).await?;
let exec_id = client.with_key("order-42").start(order_request).await?;

// Override key, raw (no hashing — caller provides exact entity ID)
let result = client.with_key_raw("order-42").execute(order_request).await?;
let exec_id = client.with_key_raw("order-42").start(order_request).await?;
```

`with_key` / `with_key_raw` return a lightweight client view with the key baked in. `execute` and `start` are defined once, not multiplied.

### Idempotency Key Derivation

| Source | Hashed | Entity ID |
|---|---|---|
| Default (no `key` attr) | always | `SHA-256(serialize(request))` |
| `key = \|req\| req.order_id`, default hash | yes | `SHA-256(serialize(req.order_id))` |
| `key = \|req\| req.order_id, hash = false` | no | `req.order_id` as-is (must be `String`/`ToString`) |
| `client.with_key("order-42")` | yes | `SHA-256("order-42")` |
| `client.with_key_raw("order-42")` | no | `"order-42"` as-is |

Hashing uses SHA-256 on the msgpack-serialized value, producing a hex-encoded string.

### Suspend Semantics

Suspension is implicit, not a user-facing result type. When a workflow hits `self.sleep()` or `self.await_deferred()`:

1. The future yields.
2. The underlying entity persists its journal position.
3. The entity goes idle and can be evicted from memory.
4. On timer expiry / deferred resolution, the entity respawns, replays the journal, and resumes from the suspension point.

From the user's perspective, it is just an `.await` that might take hours. There is no `Suspended` enum to handle.

### Underlying Entity Mapping

Each workflow type maps to an entity type. The entity is fully managed by the framework.

| Concept | Entity equivalent |
|---|---|
| Workflow struct | Entity struct (no state) |
| `execute` | Single dispatched method named `execute` |
| Activities | `#[activity]` methods (journaled, transactional via `ActivityScope`) |
| Activity groups | Composable trait-like groups (same composition pattern, no state) |
| Entity type | `Workflow/{StructName}` (e.g. `Workflow/ProcessOrder`) |
| Entity ID | Idempotency key (hashed or raw, per rules above) |

### Workflow Validation Rules (Compile Errors)

- `#[state(...)]` present → error: "workflows are stateless"
- `&mut self` on any method → error: "workflow methods must use &self"
- `#[rpc]` on a method → error: "workflows use #[activity], not #[rpc]"
- `#[workflow]` on a method → error: "workflows have a single execute entry point; use client calls for cross-workflow interaction"
- Missing `execute` method → error: "workflow must define an execute method"
- Multiple `execute` methods → error: "workflow must have exactly one execute method"
- `execute` without exactly one request parameter → error: "execute must take exactly one request parameter"

---

## Part 3: Summary of Construct Types

| | Entity | Workflow | RPC Group | Activity Group |
|---|---|---|---|---|
| **Purpose** | Addressable RPC handler | Durable orchestration | Shared RPCs | Shared activities |
| **State** | None | None | None | None |
| **Methods** | `#[rpc]` / `#[rpc(persisted)]` | `execute` + `#[activity]` | `#[rpc]` / `#[rpc(persisted)]` | `#[activity]` |
| **`&self` only** | Yes | Yes | Yes | Yes |
| **Composable into** | — | — | Entities | Workflows |
| **Backed by** | Entity directly | Hidden entity (`Workflow/{Name}`) | Composed into entity | Composed into workflow |
| **Registration** | `T::register(cluster, instance, ...groups)` | `T::register(cluster, instance, ...groups)` | Passed to entity registration | Passed to workflow registration |
| **Client** | `T::client(&cluster)` or from `register` | `T::client(&cluster)` or from `register` | Methods on parent entity's client | Called via `self` in workflow body |
| **DB transactions** | No (use DB directly in RPCs) | Yes, inside `#[activity]` via `self.transaction()` | No | Yes, via `self.transaction()` |
| **Durable primitives** | No | Yes, in `execute` (`sleep`, `await_deferred`, etc.) | No | No |

---

## Part 4: What Gets Removed Globally

### Types and Traits Removed

| Type/Trait | Status |
|---|---|
| `StateRef<S>` | Removed |
| `StateMutGuard<S>` | Removed |
| `TraitStateMutGuard<S>` | Removed |
| `#[state(...)]` attribute | Compile error on both entities and workflows |
| `#[entity_trait]` / `#[entity_trait_impl]` | Replaced by `#[rpc_group]` / `#[rpc_group_impl]` |
| `CompositeState` | Removed (no state composition) |
| `init()` method | Removed (no state to initialize) |
| `&mut self` on any method | Compile error everywhere |
| `ArcSwap` usage in entities | Removed |
| `__write_lock` on Handler | Removed |

### Database Tables

| Table | Status |
|---|---|
| `cluster_entity_state` | No longer needed — can be dropped |
| `cluster_messages` | Kept — used by `#[rpc(persisted)]` and workflow dispatch |
| `cluster_replies` | Kept — used by persisted message replies |
| `cluster_workflow_journal` | Kept — used by workflow activity journaling |
| `cluster_workflow_timers` | Kept — used by `self.sleep()` |
| `cluster_workflow_deferred` | Kept — used by `self.await_deferred()` / `self.resolve_deferred()` |

### Storage Traits

| Trait | Status |
|---|---|
| `WorkflowStorage` | Kept — used by workflows for journal entries (but no longer for entity state) |
| `MessageStorage` | Kept — used by `#[rpc(persisted)]` and workflow journaling |
| `WorkflowEngine` | Kept — used by workflows for durable primitives |
| `RunnerStorage` | Kept — unchanged |

---

## Implementation Plan

### Current Status

- **Phase 3 & 4 (Workflow macros + Client):** ✅ Done — `#[workflow]` / `#[workflow_impl]` macros exist and delegate to shared codegen. Tests for the new names added. Custom key extraction via `#[workflow_impl(key = |req| ..., hash = false)]` implemented and tested — `derive_entity_id` uses the key closure when provided, with configurable hashing (SHA-256 by default, raw when `hash = false`).
- **Phase 5 (Activity groups):** ✅ Done — `#[activity_group]` / `#[activity_group_impl]` macros implemented with composition into workflows via `activity_groups(...)`.
- **Phase 6 (Activity retry support):** ✅ Done — `#[activity(retries = N, backoff = "...")]` implemented for both standalone workflow activities and activity group activities. Retry loop uses attempt-indexed journal keys for independent journaling per retry. Durable sleep between retries via `WorkflowEngine::sleep()`. Backoff strategies: exponential (default, capped at 60s) and constant. `compute_retry_backoff()` utility added. Tests cover: retries with success, constant backoff, retry exhaustion, activity groups with retries, and backoff computation.
- **Phase 7 (Poll and execution lifecycle):** ✅ Done — `poll` method added to generated workflow client and `ClientWithKey`. `Sharding` trait extended with `replies_for(request_id)` (default returns empty, `ShardingImpl` delegates to `MessageStorage`). `EntityClient::poll_reply` computes deterministic request_id and queries storage. Workflow `execute`/`start`/`ClientWithKey` methods now use entity_id-based key_bytes for consistent request_id derivation, enabling poll without original request payload. Tests: poll returns None for unknown, poll returns result after execute, poll with key, compile-time method existence.
- **Phase 9 (partial):** ✅ Done — all `standalone_workflow` / `standalone_workflow_impl` tests in `macro_tests.rs` have been ported to new `#[workflow]` / `#[workflow_impl]` API names and old tests removed. Legacy `#[standalone_workflow]` / `#[standalone_workflow_impl]` proc_macro entry points and re-exports deleted. Internal functions renamed from `standalone_workflow_*` to `workflow_*`. All doc comments updated.
- **Phase 2 (RPC Groups):** ✅ Done — `#[rpc_group]` / `#[rpc_group_impl]` macros implemented. Generates wrapper struct, dispatch, client extension trait, access/methods traits. Composable into entities via `#[entity_impl(rpc_groups(...))]` with type-safe register accepting group instances as parameters. Tests: dispatch, multiple groups, groups with fields, persisted RPCs, multi-param RPCs, client extension, entity type name.
- **Phase 1 (partial):** ✅ Done — Pure-RPC entity codegen path added. When an entity has no `#[state]`, no `#[workflow]`, no `#[activity]` methods (only `#[rpc]` methods), `generate_pure_rpc_entity()` generates a simplified Handler without `__state`, `__write_lock`, `__state_storage`, `__state_key`, `__workflow_engine`, view structs, or ArcSwap. Methods are called directly on `__entity`. Tests: entity type, dispatch, multi-param, unknown tag, register, persisted RPC. Old stateful codegen path untouched — all existing tests pass.
- **Phase 9 (test migration, partial):** ✅ Done — `PersistedMethodEntity`, `MixedEntity`, and `PersistedIdempotentEntity` tests migrated from `#[workflow]` to `#[rpc(persisted)]`. These entities previously used `#[workflow]` purely for persisted delivery (no activities, no state); they now correctly use the new API pattern.
- **Phase 9 (test migration, key extraction):** ✅ Done — `PersistedKeyEntity` and `MultiParamPersisted` migrated from entity `#[workflow(key(...))]` to standalone `#[workflow]`/`#[workflow_impl(key = ...)]`. `PersistedKeyEntity` → `UpdateWorkflow` with `key = |req: &UpdateRequest| req.id.clone()`. `MultiParamPersisted` → `SendEmailWorkflow` with `SendEmailRequest` struct and `key = |req: &SendEmailRequest| req.order_id.clone()`. Tests verify same-key-field produces same entity_id and subset-field key extraction.
- **Phase 8 (partial):** StatelessCounter integration test entity added to `examples/cluster-tests/`. Pure-RPC entity with `#[rpc]` and `#[rpc(persisted)]` methods, manages state directly in PostgreSQL (`stateless_counter_values` table). HTTP routes, shell test script (`test_stateless_counter.sh`), and SQL migration added.
- **Phase 9 (old-pattern macro_tests cleanup):** ✅ Done — Deleted all old-pattern tests from `macro_tests.rs`: stateful Counter/PersistedCounter entities and their tests, all entity_trait tests (LoggerTrait, TraitCounter, AlphaTrait, BetaTrait, CompositeEntity), PrivateStatefulEntity, DurableProcessor (entity-level #[workflow]), StatefulDurableEntity, all state/sleep/deferred/concurrency/journaling tests that used #[state]/#[activity]/#[workflow] on entities. Removed unused helper functions (test_ctx_with_storage, test_ctx_with_storage_and_engine, test_ctx_with_engine, test_ctx_with_all_storage, TestWorkflowEngine) and unused imports.
- **Phase 9 (integration test migration, Counter):** ✅ Done — Counter entity migrated from old stateful API (`#[state(CounterState)]`, `#[workflow]`, `#[activity]`, `&mut self`) to new pure-RPC API (`#[rpc]`, `#[rpc(persisted)]`, direct PG via `counter_values` table). Request structs now include `entity_id`. HTTP API handlers updated, registration in main.rs updated. SQL migration 0018 added.
- **Phase 9 (integration test migration, KVStore):** ✅ Done — KVStore entity migrated from old stateful API (`#[state(KVStoreState)]`, `#[workflow]`, `#[activity]`, `&mut self`) to new pure-RPC API (`#[rpc]`, `#[rpc(persisted)]`, direct PG via `kv_store_entries` table). Request structs now include `entity_id`. HTTP API handlers updated, registration in main.rs updated. SQL migration 0019 added.
- **Phase 9 (integration test migration, WorkflowTest):** ✅ Done — WorkflowTest migrated from old stateful entity to new pure-RPC entity (`WorkflowTest` with `pool: PgPool` for reads) + 3 standalone workflows (`SimpleWorkflow`, `FailingWorkflow`, `LongWorkflow`) using `#[workflow]`/`#[workflow_impl]` with activities that write to PG `workflow_test_executions` table. HTTP routes updated: workflow execution uses standalone workflow clients, read queries use entity client. SQL migration 0020 added.
- **Phase 9 (integration test migration, ActivityTest):** ✅ Done — ActivityTest entity migrated from old stateful API (`#[state(ActivityTestState)]`, `#[workflow]`, `#[activity]`, `&mut self`) to new pure-RPC entity (`ActivityTest` with `pool: PgPool` for reads) + standalone workflow (`ActivityWorkflow`) using `#[workflow]`/`#[workflow_impl]` with activities that write to PG `activity_test_logs` table. HTTP routes updated: workflow execution uses standalone workflow client, read queries use entity client. SQL migration 0021 added.
- **Phase 9 (integration test migration, CrossEntity):** ✅ Done — CrossEntity entity migrated from old stateful API (`#[state(CrossEntityState)]`, `#[workflow]`, `#[activity]`, `&mut self`) to new pure-RPC API (`#[rpc]`, `#[rpc(persisted)]`, direct PG via `cross_entity_messages` and `cross_entity_ping_counts` tables). Request structs now include `entity_id`. HTTP API handlers updated, registration in main.rs updated. SQL migration 0022 added.
- **Phase 9 (integration test migration, TimerTest):** ✅ Done — TimerTest entity migrated from old stateful API (`#[state(TimerTestState)]`, `#[workflow]`, `#[activity]`, `&mut self`) to new pure-RPC entity (`TimerTest` with `pool: PgPool` for reads/mutations: `get_timer_fires`, `get_pending_timers` as `#[rpc]`, `cancel_timer`, `clear_fires` as `#[rpc(persisted)]`) + standalone workflow (`ScheduleTimerWorkflow`) using `#[workflow]`/`#[workflow_impl]` with activities that write to PG `timer_test_pending` and `timer_test_fires` tables. Durable `self.sleep()` for timer delay. HTTP routes updated: schedule uses standalone workflow client, reads/cancel/clear use entity client. SQL migration 0023 added.
- **Remaining work:**
   - Phase 1 (remaining): Emit compile errors for #[state], #[workflow], #[activity], &mut self on entities; remove old codegen
   - Phase 8 (remaining): Add standalone workflow integration test, RPC group integration test, activity group integration test
   - Phase 9 (remaining): Remove old entity_trait macros, state infrastructure; migrate remaining integration test entities (TraitTest, SqlActivityTest) in examples/cluster-tests to new API; migrate chess-cluster entities; delete UI compile-fail tests for removed features once old codegen is removed

### Phase 1: Simplify Entities

**Goal:** Strip entities down to stateless RPC handlers.

1. **Remove state support from `#[entity_impl]` macro** (`cruster-macros/src/lib.rs`)
   - Remove `#[state(...)]` parsing from entity context (keep it only for error messaging)
   - Emit compile error if `#[state(...)]` is present: "entities are stateless; use a database for state management"
   - Remove `init()` method detection and code generation
   - Remove `__state`, `__write_lock`, `__state_storage`, `__state_key` from generated Handler struct
   - Remove `StateRef`, `StateMutGuard`, `ArcSwap` usage in entity codegen

2. **Remove `#[workflow]` and `#[activity]` from entities**
   - Emit compile error if `#[workflow]` annotation found on entity method
   - Emit compile error if `#[activity]` annotation found on entity method
   - Emit compile error if `&mut self` found on any entity method
   - Remove `WorkflowScope`, `DurableContext`, `ActivityScope` wiring from entity codegen

3. **Add `#[rpc(persisted)]` variant** ✅
   - Parse `persisted` flag on `#[rpc]` attribute
   - Persisted RPCs use `send_persisted()` on the client (existing infrastructure)
   - Non-persisted RPCs use `send()` (existing infrastructure)
   - This replaces the old `#[workflow]` as the mechanism for at-least-once delivery on entities

4. **Simplify generated Handler**
   - Handler fields: `__entity` (user struct), `ctx: EntityContext`, `__sharding`, `__entity_address`, `__message_storage` (for persisted RPCs)
   - No workflow engine, no workflow storage, no state storage
   - Single view struct: `__View` — `Deref<Target = UserStruct>`, used by all RPC methods

5. **Simplify dispatch**
   - All match arms are RPC dispatches (no workflow scope wrapping, no activity scope)
   - Persisted vs non-persisted is a client-side concern (the dispatch is the same)

6. **Update `register` to be type-safe**
   - Generate `fn register(cluster, entity, ...rpc_groups) -> Result<Client, ClusterError>` on the entity struct
   - Each RPC group from `rpc_groups(...)` becomes an explicit typed parameter
   - Returns typed client

### Phase 2: Replace Entity Traits with RPC Groups

**Goal:** `#[rpc_group]` / `#[rpc_group_impl]` replace `#[entity_trait]` / `#[entity_trait_impl]`.

1. **Add `#[rpc_group]` struct-level macro**
   - Minimal marker — no state, no entity type generation
   - Validate: no `#[state(...)]`

2. **Add `#[rpc_group_impl]` impl-level macro**
   - Parse methods: only `#[rpc]` / `#[rpc(persisted)]` and unannotated helpers allowed
   - Validate: no `&mut self`, no `#[workflow]`, no `#[activity]`, no `#[state(...)]`
   - Generate:
     - `__RpcGroupView` — `Deref` to group struct
     - Wrapper methods for each RPC
     - `__{Group}Access` trait — provides `__group_ref() -> &GroupStruct`
     - `__{Group}Methods` trait — async delegation methods for each RPC
     - Client extension trait — adds group RPC methods to the parent entity's client

3. **Wire into `#[entity_impl(rpc_groups(...))]`**
   - For each group, generate field on Handler: `__{group}_instance: GroupStruct`
   - Implement `__{Group}Access` and `__{Group}Methods` for the entity's view
   - Add group RPCs to dispatch match arms
   - Add group RPCs to generated client

4. **Remove `#[entity_trait]` / `#[entity_trait_impl]`**
   - Emit deprecation/compile error pointing to `#[rpc_group]`

### Phase 3: Workflow Core Macro Infrastructure ✅

**Goal:** `#[workflow]` and `#[workflow_impl]` generate a working entity with a single `execute` dispatch.

1. **Add `#[workflow]` struct-level macro** ✅
   - Parse `WorkflowArgs`: `key` (optional closure), `hash` (bool, default true)
   - Generate hidden methods: `__workflow_name() -> &str`, `__entity_type() -> EntityType` (returns `Workflow/{Name}`)
   - Implemented as dual-purpose: on a struct → standalone workflow codegen, on a method → no-op marker (backward compatible with entity `#[workflow]`)

2. **Add `#[workflow_impl]` impl-level macro** ✅
   - Parse `WorkflowImplArgs`: `activity_groups` (list of paths)
   - Validate: exactly one `execute` method, `&self` only, no `#[state]`, no `#[rpc]`, no `#[workflow]`
   - Classify methods: `execute` (entry point), `#[activity]` (journaled side-effects), unannotated (helpers)
   - Delegates to `standalone_workflow_impl_inner()` (shared codegen with `#[standalone_workflow_impl]`)

3. **Generate Handler struct** ✅
   - Fields: `__workflow` (user struct), `ctx: EntityContext`, `__workflow_engine`, `__message_storage`, `__workflow_storage`, `__sharding`, `__entity_address`
   - No state fields

4. **Generate view structs** ✅
   - `__ExecuteView` — used by `execute`. `Deref<Target = UserStruct>`. Provides durable primitives (`sleep`, `await_deferred`, `resolve_deferred`, `on_interrupt`, `execution_id`). Provides activity delegation methods routing through `DurableContext::run()`.
   - `__ActivityView` — used by `#[activity]` methods. `Deref<Target = UserStruct>`. Provides `transaction()`. No durable primitives.

5. **Generate dispatch** ✅
   - Single match arm for tag `"execute"`: deserialize request, wrap in `WorkflowScope::run(request_id)`, construct `__ExecuteView`, call `execute`, serialize response.

6. **Generate `Entity` trait impl** ✅
   - `entity_type()` returns `Workflow/{Name}`
   - `spawn()` constructs Handler (no state loading)

7. **Generate type-safe `register`** ✅
   - `fn register(cluster, workflow, ...activity_groups) -> Result<Client, ClusterError>`
   - Each activity group from `activity_groups(...)` becomes an explicit typed parameter

8. **Add SHA-256 hashing utility** ✅
   - `fn hash_to_id(value: &[u8]) -> String` — SHA-256 hex of bytes
   - Used by clients for idempotency key derivation

### Phase 4: Workflow Client Generation ✅

**Goal:** Generate `ProcessOrderClient` with `execute`, `start`, `poll`, `with_key`, `with_key_raw`.

1. **Generate client struct** `{Name}Client` ✅
   - `execute(&self, request: T) -> Result<R, ClusterError>` — derive entity ID from request per `key`/`hash` config, send persisted message, await reply
   - `start(&self, request: T) -> Result<String, ClusterError>` — same key derivation, fire-and-forget, return entity ID
   - `poll(&self, execution_id: &str) -> Result<Option<Result<R, ClusterError>>, ClusterError>` — not yet implemented

2. **Generate key-override view** `{Name}ClientWithKey` ✅
   - `with_key(key: impl ToString) -> {Name}ClientWithKey` — stores key, hash=true
   - `with_key_raw(key: impl ToString) -> {Name}ClientWithKey` — stores key, hash=false
   - Same `execute`, `start` methods using the stored key

3. **Generate `T::client(&self)` on workflow view structs** ✅ — workflows can get clients for other workflows/entities via `WorkflowClientFactory`

### Phase 5: Activity Groups ✅

**Goal:** `#[activity_group]` / `#[activity_group_impl]` for composable activity bundles.

1. **Add `#[activity_group]` struct-level macro** ✅
   - Marker, no state

2. **Add `#[activity_group_impl]` impl-level macro** ✅
   - Only `#[activity]` and helpers allowed
   - Validate: no `&mut self`, no `#[rpc]`, no `#[workflow]`, no `#[state]`
   - Generate: view struct, wrapper methods, access/methods traits

3. **Wire into `#[workflow_impl(activity_groups(...))]`** ✅
   - Generate group instance fields on Handler
   - Implement access/methods traits on `__ExecuteView`
   - Route through `DurableContext::run()` for journaling
   - Tests added: single group, multiple groups, mixed local + group activities

### Phase 6: Activity Retry Support ✅

**Goal:** `#[activity(retries = N, backoff = "...")]` retries failed activities.

1. **Parse retry attributes** on `#[activity]` ✅
   - New `ActivityAttrArgs` parser supports `key`, `retries`, and `backoff` in any combination
   - `retries` is a `u32`, `backoff` is `"exponential"` (default) or `"constant"`
   - Validation: `backoff` requires `retries`, `retries` only valid on activities

2. **Generate retry wrapper in activity delegation** ✅
   - Implemented for both standalone workflow activities and activity group activities
   - Uses `loop` with attempt counter; parameters are cloned per iteration for owned types
   - Journal key includes attempt number (appended as LE bytes) for independent journaling per retry
   - Durable sleep between retries via `WorkflowEngine::sleep()` with name `"{activity}/retry/{attempt}"`
   - On exhaustion, returns the last error

3. **Backoff computation** ✅
   - `compute_retry_backoff(attempt, strategy, base_secs)` utility in `durable.rs`
   - Exponential: `base * 2^attempt` (capped at 60s)
   - Constant: `base` (default 1s)

4. **Tests** ✅
   - `retry_workflow_activity_succeeds_after_retries` — flaky activity succeeds on 3rd attempt
   - `constant_backoff_workflow_succeeds` — constant backoff strategy
   - `retry_workflow_exhaustion_returns_last_error` — all retries fail
   - `no_retry_workflow_succeeds` — `retries = 0` behaves like no retries
   - `activity_group_retry_succeeds` — activity group with retries
   - `test_compute_retry_backoff_exponential` / `test_compute_retry_backoff_constant` — unit tests for backoff computation

### Phase 7: Poll and Execution Lifecycle ✅

**Goal:** Support fire-and-forget execution with polling.

1. **Workflow result persistence** ✅ — handled by existing entity message reply mechanism
2. **`client.start()`** ✅ — send persisted message, don't await reply, return entity ID (already existed from Phase 4)
3. **`client.poll()`** ✅ — check `MessageStorage` for reply, return `Some(result)` or `None`
   - Added `Sharding::replies_for(request_id)` trait method (default: empty vec, `ShardingImpl` delegates to `MessageStorage`)
   - Added `EntityClient::poll_reply()` — computes deterministic request_id and queries storage
   - Generated `poll(execution_id)` on workflow client, `poll()` on `ClientWithKey`
   - Changed `execute`/`start` to use entity_id-based key_bytes for consistent request_id derivation (enables poll without original request payload)

### Phase 8: Integration Testing

**Goal:** Add test entities and workflows to `examples/cluster-tests/`.

#### Entity Tests
1. **`StatelessCounter`** — entity with `#[rpc]` and `#[rpc(persisted)]`, queries/writes PG directly
2. **`RpcGroupTest`** — entity using RPC groups, verifies composition
3. **Compile-fail tests** — verify that `#[state]`, `#[workflow]`, `#[activity]`, `&mut self` on entities produce compile errors

#### Workflow Tests
1. **`SimpleWorkflow`** — 3 sequential activities, verifies journaling and replay
2. **`RetryWorkflow`** — failing activity that succeeds on retry
3. **`GroupWorkflow`** — workflow using activity groups
4. **`LongRunningWorkflow`** — workflow with `self.sleep()`, verifies suspend/resume
5. **`CrossWorkflow`** — workflow calling another workflow via client
6. **HTTP routes and bash test scripts** following existing patterns

### Phase 9: Cleanup and Migration

1. **Remove old state infrastructure**
   - Delete `StateMutGuard`, `TraitStateMutGuard`, `StateRef` (or keep behind a `deprecated` feature flag during transition)
   - Remove `cluster_entity_state` table from SQL migrations
   - Remove `CompositeState` generation

2. **Remove old entity trait macros**
   - Delete `#[entity_trait]` / `#[entity_trait_impl]` codegen

3. **Update `specs/architecture.md`**
   - Reflect the new entity/workflow split
   - Update method types table
   - Document RPC groups and activity groups

4. **Migration guide**
   - How to extract workflows from existing entities into standalone workflows
   - How to replace entity state with direct database access
   - How to convert entity traits to RPC groups / activity groups
