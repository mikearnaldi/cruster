//! Proc macros for cruster entity definitions.
//!
//! Provides `#[entity]` and `#[entity_impl]` attribute macros that generate
//! `Entity`, `EntityHandler`, and typed client implementations from a single
//! struct + impl definition.
//!
//! # Stateless Entity Example
//!
//! ```text
//! use cruster_macros::{entity, entity_impl};
//!
//! #[entity]
//! struct UserLookup {
//!     db: Arc<Pool<Postgres>>,
//! }
//!
//! #[entity_impl]
//! impl UserLookup {
//!     #[rpc]
//!     async fn get_user(&self, id: String) -> Result<User, ClusterError> {
//!         // ...
//!     }
//! }
//! ```
//!
//! # Stateful Entity Example
//!
//! ```text
//! use cruster_macros::{entity, entity_impl};
//!
//! struct CounterState { count: i32 }
//!
//! #[entity]
//! struct Counter {}
//!
//! #[entity_impl]
//! #[state(CounterState)]
//! impl Counter {
//!     fn init(&self, _ctx: &EntityContext) -> Result<CounterState, ClusterError> {
//!         Ok(CounterState { count: 0 })
//!     }
//!
//!     // Only #[activity] methods can have &mut self
//!     #[activity]
//!     async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
//!         self.state.count += amount;
//!         Ok(self.state.count)
//!     }
//!
//!     // &self for reads
//!     #[rpc]
//!     async fn get_count(&self) -> Result<i32, ClusterError> {
//!         Ok(self.state.count)
//!     }
//! }
//! ```

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::{parse_macro_input, spanned::Spanned};

/// Attribute macro for entity struct definitions.
///
/// Generates the `Entity` trait implementation for the struct.
///
/// # Attributes
///
/// - `#[entity]` — entity type name = struct name
/// - `#[entity(name = "CustomName")]` — custom entity type name
/// - `#[entity(shard_group = "premium")]` — custom shard group
/// - `#[entity(max_idle_time_secs = 120)]` — custom max idle time
/// - `#[entity(mailbox_capacity = 50)]` — custom mailbox capacity
/// - `#[entity(concurrency = 4)]` — custom concurrency limit
/// - `#[entity(krate = "crate")]` — for internal use within cruster
#[proc_macro_attribute]
pub fn entity(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as EntityArgs);
    let input = parse_macro_input!(item as syn::ItemStruct);
    match entity_impl_inner(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Attribute macro for entity impl blocks.
///
/// Each `async fn` annotated with `#[rpc]` or `#[workflow]` becomes an RPC handler.
/// Unannotated async functions are treated as internal helpers.
///
/// # Attributes
///
/// - `#[entity_impl]` — default crate path
/// - `#[entity_impl(krate = "crate")]` — for internal use within cruster
/// - `#[entity_impl(traits(TraitA, TraitB))]` — compose entity traits
/// - `#[entity_impl(deferred_keys(SIGNAL: i32 = "signal"))]` — generate `DeferredKey` constants
///
/// # State Attribute
///
/// Use `#[state(Type)]` on the impl block to define per-instance persistent state:
///
/// ```text
/// #[entity_impl]
/// #[state(MyState)]
/// impl MyEntity {
///     fn init(&self, ctx: &EntityContext) -> Result<MyState, ClusterError> {
///         Ok(MyState::default())
///     }
///
///     #[rpc]
///     async fn get_value(&self) -> Result<i32, ClusterError> {
///         Ok(self.state.value)  // self.state is &MyState (read-only)
///     }
///
///     #[activity]
///     async fn set_value(&mut self, value: i32) -> Result<(), ClusterError> {
///         self.state.value = value;  // self.state is &mut MyState
///         Ok(())  // State auto-persisted on activity completion
///     }
///
///     #[workflow]
///     async fn do_set(&self, value: i32) -> Result<(), ClusterError> {
///         self.set_value(value).await  // Workflows call activities
///     }
/// }
/// ```
///
/// The state type must implement `Clone + Serialize + DeserializeOwned + Send + Sync`.
///
/// ## State Access Pattern
///
/// - `#[rpc]` / `#[workflow]` methods use `&self` and access `self.state` as `&State` (read-only)
/// - `#[activity]` methods use `&mut self` and access `self.state` as `&mut State` (mutable)
/// - Activities are called from workflows via `self.activity_name()` (auto-delegated)
#[proc_macro_attribute]
pub fn entity_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as ImplArgs);
    let input = parse_macro_input!(item as syn::ItemImpl);
    match entity_impl_block_inner(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Attribute macro for trait capability struct definitions.
///
/// Currently a marker with optional crate path override.
#[proc_macro_attribute]
pub fn entity_trait(attr: TokenStream, item: TokenStream) -> TokenStream {
    let _args = parse_macro_input!(attr as TraitArgs);
    let input = parse_macro_input!(item as syn::ItemStruct);
    quote! { #input }.into()
}

/// Attribute macro for trait capability impl blocks.
///
/// Each `async fn` annotated with `#[rpc]`, `#[workflow]`, or `#[activity]` becomes
/// an RPC handler that can be called on entities using this trait.
///
/// # Attributes
///
/// - `#[entity_trait_impl]` — default crate path
/// - `#[entity_trait_impl(krate = "crate")]` — for internal use within cruster
///
/// # Method Annotations
///
/// - `#[rpc]` — Read-only RPC handler (no state mutation)
/// - `#[workflow]` — Durable workflow handler (state mutations)
/// - `#[activity]` — Durable activity handler (external side effects)
///
/// # Visibility Annotations
///
/// - `#[public]` — Callable by external clients (default for `#[rpc]` and `#[workflow]`)
/// - `#[protected]` — Callable only by other entities (entity-to-entity)
/// - `#[private]` — Internal only (default for `#[activity]`)
///
/// # Example
///
/// ```text
/// #[entity_trait_impl]
/// #[state(AuditLog)]
/// impl Auditable {
///     fn init(&self) -> Result<AuditLog, ClusterError> {
///         Ok(AuditLog::default())
///     }
///
///     #[activity]
///     #[protected]
///     async fn log_action(&self, action: String) -> Result<(), ClusterError> {
///         let mut state = self.state_mut().await;
///         state.log(action);
///         Ok(())
///     }
///
///     #[rpc]
///     async fn get_log(&self) -> Result<Vec<String>, ClusterError> {
///         Ok(self.state().entries.clone())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn entity_trait_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as TraitArgs);
    let input = parse_macro_input!(item as syn::ItemImpl);
    match entity_trait_impl_inner(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Defines persistent state for an entity or entity trait.
///
/// This attribute is used together with `#[entity_impl]` or `#[entity_trait_impl]`
/// to define per-instance state that is automatically persisted.
///
/// # Usage
///
/// ```text
/// #[derive(Clone, Serialize, Deserialize)]
/// struct CounterState {
///     count: i32,
/// }
///
/// #[entity_impl]
/// #[state(CounterState)]
/// impl Counter {
///     fn init(&self, ctx: &EntityContext) -> Result<CounterState, ClusterError> {
///         Ok(CounterState { count: 0 })
///     }
///
///     #[rpc]
///     async fn get(&self) -> Result<i32, ClusterError> {
///         Ok(self.state().count)  // Lock-free read
///     }
///
///     #[rpc]
///     async fn increment(&self, amount: i32) -> Result<i32, ClusterError> {
///         let mut state = self.state_mut().await;  // Acquires write lock
///         state.count += amount;
///         Ok(state.count)  // Auto-persisted on drop
///     }
/// }
/// ```
///
/// # Requirements
///
/// The state type must implement:
/// - `Clone` — for creating mutable snapshots
/// - `serde::Serialize` + `serde::Deserialize` — for persistence
/// - `Send + Sync + 'static` — for async safety
///
/// # State Access
///
/// When `#[state(T)]` is used, the following methods are available on `self`:
///
/// - **`self.state()`** — Returns `arc_swap::Guard<Arc<T>>` for lock-free reads.
///   Dereferences directly to `&T`. Never blocks.
///
/// - **`self.state_mut().await`** — Returns `StateMutGuard<T>` for mutations.
///   Acquires an async write lock. State is automatically persisted when the
///   guard is dropped.
///
/// # Persistence
///
/// State is automatically loaded from storage when the entity spawns and saved
/// when `StateMutGuard` is dropped. For entities with traits, composite state
/// (entity + all trait states) is saved together.
#[proc_macro_attribute]
pub fn state(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // This is a marker attribute - actual parsing is done by entity_impl/entity_trait_impl.
    // We define it as a proc-macro so IDEs can provide autocomplete and documentation.
    item
}

/// Marks an async method as an RPC handler.
///
/// Use `#[rpc]` for request/response handlers. By default, RPCs use best-effort
/// delivery. Use `#[rpc(persisted)]` for at-least-once delivery via `MessageStorage`
/// deduplication.
///
/// # Variants
///
/// | Variant | Delivery | Idempotency | Use case |
/// |---|---|---|---|
/// | `#[rpc]` | Best-effort | None | Reads, queries |
/// | `#[rpc(persisted)]` | At-least-once | Via `MessageStorage` dedup | Writes, mutations |
///
/// # Usage
///
/// ```text
/// #[entity_impl]
/// impl MyEntity {
///     #[rpc]
///     async fn get_count(&self) -> Result<i32, ClusterError> {
///         Ok(42)
///     }
///
///     #[rpc(persisted)]
///     async fn update_count(&self, value: i32) -> Result<(), ClusterError> {
///         Ok(())
///     }
/// }
/// ```
///
/// # Visibility
///
/// By default, `#[rpc]` methods are `#[public]` (externally callable).
/// Combine with `#[protected]` or `#[private]` to restrict access.
///
/// # Generated Code
///
/// For each `#[rpc]` method, the macro generates:
/// - A dispatch arm in `handle_request` for the method tag
/// - A typed client method with the same signature
/// - For `#[rpc(persisted)]`, the client uses `send_persisted()` instead of `send()`
#[proc_macro_attribute]
pub fn rpc(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Dual-purpose macro: standalone workflow struct definition OR entity method marker.
///
/// ## On a struct: Standalone Workflow Definition
///
/// When applied to a struct, `#[workflow]` defines a standalone workflow — a stateless,
/// durable orchestration construct backed by a hidden entity.
///
/// ### Attributes
///
/// - `#[workflow]` — default: key = hash(serialize(request))
/// - `#[workflow(key = |req| req.order_id.clone())]` — custom key, hashed
/// - `#[workflow(key = |req| req.order_id.clone(), hash = false)]` — custom key, raw
///
/// ### Example
///
/// ```text
/// use cruster::prelude::*;
///
/// #[workflow]
/// #[derive(Clone)]
/// pub struct ProcessOrder {
///     http: HttpClient,
/// }
///
/// #[workflow_impl]
/// impl ProcessOrder {
///     async fn execute(&self, request: OrderRequest) -> Result<OrderResult, ClusterError> {
///         let reserved = self.reserve_inventory(request.items.clone()).await?;
///         Ok(OrderResult { order_id: reserved.id })
///     }
///
///     #[activity]
///     async fn reserve_inventory(&self, items: Vec<Item>) -> Result<Reservation, ClusterError> {
///         todo!()
///     }
/// }
/// ```
///
/// ## On a method: Entity Workflow Marker (Legacy)
///
/// When applied to a method inside `#[entity_impl]`, marks it as a durable workflow
/// method (persisted, publicly callable).
///
/// ```text
/// #[entity_impl]
/// #[state(CounterState)]
/// impl Counter {
///     #[workflow]
///     async fn increment(&self, amount: i32) -> Result<i32, ClusterError> {
///         let mut state = self.state_mut().await;
///         state.count += amount;
///         Ok(state.count)
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Try to parse as a struct — if it is, do standalone workflow codegen.
    // If it's a function/method, it's a no-op marker (parsed by #[entity_impl]).
    let item_clone = item.clone();
    if syn::parse::<syn::ItemStruct>(item_clone).is_ok() {
        // Struct: standalone workflow
        let args = parse_macro_input!(attr as WorkflowStructArgs);
        let input = parse_macro_input!(item as syn::ItemStruct);
        match workflow_struct_inner(args, input) {
            Ok(tokens) => tokens.into(),
            Err(e) => e.to_compile_error().into(),
        }
    } else {
        // Method/function: no-op marker for entity_impl parsing
        item
    }
}

/// Attribute macro for standalone workflow impl blocks.
///
/// Must contain exactly one `execute` method (the entry point) and zero or more
/// `#[activity]` methods (journaled side effects).
///
/// # Attributes
///
/// - `#[workflow_impl]` — default
/// - `#[workflow_impl(krate = "crate")]` — for internal use
///
/// # Example
///
/// ```text
/// #[workflow_impl]
/// impl ProcessOrder {
///     async fn execute(&self, request: OrderRequest) -> Result<OrderResult, ClusterError> {
///         let charge = self.charge(request.payment).await?;
///         Ok(OrderResult { charge_id: charge.id })
///     }
///
///     #[activity]
///     async fn charge(&self, payment: Payment) -> Result<Charge, ClusterError> {
///         todo!()
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn workflow_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as WorkflowImplArgs);
    let input = parse_macro_input!(item as syn::ItemImpl);
    match workflow_impl_inner(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Marks an async method as a durable activity (external side effects).
///
/// Use `#[activity]` for operations that have **external side effects** and need
/// to be retried on failure. Activities are called from within workflows via
/// `DurableContext::run()`.
///
/// **Do NOT use `#[activity]` for state mutations** — use `#[workflow]` instead.
///
/// # Usage
///
/// ```text
/// #[entity_impl]
/// #[state(OrderState)]
/// impl OrderProcessor {
///     /// Send confirmation email - external side effect
///     #[activity]
///     async fn send_email(&self, to: String, body: String) -> Result<(), ClusterError> {
///         email_service.send(&to, &body).await
///     }
///
///     /// Charge payment - external API call
///     #[activity]
///     async fn charge_payment(&self, card: Card, amount: u64) -> Result<PaymentId, ClusterError> {
///         payment_gateway.charge(&card, amount).await
///     }
///
///     /// Main workflow that uses activities
///     #[workflow]
///     async fn process(&self, ctx: &DurableContext, order: Order) -> Result<(), ClusterError> {
///         // Activities are called via ctx.run() for persistence and retry
///         ctx.run(|| self.charge_payment(order.card, order.total)).await?;
///         ctx.run(|| self.send_email(order.email, "Order confirmed!")).await?;
///         Ok(())
///     }
/// }
/// ```
///
/// # When to Use
///
/// - External API calls (payment, email, webhooks)
/// - Operations that may fail transiently and need retry
/// - Side effects that should only happen once per workflow execution
///
/// # Visibility
///
/// Activities are `#[private]` by default — they can only be called internally
/// or from workflows via `DurableContext`. They cannot be made `#[public]`.
///
/// # Idempotency Key
///
/// Use `#[activity(key = |req| ...)]` to deduplicate:
///
/// ```text
/// #[activity(key = |to, _body| to.clone())]
/// async fn send_email(&self, to: String, body: String) -> Result<(), ClusterError> {
///     // Only sends once per recipient within the workflow
/// }
/// ```
#[proc_macro_attribute]
pub fn activity(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method as publicly accessible.
///
/// Public methods can be called by:
/// - External clients via the typed client API
/// - Other entities via entity-to-entity communication
/// - Internal code within the same entity
///
/// # Usage
///
/// ```text
/// #[entity_impl]
/// impl MyEntity {
///     #[rpc]
///     #[public]  // This is the default for #[rpc]
///     async fn get_status(&self) -> Result<Status, ClusterError> {
///         Ok(self.status)
///     }
/// }
/// ```
///
/// # Default Visibility
///
/// - `#[rpc]` methods are `#[public]` by default
/// - `#[workflow]` methods are `#[public]` by default
/// - `#[activity]` methods are `#[private]` by default (cannot be made public)
#[proc_macro_attribute]
pub fn public(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method as protected (entity-to-entity only).
///
/// Protected methods can be called by:
/// - Other entities via entity-to-entity communication
/// - Internal code within the same entity
///
/// They CANNOT be called by external clients.
///
/// # Usage
///
/// ```text
/// #[entity_impl]
/// impl MyEntity {
///     #[rpc]
///     #[protected]
///     async fn internal_sync(&self, data: SyncData) -> Result<(), ClusterError> {
///         // Only other entities can call this
///         self.apply_sync(data)
///     }
/// }
/// ```
///
/// # Use Cases
///
/// - Inter-entity synchronization
/// - Internal cluster coordination
/// - Methods that should not be exposed to external APIs
#[proc_macro_attribute]
pub fn protected(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a method as private (internal only).
///
/// Private methods can only be called from within the same entity.
/// They are not exposed via dispatch and have no client method generated.
///
/// # Usage
///
/// ```text
/// #[entity_impl]
/// impl MyEntity {
///     #[rpc]
///     #[private]
///     async fn helper(&self, data: Data) -> Result<Processed, ClusterError> {
///         // Only callable from other methods in this entity
///         process(data)
///     }
///
///     #[rpc]
///     async fn public_method(&self, input: Input) -> Result<Output, ClusterError> {
///         let processed = self.helper(input.data).await?;
///         Ok(Output::from(processed))
///     }
/// }
/// ```
///
/// # Default Visibility
///
/// `#[activity]` methods are `#[private]` by default.
#[proc_macro_attribute]
pub fn private(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a helper method within an entity or trait impl block.
///
/// Helper methods are internal methods that can be called by other methods
/// within the entity but are not exposed as RPC handlers. They follow the
/// same state access rules as other methods:
///
/// - `&self` — read-only access to state via `self.state`
/// - `&mut self` — **not allowed** (only `#[activity]` can mutate state)
///
/// All methods in an entity impl block must be annotated with one of:
/// `#[rpc]`, `#[workflow]`, `#[activity]`, or `#[method]`.
///
/// # Example
///
/// ```text
/// #[entity_impl]
/// #[state(MyState)]
/// impl MyEntity {
///     #[method]
///     fn compute_bonus(&self, amount: i32) -> i32 {
///         self.state.multiplier * amount
///     }
///
///     #[activity]
///     async fn apply_bonus(&mut self, amount: i32) -> Result<i32, ClusterError> {
///         self.state.value += self.compute_bonus(amount);
///         Ok(self.state.value)
///     }
/// }
/// ```
///
/// # Visibility
///
/// `#[method]` is `#[private]` by default. Use `#[protected]` to make it
/// callable from other entities.
#[proc_macro_attribute]
pub fn method(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Attribute macro for RPC group struct definitions.
///
/// RPC groups bundle reusable `#[rpc]` / `#[rpc(persisted)]` methods that can be
/// composed into multiple entities via `#[entity_impl(rpc_groups(...))]`.
///
/// RPC groups are stateless — no `#[state(...)]` is allowed.
///
/// # Example
///
/// ```text
/// use cruster::prelude::*;
///
/// #[rpc_group]
/// #[derive(Clone)]
/// pub struct HealthCheck;
///
/// #[rpc_group_impl]
/// impl HealthCheck {
///     #[rpc]
///     async fn health(&self) -> Result<String, ClusterError> {
///         Ok("ok".to_string())
///     }
/// }
/// ```
///
/// Then compose into an entity:
///
/// ```text
/// #[entity_impl(rpc_groups(HealthCheck))]
/// impl MyEntity {
///     // ... own RPCs
///     // health() is also callable on this entity's client
/// }
/// ```
#[proc_macro_attribute]
pub fn rpc_group(attr: TokenStream, item: TokenStream) -> TokenStream {
    let _args = parse_macro_input!(attr as TraitArgs);
    let input = parse_macro_input!(item as syn::ItemStruct);
    // Pass-through — marker only, like #[entity_trait] and #[activity_group]
    quote! { #input }.into()
}

/// Attribute macro for RPC group impl blocks.
///
/// Parses `#[rpc]` / `#[rpc(persisted)]` methods and generates dispatch, client extension,
/// and delegation traits so the group's RPCs can be composed into entities.
///
/// # Allowed method annotations
///
/// - `#[rpc]` — best-effort RPC handler
/// - `#[rpc(persisted)]` — at-least-once RPC handler
/// - Unannotated methods — helpers
///
/// # Forbidden annotations
///
/// - `#[activity]` — RPC groups don't have activities
/// - `#[workflow]` — RPC groups are not workflows
/// - `#[state(...)]` — RPC groups are stateless
/// - `&mut self` — all methods must use `&self`
///
/// # Example
///
/// ```text
/// #[rpc_group_impl]
/// impl HealthCheck {
///     #[rpc]
///     async fn health(&self) -> Result<String, ClusterError> {
///         Ok("ok".to_string())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn rpc_group_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as RpcGroupImplArgs);
    let input = parse_macro_input!(item as syn::ItemImpl);
    match rpc_group_impl_inner(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Attribute macro for activity group struct definitions.
///
/// Activity groups bundle reusable `#[activity]` methods that can be composed
/// into multiple workflows via `#[workflow_impl(activity_groups(...))]`.
///
/// Activity groups are stateless — no `#[state(...)]` is allowed.
///
/// # Example
///
/// ```text
/// use cruster::prelude::*;
///
/// #[activity_group]
/// #[derive(Clone)]
/// pub struct Payments {
///     stripe: StripeClient,
/// }
///
/// #[activity_group_impl]
/// impl Payments {
///     #[activity(retries = 3)]
///     async fn charge(&self, info: PaymentInfo, amount: u64) -> Result<Charge, ClusterError> {
///         self.stripe.charge(info, amount).await
///     }
///
///     #[activity]
///     async fn refund(&self, transaction_id: String) -> Result<(), ClusterError> {
///         self.stripe.refund(transaction_id).await
///     }
/// }
/// ```
///
/// Then compose into a workflow:
///
/// ```text
/// #[workflow_impl(activity_groups(Payments))]
/// impl ProcessOrder {
///     async fn execute(&self, request: OrderRequest) -> Result<OrderResult, ClusterError> {
///         let charge = self.charge(request.payment, request.total).await?;
///         Ok(OrderResult { charge_id: charge.id })
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn activity_group(attr: TokenStream, item: TokenStream) -> TokenStream {
    let _args = parse_macro_input!(attr as TraitArgs);
    let input = parse_macro_input!(item as syn::ItemStruct);
    // Pass-through — marker only, like #[entity_trait]
    quote! { #input }.into()
}

/// Attribute macro for activity group impl blocks.
///
/// Parses `#[activity]` methods and generates access/delegation traits so the
/// group's activities can be composed into workflows.
///
/// # Allowed method annotations
///
/// - `#[activity]` — durable activity (journaled side effect)
/// - Unannotated methods — helpers
///
/// # Forbidden annotations
///
/// - `#[rpc]` — activities groups don't have RPCs
/// - `#[workflow]` — activity groups are not workflows
/// - `#[state(...)]` — activity groups are stateless
/// - `&mut self` — all methods must use `&self`
///
/// # Example
///
/// ```text
/// #[activity_group_impl]
/// impl Payments {
///     #[activity]
///     async fn charge(&self, info: PaymentInfo, amount: u64) -> Result<Charge, ClusterError> {
///         todo!()
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn activity_group_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as ActivityGroupImplArgs);
    let input = parse_macro_input!(item as syn::ItemImpl);
    match activity_group_impl_inner(args, input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

// --- Argument parsing ---

struct EntityArgs {
    name: Option<String>,
    shard_group: Option<String>,
    max_idle_time_secs: Option<u64>,
    mailbox_capacity: Option<usize>,
    concurrency: Option<usize>,
    krate: Option<syn::Path>,
}

impl syn::parse::Parse for EntityArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = EntityArgs {
            name: None,
            shard_group: None,
            max_idle_time_secs: None,
            mailbox_capacity: None,
            concurrency: None,
            krate: None,
        };

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;

            match ident.to_string().as_str() {
                "name" => {
                    let lit: syn::LitStr = input.parse()?;
                    args.name = Some(lit.value());
                }
                "shard_group" => {
                    let lit: syn::LitStr = input.parse()?;
                    args.shard_group = Some(lit.value());
                }
                "max_idle_time_secs" => {
                    let lit: syn::LitInt = input.parse()?;
                    args.max_idle_time_secs = Some(lit.base10_parse()?);
                }
                "mailbox_capacity" => {
                    let lit: syn::LitInt = input.parse()?;
                    args.mailbox_capacity = Some(lit.base10_parse()?);
                }
                "concurrency" => {
                    let lit: syn::LitInt = input.parse()?;
                    args.concurrency = Some(lit.base10_parse()?);
                }
                "krate" => {
                    let lit: syn::LitStr = input.parse()?;
                    args.krate = Some(lit.parse()?);
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown entity attribute: {other}"),
                    ));
                }
            }

            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }

        Ok(args)
    }
}

struct ImplArgs {
    krate: Option<syn::Path>,
    traits: Vec<syn::Path>,
    rpc_groups: Vec<syn::Path>,
    deferred_keys: Vec<DeferredKeyDecl>,
}

struct DeferredKeyDecl {
    ident: syn::Ident,
    ty: syn::Type,
    name: syn::LitStr,
}

impl syn::parse::Parse for DeferredKeyDecl {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ident: syn::Ident = input.parse()?;
        input.parse::<syn::Token![:]>()?;
        let ty: syn::Type = input.parse()?;
        if !input.peek(syn::Token![=]) {
            return Err(syn::Error::new(
                input.span(),
                "expected `= \"name\"` for deferred key",
            ));
        }
        input.parse::<syn::Token![=]>()?;
        let name: syn::LitStr = input.parse()?;
        Ok(DeferredKeyDecl { ident, ty, name })
    }
}

impl syn::parse::Parse for ImplArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = ImplArgs {
            krate: None,
            traits: Vec::new(),
            rpc_groups: Vec::new(),
            deferred_keys: Vec::new(),
        };
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            match ident.to_string().as_str() {
                "krate" => {
                    input.parse::<syn::Token![=]>()?;
                    let lit: syn::LitStr = input.parse()?;
                    args.krate = Some(lit.parse()?);
                }
                "traits" => {
                    let content;
                    syn::parenthesized!(content in input);
                    while !content.is_empty() {
                        let path: syn::Path = content.parse()?;
                        args.traits.push(path);
                        if !content.is_empty() {
                            content.parse::<syn::Token![,]>()?;
                        }
                    }
                }
                "rpc_groups" => {
                    let content;
                    syn::parenthesized!(content in input);
                    while !content.is_empty() {
                        let path: syn::Path = content.parse()?;
                        args.rpc_groups.push(path);
                        if !content.is_empty() {
                            content.parse::<syn::Token![,]>()?;
                        }
                    }
                }
                "deferred_keys" => {
                    let content;
                    syn::parenthesized!(content in input);
                    let decls = content.parse_terminated(DeferredKeyDecl::parse, syn::Token![,])?;
                    args.deferred_keys.extend(decls);
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown entity_impl attribute: {other}"),
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(args)
    }
}

struct TraitArgs {
    krate: Option<syn::Path>,
}

impl syn::parse::Parse for TraitArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = TraitArgs { krate: None };
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            match ident.to_string().as_str() {
                "krate" => {
                    let lit: syn::LitStr = input.parse()?;
                    args.krate = Some(lit.parse()?);
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown entity_trait attribute: {other}"),
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(args)
    }
}

fn default_crate_path() -> syn::Path {
    syn::parse_str("cruster").unwrap()
}

fn replace_last_segment(path: &syn::Path, ident: syn::Ident) -> syn::Path {
    let mut new_path = path.clone();
    if let Some(last) = new_path.segments.last_mut() {
        last.ident = ident;
        last.arguments = syn::PathArguments::None;
    }
    new_path
}

struct TraitInfo {
    path: syn::Path,
    ident: syn::Ident,
    field: syn::Ident,
    wrapper_path: syn::Path,
    state_info_path: syn::Path,
    state_init_path: syn::Path,
    missing_reason: String,
}

#[allow(dead_code)]
struct RpcGroupInfo {
    path: syn::Path,
    ident: syn::Ident,
    field: syn::Ident,
    wrapper_path: syn::Path,
    access_trait_path: syn::Path,
    methods_trait_path: syn::Path,
}

fn rpc_group_infos_from_paths(paths: &[syn::Path]) -> Vec<RpcGroupInfo> {
    paths
        .iter()
        .map(|path| {
            let ident = path
                .segments
                .last()
                .expect("rpc group path missing segment")
                .ident
                .clone();
            let snake = to_snake(&ident.to_string());
            let field = format_ident!("__rpc_group_{}", snake);
            let wrapper_ident = format_ident!("__{}RpcGroupWrapper", ident);
            let access_trait_ident = format_ident!("__{}RpcGroupAccess", ident);
            let methods_trait_ident = format_ident!("__{}RpcGroupMethods", ident);
            let wrapper_path = replace_last_segment(path, wrapper_ident);
            let access_trait_path = replace_last_segment(path, access_trait_ident);
            let methods_trait_path = replace_last_segment(path, methods_trait_ident);
            RpcGroupInfo {
                path: path.clone(),
                ident,
                field,
                wrapper_path,
                access_trait_path,
                methods_trait_path,
            }
        })
        .collect()
}

fn trait_infos_from_paths(paths: &[syn::Path]) -> Vec<TraitInfo> {
    paths
        .iter()
        .map(|path| {
            let ident = path
                .segments
                .last()
                .expect("trait path missing segment")
                .ident
                .clone();
            let field = format_ident!("__trait_{}", to_snake(&ident.to_string()));
            let wrapper_ident = format_ident!("{}StateWrapper", ident);
            let state_info_ident = format_ident!("__{}TraitStateInfo", ident);
            let state_init_ident = format_ident!("__{}TraitStateInit", ident);
            let wrapper_path = replace_last_segment(path, wrapper_ident);
            let state_info_path = replace_last_segment(path, state_info_ident);
            let state_init_path = replace_last_segment(path, state_init_ident);
            let missing_reason = format!("missing trait dependency: {ident}");
            TraitInfo {
                path: path.clone(),
                ident,
                field,
                wrapper_path,
                state_info_path,
                state_init_path,
                missing_reason,
            }
        })
        .collect()
}

// --- #[entity] code generation ---

fn entity_impl_inner(
    args: EntityArgs,
    input: syn::ItemStruct,
) -> syn::Result<proc_macro2::TokenStream> {
    let krate = args.krate.clone().unwrap_or_else(default_crate_path);
    let struct_name = &input.ident;
    let entity_name = args.name.unwrap_or_else(|| struct_name.to_string());
    let shard_group_value = if let Some(sg) = &args.shard_group {
        quote! { #sg }
    } else {
        quote! { "default" }
    };
    let max_idle_value = if let Some(secs) = args.max_idle_time_secs {
        quote! { ::std::option::Option::Some(::std::time::Duration::from_secs(#secs)) }
    } else {
        quote! { ::std::option::Option::None }
    };
    let mailbox_value = if let Some(cap) = args.mailbox_capacity {
        quote! { ::std::option::Option::Some(#cap) }
    } else {
        quote! { ::std::option::Option::None }
    };
    let concurrency_value = if let Some(c) = args.concurrency {
        quote! { ::std::option::Option::Some(#c) }
    } else {
        quote! { ::std::option::Option::None }
    };

    Ok(quote! {
        #input

        #[allow(dead_code)]
        impl #struct_name {
            #[doc(hidden)]
            fn __entity_type(&self) -> #krate::types::EntityType {
                #krate::types::EntityType::new(#entity_name)
            }

            #[doc(hidden)]
            fn __shard_group(&self) -> &str {
                #shard_group_value
            }

            #[doc(hidden)]
            fn __shard_group_for(&self, _entity_id: &#krate::types::EntityId) -> &str {
                self.__shard_group()
            }

            #[doc(hidden)]
            fn __max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                #max_idle_value
            }

            #[doc(hidden)]
            fn __mailbox_capacity(&self) -> ::std::option::Option<usize> {
                #mailbox_value
            }

            #[doc(hidden)]
            fn __concurrency(&self) -> ::std::option::Option<usize> {
                #concurrency_value
            }
        }
    })
}

// --- #[entity_impl] code generation ---

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RpcKind {
    Rpc,
    Workflow,
    Activity,
    Method,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RpcVisibility {
    Public,
    Protected,
    Private,
}

impl RpcKind {
    fn is_persisted(&self) -> bool {
        matches!(self, RpcKind::Workflow | RpcKind::Activity)
    }
}

impl RpcMethod {
    /// Whether the client should use persisted delivery for this RPC.
    fn uses_persisted_delivery(&self) -> bool {
        self.kind.is_persisted() || self.rpc_persisted
    }
}

impl RpcVisibility {
    fn is_public(&self) -> bool {
        matches!(self, RpcVisibility::Public)
    }

    fn is_private(&self) -> bool {
        matches!(self, RpcVisibility::Private)
    }
}

struct RpcMethod {
    name: syn::Ident,
    tag: String,
    params: Vec<RpcParam>,
    response_type: syn::Type,
    is_mut: bool,
    kind: RpcKind,
    visibility: RpcVisibility,
    /// Optional idempotency key closure for workflows/activities.
    persist_key: Option<syn::ExprClosure>,
    /// Whether this RPC takes a `&DurableContext` parameter — enables workflow capabilities.
    has_durable_context: bool,
    /// Whether this RPC uses persisted (at-least-once) delivery via `#[rpc(persisted)]`.
    rpc_persisted: bool,
    /// Max retry attempts for activities (from `#[activity(retries = N)]`).
    /// Used by standalone workflow codegen; entity-based activities do not use retries from here.
    #[allow(dead_code)]
    retries: Option<u32>,
    /// Backoff strategy for activity retries: `"exponential"` or `"constant"`.
    /// Used by standalone workflow codegen; entity-based activities do not use retries from here.
    #[allow(dead_code)]
    backoff: Option<String>,
}

impl RpcMethod {
    fn is_dispatchable(&self) -> bool {
        self.visibility.is_public() && !matches!(self.kind, RpcKind::Activity | RpcKind::Method)
    }

    fn is_client_visible(&self) -> bool {
        self.visibility.is_public() && !matches!(self.kind, RpcKind::Activity | RpcKind::Method)
    }

    fn is_trait_visible(&self) -> bool {
        !self.visibility.is_private() && !matches!(self.kind, RpcKind::Method)
    }
}

struct RpcParam {
    name: syn::Ident,
    ty: syn::Type,
}

fn entity_impl_block_inner(
    args: ImplArgs,
    input: syn::ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let krate = args.krate.unwrap_or_else(default_crate_path);
    let traits = args.traits;
    let rpc_groups = args.rpc_groups;
    let deferred_keys = args.deferred_keys;
    let mut input = input;
    let state_info = parse_state_attr(&mut input.attrs)?;
    let self_ty = &input.self_ty;

    let struct_name = match self_ty.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.clone())
            .ok_or_else(|| syn::Error::new(self_ty.span(), "expected struct name"))?,
        _ => return Err(syn::Error::new(self_ty.span(), "expected struct name")),
    };

    let handler_name = format_ident!("{}Handler", struct_name);
    let client_name = format_ident!("{}Client", struct_name);

    // Detect `#[state(Type, ...)]` on the impl block.
    let state_type: Option<syn::Type> = state_info.as_ref().map(|info| info.ty.clone());
    let mut has_init = false;
    let mut rpcs = Vec::new();
    let mut original_methods = Vec::new();
    let mut init_method: Option<syn::ImplItemFn> = None;

    for item in &input.items {
        match item {
            syn::ImplItem::Type(type_item) if type_item.ident == "State" => {
                return Err(syn::Error::new(
                    type_item.span(),
                    "use #[state(Type)] on the impl block instead of `type State`",
                ));
            }
            syn::ImplItem::Fn(method) => {
                let has_rpc_attrs = parse_kind_attr(&method.attrs)?.is_some()
                    || parse_visibility_attr(&method.attrs)?.is_some();
                if method.sig.ident == "init" && method.sig.asyncness.is_none() {
                    if has_rpc_attrs {
                        return Err(syn::Error::new(
                            method.sig.span(),
                            "RPC annotations are only valid on async methods",
                        ));
                    }
                    has_init = true;
                    init_method = Some(method.clone());
                } else if method.sig.asyncness.is_some() {
                    if let Some(rpc) = parse_rpc_method(method)? {
                        rpcs.push(rpc);
                    }
                } else if has_rpc_attrs {
                    return Err(syn::Error::new(
                        method.sig.span(),
                        "RPC annotations are only valid on async methods",
                    ));
                }
                original_methods.push(method.clone());
            }
            _ => {}
        }
    }

    let is_stateful = state_type.is_some();

    if is_stateful && !has_init {
        return Err(syn::Error::new(
            input.self_ty.span(),
            "stateful entities with #[state(...)] must define `fn init(&self, ctx: &EntityContext) -> Result<State, ClusterError>`",
        ));
    }

    if has_init && !is_stateful {
        return Err(syn::Error::new(
            init_method.as_ref().unwrap().sig.span(),
            "`fn init` is only valid when #[state(...)] is also defined",
        ));
    }

    let entity_tokens = generate_entity(
        &krate,
        &struct_name,
        &handler_name,
        &client_name,
        state_type.as_ref(),
        &traits,
        &rpc_groups,
        &rpcs,
        &original_methods,
    )?;

    let deferred_consts = generate_deferred_key_consts(&krate, &deferred_keys)?;

    Ok(quote! {
        #entity_tokens
        #deferred_consts
    })
}

fn generate_deferred_key_consts(
    krate: &syn::Path,
    deferred_keys: &[DeferredKeyDecl],
) -> syn::Result<proc_macro2::TokenStream> {
    if deferred_keys.is_empty() {
        return Ok(quote! {});
    }

    let mut seen_idents = HashSet::new();
    let mut seen_names = HashSet::new();
    for decl in deferred_keys {
        let ident = decl.ident.to_string();
        if !seen_idents.insert(ident.clone()) {
            return Err(syn::Error::new(
                decl.ident.span(),
                format!("duplicate deferred key constant: {ident}"),
            ));
        }
        let name = decl.name.value();
        if !seen_names.insert(name.clone()) {
            return Err(syn::Error::new(
                decl.name.span(),
                format!("duplicate deferred key name: {name}"),
            ));
        }
    }

    let consts: Vec<_> = deferred_keys
        .iter()
        .map(|decl| {
            let ident = &decl.ident;
            let ty = &decl.ty;
            let name = &decl.name;
            quote! {
                #[allow(dead_code)]
                pub const #ident: #krate::__internal::DeferredKey<#ty> =
                    #krate::__internal::DeferredKey::new(#name);
            }
        })
        .collect();

    Ok(quote! {
        #(#consts)*
    })
}

fn entity_trait_impl_inner(
    args: TraitArgs,
    input: syn::ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let krate = args.krate.unwrap_or_else(default_crate_path);
    let mut input = input;
    let state_info = parse_state_attr(&mut input.attrs)?;
    let self_ty = &input.self_ty;

    let trait_ident = match self_ty.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.clone())
            .ok_or_else(|| syn::Error::new(self_ty.span(), "expected trait struct name"))?,
        _ => {
            return Err(syn::Error::new(
                self_ty.span(),
                "expected trait struct name",
            ))
        }
    };

    let mut rpcs = Vec::new();
    let state_type: Option<syn::Type> = state_info.as_ref().map(|info| info.ty.clone());
    let mut has_init = false;
    let mut init_method: Option<syn::ImplItemFn> = None;
    let mut original_methods = Vec::new();

    for item in &input.items {
        match item {
            syn::ImplItem::Type(type_item) if type_item.ident == "State" => {
                return Err(syn::Error::new(
                    type_item.span(),
                    "use #[state(Type)] on the impl block instead of `type State`",
                ));
            }
            syn::ImplItem::Fn(method) => {
                let has_rpc_attrs = parse_kind_attr(&method.attrs)?.is_some()
                    || parse_visibility_attr(&method.attrs)?.is_some();
                if method.sig.ident == "init" && method.sig.asyncness.is_none() {
                    if has_rpc_attrs {
                        return Err(syn::Error::new(
                            method.sig.span(),
                            "RPC annotations are only valid on async methods",
                        ));
                    }
                    has_init = true;
                    init_method = Some(method.clone());
                } else if method.sig.asyncness.is_some() {
                    if let Some(rpc) = parse_rpc_method(method)? {
                        if rpc.has_durable_context {
                            return Err(syn::Error::new(
                                method.sig.span(),
                                "DurableContext parameters are not supported in entity traits",
                            ));
                        }
                        rpcs.push(rpc);
                    }
                } else if has_rpc_attrs {
                    return Err(syn::Error::new(
                        method.sig.span(),
                        "RPC annotations are only valid on async methods",
                    ));
                }
                original_methods.push(method.clone());
            }
            _ => {}
        }
    }

    let is_stateful = state_type.is_some();

    if is_stateful && !has_init {
        return Err(syn::Error::new(
            input.self_ty.span(),
            "entity traits with #[state(...)] must define `fn init(&self) -> Result<State, ClusterError>`",
        ));
    }

    if has_init && !is_stateful {
        return Err(syn::Error::new(
            init_method.as_ref().unwrap().sig.span(),
            "`fn init` is only valid when #[state(...)] is also defined",
        ));
    }

    let mut cleaned_impl = input.clone();
    cleaned_impl.attrs.retain(|a| !a.path().is_ident("state"));
    for item in &mut cleaned_impl.items {
        if let syn::ImplItem::Fn(method) = item {
            method.attrs.retain(|a| {
                !a.path().is_ident("rpc")
                    && !a.path().is_ident("workflow")
                    && !a.path().is_ident("activity")
                    && !a.path().is_ident("method")
                    && !a.path().is_ident("public")
                    && !a.path().is_ident("protected")
                    && !a.path().is_ident("private")
            });
        }
    }
    cleaned_impl.items.retain(|item| match item {
        syn::ImplItem::Type(_) => false,
        syn::ImplItem::Fn(method) => method.sig.asyncness.is_none() && method.sig.ident != "init",
        _ => true,
    });

    let wrapper_name = format_ident!("{}StateWrapper", trait_ident);
    let state_type = state_type.unwrap_or_else(|| syn::parse_str("()").unwrap());

    let init_takes_ctx = if let Some(init_method) = init_method.as_ref() {
        match init_method.sig.inputs.len() {
            1 => false,
            2 => true,
            _ => {
                return Err(syn::Error::new(
                    init_method.sig.span(),
                    "trait init must take either `&self` or `&self, &EntityContext`",
                ))
            }
        }
    } else {
        false
    };

    let init_call = if is_stateful {
        if init_takes_ctx {
            quote! { self.init(ctx) }
        } else {
            quote! {
                let _ = ctx;
                self.init()
            }
        }
    } else {
        quote! {
            let _ = ctx;
            ::std::result::Result::Ok(())
        }
    };

    let init_method_impl = if is_stateful {
        let init_method = init_method.as_ref().unwrap();
        let init_sig = &init_method.sig;
        let init_block = &init_method.block;
        let init_attrs = &init_method.attrs;
        let init_vis = &init_method.vis;
        quote! {
            impl #trait_ident {
                #(#init_attrs)*
                #init_vis #init_sig #init_block
            }
        }
    } else {
        quote! {}
    };

    // Generate view structs for trait methods (similar to entity view structs)
    let read_view_name = format_ident!("__{}ReadView", wrapper_name);
    let mut_view_name = format_ident!("__{}MutView", wrapper_name);

    let mut read_methods = Vec::new();
    let mut mut_methods = Vec::new();
    let mut wrapper_methods = Vec::new();

    for method in original_methods
        .iter()
        .filter(|m| m.sig.asyncness.is_some())
    {
        let method_name = &method.sig.ident;
        let rpc_info = rpcs.iter().find(|r| r.name == *method_name);
        let is_activity = rpc_info.is_some_and(|r| matches!(r.kind, RpcKind::Activity));

        let block = &method.block;
        let output = &method.sig.output;
        let generics = &method.sig.generics;
        let where_clause = &generics.where_clause;
        let attrs: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| {
                !a.path().is_ident("rpc")
                    && !a.path().is_ident("workflow")
                    && !a.path().is_ident("activity")
                    && !a.path().is_ident("method")
                    && !a.path().is_ident("public")
                    && !a.path().is_ident("protected")
                    && !a.path().is_ident("private")
            })
            .collect();
        let vis = &method.vis;

        // Get params excluding &self or &mut self
        let params: Vec<_> = method.sig.inputs.iter().skip(1).cloned().collect();
        let param_names: Vec<_> = method
            .sig
            .inputs
            .iter()
            .skip(1)
            .filter_map(|arg| {
                if let syn::FnArg::Typed(pat_type) = arg {
                    if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                        return Some(pat_ident.ident.clone());
                    }
                }
                None
            })
            .collect();

        if is_activity {
            // Activity methods go on MutView with &mut self, access self.state directly
            mut_methods.push(quote! {
                #(#attrs)*
                #vis async fn #method_name #generics (&mut self, #(#params),*) #output #where_clause
                    #block
            });

            // Wrapper acquires write lock and creates MutView
            wrapper_methods.push(quote! {
                #(#attrs)*
                #vis async fn #method_name #generics (&self, #(#params),*) #output #where_clause {
                    let __lock = self.__write_lock.clone().lock_owned().await;
                    let mut __guard = #krate::__internal::TraitStateMutGuard::new(
                        self.__state.clone(),
                        __lock,
                    );
                    let mut __view = #mut_view_name {
                        __wrapper: self,
                        state: &mut *__guard,
                    };
                    __view.#method_name(#(#param_names),*).await
                }
            });
        } else {
            // Read-only methods go on ReadView with &mut self, access self.state via StateRef proxy
            read_methods.push(quote! {
                #(#attrs)*
                #vis async fn #method_name #generics (&mut self, #(#params),*) #output #where_clause
                    #block
            });

            // Wrapper loads state into StateRef and creates ReadView
            wrapper_methods.push(quote! {
                #(#attrs)*
                #vis async fn #method_name #generics (&self, #(#params),*) #output #where_clause {
                    let mut __view = #read_view_name {
                        __wrapper: self,
                        state: #krate::__internal::StateRef::__new(self.__state.load_full()),
                    };
                    __view.#method_name(#(#param_names),*).await
                }
            });
        }
    }

    // Generate activity delegation methods for read view (so workflows can call activities)
    let activity_delegations: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| matches!(rpc.kind, RpcKind::Activity))
        .map(|rpc| {
            let method_name = &rpc.name;
            let method_info = original_methods
                .iter()
                .find(|m| m.sig.ident == *method_name)
                .unwrap();
            let params: Vec<_> = method_info.sig.inputs.iter().skip(1).collect();
            let param_names: Vec<_> = method_info
                .sig
                .inputs
                .iter()
                .skip(1)
                .filter_map(|arg| {
                    if let syn::FnArg::Typed(pat_type) = arg {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some(&pat_ident.ident);
                        }
                    }
                    None
                })
                .collect();
            let output = &method_info.sig.output;
            let generics = &method_info.sig.generics;
            let where_clause = &generics.where_clause;

            quote! {
                #[inline]
                async fn #method_name #generics (&mut self, #(#params),*) #output #where_clause {
                    let __result = self.__wrapper.#method_name(#(#param_names),*).await;
                    // Refresh state from ArcSwap after activity commits
                    self.state.__refresh(self.__wrapper.__state.load_full());
                    __result
                }
            }
        })
        .collect();

    let view_structs = quote! {
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #read_view_name<'a> {
            #[allow(dead_code)]
            __wrapper: &'a #wrapper_name,
            state: #krate::__internal::StateRef<#state_type>,
        }

        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #mut_view_name<'a> {
            #[allow(dead_code)]
            __wrapper: &'a #wrapper_name,
            state: &'a mut #state_type,
        }

        // Allow trait methods to access the underlying trait struct's fields via Deref
        impl ::std::ops::Deref for #read_view_name<'_> {
            type Target = #trait_ident;
            fn deref(&self) -> &Self::Target {
                &self.__wrapper.__trait
            }
        }

        impl ::std::ops::Deref for #mut_view_name<'_> {
            type Target = #trait_ident;
            fn deref(&self) -> &Self::Target {
                &self.__wrapper.__trait
            }
        }
    };

    let view_impls = quote! {
        impl #read_view_name<'_> {
            #(#activity_delegations)*
            #(#read_methods)*
        }

        impl #mut_view_name<'_> {
            #(#mut_methods)*
        }
    };

    let dispatch_impl = generate_trait_dispatch_impl(&krate, &wrapper_name, &rpcs);
    let client_ext = generate_trait_client_ext(&krate, &trait_ident, &rpcs);
    let access_trait_ident = format_ident!("__{}TraitAccess", trait_ident);
    let methods_trait_ident = format_ident!("__{}TraitMethods", trait_ident);
    let state_info_ident = format_ident!("__{}TraitStateInfo", trait_ident);
    let state_init_ident = format_ident!("__{}TraitStateInit", trait_ident);
    let missing_reason = format!("missing trait dependency: {trait_ident}");

    // With ArcSwap pattern, all methods use &self - mutability is handled via state_mut()
    let methods_impls: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| rpc.is_trait_visible())
        .map(|rpc| {
            let method_name = &rpc.name;
            let resp_type = &rpc.response_type;
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();
            let param_defs: Vec<_> = param_names
                .iter()
                .zip(param_types.iter())
                .map(|(name, ty)| quote! { #name: #ty })
                .collect();
            quote! {
                async fn #method_name(
                    &self,
                    #(#param_defs),*
                ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                    let __trait = self.__trait_ref().ok_or_else(|| #krate::error::ClusterError::MalformedMessage {
                        reason: ::std::string::String::from(#missing_reason),
                        source: ::std::option::Option::None,
                    })?;
                    __trait.#method_name(#(#param_names),*).await
                }
            }
        })
        .collect();

    let trait_helpers = quote! {
        #[doc(hidden)]
        pub trait #access_trait_ident {
            fn __trait_ref(&self) -> ::std::option::Option<&::std::sync::Arc<#wrapper_name>>;
        }

        #[doc(hidden)]
        #[async_trait::async_trait]
        pub trait #methods_trait_ident: #access_trait_ident {
            #(#methods_impls)*
        }

        #[async_trait::async_trait]
        impl<T> #methods_trait_ident for T where T: #access_trait_ident + ::std::marker::Sync + ::std::marker::Send {}
    };

    let state_traits = quote! {
        #[doc(hidden)]
        pub trait #state_info_ident {
            type State;
        }

        impl #state_info_ident for #trait_ident {
            type State = #state_type;
        }

        #[doc(hidden)]
        pub trait #state_init_ident {
            fn __init_state(
                &self,
                ctx: &#krate::entity::EntityContext,
            ) -> ::std::result::Result<#state_type, #krate::error::ClusterError>;
        }

        impl #state_init_ident for #trait_ident {
            fn __init_state(
                &self,
                ctx: &#krate::entity::EntityContext,
            ) -> ::std::result::Result<#state_type, #krate::error::ClusterError> {
                #init_call
            }
        }
    };

    let wrapper_def = quote! {
        #[doc(hidden)]
        pub struct #wrapper_name {
            /// State with lock-free reads via ArcSwap.
            __state: ::std::sync::Arc<arc_swap::ArcSwap<#state_type>>,
            /// Write lock for state mutations.
            __write_lock: ::std::sync::Arc<tokio::sync::Mutex<()>>,
            /// The trait instance.
            __trait: #trait_ident,
        }

        impl ::std::ops::Deref for #wrapper_name {
            type Target = #trait_ident;
            fn deref(&self) -> &Self::Target {
                &self.__trait
            }
        }

        #view_structs

        #view_impls

        impl #wrapper_name {
            #[doc(hidden)]
            pub fn __new(inner: #trait_ident, state: #state_type) -> Self {
                Self {
                    __state: ::std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(state)),
                    __write_lock: ::std::sync::Arc::new(tokio::sync::Mutex::new(())),
                    __trait: inner,
                }
            }

            #[doc(hidden)]
            pub fn __state_arc(&self) -> &::std::sync::Arc<arc_swap::ArcSwap<#state_type>> {
                &self.__state
            }

            #(#wrapper_methods)*
        }
    };

    // Only output cleaned_impl if it has non-init sync methods
    // Outputting an empty impl block can confuse rust-analyzer's source mapping
    let cleaned_impl_tokens = if cleaned_impl.items.is_empty() {
        quote! {}
    } else {
        quote! { #cleaned_impl }
    };

    Ok(quote! {
        #cleaned_impl_tokens
        #init_method_impl
        #state_traits
        #wrapper_def
        #dispatch_impl
        #client_ext
        #trait_helpers
    })
}

/// Generate code for entities (both stateful and stateless).
///
/// When `state_type` is `Some`, the entity has user-defined state accessed via `self.state`.
/// When `state_type` is `None`, the entity is stateless — `()` is used internally and
/// views Deref to the entity struct so `self.field` keeps working.
///
/// Persistence infrastructure (storage, sharding, journaling, durable builtins) is
/// Generate simplified entity code for "pure RPC" entities.
///
/// Pure-RPC entities have no `#[state]`, no `#[workflow]`, no `#[activity]`,
/// and no `&mut self` methods. The generated Handler is minimal:
/// - `__entity`: the user struct (methods called directly via Deref)
/// - `ctx`: EntityContext
/// - `__sharding`, `__entity_address`: for inter-entity communication
/// - `__message_storage`: for persisted RPCs
///
/// No view structs, no ArcSwap, no write locks, no workflow engine.
#[allow(clippy::too_many_arguments)]
fn generate_pure_rpc_entity(
    krate: &syn::Path,
    struct_name: &syn::Ident,
    handler_name: &syn::Ident,
    client_name: &syn::Ident,
    rpc_group_infos: &[RpcGroupInfo],
    rpcs: &[RpcMethod],
    original_methods: &[syn::ImplItemFn],
) -> syn::Result<proc_macro2::TokenStream> {
    let has_rpc_groups = !rpc_group_infos.is_empty();
    let struct_name_str = struct_name.to_string();

    // --- Entity trait impl (only when no rpc_groups — groups generate their own) ---
    let entity_impl = if has_rpc_groups {
        quote! {}
    } else {
        quote! {
            #[async_trait::async_trait]
            impl #krate::entity::Entity for #struct_name {
                fn entity_type(&self) -> #krate::types::EntityType {
                    self.__entity_type()
                }

                fn shard_group(&self) -> &str {
                    self.__shard_group()
                }

                fn shard_group_for(&self, entity_id: &#krate::types::EntityId) -> &str {
                    self.__shard_group_for(entity_id)
                }

                fn max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.__max_idle_time()
                }

                fn mailbox_capacity(&self) -> ::std::option::Option<usize> {
                    self.__mailbox_capacity()
                }

                fn concurrency(&self) -> ::std::option::Option<usize> {
                    self.__concurrency()
                }

                async fn spawn(
                    &self,
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn #krate::entity::EntityHandler>,
                    #krate::error::ClusterError,
                > {
                    let handler = #handler_name::__new(self.clone(), ctx).await?;
                    ::std::result::Result::Ok(::std::boxed::Box::new(handler))
                }
            }
        }
    };

    // --- Dispatch arms: call methods directly on __entity ---
    let dispatch_arms: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| rpc.is_dispatchable())
        .map(|rpc| {
            let tag = &rpc.tag;
            let method_name = &rpc.name;
            let param_count = rpc.params.len();
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();

            let deserialize_request = match param_count {
                0 => quote! {},
                1 => {
                    let name = &param_names[0];
                    let ty = &param_types[0];
                    quote! {
                        let #name: #ty = rmp_serde::from_slice(payload)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    }
                }
                _ => quote! {
                    let (#(#param_names),*): (#(#param_types),*) = rmp_serde::from_slice(payload)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                },
            };

            let mut call_args: Vec<proc_macro2::TokenStream> = Vec::new();
            for name in &param_names {
                call_args.push(quote! { #name });
            }
            let call_args = quote! { #(#call_args),* };
            let method_call = quote! { self.__entity.#method_name(#call_args).await? };

            quote! {
                #tag => {
                    #deserialize_request
                    let response = { #method_call };
                    rmp_serde::to_vec(&response)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })
                }
            }
        })
        .collect();

    // --- Client methods (reuse existing function) ---
    let client_methods = generate_client_methods(krate, rpcs);

    // --- RPC group infrastructure ---
    let rpc_group_field_defs: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            quote! { #field: #wrapper_path, }
        })
        .collect();

    let rpc_group_params: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let path = &info.path;
            let field = &info.field;
            quote! { #field: #path }
        })
        .collect();

    let rpc_group_field_inits: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            quote! { #field: #wrapper_path::new(#field), }
        })
        .collect();

    let rpc_group_dispatch_checks: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            quote! {
                if let ::std::option::Option::Some(response) = self.#field.__dispatch(tag, payload, headers).await? {
                    return ::std::result::Result::Ok(response);
                }
            }
        })
        .collect();

    let rpc_group_handler_access_impls: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let wrapper_path = &info.wrapper_path;
            let access_trait_path = &info.access_trait_path;
            let field = &info.field;
            quote! {
                impl #access_trait_path for #handler_name {
                    fn __rpc_group_wrapper(&self) -> &#wrapper_path {
                        &self.#field
                    }
                }
            }
        })
        .collect();

    let rpc_group_use_tokens: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let methods_trait_path = &info.methods_trait_path;
            quote! {
                #[allow(unused_imports)]
                use #methods_trait_path as _;
            }
        })
        .collect();

    let dispatch_fallback = if has_rpc_groups {
        quote! {{
            #(#rpc_group_dispatch_checks)*
            ::std::result::Result::Err(
                #krate::error::ClusterError::MalformedMessage {
                    reason: ::std::format!("unknown RPC tag: {tag}"),
                    source: ::std::option::Option::None,
                }
            )
        }}
    } else {
        quote! {{
            ::std::result::Result::Err(
                #krate::error::ClusterError::MalformedMessage {
                    reason: ::std::format!("unknown RPC tag: {tag}"),
                    source: ::std::option::Option::None,
                }
            )
        }}
    };

    // --- Method impls on struct (user's original method bodies) ---
    let method_impls: Vec<proc_macro2::TokenStream> = original_methods
        .iter()
        .map(|m| {
            let attrs: Vec<_> = m
                .attrs
                .iter()
                .filter(|a| {
                    !a.path().is_ident("rpc")
                        && !a.path().is_ident("workflow")
                        && !a.path().is_ident("activity")
                        && !a.path().is_ident("method")
                        && !a.path().is_ident("public")
                        && !a.path().is_ident("protected")
                        && !a.path().is_ident("private")
                })
                .collect();
            let vis = &m.vis;
            let sig = &m.sig;
            let block = &m.block;
            quote! {
                #(#attrs)*
                #vis #sig #block
            }
        })
        .collect();

    // --- Constructor functions ---
    let new_fn = if has_rpc_groups {
        quote! {}
    } else {
        quote! {
            #[doc(hidden)]
            pub async fn __new(entity: #struct_name, ctx: #krate::entity::EntityContext) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                let __sharding = ctx.sharding.clone();
                let __entity_address = ctx.address.clone();
                let __message_storage = ctx.message_storage.clone();
                ::std::result::Result::Ok(Self {
                    __entity: entity,
                    ctx,
                    __sharding,
                    __entity_address,
                    __message_storage,
                })
            }
        }
    };

    let new_with_rpc_groups_fn = if has_rpc_groups {
        quote! {
            #[doc(hidden)]
            pub async fn __new_with_rpc_groups(
                entity: #struct_name,
                #(#rpc_group_params,)*
                ctx: #krate::entity::EntityContext,
            ) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                let __sharding = ctx.sharding.clone();
                let __entity_address = ctx.address.clone();
                let __message_storage = ctx.message_storage.clone();
                ::std::result::Result::Ok(Self {
                    __entity: entity,
                    ctx,
                    __sharding,
                    __entity_address,
                    __message_storage,
                    #(#rpc_group_field_inits)*
                })
            }
        }
    } else {
        quote! {}
    };

    // --- Sharding helper methods (always available) ---
    let sharding_builtin_impls = quote! {
        /// Get the sharding interface for inter-entity communication.
        pub fn sharding(&self) -> ::std::option::Option<&::std::sync::Arc<dyn #krate::sharding::Sharding>> {
            self.__sharding.as_ref()
        }

        /// Get the entity's own address.
        pub fn entity_address(&self) -> &#krate::types::EntityAddress {
            &self.__entity_address
        }

        /// Get the entity ID.
        pub fn entity_id(&self) -> &#krate::types::EntityId {
            &self.__entity_address.entity_id
        }

        /// Create an entity client for this entity type.
        pub fn self_client(&self) -> ::std::option::Option<#krate::entity_client::EntityClient> {
            self.__sharding.as_ref().map(|s| {
                ::std::sync::Arc::clone(s).make_client(self.__entity_address.entity_type.clone())
            })
        }
    };

    // --- WithRpcGroups wrapper (if applicable) ---
    let with_rpc_groups_impl = if has_rpc_groups {
        let with_groups_name = format_ident!("{}WithRpcGroups", struct_name);
        let rpc_group_option_fields: Vec<proc_macro2::TokenStream> = rpc_group_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                quote! { #field: #path, }
            })
            .collect();
        let register_rpc_group_params: Vec<proc_macro2::TokenStream> = rpc_group_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                quote! { #field: #path }
            })
            .collect();
        let register_rpc_group_fields: Vec<_> =
            rpc_group_infos.iter().map(|info| &info.field).collect();
        quote! {
            /// Wrapper struct that bundles an entity with its RPC groups.
            #[doc(hidden)]
            pub struct #with_groups_name {
                pub entity: #struct_name,
                #(pub #rpc_group_option_fields)*
            }

            #[async_trait::async_trait]
            impl #krate::entity::Entity for #with_groups_name
            where
                #struct_name: ::std::clone::Clone,
            {
                fn entity_type(&self) -> #krate::types::EntityType {
                    self.entity.__entity_type()
                }

                fn shard_group(&self) -> &str {
                    self.entity.__shard_group()
                }

                fn shard_group_for(&self, entity_id: &#krate::types::EntityId) -> &str {
                    self.entity.__shard_group_for(entity_id)
                }

                fn max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.entity.__max_idle_time()
                }

                fn mailbox_capacity(&self) -> ::std::option::Option<usize> {
                    self.entity.__mailbox_capacity()
                }

                fn concurrency(&self) -> ::std::option::Option<usize> {
                    self.entity.__concurrency()
                }

                async fn spawn(
                    &self,
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn #krate::entity::EntityHandler>,
                    #krate::error::ClusterError,
                > {
                    let handler = #handler_name::__new_with_rpc_groups(
                        self.entity.clone(),
                        #(self.#register_rpc_group_fields.clone(),)*
                        ctx,
                    )
                    .await?;
                    ::std::result::Result::Ok(::std::boxed::Box::new(handler))
                }
            }

            impl #struct_name {
                /// Register this entity with RPC groups and return a typed client.
                pub async fn register(
                    self,
                    sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>,
                    #(#register_rpc_group_params),*
                ) -> ::std::result::Result<#client_name, #krate::error::ClusterError> {
                    let entity_with_groups = #with_groups_name {
                        entity: self,
                        #(#register_rpc_group_fields,)*
                    };
                    sharding.register_entity(::std::sync::Arc::new(entity_with_groups)).await?;
                    ::std::result::Result::Ok(#client_name::new(sharding))
                }
            }
        }
    } else {
        quote! {}
    };

    // --- Register impl for entities without groups ---
    let register_impl = if has_rpc_groups {
        quote! {} // Already generated in with_rpc_groups_impl
    } else {
        quote! {
            impl #struct_name {
                /// Register this entity with the cluster and return a typed client.
                pub async fn register(
                    self,
                    sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>,
                ) -> ::std::result::Result<#client_name, #krate::error::ClusterError> {
                    sharding.register_entity(::std::sync::Arc::new(self)).await?;
                    ::std::result::Result::Ok(#client_name::new(sharding))
                }
            }
        }
    };

    Ok(quote! {
        #(#rpc_group_use_tokens)*

        // Method implementations directly on the entity struct
        impl #struct_name {
            #(#method_impls)*
        }

        #with_rpc_groups_impl
        #entity_impl

        /// Generated handler for the pure-RPC entity.
        #[doc(hidden)]
        pub struct #handler_name {
            /// Entity instance.
            #[allow(dead_code)]
            __entity: #struct_name,
            /// Entity context.
            #[allow(dead_code)]
            ctx: #krate::entity::EntityContext,
            /// Sharding interface for inter-entity communication.
            __sharding: ::std::option::Option<::std::sync::Arc<dyn #krate::sharding::Sharding>>,
            /// Entity address for self-referencing.
            __entity_address: #krate::types::EntityAddress,
            /// Message storage for persisted RPCs.
            #[allow(dead_code)]
            __message_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::MessageStorage>>,
            #(#rpc_group_field_defs)*
        }

        impl #handler_name {
            #new_fn
            #new_with_rpc_groups_fn

            #sharding_builtin_impls
        }

        #[async_trait::async_trait]
        impl #krate::entity::EntityHandler for #handler_name {
            async fn handle_request(
                &self,
                tag: &str,
                payload: &[u8],
                headers: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<::std::vec::Vec<u8>, #krate::error::ClusterError> {
                #[allow(unused_variables)]
                let headers = headers;
                match tag {
                    #(#dispatch_arms,)*
                    _ => #dispatch_fallback,
                }
            }
        }

        #register_impl

        /// Generated typed client for the entity.
        pub struct #client_name {
            inner: #krate::entity_client::EntityClient,
        }

        impl #client_name {
            /// Create a new typed client from a sharding instance.
            pub fn new(sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>) -> Self {
                Self {
                    inner: #krate::entity_client::EntityClient::new(
                        sharding,
                        #krate::types::EntityType::new(#struct_name_str),
                    ),
                }
            }

            /// Access the underlying untyped [`EntityClient`].
            pub fn inner(&self) -> &#krate::entity_client::EntityClient {
                &self.inner
            }

            #(#client_methods)*
        }

        impl #krate::entity_client::EntityClientAccessor for #client_name {
            fn entity_client(&self) -> &#krate::entity_client::EntityClient {
                &self.inner
            }
        }

        #(#rpc_group_handler_access_impls)*
    })
}

/// always generated regardless of whether the entity has state.
#[allow(clippy::too_many_arguments)]
fn generate_entity(
    krate: &syn::Path,
    struct_name: &syn::Ident,
    handler_name: &syn::Ident,
    client_name: &syn::Ident,
    state_type: Option<&syn::Type>,
    traits: &[syn::Path],
    rpc_groups: &[syn::Path],
    rpcs: &[RpcMethod],
    original_methods: &[syn::ImplItemFn],
) -> syn::Result<proc_macro2::TokenStream> {
    let is_stateful = state_type.is_some();
    // Use () as the internal state type for stateless entities
    let unit_type: syn::Type = syn::parse_quote!(());
    let state_type: &syn::Type = state_type.unwrap_or(&unit_type);
    let trait_infos = trait_infos_from_paths(traits);
    let has_traits = !trait_infos.is_empty();
    let rpc_group_infos = rpc_group_infos_from_paths(rpc_groups);
    let has_rpc_groups = !rpc_group_infos.is_empty();

    // An entity is "pure RPC" when it has no state, no traits, and all methods
    // are plain #[rpc] (no #[workflow], #[activity], or &mut self).
    // Pure-RPC entities use a simplified Handler without state, write locks,
    // view structs, or workflow/activity infrastructure.
    let is_pure_rpc =
        !is_stateful && !has_traits && rpcs.iter().all(|r| matches!(r.kind, RpcKind::Rpc));

    // For pure-RPC entities, delegate to a streamlined code path.
    if is_pure_rpc {
        return generate_pure_rpc_entity(
            krate,
            struct_name,
            handler_name,
            client_name,
            &rpc_group_infos,
            rpcs,
            original_methods,
        );
    }

    let entity_impl = if has_traits || has_rpc_groups {
        quote! {}
    } else {
        quote! {
            #[async_trait::async_trait]
            impl #krate::entity::Entity for #struct_name {
                fn entity_type(&self) -> #krate::types::EntityType {
                    self.__entity_type()
                }

                fn shard_group(&self) -> &str {
                    self.__shard_group()
                }

                fn shard_group_for(&self, entity_id: &#krate::types::EntityId) -> &str {
                    self.__shard_group_for(entity_id)
                }

                fn max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.__max_idle_time()
                }

                fn mailbox_capacity(&self) -> ::std::option::Option<usize> {
                    self.__mailbox_capacity()
                }

                fn concurrency(&self) -> ::std::option::Option<usize> {
                    self.__concurrency()
                }

                async fn spawn(
                    &self,
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn #krate::entity::EntityHandler>,
                    #krate::error::ClusterError,
                > {
                    let handler = #handler_name::__new(self.clone(), ctx).await?;
                    ::std::result::Result::Ok(::std::boxed::Box::new(handler))
                }
            }
        }
    };
    let save_state_code = if is_stateful {
        if has_traits {
            let composite_ref_name = format_ident!("{}CompositeStateRef", struct_name);
            let trait_state_refs: Vec<proc_macro2::TokenStream> = trait_infos
                .iter()
                .map(|info| {
                    let field = &info.field;
                    let missing_reason = &info.missing_reason;
                    quote! {
                        let #field = guard.#field.as_ref().ok_or_else(|| {
                            #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::string::String::from(#missing_reason),
                                source: ::std::option::Option::None,
                            }
                        })?;
                    }
                })
                .collect();
            let composite_field_defs: Vec<proc_macro2::TokenStream> = trait_infos
                .iter()
                .map(|info| {
                    let field = &info.field;
                    quote! { #field: #field.__state() }
                })
                .collect();
            quote! {
                #(#trait_state_refs)*
                let composite = #composite_ref_name {
                    entity: &guard.state,
                    #(#composite_field_defs,)*
                };
                let state_bytes = rmp_serde::to_vec(&composite)
                    .map_err(|e| #krate::error::ClusterError::PersistenceError {
                        reason: ::std::format!("failed to serialize state: {e}"),
                        source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                    })?;
                if let Some(ref storage) = self.__state_storage {
                    storage.save(&self.__state_key, &state_bytes).await.map_err(|e| {
                        tracing::warn!(
                            state_key = %self.__state_key,
                            error = %e,
                            "failed to persist entity state"
                        );
                        e
                    })?;
                }
            }
        } else {
            quote! {
                // Auto-save persisted state after mutation
                if let Some(ref storage) = self.__state_storage {
                    let state_bytes = rmp_serde::to_vec(&guard.state)
                        .map_err(|e| #krate::error::ClusterError::PersistenceError {
                            reason: ::std::format!("failed to serialize state: {e}"),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                    storage.save(&self.__state_key, &state_bytes).await.map_err(|e| {
                        tracing::warn!(
                            state_key = %self.__state_key,
                            error = %e,
                            "failed to persist entity state"
                        );
                        e
                    })?;
                }
            }
        }
    } else {
        quote! {}
    };

    let dispatch_arms = generate_dispatch_arms(
        krate,
        rpcs,
        true,
        Some(&save_state_code),
        has_traits && is_stateful,
    );
    let client_methods = generate_client_methods(krate, rpcs);

    // Generate view structs and method implementations.
    //
    // The pattern is:
    // 1. __ReadView struct has `state: &State` for read-only access
    // 2. __MutView struct has `state: &mut State` for mutable access (activities only)
    // 3. User's method bodies are implemented on the view structs (so self.state works)
    // 4. Wrapper methods on Handler acquire locks and delegate to view methods
    let read_view_name = format_ident!("__{}ReadView", handler_name);
    let mut_view_name = format_ident!("__{}MutView", handler_name);

    // Collect methods by their access pattern
    let mut read_methods: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut mut_methods: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut wrapper_methods: Vec<proc_macro2::TokenStream> = Vec::new();

    for m in original_methods.iter().filter(|m| m.sig.ident != "init") {
        let method_name = &m.sig.ident;
        let block = &m.block;

        // Filter out RPC kind/visibility attributes — consumed by the macro
        let attrs: Vec<_> = m
            .attrs
            .iter()
            .filter(|a| {
                !a.path().is_ident("rpc")
                    && !a.path().is_ident("workflow")
                    && !a.path().is_ident("activity")
                    && !a.path().is_ident("method")
                    && !a.path().is_ident("public")
                    && !a.path().is_ident("protected")
                    && !a.path().is_ident("private")
            })
            .collect();
        let vis = &m.vis;
        let output = &m.sig.output;
        let generics = &m.sig.generics;
        let where_clause = &generics.where_clause;

        // Get params (skip &self or &mut self)
        let params: Vec<_> = m.sig.inputs.iter().skip(1).collect();
        let param_names: Vec<_> = m
            .sig
            .inputs
            .iter()
            .skip(1)
            .filter_map(|arg| {
                if let syn::FnArg::Typed(pat_type) = arg {
                    if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                        return Some(&pat_ident.ident);
                    }
                }
                None
            })
            .collect();

        // Check if this is a mutable method (activity with &mut self)
        let rpc_info = rpcs.iter().find(|rpc| rpc.name == *method_name);
        let is_mut = rpc_info.map(|r| r.is_mut).unwrap_or(false);
        let is_activity = rpc_info
            .map(|r| matches!(r.kind, RpcKind::Activity))
            .unwrap_or(false);
        let is_async = m.sig.asyncness.is_some();

        let async_token = if is_async {
            quote! { async }
        } else {
            quote! {}
        };
        let await_token = if is_async {
            quote! { .await }
        } else {
            quote! {}
        };

        if is_mut {
            // Mutable method: goes on MutView, wrapper acquires write lock
            mut_methods.push(quote! {
                #(#attrs)*
                #vis #async_token fn #method_name #generics (&mut self, #(#params),*) #output #where_clause
                    #block
            });

            // Activities always use ActivityScope for transactional persistence
            // (journal + state in one commit). Non-activity mutable methods use
            // StateMutGuard directly.
            if is_activity {
                wrapper_methods.push(quote! {
                    #(#attrs)*
                    #vis async fn #method_name #generics (&self, #(#params),*) #output #where_clause {
                        let __storage = self.__state_storage.clone()
                            .ok_or_else(|| #krate::error::ClusterError::PersistenceError {
                                reason: "activity requires storage but none configured".to_string(),
                                source: ::std::option::Option::None,
                            })?;
                        let __lock = self.__write_lock.clone().lock_owned().await;
                        // IMPORTANT: The guard must be created and dropped INSIDE the ActivityScope
                        // so that state mutations are properly buffered for transactional persistence.
                        let __arc_swap = self.__state.clone();
                        let __storage_opt = self.__state_storage.clone();
                        let __storage_key = self.__state_key.clone();
                        let __handler = self;
                        #krate::__internal::ActivityScope::run(&__storage, || async move {
                            let mut __guard = #krate::__internal::StateMutGuard::new(
                                __arc_swap,
                                __storage_opt,
                                __storage_key,
                                __lock,
                            );
                            let mut __view = #mut_view_name {
                                __handler,
                                state: &mut *__guard,
                            };
                            __view.#method_name(#(#param_names),*) #await_token
                            // __guard drops here, inside ActivityScope, so writes are buffered
                        }).await
                    }
                });
            } else {
                wrapper_methods.push(quote! {
                    #(#attrs)*
                    #vis async fn #method_name #generics (&self, #(#params),*) #output #where_clause {
                        let __lock = self.__write_lock.clone().lock_owned().await;
                        let mut __guard = #krate::__internal::StateMutGuard::new(
                            self.__state.clone(),
                            self.__state_storage.clone(),
                            self.__state_key.clone(),
                            __lock,
                        );
                        let mut __view = #mut_view_name {
                            __handler: self,
                            state: &mut *__guard,
                        };
                        __view.#method_name(#(#param_names),*) #await_token
                    }
                });
            }
        } else {
            // Read-only method: goes on ReadView, wrapper uses StateRef proxy
            read_methods.push(quote! {
                #(#attrs)*
                #vis #async_token fn #method_name #generics (&mut self, #(#params),*) #output #where_clause
                    #block
            });

            wrapper_methods.push(quote! {
                #(#attrs)*
                #vis #async_token fn #method_name #generics (&self, #(#params),*) #output #where_clause {
                    let mut __view = #read_view_name {
                        __handler: self,
                        state: #krate::__internal::StateRef::__new(self.__state.load_full()),
                    };
                    __view.#method_name(#(#param_names),*) #await_token
                }
            });
        }
    }

    // Generate view struct definitions
    //
    // For stateless entities, views Deref to the entity struct so `self.field` resolves
    // to entity fields. The `state` field is `()` and unused.
    let view_deref_impls = if !is_stateful {
        quote! {
            impl ::std::ops::Deref for #read_view_name<'_> {
                type Target = #struct_name;
                fn deref(&self) -> &Self::Target {
                    &self.__handler.__entity
                }
            }

            impl ::std::ops::Deref for #mut_view_name<'_> {
                type Target = #struct_name;
                fn deref(&self) -> &Self::Target {
                    &self.__handler.__entity
                }
            }
        }
    } else {
        quote! {}
    };

    let view_structs = quote! {
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #read_view_name<'a> {
            #[allow(dead_code)]
            __handler: &'a #handler_name,
            state: #krate::__internal::StateRef<#state_type>,
        }

        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #mut_view_name<'a> {
            #[allow(dead_code)]
            __handler: &'a #handler_name,
            state: &'a mut #state_type,
        }

        #view_deref_impls
    };

    // Generate delegation methods for view structs
    // These let user code call handler methods directly on self instead of self.__handler
    let view_delegation_methods = quote! {
        /// Get the entity ID.
        #[inline]
        fn entity_id(&self) -> &#krate::types::EntityId {
            self.__handler.entity_id()
        }

        /// Get a client to call back into this entity.
        #[inline]
        fn self_client(&self) -> ::std::option::Option<#krate::entity_client::EntityClient> {
            self.__handler.self_client()
        }

        /// Durable sleep that survives entity restarts.
        #[inline]
        async fn sleep(&self, name: &str, duration: ::std::time::Duration) -> ::std::result::Result<(), #krate::error::ClusterError> {
            self.__handler.sleep(name, duration).await
        }

        /// Wait for an external signal to resolve a typed value.
        #[inline]
        async fn await_deferred<T, K>(&self, key: K) -> ::std::result::Result<T, #krate::error::ClusterError>
        where
            T: serde::Serialize + serde::de::DeserializeOwned,
            K: #krate::__internal::DeferredKeyLike<T>,
        {
            self.__handler.await_deferred(key).await
        }

        /// Resolve a deferred value, resuming any entity method waiting on it.
        #[inline]
        async fn resolve_deferred<T, K>(&self, key: K, value: &T) -> ::std::result::Result<(), #krate::error::ClusterError>
        where
            T: serde::Serialize,
            K: #krate::__internal::DeferredKeyLike<T>,
        {
            self.__handler.resolve_deferred(key, value).await
        }

        /// Get the sharding interface for inter-entity communication.
        #[inline]
        fn sharding(&self) -> ::std::option::Option<&::std::sync::Arc<dyn #krate::sharding::Sharding>> {
            self.__handler.sharding()
        }

        /// Get the entity's own address.
        #[inline]
        fn entity_address(&self) -> &#krate::types::EntityAddress {
            self.__handler.entity_address()
        }
    };

    // Generate activity delegation methods for read view (so workflows can call activities)
    //
    // When message_storage is available, activity calls are routed through
    // `DurableContext::run()` for journaling: on first execution the result is
    // cached in MessageStorage; on replay (crash recovery) the cached result
    // is returned without re-executing the activity body.
    //
    // The journal key is derived from the activity arguments (serialized via
    // msgpack), or from a user-provided `key(...)` closure if one was specified
    // on the `#[activity(key(...))]` annotation. This matches the client-side
    // idempotency key derivation.
    let activity_delegations: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| matches!(rpc.kind, RpcKind::Activity))
        .map(|rpc| {
            let method_name = &rpc.name;
            let method_name_str = method_name.to_string();
            let method_info = original_methods
                .iter()
                .find(|m| m.sig.ident == *method_name)
                .unwrap();
            // Collect all params (including DurableContext) for the method signature
            let params: Vec<_> = method_info.sig.inputs.iter().skip(1).collect();
            // Collect all param names (including DurableContext)
            let param_names: Vec<_> = method_info
                .sig
                .inputs
                .iter()
                .skip(1)
                .filter_map(|arg| {
                    if let syn::FnArg::Typed(pat_type) = arg {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some(&pat_ident.ident);
                        }
                    }
                    None
                })
                .collect();
            let output = &method_info.sig.output;
            let generics = &method_info.sig.generics;
            let where_clause = &generics.where_clause;

            // Wire params are those that are NOT DurableContext (i.e., the actual
            // user-visible arguments). These are the same params used by the client
            // for serialization / idempotency key derivation.
            let wire_param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let wire_param_count = wire_param_names.len();

            // Build the key-bytes computation.
            // With an explicit key(...) closure: serialize the closure's output.
            // Without: serialize the wire arguments (same as client-side default).
            let key_bytes_code = if let Some(persist_key) = &rpc.persist_key {
                match wire_param_count {
                    0 => quote! {
                        let __journal_key = (#persist_key)();
                        let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                    1 => {
                        let name = &wire_param_names[0];
                        quote! {
                            let __journal_key = (#persist_key)(#name);
                            let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                                .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                    reason: ::std::format!("failed to serialize journal key: {e}"),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                        }
                    }
                    _ => quote! {
                        let __journal_key = (#persist_key)(#(&#wire_param_names),*);
                        let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                }
            } else {
                // Default: serialize all wire arguments as the key (same as client-side)
                match wire_param_count {
                    0 => quote! {
                        let __journal_key_bytes = rmp_serde::to_vec(&()).unwrap_or_default();
                    },
                    1 => {
                        let name = &wire_param_names[0];
                        quote! {
                            let __journal_key_bytes = rmp_serde::to_vec(&#name)
                                .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                    reason: ::std::format!("failed to serialize journal key: {e}"),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                        }
                    }
                    _ => quote! {
                        let __journal_key_bytes = rmp_serde::to_vec(&(#(&#wire_param_names),*))
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                }
            };

            // Journal wrapping: always wrap activity calls with DurableContext
            // when engine, message_storage, and state_storage are available.
            // The journal result is stored in WorkflowStorage (same transaction
            // as state/journal changes) to ensure atomicity.
            //
            // The journal key includes the workflow execution's request ID (from
            // WorkflowScope) so that activities are scoped per workflow execution.
            // Without this, two different workflow executions calling the same
            // zero-argument activity would collide on the same journal entry.
            let journal_body = quote! {
                if let (
                    ::std::option::Option::Some(__engine),
                    ::std::option::Option::Some(__msg_storage),
                    ::std::option::Option::Some(__wf_storage),
                ) = (
                    self.__handler.__workflow_engine.as_ref(),
                    self.__handler.__message_storage.as_ref(),
                    self.__handler.__state_storage.as_ref(),
                ) {
                    #key_bytes_code
                    // Prepend the workflow execution's request ID to the key bytes
                    // so that the same activity called from different workflow
                    // executions produces different journal keys.
                    let __journal_key_bytes = {
                        let mut __scoped = ::std::vec::Vec::new();
                        if let ::std::option::Option::Some(__wf_id) = #krate::__internal::WorkflowScope::current() {
                            __scoped.extend_from_slice(&__wf_id.to_le_bytes());
                        }
                        __scoped.extend_from_slice(&__journal_key_bytes);
                        __scoped
                    };
                    let __journal_ctx = #krate::__internal::DurableContext::with_journal_storage(
                        ::std::sync::Arc::clone(__engine),
                        self.__handler.ctx.address.entity_type.0.clone(),
                        self.__handler.ctx.address.entity_id.0.clone(),
                        ::std::sync::Arc::clone(__msg_storage),
                        ::std::sync::Arc::clone(__wf_storage),
                    );
                    __journal_ctx.run(#method_name_str, &__journal_key_bytes, || {
                        self.__handler.#method_name(#(#param_names),*)
                    }).await
                } else {
                    // No journal available — execute directly (backward-compatible)
                    self.__handler.#method_name(#(#param_names),*).await
                }
            };

            quote! {
                #[inline]
                async fn #method_name #generics (&mut self, #(#params),*) #output #where_clause {
                    let __result = { #journal_body };
                    // Refresh state from ArcSwap after activity commits (or journal hit)
                    self.state.__refresh(self.__handler.__state.load_full());
                    __result
                }
            }
        })
        .collect();

    // Generate trait access implementations for view structs (so workflows can call trait methods)
    let view_trait_access_impls: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let path = &info.path;
            let ident = &info.ident;
            let wrapper_path = &info.wrapper_path;
            let access_trait_ident = format_ident!("__{}TraitAccess", ident);
            let access_trait_path = replace_last_segment(path, access_trait_ident);
            quote! {
                impl #access_trait_path for #read_view_name<'_> {
                    fn __trait_ref(&self) -> ::std::option::Option<&::std::sync::Arc<#wrapper_path>> {
                        #access_trait_path::__trait_ref(self.__handler)
                    }
                }

                impl #access_trait_path for #mut_view_name<'_> {
                    fn __trait_ref(&self) -> ::std::option::Option<&::std::sync::Arc<#wrapper_path>> {
                        #access_trait_path::__trait_ref(self.__handler)
                    }
                }
            }
        })
        .collect();

    // Generate impl blocks for view structs
    let view_impls = quote! {
        impl #read_view_name<'_> {
            #view_delegation_methods
            #(#activity_delegations)*
            #(#read_methods)*
        }

        impl #mut_view_name<'_> {
            #view_delegation_methods
            #(#mut_methods)*
        }

        #(#view_trait_access_impls)*
    };

    // init goes on the entity struct itself (only for stateful entities)
    let init_method_impl = if is_stateful {
        let init_method = original_methods
            .iter()
            .find(|m| m.sig.ident == "init")
            .expect("stateful entity must have init method");
        let init_sig = &init_method.sig;
        let init_block = &init_method.block;
        let init_attrs = &init_method.attrs;
        let init_vis = &init_method.vis;
        quote! {
            #(#init_attrs)*
            #init_vis #init_sig #init_block
        }
    } else {
        quote! {}
    };

    let struct_name_str = struct_name.to_string();
    let _state_wrapper_name = format_ident!("{}StateWrapper", struct_name);

    // --- RPC Group field definitions and initialization ---
    let rpc_group_field_defs: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            quote! { #field: #wrapper_path, }
        })
        .collect();

    let rpc_group_params: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let path = &info.path;
            let field = &info.field;
            quote! { #field: #path }
        })
        .collect();

    let rpc_group_field_inits: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            quote! { #field: #wrapper_path::new(#field), }
        })
        .collect();

    // Default init (used when no rpc_groups are passed, e.g., for entities without groups)
    // This is unused if has_rpc_groups is false because the fields won't exist.

    // Dispatch checks for RPC groups
    let rpc_group_dispatch_checks: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            quote! {
                if let ::std::option::Option::Some(response) = self.#field.__dispatch(tag, payload, headers).await? {
                    return ::std::result::Result::Ok(response);
                }
            }
        })
        .collect();

    // Access trait impls for the handler (so view structs can access group wrappers)
    let rpc_group_handler_access_impls: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let wrapper_path = &info.wrapper_path;
            let access_trait_path = &info.access_trait_path;
            let field = &info.field;
            quote! {
                impl #access_trait_path for #handler_name {
                    fn __rpc_group_wrapper(&self) -> &#wrapper_path {
                        &self.#field
                    }
                }
            }
        })
        .collect();

    // Access trait impls for view structs (delegate to handler)
    let rpc_group_view_access_impls: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let wrapper_path = &info.wrapper_path;
            let access_trait_path = &info.access_trait_path;
            quote! {
                impl #access_trait_path for #read_view_name<'_> {
                    fn __rpc_group_wrapper(&self) -> &#wrapper_path {
                        #access_trait_path::__rpc_group_wrapper(self.__handler)
                    }
                }

                impl #access_trait_path for #mut_view_name<'_> {
                    fn __rpc_group_wrapper(&self) -> &#wrapper_path {
                        #access_trait_path::__rpc_group_wrapper(self.__handler)
                    }
                }
            }
        })
        .collect();

    // Use statements for RPC group methods traits (bring them into scope)
    let rpc_group_use_tokens: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let methods_trait_path = &info.methods_trait_path;
            quote! {
                #[allow(unused_imports)]
                use #methods_trait_path as _;
            }
        })
        .collect();

    let trait_field_defs: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            quote! { #field: ::std::option::Option<::std::sync::Arc<#wrapper_path>>, }
        })
        .collect();

    let trait_field_init_none: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            quote! { #field: ::std::option::Option::None, }
        })
        .collect();

    let trait_params: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let path = &info.path;
            let ident = &info.ident;
            let param = format_ident!("__trait_{}", to_snake(&ident.to_string()));
            quote! { #param: #path }
        })
        .collect();

    let trait_state_inits: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let path = &info.path;
            let ident = &info.ident;
            let state_init_path = &info.state_init_path;
            let param = format_ident!("__trait_{}", to_snake(&ident.to_string()));
            let state_var = format_ident!("__trait_{}_state", to_snake(&ident.to_string()));
            quote! {
                let #state_var = <#path as #state_init_path>::__init_state(&#param, &ctx)?;
            }
        })
        .collect();

    let trait_field_init_some: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let ident = &info.ident;
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            let param = format_ident!("__trait_{}", to_snake(&ident.to_string()));
            let state_var = format_ident!("__trait_{}_state", to_snake(&ident.to_string()));
            quote! {
                #field: ::std::option::Option::Some(::std::sync::Arc::new(#wrapper_path::__new(
                    #param,
                    #state_var,
                ))),
            }
        })
        .collect();

    let trait_param_idents: Vec<syn::Ident> = trait_infos
        .iter()
        .map(|info| {
            let ident = &info.ident;
            format_ident!("__trait_{}", to_snake(&ident.to_string()))
        })
        .collect();

    let trait_state_vars: Vec<syn::Ident> = trait_infos
        .iter()
        .map(|info| format_ident!("__trait_{}_state", to_snake(&info.ident.to_string())))
        .collect();

    let composite_state_name = format_ident!("{}CompositeState", struct_name);
    let composite_ref_name = format_ident!("{}CompositeStateRef", struct_name);

    let composite_state_defs = if is_stateful && has_traits {
        let composite_fields: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                let state_info_path = &info.state_info_path;
                quote! { #field: <#path as #state_info_path>::State, }
            })
            .collect();
        let composite_ref_fields: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                let state_info_path = &info.state_info_path;
                quote! { #field: &'a <#path as #state_info_path>::State, }
            })
            .collect();
        quote! {
            #[derive(serde::Serialize, serde::Deserialize)]
            struct #composite_state_name {
                entity: #state_type,
                #(#composite_fields)*
            }

            #[derive(serde::Serialize)]
            struct #composite_ref_name<'a> {
                entity: &'a #state_type,
                #(#composite_ref_fields)*
            }
        }
    } else {
        quote! {}
    };

    // Generate state loading code (entity-only)
    // For stateful entities: load from storage or fall back to init()
    // For stateless entities: state is always ()
    let state_init_code = if is_stateful {
        quote! {
            // Try to load persisted state from storage
            let state: #state_type = if let Some(ref storage) = ctx.state_storage {
                let key = ::std::format!(
                    "entity/{}/{}/state",
                    ctx.address.entity_type.0,
                    ctx.address.entity_id.0,
                );
                match storage.load(&key).await {
                    Ok(Some(bytes)) => {
                        rmp_serde::from_slice(&bytes).map_err(|e| {
                            #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to deserialize persisted state: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            }
                        })?
                    }
                    Ok(None) => entity.init(&ctx)?,
                    Err(e) => {
                        tracing::warn!(
                            entity_type = %ctx.address.entity_type.0,
                            entity_id = %ctx.address.entity_id.0,
                            error = %e,
                            "failed to load persisted state, falling back to init"
                        );
                        entity.init(&ctx)?
                    }
                }
            } else {
                entity.init(&ctx)?
            };
        }
    } else {
        // Stateless entity — no init method, state is ()
        quote! {
            let state: () = ();
        }
    };

    let state_init_with_traits_code = if has_traits {
        if is_stateful {
            let composite_fields: Vec<proc_macro2::TokenStream> = trait_infos
                .iter()
                .map(|info| {
                    let field = &info.field;
                    quote! { composite.#field }
                })
                .collect();
            let trait_state_inits = &trait_state_inits;
            quote! {
                let (state, #(#trait_state_vars),*) = if let Some(ref storage) = ctx.state_storage {
                    let key = ::std::format!(
                        "entity/{}/{}/state",
                        ctx.address.entity_type.0,
                        ctx.address.entity_id.0,
                    );
                    match storage.load(&key).await {
                        Ok(Some(bytes)) => {
                            match rmp_serde::from_slice::<#composite_state_name>(&bytes) {
                                Ok(composite) => {
                                    (composite.entity, #(#composite_fields),*)
                                }
                                Err(composite_err) => match rmp_serde::from_slice::<#state_type>(&bytes) {
                                    Ok(state_only) => {
                                        let state = state_only;
                                        #(#trait_state_inits)*
                                        (state, #(#trait_state_vars),*)
                                    }
                                    Err(state_err) => {
                                        return ::std::result::Result::Err(
                                            #krate::error::ClusterError::PersistenceError {
                                                reason: ::std::format!(
                                                    "failed to deserialize persisted state: composite={composite_err}; state={state_err}"
                                                ),
                                                source: ::std::option::Option::Some(::std::boxed::Box::new(state_err)),
                                            }
                                        );
                                    }
                                },
                            }
                        }
                        Ok(None) => {
                            let state = entity.init(&ctx)?;
                            #(#trait_state_inits)*
                            (state, #(#trait_state_vars),*)
                        }
                        Err(e) => {
                            tracing::warn!(
                                entity_type = %ctx.address.entity_type.0,
                                entity_id = %ctx.address.entity_id.0,
                                error = %e,
                                "failed to load persisted state, falling back to init"
                            );
                            let state = entity.init(&ctx)?;
                            #(#trait_state_inits)*
                            (state, #(#trait_state_vars),*)
                        }
                    }
                } else {
                    let state = entity.init(&ctx)?;
                    #(#trait_state_inits)*
                    (state, #(#trait_state_vars),*)
                };
            }
        } else {
            let trait_state_inits = &trait_state_inits;
            if is_stateful {
                quote! {
                    let state: #state_type = entity.init(&ctx)?;
                    #(#trait_state_inits)*
                }
            } else {
                quote! {
                    let state: () = ();
                    #(#trait_state_inits)*
                }
            }
        }
    } else {
        quote! { #state_init_code }
    };

    // Workflow engine, message storage, and workflow storage are always needed for
    // persistence (journaling, durable workflows, sharding).
    let has_durable = true;

    // Handler always has storage fields for persistence (journal, state if applicable)
    let handler_storage_field = quote! {
        __state_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowStorage>>,
        __state_key: ::std::string::String,
    };

    let handler_storage_init = quote! {
        let __state_key = ::std::format!(
            "entity/{}/{}/state",
            ctx.address.entity_type.0,
            ctx.address.entity_id.0,
        );
        let __state_storage = ctx.state_storage.clone();
    };

    let handler_storage_fields_init = quote! {
        __state_storage,
        __state_key,
    };

    let durable_field = if has_durable {
        quote! {
            __workflow_engine: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowEngine>>,
            __message_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::MessageStorage>>,
        }
    } else {
        quote! {}
    };
    let durable_field_init = if has_durable {
        quote! {
            __workflow_engine: ctx.workflow_engine.clone(),
            __message_storage: ctx.message_storage.clone(),
        }
    } else {
        quote! {}
    };

    // Generate built-in durable methods (sleep, await_deferred, resolve_deferred, on_interrupt)
    // These are always generated — persistence is always enabled.
    let durable_builtin_impls = {
        quote! {
            /// Durable sleep that survives entity restarts.
            ///
            /// Schedules a clock that fires after the given duration. The method
            /// suspends until the clock fires. If the entity restarts, the clock
            /// is already scheduled and will still fire.
            pub async fn sleep(&self, name: &str, duration: ::std::time::Duration) -> ::std::result::Result<(), #krate::error::ClusterError> {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "sleep() requires a workflow engine — ensure EntityContext has workflow_engine set".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.sleep(name, duration).await
            }

            /// Wait for an external signal to resolve a typed value.
            ///
            /// Creates a named suspension point. The entity method pauses here
            /// until an external caller resolves the deferred via `resolve_deferred()`.
            pub async fn await_deferred<T, K>(&self, key: K) -> ::std::result::Result<T, #krate::error::ClusterError>
            where
                T: serde::Serialize + serde::de::DeserializeOwned,
                K: #krate::__internal::DeferredKeyLike<T>,
            {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "await_deferred() requires a workflow engine — ensure EntityContext has workflow_engine set".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.await_deferred(key).await
            }

            /// Resolve a deferred value, resuming any entity method waiting on it.
            pub async fn resolve_deferred<T, K>(&self, key: K, value: &T) -> ::std::result::Result<(), #krate::error::ClusterError>
            where
                T: serde::Serialize,
                K: #krate::__internal::DeferredKeyLike<T>,
            {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "resolve_deferred() requires a workflow engine — ensure EntityContext has workflow_engine set".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.resolve_deferred(key, value).await
            }

            /// Wait for an interrupt signal.
            ///
            /// Returns when the interrupt signal is resolved for this entity.
            pub async fn on_interrupt(&self) -> ::std::result::Result<(), #krate::error::ClusterError> {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "on_interrupt() requires a workflow engine — ensure EntityContext has workflow_engine set".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.on_interrupt().await
            }
        }
    };

    // Generate sharding helper methods — always generated.
    let sharding_builtin_impls = {
        quote! {
            /// Get the sharding interface for inter-entity communication.
            ///
            /// Returns `Some` if the entity was configured with sharding.
            /// Use this to send messages to other entities or schedule messages.
            pub fn sharding(&self) -> ::std::option::Option<&::std::sync::Arc<dyn #krate::sharding::Sharding>> {
                self.__sharding.as_ref()
            }

            /// Get the entity's own address.
            ///
            /// Useful for scheduling messages to self via `sharding().notify_at()`.
            pub fn entity_address(&self) -> &#krate::types::EntityAddress {
                &self.__entity_address
            }

            /// Get the entity ID as a string for use with `EntityClient`.
            pub fn entity_id(&self) -> &#krate::types::EntityId {
                &self.__entity_address.entity_id
            }

            /// Create an entity client for this entity type.
            ///
            /// Useful for sending scheduled messages to self.
            pub fn self_client(&self) -> ::std::option::Option<#krate::entity_client::EntityClient> {
                self.__sharding.as_ref().map(|s| {
                    ::std::sync::Arc::clone(s).make_client(self.__entity_address.entity_type.clone())
                })
            }
        }
    };

    // Build the DurableContext wrapper fields — kept for future trait system migration
    let _durable_ctx_wrapper_field = quote! {
        /// Built-in durable context for `sleep()`, `await_deferred()`, `resolve_deferred()`.
        __durable_ctx: ::std::option::Option<#krate::__internal::DurableContext>,
    };

    let _durable_ctx_wrapper_init = {
        quote! {
            let __durable_ctx = ctx.workflow_engine.as_ref().map(|engine| {
                #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    ctx.address.entity_type.0.clone(),
                    ctx.address.entity_id.0.clone(),
                )
            });
        }
    };

    let _durable_ctx_wrapper_field_init = quote! { __durable_ctx, };

    // Sharding context wrapper fields — kept for future trait system migration
    let _sharding_ctx_wrapper_field = quote! {
        /// Sharding interface for inter-entity communication and scheduled messages.
        __sharding: ::std::option::Option<::std::sync::Arc<dyn #krate::sharding::Sharding>>,
        /// Entity address for self-referencing in scheduled messages.
        __entity_address: #krate::types::EntityAddress,
    };

    let _sharding_ctx_wrapper_init = quote! {
        let __sharding = ctx.sharding.clone();
        let __entity_address = ctx.address.clone();
    };

    let _sharding_ctx_wrapper_field_init = quote! { __sharding, __entity_address, };

    // Sharding fields on Handler — always generated
    let sharding_handler_field = quote! {
        /// Sharding interface for inter-entity communication.
        __sharding: ::std::option::Option<::std::sync::Arc<dyn #krate::sharding::Sharding>>,
        /// Entity address for self-referencing.
        __entity_address: #krate::types::EntityAddress,
    };

    // Handler construction is always async (may load state from storage).
    // Skip __new when has_rpc_groups since the fields are non-Optional and
    // construction goes through __new_with_rpc_groups instead.
    let new_fn = if has_rpc_groups {
        quote! {}
    } else {
        quote! {
            #[doc(hidden)]
            pub async fn __new(entity: #struct_name, ctx: #krate::entity::EntityContext) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                #state_init_code
                #handler_storage_init
                let __sharding = ctx.sharding.clone();
                let __entity_address = ctx.address.clone();
                ::std::result::Result::Ok(Self {
                    __state: ::std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(state)),
                    __write_lock: ::std::sync::Arc::new(tokio::sync::Mutex::new(())),
                    __entity: entity,
                    #durable_field_init
                    ctx,
                    #handler_storage_fields_init
                    __sharding,
                    __entity_address,
                    #(#trait_field_init_none)*
                })
            }
        }
    };

    let new_with_traits_fn = if has_traits {
        quote! {
            #[doc(hidden)]
            pub async fn __new_with_traits(
                entity: #struct_name,
                #(#trait_params,)*
                ctx: #krate::entity::EntityContext,
            ) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                #state_init_with_traits_code
                #handler_storage_init
                let __sharding = ctx.sharding.clone();
                let __entity_address = ctx.address.clone();
                ::std::result::Result::Ok(Self {
                    __state: ::std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(state)),
                    __write_lock: ::std::sync::Arc::new(tokio::sync::Mutex::new(())),
                    __entity: entity,
                    #durable_field_init
                    ctx,
                    #handler_storage_fields_init
                    __sharding,
                    __entity_address,
                    #(#trait_field_init_some)*
                })
            }
        }
    } else {
        quote! {}
    };

    let new_with_rpc_groups_fn = if has_rpc_groups {
        quote! {
            #[doc(hidden)]
            pub async fn __new_with_rpc_groups(
                entity: #struct_name,
                #(#rpc_group_params,)*
                ctx: #krate::entity::EntityContext,
            ) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                #state_init_code
                #handler_storage_init
                let __sharding = ctx.sharding.clone();
                let __entity_address = ctx.address.clone();
                ::std::result::Result::Ok(Self {
                    __state: ::std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(state)),
                    __write_lock: ::std::sync::Arc::new(tokio::sync::Mutex::new(())),
                    __entity: entity,
                    #durable_field_init
                    ctx,
                    #handler_storage_fields_init
                    __sharding,
                    __entity_address,
                    #(#trait_field_init_none)*
                    #(#rpc_group_field_inits)*
                })
            }
        }
    } else {
        quote! {}
    };

    // Generate composite state save method for stateful entities with traits
    let save_composite_state_method = if has_traits && is_stateful {
        let composite_ref_name = format_ident!("{}CompositeStateRef", struct_name);
        let trait_state_loads: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                quote! {
                    let #field = self.#field.as_ref().expect("trait field should be set");
                    let #field = &**#field.__state_arc().load();
                }
            })
            .collect();
        let composite_field_refs: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                quote! { #field: #field }
            })
            .collect();
        quote! {
            /// Save composite state (entity + all traits) to storage.
            #[doc(hidden)]
            async fn __save_composite_state(&self) -> ::std::result::Result<(), #krate::error::ClusterError> {
                if let Some(ref storage) = self.__state_storage {
                    let entity_state = &**self.__state.load();
                    #(#trait_state_loads)*
                    let composite = #composite_ref_name {
                        entity: entity_state,
                        #(#composite_field_refs,)*
                    };
                    let bytes = rmp_serde::to_vec(&composite)
                        .map_err(|e| #krate::error::ClusterError::PersistenceError {
                            reason: ::std::format!("failed to serialize composite state: {e}"),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                    storage.save(&self.__state_key, &bytes).await?;
                }
                ::std::result::Result::Ok(())
            }
        }
    } else {
        quote! {}
    };

    let trait_dispatch_checks: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            // Save composite state after successful trait dispatch if entity has state
            let save_after_dispatch = if is_stateful {
                quote! {
                    self.__save_composite_state().await?;
                }
            } else {
                quote! {}
            };
            quote! {
                if let ::std::option::Option::Some(ref __trait) = self.#field {
                    if let ::std::option::Option::Some(response) = __trait.__dispatch(tag, payload, headers).await? {
                        #save_after_dispatch
                        return ::std::result::Result::Ok(response);
                    }
                }
            }
        })
        .collect();

    let trait_dispatch_fallback = if has_traits || has_rpc_groups {
        quote! {{
            #(#rpc_group_dispatch_checks)*
            #(#trait_dispatch_checks)*
            ::std::result::Result::Err(
                #krate::error::ClusterError::MalformedMessage {
                    reason: ::std::format!("unknown RPC tag: {tag}"),
                    source: ::std::option::Option::None,
                }
            )
        }}
    } else {
        quote! {{
            ::std::result::Result::Err(
                #krate::error::ClusterError::MalformedMessage {
                    reason: ::std::format!("unknown RPC tag: {tag}"),
                    source: ::std::option::Option::None,
                }
            )
        }}
    };

    // Generate register method - different signatures depending on whether traits/rpc_groups are used
    let register_rpc_group_params: Vec<proc_macro2::TokenStream> = rpc_group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let path = &info.path;
            quote! { #field: #path }
        })
        .collect();
    let register_rpc_group_fields: Vec<_> =
        rpc_group_infos.iter().map(|info| &info.field).collect();

    let register_impl = if has_traits {
        // With traits: register takes trait dependencies as parameters (rpc_groups not yet combined with traits)
        let register_trait_params: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let param = &info.field;
                let path = &info.path;
                quote! { #param: #path }
            })
            .collect();
        let trait_with_calls: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                quote! { .with(#field) }
            })
            .collect();
        quote! {
            impl #struct_name {
                /// Register this entity with the cluster and return a typed client.
                ///
                /// This is the preferred way to register entities as it returns a typed client
                /// with methods matching the entity's RPC interface.
                pub async fn register(
                    self,
                    sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>,
                    #(#register_trait_params),*
                ) -> ::std::result::Result<#client_name, #krate::error::ClusterError> {
                    let entity_with_traits = self #(#trait_with_calls)*;
                    sharding.register_entity(::std::sync::Arc::new(entity_with_traits)).await?;
                    ::std::result::Result::Ok(#client_name::new(sharding))
                }
            }
        }
    } else if has_rpc_groups {
        // With RPC groups: register takes group instances as parameters
        let with_groups_name_reg = format_ident!("{}WithRpcGroups", struct_name);
        quote! {
            impl #struct_name {
                /// Register this entity with RPC groups and return a typed client.
                ///
                /// Each RPC group parameter provides additional RPC methods that are
                /// callable on this entity. Forget a group — compile error.
                pub async fn register(
                    self,
                    sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>,
                    #(#register_rpc_group_params),*
                ) -> ::std::result::Result<#client_name, #krate::error::ClusterError> {
                    let entity_with_groups = #with_groups_name_reg {
                        entity: self,
                        #(#register_rpc_group_fields,)*
                    };
                    sharding.register_entity(::std::sync::Arc::new(entity_with_groups)).await?;
                    ::std::result::Result::Ok(#client_name::new(sharding))
                }
            }
        }
    } else {
        // Without traits or groups: simple register
        quote! {
            impl #struct_name {
                /// Register this entity with the cluster and return a typed client.
                ///
                /// This is the preferred way to register entities as it returns a typed client
                /// with methods matching the entity's RPC interface.
                pub async fn register(
                    self,
                    sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>,
                ) -> ::std::result::Result<#client_name, #krate::error::ClusterError> {
                    sharding.register_entity(::std::sync::Arc::new(self)).await?;
                    ::std::result::Result::Ok(#client_name::new(sharding))
                }
            }
        }
    };

    let with_traits_name = format_ident!("{}WithTraits", struct_name);
    let with_trait_trait_name = format_ident!("__{}WithTrait", struct_name);
    let trait_use_tokens: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let path = &info.path;
            let ident = &info.ident;
            let methods_trait_ident = format_ident!("__{}TraitMethods", ident);
            let methods_trait_path = replace_last_segment(path, methods_trait_ident);
            quote! {
                #[allow(unused_imports)]
                use #methods_trait_path as _;
            }
        })
        .collect();

    // Generate trait access implementations for the handler
    let trait_access_impls: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let path = &info.path;
            let ident = &info.ident;
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            let access_trait_ident = format_ident!("__{}TraitAccess", ident);
            let access_trait_path = replace_last_segment(path, access_trait_ident);
            quote! {
                impl #access_trait_path for #handler_name {
                    fn __trait_ref(&self) -> ::std::option::Option<&::std::sync::Arc<#wrapper_path>> {
                        self.#field.as_ref()
                    }
                }
            }
        })
        .collect();

    let with_traits_impl = if has_traits {
        let trait_option_fields: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                quote! { #field: ::std::option::Option<#path>, }
            })
            .collect();
        let trait_setters: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let path = &info.path;
                let field = &info.field;
                quote! {
                    impl #with_trait_trait_name<#path> for #with_traits_name {
                        fn __with_trait(&mut self, value: #path) {
                            self.#field = ::std::option::Option::Some(value);
                        }
                    }
                }
            })
            .collect();

        let trait_missing_guards: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let path = &info.path;
                let ident = &info.ident;
                let field = &info.field;
                let missing_reason = &info.missing_reason;
                let param = format_ident!("__trait_{}", to_snake(&ident.to_string()));
                quote! {
                    let #param: #path = self.#field.clone().ok_or_else(|| {
                        #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::string::String::from(#missing_reason),
                            source: ::std::option::Option::None,
                        }
                    })?;
                }
            })
            .collect();

        let trait_bounds: Vec<proc_macro2::TokenStream> = trait_infos
            .iter()
            .map(|info| {
                let path = &info.path;
                quote! { #path: ::std::clone::Clone }
            })
            .collect();

        let trait_field_init_none_tokens = &trait_field_init_none;

        quote! {
            #[doc(hidden)]
            pub struct #with_traits_name {
                entity: #struct_name,
                #(#trait_option_fields)*
            }

            trait #with_trait_trait_name<T> {
                fn __with_trait(&mut self, value: T);
            }

            impl #struct_name {
                pub fn with<T>(self, value: T) -> #with_traits_name
                where
                    #with_traits_name: #with_trait_trait_name<T>,
                {
                    let mut bundle = #with_traits_name {
                        entity: self,
                        #(#trait_field_init_none_tokens)*
                    };
                    bundle.__with_trait(value);
                    bundle
                }
            }

            impl #with_traits_name {
                pub fn with<T>(mut self, value: T) -> Self
                where
                    Self: #with_trait_trait_name<T>,
                {
                    self.__with_trait(value);
                    self
                }
            }

            #(#trait_setters)*

            #[async_trait::async_trait]
            impl #krate::entity::Entity for #with_traits_name
            where
                #struct_name: ::std::clone::Clone,
                #(#trait_bounds,)*
            {
                fn entity_type(&self) -> #krate::types::EntityType {
                    self.entity.__entity_type()
                }

                fn shard_group(&self) -> &str {
                    self.entity.__shard_group()
                }

                fn shard_group_for(&self, entity_id: &#krate::types::EntityId) -> &str {
                    self.entity.__shard_group_for(entity_id)
                }

                fn max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.entity.__max_idle_time()
                }

                fn mailbox_capacity(&self) -> ::std::option::Option<usize> {
                    self.entity.__mailbox_capacity()
                }

                fn concurrency(&self) -> ::std::option::Option<usize> {
                    self.entity.__concurrency()
                }

                async fn spawn(
                    &self,
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn #krate::entity::EntityHandler>,
                    #krate::error::ClusterError,
                > {
                    #(#trait_missing_guards)*
                    let handler = #handler_name::__new_with_traits(
                        self.entity.clone(),
                        #(#trait_param_idents,)*
                        ctx,
                    )
                    .await?;
                    ::std::result::Result::Ok(::std::boxed::Box::new(handler))
                }
            }
        }
    } else {
        quote! {}
    };

    let with_rpc_groups_impl = if has_rpc_groups && !has_traits {
        let with_groups_name_impl = format_ident!("{}WithRpcGroups", struct_name);
        let rpc_group_option_fields: Vec<proc_macro2::TokenStream> = rpc_group_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                quote! { #field: #path, }
            })
            .collect();
        let rpc_group_field_idents: Vec<_> =
            rpc_group_infos.iter().map(|info| &info.field).collect();
        quote! {
            #[doc(hidden)]
            pub struct #with_groups_name_impl {
                entity: #struct_name,
                #(#rpc_group_option_fields)*
            }

            #[async_trait::async_trait]
            impl #krate::entity::Entity for #with_groups_name_impl
            where
                #struct_name: ::std::clone::Clone,
            {
                fn entity_type(&self) -> #krate::types::EntityType {
                    self.entity.__entity_type()
                }

                fn shard_group(&self) -> &str {
                    self.entity.__shard_group()
                }

                fn shard_group_for(&self, entity_id: &#krate::types::EntityId) -> &str {
                    self.entity.__shard_group_for(entity_id)
                }

                fn max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.entity.__max_idle_time()
                }

                fn mailbox_capacity(&self) -> ::std::option::Option<usize> {
                    self.entity.__mailbox_capacity()
                }

                fn concurrency(&self) -> ::std::option::Option<usize> {
                    self.entity.__concurrency()
                }

                async fn spawn(
                    &self,
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn #krate::entity::EntityHandler>,
                    #krate::error::ClusterError,
                > {
                    let handler = #handler_name::__new_with_rpc_groups(
                        self.entity.clone(),
                        #(self.#rpc_group_field_idents.clone(),)*
                        ctx,
                    )
                    .await?;
                    ::std::result::Result::Ok(::std::boxed::Box::new(handler))
                }
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #(#trait_use_tokens)*
        #(#rpc_group_use_tokens)*

        // Emit `init` on the entity struct (stateful entities only).
        impl #struct_name {
            #init_method_impl
        }

        #composite_state_defs

        #with_traits_impl
        #with_rpc_groups_impl
        #entity_impl

        // View structs for state access pattern
        #view_structs

        // Method implementations on view structs
        #view_impls

        /// Generated handler for the stateful entity.
        #[doc(hidden)]
        pub struct #handler_name {
            /// State with lock-free reads via ArcSwap.
            __state: ::std::sync::Arc<arc_swap::ArcSwap<#state_type>>,
            /// Write lock for state mutations.
            __write_lock: ::std::sync::Arc<tokio::sync::Mutex<()>>,
            /// Entity instance.
            #[allow(dead_code)]
            __entity: #struct_name,
            /// Entity context.
            #[allow(dead_code)]
            ctx: #krate::entity::EntityContext,
            #handler_storage_field
            #durable_field
            #sharding_handler_field
            #(#trait_field_defs)*
            #(#rpc_group_field_defs)*
        }

        impl #handler_name {
            #new_fn
            #new_with_traits_fn
            #new_with_rpc_groups_fn

            #save_composite_state_method

            #durable_builtin_impls
            #sharding_builtin_impls

            // Wrapper methods that acquire state and delegate to view methods
            #(#wrapper_methods)*
        }

        #[async_trait::async_trait]
        impl #krate::entity::EntityHandler for #handler_name {
            async fn handle_request(
                &self,
                tag: &str,
                payload: &[u8],
                headers: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<::std::vec::Vec<u8>, #krate::error::ClusterError> {
                #[allow(unused_variables)]
                let headers = headers;
                match tag {
                    #(#dispatch_arms,)*
                    _ => #trait_dispatch_fallback,
                }
            }
        }

        #register_impl

        /// Generated typed client for the entity.
        pub struct #client_name {
            inner: #krate::entity_client::EntityClient,
        }

        impl #client_name {
            /// Create a new typed client from a sharding instance.
            pub fn new(sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>) -> Self {
                Self {
                    inner: #krate::entity_client::EntityClient::new(
                        sharding,
                        #krate::types::EntityType::new(#struct_name_str),
                    ),
                }
            }

            /// Access the underlying untyped [`EntityClient`].
            pub fn inner(&self) -> &#krate::entity_client::EntityClient {
                &self.inner
            }

            #(#client_methods)*
        }

        impl #krate::entity_client::EntityClientAccessor for #client_name {
            fn entity_client(&self) -> &#krate::entity_client::EntityClient {
                &self.inner
            }
        }

        #(#trait_access_impls)*
        #(#rpc_group_handler_access_impls)*
        #(#rpc_group_view_access_impls)*
    })
}

fn generate_dispatch_arms(
    krate: &syn::Path,
    rpcs: &[RpcMethod],
    stateful: bool,
    save_state_code: Option<&proc_macro2::TokenStream>,
    save_composite_state: bool,
) -> Vec<proc_macro2::TokenStream> {
    rpcs
        .iter()
        .filter(|rpc| rpc.is_dispatchable())
        .map(|rpc| {
            let tag = &rpc.tag;
            let method_name = &rpc.name;
            let param_count = rpc.params.len();
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();

            let deserialize_request = match param_count {
                0 => quote! {},
                1 => {
                    let name = &param_names[0];
                    let ty = &param_types[0];
                    quote! {
                        let #name: #ty = rmp_serde::from_slice(payload)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    }
                }
                _ => quote! {
                    let (#(#param_names),*): (#(#param_types),*) = rmp_serde::from_slice(payload)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                },
            };

            // Construct DurableContext if needed
            let durable_ctx_code = if rpc.has_durable_context {
                quote! {
                    let __durable_engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                        #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("method '{}' requires a DurableContext but no workflow engine was provided", #tag),
                            source: ::std::option::Option::None,
                        }
                    })?;
                    let __durable_ctx = #krate::__internal::DurableContext::new(
                        ::std::sync::Arc::clone(__durable_engine),
                        self.ctx.address.entity_type.0.clone(),
                        self.ctx.address.entity_id.0.clone(),
                    );
                }
            } else {
                quote! {}
            };

            let mut call_args = Vec::new();
            if rpc.has_durable_context {
                call_args.push(quote! { &__durable_ctx });
            }
            match param_count {
                0 => {}
                1 => {
                    let name = &param_names[0];
                    call_args.push(quote! { #name });
                }
                _ => {
                    for name in &param_names {
                        call_args.push(quote! { #name });
                    }
                }
            }
            let call_args = quote! { #(#call_args),* };

            // With ArcSwap-based design, state_mut() inside methods handles locking and saving.
            // For entities with traits, we also need to save composite state after method calls.
            let _ = save_state_code; // unused with new design
            let _ = rpc.is_mut; // unused with new design

            let post_call_save = if save_composite_state {
                quote! { self.__save_composite_state().await?; }
            } else {
                quote! {}
            };

            // For workflow methods, wrap the call in a WorkflowScope so that
            // activity journal keys are scoped per workflow execution.
            let is_workflow = matches!(rpc.kind, RpcKind::Workflow);

            // After a workflow completes successfully, mark all journal entries
            // as completed so they become eligible for TTL-based cleanup.
            let mark_journal_completed_code = quote! {
                if let ::std::option::Option::Some(ref __wf_storage) = self.__state_storage {
                    for __key in &__journal_keys {
                        let _ = __wf_storage.mark_completed(__key).await;
                    }
                }
            };

            if stateful {
                // Call method directly on self - method uses state() or state_mut() internally
                let method_call = quote! { self.#method_name(#call_args).await };
                if is_workflow {
                    quote! {
                        #tag => {
                            #deserialize_request
                            #durable_ctx_code
                            let __request_id = headers
                                .get(#krate::__internal::REQUEST_ID_HEADER_KEY)
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0);
                            let (__wf_result, __journal_keys) = #krate::__internal::WorkflowScope::run(__request_id, || async {
                                #method_call
                            }).await;
                            let response = __wf_result?;
                            #post_call_save
                            #mark_journal_completed_code
                            rmp_serde::to_vec(&response)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })
                        }
                    }
                } else {
                    let wrapped_call = quote! { #method_call? };
                    quote! {
                        #tag => {
                            #deserialize_request
                            #durable_ctx_code
                            let response = { #wrapped_call };
                            #post_call_save
                            rmp_serde::to_vec(&response)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })
                        }
                    }
                }
            } else {
                // Stateless — call directly on entity
                let method_call = quote! { self.entity.#method_name(#call_args).await };
                if is_workflow {
                    quote! {
                        #tag => {
                            #deserialize_request
                            #durable_ctx_code
                            let __request_id = headers
                                .get(#krate::__internal::REQUEST_ID_HEADER_KEY)
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0);
                            let (__wf_result, __journal_keys) = #krate::__internal::WorkflowScope::run(__request_id, || async {
                                #method_call
                            }).await;
                            let response = __wf_result?;
                            #mark_journal_completed_code
                            rmp_serde::to_vec(&response)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })
                        }
                    }
                } else {
                    let wrapped_call = quote! { #method_call? };
                    quote! {
                        #tag => {
                            #deserialize_request
                            #durable_ctx_code
                            let response = { #wrapped_call };
                            rmp_serde::to_vec(&response)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })
                        }
                    }
                }
            }
        })
        .collect()
}

fn generate_client_methods(krate: &syn::Path, rpcs: &[RpcMethod]) -> Vec<proc_macro2::TokenStream> {
    rpcs.iter()
        .filter(|rpc| rpc.is_client_visible())
        .map(|rpc| {
            let method_name = &rpc.name;
            let tag = &rpc.tag;
            let resp_type = &rpc.response_type;
            let persist_key = rpc.persist_key.as_ref();
            let param_count = rpc.params.len();
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();
            let param_defs: Vec<_> = param_names
                .iter()
                .zip(param_types.iter())
                .map(|(name, ty)| quote! { #name: &#ty })
                .collect();
            if rpc.uses_persisted_delivery() {
                match (persist_key, param_count) {
                    (Some(persist_key), 0) => quote! {
                        pub async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let key = (#persist_key)();
                            let key_bytes = rmp_serde::to_vec(&key)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!(
                                        "failed to serialize persist key for '{}': {e}",
                                        #tag
                                    ),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                            self.inner
                                .send_persisted_with_key(
                                    entity_id,
                                    #tag,
                                    &(),
                                    ::std::option::Option::Some(key_bytes),
                                    #krate::schema::Uninterruptible::No,
                                )
                                .await
                        }
                    },
                    (Some(persist_key), 1) => {
                        let name = &param_names[0];
                        let def = &param_defs[0];
                        quote! {
                            pub async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                let key = (#persist_key)(#name);
                                let key_bytes = rmp_serde::to_vec(&key)
                                    .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                        reason: ::std::format!(
                                            "failed to serialize persist key for '{}': {e}",
                                            #tag
                                        ),
                                        source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                    })?;
                                self.inner
                                    .send_persisted_with_key(
                                        entity_id,
                                        #tag,
                                        #name,
                                        ::std::option::Option::Some(key_bytes),
                                        #krate::schema::Uninterruptible::No,
                                    )
                                    .await
                            }
                        }
                    }
                    (Some(persist_key), _) => quote! {
                        pub async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let key = (#persist_key)(#(#param_names),*);
                            let key_bytes = rmp_serde::to_vec(&key)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!(
                                        "failed to serialize persist key for '{}': {e}",
                                        #tag
                                    ),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                            let request = (#(#param_names),*);
                            self.inner
                                .send_persisted_with_key(
                                    entity_id,
                                    #tag,
                                    &request,
                                    ::std::option::Option::Some(key_bytes),
                                    #krate::schema::Uninterruptible::No,
                                )
                                .await
                        }
                    },
                    (None, 0) => quote! {
                        pub async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            self.inner
                                .send_persisted(entity_id, #tag, &(), #krate::schema::Uninterruptible::No)
                                .await
                        }
                    },
                    (None, 1) => {
                        let name = &param_names[0];
                        let def = &param_defs[0];
                        quote! {
                            pub async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                self.inner
                                    .send_persisted(entity_id, #tag, #name, #krate::schema::Uninterruptible::No)
                                    .await
                            }
                        }
                    }
                    (None, _) => quote! {
                        pub async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let request = (#(#param_names),*);
                            self.inner
                                .send_persisted(entity_id, #tag, &request, #krate::schema::Uninterruptible::No)
                                .await
                        }
                    },
                }
            } else {
                match param_count {
                    0 => quote! {
                        pub async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            self.inner.send(entity_id, #tag, &()).await
                        }
                    },
                    1 => {
                        let def = &param_defs[0];
                        let name = &param_names[0];
                        quote! {
                            pub async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                self.inner.send(entity_id, #tag, #name).await
                            }
                        }
                    }
                    _ => quote! {
                        pub async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let request = (#(#param_names),*);
                            self.inner.send(entity_id, #tag, &request).await
                        }
                    },
                }
            }
        })
        .collect()
}

fn generate_trait_dispatch_impl(
    krate: &syn::Path,
    trait_ident: &syn::Ident,
    rpcs: &[RpcMethod],
) -> proc_macro2::TokenStream {
    let dispatch_arms: Vec<_> = rpcs
        .iter()
        .filter(|rpc| rpc.is_dispatchable())
        .map(|rpc| {
            let tag = &rpc.tag;
            let method_name = &rpc.name;
            let param_count = rpc.params.len();
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();

            let deserialize_request = match param_count {
                0 => quote! {},
                1 => {
                    let name = &param_names[0];
                    let ty = &param_types[0];
                    quote! {
                        let #name: #ty = rmp_serde::from_slice(payload)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    }
                }
                _ => quote! {
                    let (#(#param_names),*): (#(#param_types),*) = rmp_serde::from_slice(payload)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                },
            };

            let mut call_args = Vec::new();
            match param_count {
                0 => {}
                1 => {
                    let name = &param_names[0];
                    call_args.push(quote! { #name });
                }
                _ => {
                    for name in &param_names {
                        call_args.push(quote! { #name });
                    }
                }
            }
            let call_args = quote! { #(#call_args),* };

            // With ArcSwap pattern, methods handle their own locking via state_mut()
            quote! {
                #tag => {
                    #deserialize_request
                    let response = self.#method_name(#call_args).await?;
                    let bytes = rmp_serde::to_vec(&response)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                    ::std::result::Result::Ok(::std::option::Option::Some(bytes))
                }
            }
        })
        .collect();

    quote! {
        impl #trait_ident {
            #[doc(hidden)]
            pub async fn __dispatch(
                &self,
                tag: &str,
                payload: &[u8],
                headers: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<::std::option::Option<::std::vec::Vec<u8>>, #krate::error::ClusterError> {
                let _ = headers;
                match tag {
                    #(#dispatch_arms,)*
                    _ => ::std::result::Result::Ok(::std::option::Option::None),
                }
            }
        }
    }
}

fn generate_trait_client_ext(
    krate: &syn::Path,
    trait_ident: &syn::Ident,
    rpcs: &[RpcMethod],
) -> proc_macro2::TokenStream {
    let client_methods: Vec<_> = rpcs
        .iter()
        .filter(|rpc| rpc.is_client_visible())
        .map(|rpc| {
            let method_name = &rpc.name;
            let tag = &rpc.tag;
            let resp_type = &rpc.response_type;
            let persist_key = rpc.persist_key.as_ref();
            let param_count = rpc.params.len();
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();
            let param_defs: Vec<_> = param_names
                .iter()
                .zip(param_types.iter())
                .map(|(name, ty)| quote! { #name: &#ty })
                .collect();

            if rpc.uses_persisted_delivery() {
                match (persist_key, param_count) {
                    (Some(persist_key), 0) => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let key = (#persist_key)();
                            let key_bytes = rmp_serde::to_vec(&key)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!(
                                        "failed to serialize persist key for '{}': {e}",
                                        #tag
                                    ),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                            self.entity_client()
                                .send_persisted_with_key(
                                    entity_id,
                                    #tag,
                                    &(),
                                    ::std::option::Option::Some(key_bytes),
                                    #krate::schema::Uninterruptible::No,
                                )
                                .await
                        }
                    },
                    (Some(persist_key), 1) => {
                        let name = &param_names[0];
                        let def = &param_defs[0];
                        quote! {
                            async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                let key = (#persist_key)(#name);
                                let key_bytes = rmp_serde::to_vec(&key)
                                    .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                        reason: ::std::format!(
                                            "failed to serialize persist key for '{}': {e}",
                                            #tag
                                        ),
                                        source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                    })?;
                                self.entity_client()
                                    .send_persisted_with_key(
                                        entity_id,
                                        #tag,
                                        #name,
                                        ::std::option::Option::Some(key_bytes),
                                        #krate::schema::Uninterruptible::No,
                                    )
                                    .await
                            }
                        }
                    }
                    (Some(persist_key), _) => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let key = (#persist_key)(#(#param_names),*);
                            let key_bytes = rmp_serde::to_vec(&key)
                                .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                    reason: ::std::format!(
                                        "failed to serialize persist key for '{}': {e}",
                                        #tag
                                    ),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                            let request = (#(#param_names),*);
                            self.entity_client()
                                .send_persisted_with_key(
                                    entity_id,
                                    #tag,
                                    &request,
                                    ::std::option::Option::Some(key_bytes),
                                    #krate::schema::Uninterruptible::No,
                                )
                                .await
                        }
                    },
                    (None, 0) => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            self.entity_client()
                                .send_persisted(entity_id, #tag, &(), #krate::schema::Uninterruptible::No)
                                .await
                        }
                    },
                    (None, 1) => {
                        let def = &param_defs[0];
                        let name = &param_names[0];
                        quote! {
                            async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                self.entity_client()
                                    .send_persisted(entity_id, #tag, #name, #krate::schema::Uninterruptible::No)
                                    .await
                            }
                        }
                    }
                    (None, _) => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let request = (#(#param_names),*);
                            self.entity_client()
                                .send_persisted(entity_id, #tag, &request, #krate::schema::Uninterruptible::No)
                                .await
                        }
                    },
                }
            } else {
                match param_count {
                    0 => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            self.entity_client().send(entity_id, #tag, &()).await
                        }
                    },
                    1 => {
                        let def = &param_defs[0];
                        let name = &param_names[0];
                        quote! {
                            async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                self.entity_client().send(entity_id, #tag, #name).await
                            }
                        }
                    }
                    _ => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let request = (#(#param_names),*);
                            self.entity_client().send(entity_id, #tag, &request).await
                        }
                    },
                }
            }
        })
        .collect();

    let client_ext_name = format_ident!("{}ClientExt", trait_ident);
    quote! {
        #[async_trait::async_trait]
        pub trait #client_ext_name: #krate::entity_client::EntityClientAccessor {
            #(#client_methods)*
        }

        impl<T> #client_ext_name for T where T: #krate::entity_client::EntityClientAccessor {}
    }
}

/// Check if a type is `DurableContext` or `&DurableContext`.
fn is_durable_context_type(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Reference(r) => is_durable_context_type(&r.elem),
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident == "DurableContext")
            .unwrap_or(false),
        _ => false,
    }
}

struct StateArgs {
    ty: syn::Type,
}

impl syn::parse::Parse for StateArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ty: syn::Type = input.parse()?;

        // State is always persistent - in_memory was removed because:
        // 1. Activities are the only place that can mutate state
        // 2. Activities are journaled and replay on entity rehydration
        // 3. Non-persisted state would be lost on eviction, breaking replay
        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "unexpected tokens in #[state(...)]; state is always persistent",
            ));
        }

        Ok(StateArgs { ty })
    }
}

fn parse_state_attr(attrs: &mut Vec<syn::Attribute>) -> syn::Result<Option<StateArgs>> {
    let mut state_attr: Option<StateArgs> = None;
    let mut i = 0;
    while i < attrs.len() {
        if attrs[i].path().is_ident("state") {
            if state_attr.is_some() {
                return Err(syn::Error::new(
                    attrs[i].span(),
                    "duplicate #[state(...)] attribute",
                ));
            }
            let args = attrs[i].parse_args::<StateArgs>()?;
            state_attr = Some(args);
            attrs.remove(i);
            continue;
        }
        i += 1;
    }
    Ok(state_attr)
}

struct RpcArgs {
    persisted: bool,
}

impl syn::parse::Parse for RpcArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = RpcArgs { persisted: false };
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            match ident.to_string().as_str() {
                "persisted" => {
                    args.persisted = true;
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown rpc attribute: {other}; expected `persisted`"),
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(args)
    }
}

struct KeyArgs {
    key: Option<syn::ExprClosure>,
}

impl syn::parse::Parse for KeyArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(KeyArgs { key: None });
        }

        let ident: syn::Ident = input.parse()?;
        if ident != "key" {
            return Err(syn::Error::new(
                ident.span(),
                "expected `key` in #[workflow(key(...))] or #[activity(key(...))]",
            ));
        }

        if input.peek(syn::Token![=]) {
            input.parse::<syn::Token![=]>()?;
        }

        let expr: syn::Expr = if input.peek(syn::token::Paren) {
            let content;
            syn::parenthesized!(content in input);
            content.parse()?
        } else {
            input.parse()?
        };

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "unexpected tokens in #[workflow(...)] or #[activity(...)]",
            ));
        }

        match expr {
            syn::Expr::Closure(closure) => Ok(KeyArgs { key: Some(closure) }),
            _ => Err(syn::Error::new(
                expr.span(),
                "key must be a closure, e.g. #[workflow(key(|req| ...))]",
            )),
        }
    }
}

/// Parsed arguments for `#[activity(...)]` supporting `key`, `retries`, and `backoff`.
///
/// Supported forms:
/// - `#[activity]` — no arguments
/// - `#[activity(key = |req| ...)]` — custom journal key
/// - `#[activity(retries = 3)]` — retry with default (exponential) backoff
/// - `#[activity(retries = 3, backoff = "exponential")]` — explicit backoff strategy
/// - `#[activity(retries = 3, backoff = "constant")]` — constant backoff
/// - All combinations of the above
struct ActivityAttrArgs {
    key: Option<syn::ExprClosure>,
    retries: Option<u32>,
    backoff: Option<String>,
}

impl syn::parse::Parse for ActivityAttrArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut key = None;
        let mut retries = None;
        let mut backoff = None;

        if input.is_empty() {
            return Ok(ActivityAttrArgs {
                key,
                retries,
                backoff,
            });
        }

        loop {
            if input.is_empty() {
                break;
            }

            let ident: syn::Ident = input.parse()?;
            if ident == "key" {
                if key.is_some() {
                    return Err(syn::Error::new(ident.span(), "duplicate `key` argument"));
                }
                if input.peek(syn::Token![=]) {
                    input.parse::<syn::Token![=]>()?;
                }
                let expr: syn::Expr = if input.peek(syn::token::Paren) {
                    let content;
                    syn::parenthesized!(content in input);
                    content.parse()?
                } else {
                    input.parse()?
                };
                match expr {
                    syn::Expr::Closure(closure) => key = Some(closure),
                    _ => {
                        return Err(syn::Error::new(
                            expr.span(),
                            "key must be a closure, e.g. #[activity(key = |req| ...)]",
                        ))
                    }
                }
            } else if ident == "retries" {
                if retries.is_some() {
                    return Err(syn::Error::new(
                        ident.span(),
                        "duplicate `retries` argument",
                    ));
                }
                input.parse::<syn::Token![=]>()?;
                let lit: syn::LitInt = input.parse()?;
                retries = Some(lit.base10_parse::<u32>()?);
            } else if ident == "backoff" {
                if backoff.is_some() {
                    return Err(syn::Error::new(
                        ident.span(),
                        "duplicate `backoff` argument",
                    ));
                }
                input.parse::<syn::Token![=]>()?;
                let lit: syn::LitStr = input.parse()?;
                let value = lit.value();
                if value != "exponential" && value != "constant" {
                    return Err(syn::Error::new(
                        lit.span(),
                        "backoff must be \"exponential\" or \"constant\"",
                    ));
                }
                backoff = Some(value);
            } else {
                return Err(syn::Error::new(
                    ident.span(),
                    "expected `key`, `retries`, or `backoff` in #[activity(...)]",
                ));
            }

            // Consume optional comma separator
            if input.peek(syn::Token![,]) {
                input.parse::<syn::Token![,]>()?;
            }
        }

        // Validate: backoff without retries is an error
        if backoff.is_some() && retries.is_none() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "`backoff` requires `retries` to be specified",
            ));
        }

        Ok(ActivityAttrArgs {
            key,
            retries,
            backoff,
        })
    }
}

/// Parsed result from `parse_kind_attr`.
struct KindAttrInfo {
    kind: RpcKind,
    key: Option<syn::ExprClosure>,
    rpc_persisted: bool,
    /// Max retry attempts for activities (from `#[activity(retries = N)]`).
    retries: Option<u32>,
    /// Backoff strategy for activity retries: `"exponential"` or `"constant"`.
    backoff: Option<String>,
}

fn parse_kind_attr(attrs: &[syn::Attribute]) -> syn::Result<Option<KindAttrInfo>> {
    let mut kind: Option<RpcKind> = None;
    let mut key: Option<syn::ExprClosure> = None;
    let mut rpc_persisted = false;
    let mut retries: Option<u32> = None;
    let mut backoff: Option<String> = None;

    for attr in attrs {
        if attr.path().is_ident("rpc") {
            if kind.is_some() {
                return Err(syn::Error::new(attr.span(), "multiple RPC kind attributes"));
            }
            match &attr.meta {
                syn::Meta::Path(_) => {
                    kind = Some(RpcKind::Rpc);
                }
                syn::Meta::List(_) => {
                    let args = attr.parse_args::<RpcArgs>()?;
                    kind = Some(RpcKind::Rpc);
                    rpc_persisted = args.persisted;
                }
                _ => {
                    return Err(syn::Error::new(
                        attr.span(),
                        "expected #[rpc] or #[rpc(persisted)]",
                    ))
                }
            }
        }

        if attr.path().is_ident("workflow") {
            if kind.is_some() {
                return Err(syn::Error::new(attr.span(), "multiple RPC kind attributes"));
            }
            let args = match &attr.meta {
                syn::Meta::Path(_) => KeyArgs { key: None },
                syn::Meta::List(_) => attr.parse_args::<KeyArgs>()?,
                syn::Meta::NameValue(_) => {
                    return Err(syn::Error::new(
                        attr.span(),
                        "expected #[workflow] or #[workflow(key(...))]",
                    ))
                }
            };
            kind = Some(RpcKind::Workflow);
            if args.key.is_some() {
                key = args.key;
            }
        }

        if attr.path().is_ident("activity") {
            if kind.is_some() {
                return Err(syn::Error::new(attr.span(), "multiple RPC kind attributes"));
            }
            let args = match &attr.meta {
                syn::Meta::Path(_) => ActivityAttrArgs {
                    key: None,
                    retries: None,
                    backoff: None,
                },
                syn::Meta::List(_) => attr.parse_args::<ActivityAttrArgs>()?,
                syn::Meta::NameValue(_) => {
                    return Err(syn::Error::new(
                        attr.span(),
                        "expected #[activity] or #[activity(...)]",
                    ))
                }
            };
            kind = Some(RpcKind::Activity);
            if args.key.is_some() {
                key = args.key;
            }
            retries = args.retries;
            backoff = args.backoff;
        }

        if attr.path().is_ident("method") {
            if kind.is_some() {
                return Err(syn::Error::new(attr.span(), "multiple RPC kind attributes"));
            }
            match &attr.meta {
                syn::Meta::Path(_) => {
                    kind = Some(RpcKind::Method);
                }
                _ => {
                    return Err(syn::Error::new(
                        attr.span(),
                        "#[method] does not take arguments",
                    ))
                }
            }
        }
    }

    Ok(kind.map(|kind| KindAttrInfo {
        kind,
        key,
        rpc_persisted,
        retries,
        backoff,
    }))
}

fn parse_visibility_attr(attrs: &[syn::Attribute]) -> syn::Result<Option<RpcVisibility>> {
    let mut visibility: Option<RpcVisibility> = None;

    for attr in attrs {
        let next = if attr.path().is_ident("public") {
            Some(RpcVisibility::Public)
        } else if attr.path().is_ident("protected") {
            Some(RpcVisibility::Protected)
        } else if attr.path().is_ident("private") {
            Some(RpcVisibility::Private)
        } else {
            None
        };

        if let Some(next) = next {
            match &attr.meta {
                syn::Meta::Path(_) => {}
                _ => {
                    return Err(syn::Error::new(
                        attr.span(),
                        "visibility attributes do not take arguments",
                    ))
                }
            }
            if visibility.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "multiple visibility modifiers are not allowed",
                ));
            }
            visibility = Some(next);
        }
    }

    Ok(visibility)
}

fn parse_rpc_method(method: &syn::ImplItemFn) -> syn::Result<Option<RpcMethod>> {
    let name = method.sig.ident.clone();
    let tag = name.to_string();

    let kind_info = parse_kind_attr(&method.attrs)?;
    let visibility_attr = parse_visibility_attr(&method.attrs)?;

    let KindAttrInfo {
        kind,
        key: persist_key,
        rpc_persisted,
        retries,
        backoff,
    } = match kind_info {
        Some(info) => info,
        None => {
            if visibility_attr.is_some() {
                return Err(syn::Error::new(
                    method.sig.span(),
                    "visibility modifiers require #[rpc], #[workflow], #[activity], or #[method]",
                ));
            }
            return Ok(None);
        }
    };

    // #[method] can be sync or async; others must be async
    if method.sig.asyncness.is_none() && !matches!(kind, RpcKind::Method) {
        return Err(syn::Error::new(
            method.sig.span(),
            "#[rpc]/#[workflow]/#[activity] can only be applied to async methods",
        ));
    }

    if matches!(kind, RpcKind::Rpc | RpcKind::Method) && persist_key.is_some() {
        return Err(syn::Error::new(
            method.sig.span(),
            "#[rpc] and #[method] do not support key(...) — use #[workflow(key(...))] or #[activity(key(...))]",
        ));
    }

    // #[rpc(persisted)] cannot be combined with #[method] or other kinds
    if rpc_persisted && !matches!(kind, RpcKind::Rpc) {
        return Err(syn::Error::new(
            method.sig.span(),
            "persisted flag is only valid on #[rpc(persisted)]",
        ));
    }

    let visibility = match (kind, visibility_attr) {
        // Activity and Method cannot be public
        (_, Some(RpcVisibility::Public)) if matches!(kind, RpcKind::Activity | RpcKind::Method) => {
            return Err(syn::Error::new(
                method.sig.span(),
                "#[activity] and #[method] cannot be #[public]",
            ))
        }
        (RpcKind::Activity | RpcKind::Method, None) => RpcVisibility::Private,
        (RpcKind::Rpc | RpcKind::Workflow, None) => RpcVisibility::Public,
        (_, Some(vis)) => vis,
    };

    // Detect &mut self vs &self
    let is_mut = method
        .sig
        .inputs
        .first()
        .map(|arg| match arg {
            syn::FnArg::Receiver(r) => r.mutability.is_some(),
            _ => false,
        })
        .unwrap_or(false);

    // Only #[activity] can use &mut self (state mutation)
    if is_mut && !matches!(kind, RpcKind::Activity) {
        return Err(syn::Error::new(
            method.sig.span(),
            "only #[activity] methods can use `&mut self` for state mutation; use `&self` for read-only access",
        ));
    }

    let mut params = Vec::new();
    let mut has_durable_context = false;
    let mut saw_non_ctx_param = false;
    let mut param_index = 0usize;
    for arg in method.sig.inputs.iter().skip(1) {
        match arg {
            syn::FnArg::Typed(pat_type) => {
                if is_durable_context_type(&pat_type.ty) {
                    if has_durable_context {
                        return Err(syn::Error::new(
                            arg.span(),
                            "duplicate DurableContext parameter",
                        ));
                    }
                    if saw_non_ctx_param {
                        return Err(syn::Error::new(
                            arg.span(),
                            "DurableContext must be the first parameter after &self",
                        ));
                    }
                    has_durable_context = true;
                    continue; // DurableContext is framework-provided, not a wire parameter
                }
                saw_non_ctx_param = true;
                let name = match &*pat_type.pat {
                    syn::Pat::Ident(ident) => ident.ident.clone(),
                    syn::Pat::Wild(_) => {
                        let ident = format_ident!("__arg{param_index}");
                        ident
                    }
                    _ => {
                        return Err(syn::Error::new(
                            pat_type.pat.span(),
                            "entity RPC parameters must be simple identifiers",
                        ))
                    }
                };
                param_index += 1;
                params.push(RpcParam {
                    name,
                    ty: (*pat_type.ty).clone(),
                });
            }
            syn::FnArg::Receiver(_) => {}
        }
    }

    // DurableContext requires workflow/activity.
    if has_durable_context && matches!(kind, RpcKind::Rpc | RpcKind::Method) {
        return Err(syn::Error::new(
            method.sig.span(),
            "methods with `&DurableContext` must be marked #[workflow] or #[activity]",
        ));
    }

    // #[method] can have any return type; others must return Result<T, ClusterError>
    let response_type = match &method.sig.output {
        syn::ReturnType::Type(_, ty) => {
            if matches!(kind, RpcKind::Method) {
                // For #[method], use the type as-is (might not be Result)
                (**ty).clone()
            } else {
                extract_result_ok_type(ty)?
            }
        }
        syn::ReturnType::Default => {
            if matches!(kind, RpcKind::Method) {
                // () return type for methods
                syn::parse_quote!(())
            } else {
                return Err(syn::Error::new(
                    method.sig.span(),
                    "entity RPC methods must return Result<T, ClusterError>",
                ));
            }
        }
    };

    // Validate: retries/backoff only valid on activities
    if retries.is_some() && !matches!(kind, RpcKind::Activity) {
        return Err(syn::Error::new(
            method.sig.span(),
            "`retries` is only valid on #[activity(retries = N)]",
        ));
    }

    Ok(Some(RpcMethod {
        name,
        tag,
        params,
        response_type,
        is_mut,
        kind,
        visibility,
        persist_key,
        has_durable_context,
        rpc_persisted,
        retries,
        backoff,
    }))
}

fn to_snake(input: &str) -> String {
    let mut out = String::new();
    let mut prev_is_upper = false;
    let mut prev_is_lower = false;
    let chars: Vec<char> = input.chars().collect();
    for (i, ch) in chars.iter().enumerate() {
        let is_upper = ch.is_uppercase();
        let is_lower = ch.is_lowercase();
        let next_is_lower = chars.get(i + 1).map(|c| c.is_lowercase()).unwrap_or(false);

        if is_upper {
            if prev_is_lower || (prev_is_upper && next_is_lower) {
                out.push('_');
            }
            for lower in ch.to_lowercase() {
                out.push(lower);
            }
        } else if ch.is_alphanumeric() || *ch == '_' {
            out.push(*ch);
        }

        prev_is_upper = is_upper;
        prev_is_lower = is_lower;
    }
    out
}

fn extract_result_ok_type(ty: &syn::Type) -> syn::Result<syn::Type> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Result" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(ok_type)) = args.args.first() {
                        return Ok(ok_type.clone());
                    }
                }
            }
        }
    }
    Err(syn::Error::new(
        ty.span(),
        "expected Result<T, ClusterError> return type",
    ))
}

// =============================================================================
// Activity Group Macros — argument parsing and codegen
// =============================================================================

struct ActivityGroupImplArgs {
    krate: Option<syn::Path>,
}

impl syn::parse::Parse for ActivityGroupImplArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = ActivityGroupImplArgs { krate: None };
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            match ident.to_string().as_str() {
                "krate" => {
                    input.parse::<syn::Token![=]>()?;
                    let lit: syn::LitStr = input.parse()?;
                    args.krate = Some(lit.parse()?);
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown activity_group_impl attribute: {other}"),
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(args)
    }
}

/// Info about a parsed activity within an activity group.
struct ActivityGroupActivityInfo {
    name: syn::Ident,
    params: Vec<RpcParam>,
    #[allow(dead_code)]
    response_type: syn::Type,
    persist_key: Option<syn::ExprClosure>,
    original_method: syn::ImplItemFn,
    /// Max retry attempts (from `#[activity(retries = N)]`).
    retries: Option<u32>,
    /// Backoff strategy: `"exponential"` (default) or `"constant"`.
    backoff: Option<String>,
}

/// Generates the codegen for `#[activity_group_impl]`.
///
/// For an activity group `Payments`, this generates:
/// - `__PaymentsActivityGroupWrapper` — owns group + DurableContext refs, has delegation methods
/// - `__PaymentsActivityGroupView` — `Deref<Target = Payments>` for activity body execution
/// - `__PaymentsActivityGroupAccess` trait — `fn __payments_wrapper(&self) -> &Wrapper`
/// - `__PaymentsActivityGroupMethods` trait — blanket impl that delegates to wrapper
fn activity_group_impl_inner(
    args: ActivityGroupImplArgs,
    input: syn::ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let krate = args.krate.unwrap_or_else(default_crate_path);
    let self_ty = &input.self_ty;

    let struct_name = match self_ty.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.clone())
            .ok_or_else(|| syn::Error::new(self_ty.span(), "expected struct name"))?,
        _ => return Err(syn::Error::new(self_ty.span(), "expected struct name")),
    };

    let wrapper_name = format_ident!("__{}ActivityGroupWrapper", struct_name);
    let access_trait_name = format_ident!("__{}ActivityGroupAccess", struct_name);
    let methods_trait_name = format_ident!("__{}ActivityGroupMethods", struct_name);
    let activity_view_name = format_ident!("__{}ActivityGroupView", struct_name);

    // Validate: no #[state] on the impl
    for attr in &input.attrs {
        if attr.path().is_ident("state") {
            return Err(syn::Error::new(
                attr.span(),
                "activity groups are stateless; remove #[state(...)]",
            ));
        }
    }

    // Parse methods: find activities and helpers
    let mut activities: Vec<ActivityGroupActivityInfo> = Vec::new();
    let mut all_methods: Vec<syn::ImplItemFn> = Vec::new();

    for item in &input.items {
        if let syn::ImplItem::Fn(method) = item {
            // Validate: no forbidden annotations
            for attr in &method.attrs {
                if attr.path().is_ident("state") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "activity groups are stateless; remove #[state(...)]",
                    ));
                }
                if attr.path().is_ident("rpc") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "activity groups use #[activity], not #[rpc]",
                    ));
                }
                if attr.path().is_ident("workflow") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "activity groups use #[activity], not #[workflow]",
                    ));
                }
            }

            // Validate: no &mut self
            if let Some(syn::FnArg::Receiver(r)) = method.sig.inputs.first() {
                if r.mutability.is_some() {
                    return Err(syn::Error::new(
                        r.span(),
                        "activity group methods must use &self, not &mut self",
                    ));
                }
            }

            let is_activity = method.attrs.iter().any(|a| a.path().is_ident("activity"));

            if is_activity {
                if method.sig.asyncness.is_none() {
                    return Err(syn::Error::new(
                        method.sig.span(),
                        "#[activity] methods must be async",
                    ));
                }

                // Parse activity attributes (key, retries, backoff)
                let (persist_key, act_retries, act_backoff) = {
                    let mut key = None;
                    let mut retries = None;
                    let mut backoff = None;
                    for attr in &method.attrs {
                        if attr.path().is_ident("activity") {
                            let args = match &attr.meta {
                                syn::Meta::Path(_) => ActivityAttrArgs {
                                    key: None,
                                    retries: None,
                                    backoff: None,
                                },
                                syn::Meta::List(_) => attr.parse_args::<ActivityAttrArgs>()?,
                                _ => {
                                    return Err(syn::Error::new(
                                        attr.span(),
                                        "expected #[activity] or #[activity(...)]",
                                    ))
                                }
                            };
                            key = args.key;
                            retries = args.retries;
                            backoff = args.backoff;
                        }
                    }
                    (key, retries, backoff)
                };

                // Parse params (skip &self)
                let mut params = Vec::new();
                for arg in method.sig.inputs.iter().skip(1) {
                    if let syn::FnArg::Typed(pat_type) = arg {
                        let name = match &*pat_type.pat {
                            syn::Pat::Ident(ident) => ident.ident.clone(),
                            _ => {
                                return Err(syn::Error::new(
                                    pat_type.pat.span(),
                                    "activity parameters must be simple identifiers",
                                ))
                            }
                        };
                        params.push(RpcParam {
                            name,
                            ty: (*pat_type.ty).clone(),
                        });
                    }
                }

                let response_type = extract_result_ok_type(match &method.sig.output {
                    syn::ReturnType::Type(_, ty) => ty,
                    syn::ReturnType::Default => {
                        return Err(syn::Error::new(
                            method.sig.span(),
                            "#[activity] must return Result<T, ClusterError>",
                        ))
                    }
                })?;

                activities.push(ActivityGroupActivityInfo {
                    name: method.sig.ident.clone(),
                    params,
                    response_type,
                    persist_key,
                    original_method: method.clone(),
                    retries: act_retries,
                    backoff: act_backoff,
                });
            }

            all_methods.push(method.clone());
        }
    }

    // --- Generate activity view struct ---
    // This view is used when executing the actual activity body.
    // It Derefs to the group struct so `self.stripe`, etc. work.
    let mut activity_view_methods = Vec::new();
    let mut helper_view_methods = Vec::new();

    for method in &all_methods {
        let is_activity = method.attrs.iter().any(|a| a.path().is_ident("activity"));
        let block = &method.block;
        let output = &method.sig.output;
        let name = &method.sig.ident;
        let params: Vec<_> = method.sig.inputs.iter().skip(1).collect();
        let attrs: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| {
                !a.path().is_ident("activity")
                    && !a.path().is_ident("public")
                    && !a.path().is_ident("protected")
                    && !a.path().is_ident("private")
            })
            .collect();
        let vis = &method.vis;

        if is_activity {
            activity_view_methods.push(quote! {
                #(#attrs)*
                #vis async fn #name(&self, #(#params),*) #output
                    #block
            });
        } else {
            let async_token = if method.sig.asyncness.is_some() {
                quote! { async }
            } else {
                quote! {}
            };
            helper_view_methods.push(quote! {
                #(#attrs)*
                #vis #async_token fn #name(&self, #(#params),*) #output
                    #block
            });
        }
    }

    let view_struct = quote! {
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        pub struct #activity_view_name<'a> {
            __group: &'a #struct_name,
        }

        impl ::std::ops::Deref for #activity_view_name<'_> {
            type Target = #struct_name;
            fn deref(&self) -> &Self::Target {
                self.__group
            }
        }

        impl #activity_view_name<'_> {
            #(#activity_view_methods)*
            #(#helper_view_methods)*
        }
    };

    // --- Generate wrapper struct ---
    // The wrapper owns the group instance + DurableContext references.
    // It provides delegation methods that wrap each activity in DurableContext::run().

    let wrapper_delegation_methods: Vec<proc_macro2::TokenStream> = activities
        .iter()
        .map(|act| {
            let method_name = &act.name;
            let method_name_str = method_name.to_string();
            let method_info = &act.original_method;
            let params: Vec<_> = method_info.sig.inputs.iter().skip(1).collect();
            let param_names: Vec<_> = method_info
                .sig
                .inputs
                .iter()
                .skip(1)
                .filter_map(|arg| {
                    if let syn::FnArg::Typed(pat_type) = arg {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some(&pat_ident.ident);
                        }
                    }
                    None
                })
                .collect();
            let output = &method_info.sig.output;

            let wire_param_names: Vec<_> = act.params.iter().map(|p| &p.name).collect();
            let wire_param_count = wire_param_names.len();

            // Build key-bytes computation (same as inline activities)
            let key_bytes_code = if let Some(persist_key) = &act.persist_key {
                match wire_param_count {
                    0 => quote! {
                        let __journal_key = (#persist_key)();
                        let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                    1 => {
                        let name = &wire_param_names[0];
                        quote! {
                            let __journal_key = (#persist_key)(#name);
                            let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                                .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                    reason: ::std::format!("failed to serialize journal key: {e}"),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                        }
                    }
                    _ => quote! {
                        let __journal_key = (#persist_key)(#(&#wire_param_names),*);
                        let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                }
            } else {
                match wire_param_count {
                    0 => quote! {
                        let __journal_key_bytes = rmp_serde::to_vec(&()).unwrap_or_default();
                    },
                    1 => {
                        let name = &wire_param_names[0];
                        quote! {
                            let __journal_key_bytes = rmp_serde::to_vec(&#name)
                                .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                    reason: ::std::format!("failed to serialize journal key: {e}"),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                        }
                    }
                    _ => quote! {
                        let __journal_key_bytes = rmp_serde::to_vec(&(#(&#wire_param_names),*))
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                }
            };

            // Generate the journal body — either single-shot or retry loop
            let max_retries = act.retries.unwrap_or(0);
            let journal_body = if max_retries == 0 {
                // No retry — single shot
                quote! {
                    let __journal_ctx = #krate::__internal::DurableContext::with_journal_storage(
                        ::std::sync::Arc::clone(__engine),
                        self.__entity_type.clone(),
                        self.__entity_id.clone(),
                        ::std::sync::Arc::clone(__msg_storage),
                        ::std::sync::Arc::clone(__wf_storage),
                    );
                    let __activity_view = #activity_view_name {
                        __group: &self.__group,
                    };
                    __journal_ctx.run(#method_name_str, &__journal_key_bytes, || {
                        __activity_view.#method_name(#(#param_names),*)
                    }).await
                }
            } else {
                let backoff_str = act.backoff.as_deref().unwrap_or("exponential");
                // Clone params at start of each iteration so owned types survive across retries
                let param_clones: Vec<proc_macro2::TokenStream> = param_names
                    .iter()
                    .map(|name| {
                        let clone_name = format_ident!("__{}_clone", name);
                        quote! { let #clone_name = #name.clone(); }
                    })
                    .collect();
                let cloned_param_names: Vec<syn::Ident> = param_names
                    .iter()
                    .map(|name| format_ident!("__{}_clone", name))
                    .collect();
                quote! {
                    let mut __attempt = 0u32;
                    loop {
                        // Clone params for this attempt (owned types may not be Copy)
                        #(#param_clones)*
                        let __retry_key_bytes = {
                            let mut __k = __journal_key_bytes.clone();
                            __k.extend_from_slice(&__attempt.to_le_bytes());
                            __k
                        };
                        let __journal_ctx = #krate::__internal::DurableContext::with_journal_storage(
                            ::std::sync::Arc::clone(__engine),
                            self.__entity_type.clone(),
                            self.__entity_id.clone(),
                            ::std::sync::Arc::clone(__msg_storage),
                            ::std::sync::Arc::clone(__wf_storage),
                        );
                        let __activity_view = #activity_view_name {
                            __group: &self.__group,
                        };
                        match __journal_ctx.run(#method_name_str, &__retry_key_bytes, || {
                            __activity_view.#method_name(#(#cloned_param_names),*)
                        }).await {
                            ::std::result::Result::Ok(__val) => {
                                break ::std::result::Result::Ok(__val);
                            }
                            ::std::result::Result::Err(__e) if __attempt < #max_retries => {
                                let __delay = #krate::__internal::compute_retry_backoff(
                                    __attempt, #backoff_str, 1,
                                );
                                let __sleep_name = ::std::format!(
                                    "{}/retry/{}", #method_name_str, __attempt
                                );
                                __engine.sleep(
                                    &self.__entity_type,
                                    &self.__entity_id,
                                    &__sleep_name,
                                    __delay,
                                ).await?;
                                __attempt += 1;
                            }
                            ::std::result::Result::Err(__e) => {
                                break ::std::result::Result::Err(__e);
                            }
                        }
                    }
                }
            };

            quote! {
                pub async fn #method_name(&self, #(#params),*) #output {
                    if let (
                        ::std::option::Option::Some(__engine),
                        ::std::option::Option::Some(__msg_storage),
                        ::std::option::Option::Some(__wf_storage),
                    ) = (
                        self.__workflow_engine.as_ref(),
                        self.__message_storage.as_ref(),
                        self.__workflow_storage.as_ref(),
                    ) {
                        #key_bytes_code
                        // Scope the journal key per workflow execution
                        let __journal_key_bytes = {
                            let mut __scoped = ::std::vec::Vec::new();
                            if let ::std::option::Option::Some(__wf_id) = #krate::__internal::WorkflowScope::current() {
                                __scoped.extend_from_slice(&__wf_id.to_le_bytes());
                            }
                            __scoped.extend_from_slice(&__journal_key_bytes);
                            __scoped
                        };
                        #journal_body
                    } else {
                        // No journal — execute directly
                        let __activity_view = #activity_view_name {
                            __group: &self.__group,
                        };
                        __activity_view.#method_name(#(#param_names),*).await
                    }
                }
            }
        })
        .collect();

    let wrapper_struct = quote! {
        /// Generated wrapper for the activity group.
        ///
        /// Owns the group instance and DurableContext references.
        /// Provides delegation methods that wrap each activity in journal tracking.
        #[doc(hidden)]
        pub struct #wrapper_name {
            __group: #struct_name,
            __workflow_engine: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowEngine>>,
            __message_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::MessageStorage>>,
            __workflow_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowStorage>>,
            __entity_type: ::std::string::String,
            __entity_id: ::std::string::String,
        }

        impl #wrapper_name {
            /// Create a new wrapper with DurableContext references.
            pub fn new(
                group: #struct_name,
                workflow_engine: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowEngine>>,
                message_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::MessageStorage>>,
                workflow_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowStorage>>,
                entity_type: ::std::string::String,
                entity_id: ::std::string::String,
            ) -> Self {
                Self {
                    __group: group,
                    __workflow_engine: workflow_engine,
                    __message_storage: message_storage,
                    __workflow_storage: workflow_storage,
                    __entity_type: entity_type,
                    __entity_id: entity_id,
                }
            }

            /// Get a reference to the group instance.
            pub fn group(&self) -> &#struct_name {
                &self.__group
            }

            // --- Activity delegation methods (journaled) ---
            #(#wrapper_delegation_methods)*
        }
    };

    // --- Generate access trait ---
    let access_trait = quote! {
        #[doc(hidden)]
        pub trait #access_trait_name {
            fn __activity_group_wrapper(&self) -> &#wrapper_name;
        }
    };

    // --- Generate methods trait with blanket impl ---
    // Blanket impl for any type that implements the access trait.
    // Each method delegates to the wrapper's corresponding method.
    let blanket_methods: Vec<proc_macro2::TokenStream> = activities
        .iter()
        .map(|act| {
            let method_name = &act.name;
            let method_info = &act.original_method;
            let params: Vec<_> = method_info.sig.inputs.iter().skip(1).collect();
            let param_names: Vec<_> = method_info
                .sig
                .inputs
                .iter()
                .skip(1)
                .filter_map(|arg| {
                    if let syn::FnArg::Typed(pat_type) = arg {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some(&pat_ident.ident);
                        }
                    }
                    None
                })
                .collect();
            let output = &method_info.sig.output;

            quote! {
                async fn #method_name(&self, #(#params),*) #output {
                    self.__activity_group_wrapper().#method_name(#(#param_names),*).await
                }
            }
        })
        .collect();

    let methods_trait = quote! {
        /// Trait providing activity group methods via blanket implementation.
        ///
        /// Automatically implemented for any type that implements the access trait.
        #[doc(hidden)]
        #[allow(async_fn_in_trait)]
        pub trait #methods_trait_name: #access_trait_name {
            #(#blanket_methods)*
        }

        // Blanket impl: any type that provides the access trait gets the methods.
        impl<T: #access_trait_name> #methods_trait_name for T {}
    };

    Ok(quote! {
        #view_struct
        #wrapper_struct
        #access_trait
        #methods_trait
    })
}

// =============================================================================
// RPC Group Macros — argument parsing and codegen
// =============================================================================

struct RpcGroupImplArgs {
    krate: Option<syn::Path>,
}

impl syn::parse::Parse for RpcGroupImplArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = RpcGroupImplArgs { krate: None };
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            match ident.to_string().as_str() {
                "krate" => {
                    input.parse::<syn::Token![=]>()?;
                    let lit: syn::LitStr = input.parse()?;
                    args.krate = Some(lit.parse()?);
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown rpc_group_impl attribute: {other}"),
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(args)
    }
}

/// Generates the codegen for `#[rpc_group_impl]`.
///
/// For an RPC group `HealthCheck`, this generates:
/// - `__HealthCheckRpcGroupWrapper` — owns group instance, has delegation methods
/// - `__HealthCheckRpcGroupView` — `Deref<Target = HealthCheck>` for RPC body execution
/// - `__HealthCheckRpcGroupAccess` trait — `fn __rpc_group_wrapper(&self) -> &Wrapper`
/// - `__HealthCheckRpcGroupMethods` trait — blanket impl that delegates to wrapper
/// - `HealthCheckClientExt` trait — adds group RPC methods to the parent entity's client
/// - `__dispatch` method on wrapper — handles RPC dispatch
fn rpc_group_impl_inner(
    args: RpcGroupImplArgs,
    input: syn::ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let krate = args.krate.unwrap_or_else(default_crate_path);
    let self_ty = &input.self_ty;

    let struct_name = match self_ty.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.clone())
            .ok_or_else(|| syn::Error::new(self_ty.span(), "expected struct name"))?,
        _ => return Err(syn::Error::new(self_ty.span(), "expected struct name")),
    };

    let wrapper_name = format_ident!("__{}RpcGroupWrapper", struct_name);
    let access_trait_name = format_ident!("__{}RpcGroupAccess", struct_name);
    let methods_trait_name = format_ident!("__{}RpcGroupMethods", struct_name);
    let rpc_view_name = format_ident!("__{}RpcGroupView", struct_name);
    let client_ext_name = format_ident!("{}ClientExt", struct_name);

    // Validate: no #[state] on the impl
    for attr in &input.attrs {
        if attr.path().is_ident("state") {
            return Err(syn::Error::new(
                attr.span(),
                "RPC groups are stateless; remove #[state(...)]",
            ));
        }
    }

    // Parse methods: find RPCs and helpers
    let mut rpcs: Vec<RpcMethod> = Vec::new();
    let mut all_methods: Vec<syn::ImplItemFn> = Vec::new();

    for item in &input.items {
        if let syn::ImplItem::Fn(method) = item {
            // Validate: no forbidden annotations
            for attr in &method.attrs {
                if attr.path().is_ident("state") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "RPC groups are stateless; remove #[state(...)]",
                    ));
                }
                if attr.path().is_ident("activity") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "RPC groups use #[rpc], not #[activity]",
                    ));
                }
                if attr.path().is_ident("workflow") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "RPC groups use #[rpc], not #[workflow]",
                    ));
                }
            }

            // Validate: no &mut self
            if let Some(syn::FnArg::Receiver(r)) = method.sig.inputs.first() {
                if r.mutability.is_some() {
                    return Err(syn::Error::new(
                        r.span(),
                        "RPC group methods must use &self, not &mut self",
                    ));
                }
            }

            let is_rpc = method.attrs.iter().any(|a| a.path().is_ident("rpc"));

            if is_rpc {
                if method.sig.asyncness.is_none() {
                    return Err(syn::Error::new(
                        method.sig.span(),
                        "#[rpc] methods must be async",
                    ));
                }

                // Parse the RPC method to get full info
                if let Some(rpc) = parse_rpc_method(method)? {
                    rpcs.push(rpc);
                }
            }

            all_methods.push(method.clone());
        }
    }

    // --- Generate RPC view struct ---
    // This view is used when executing the actual RPC body.
    // It Derefs to the group struct so `self.field` works.
    let mut rpc_view_methods = Vec::new();
    let mut helper_view_methods = Vec::new();

    for method in &all_methods {
        let is_rpc = method.attrs.iter().any(|a| a.path().is_ident("rpc"));
        let block = &method.block;
        let output = &method.sig.output;
        let name = &method.sig.ident;
        let params: Vec<_> = method.sig.inputs.iter().skip(1).collect();
        let attrs: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| {
                !a.path().is_ident("rpc")
                    && !a.path().is_ident("public")
                    && !a.path().is_ident("protected")
                    && !a.path().is_ident("private")
            })
            .collect();
        let vis = &method.vis;

        if is_rpc {
            rpc_view_methods.push(quote! {
                #(#attrs)*
                #vis async fn #name(&self, #(#params),*) #output
                    #block
            });
        } else {
            let async_token = if method.sig.asyncness.is_some() {
                quote! { async }
            } else {
                quote! {}
            };
            helper_view_methods.push(quote! {
                #(#attrs)*
                #vis #async_token fn #name(&self, #(#params),*) #output
                    #block
            });
        }
    }

    let view_struct = quote! {
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        pub struct #rpc_view_name<'a> {
            __group: &'a #struct_name,
        }

        impl ::std::ops::Deref for #rpc_view_name<'_> {
            type Target = #struct_name;
            fn deref(&self) -> &Self::Target {
                self.__group
            }
        }

        impl #rpc_view_name<'_> {
            #(#rpc_view_methods)*
            #(#helper_view_methods)*
        }
    };

    // --- Generate wrapper struct ---
    // The wrapper owns the group instance and delegates dispatch to view methods.
    let wrapper_delegation_methods: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| rpc.is_trait_visible())
        .map(|rpc| {
            let method_name = &rpc.name;
            let resp_type = &rpc.response_type;
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();
            let param_defs: Vec<_> = param_names
                .iter()
                .zip(param_types.iter())
                .map(|(name, ty)| quote! { #name: #ty })
                .collect();
            quote! {
                pub async fn #method_name(
                    &self,
                    #(#param_defs),*
                ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                    let __view = #rpc_view_name { __group: &self.__group };
                    __view.#method_name(#(#param_names),*).await
                }
            }
        })
        .collect();

    // --- Generate __dispatch ---
    let dispatch_arms: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| rpc.is_dispatchable())
        .map(|rpc| {
            let tag = &rpc.tag;
            let method_name = &rpc.name;
            let param_count = rpc.params.len();
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();

            let deserialize_request = match param_count {
                0 => quote! {},
                1 => {
                    let name = &param_names[0];
                    let ty = &param_types[0];
                    quote! {
                        let #name: #ty = rmp_serde::from_slice(payload)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    }
                }
                _ => quote! {
                    let (#(#param_names),*): (#(#param_types),*) = rmp_serde::from_slice(payload)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to deserialize request for '{}': {e}", #tag),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                },
            };

            let mut call_args = Vec::new();
            match param_count {
                0 => {}
                1 => {
                    let name = &param_names[0];
                    call_args.push(quote! { #name });
                }
                _ => {
                    for name in &param_names {
                        call_args.push(quote! { #name });
                    }
                }
            }
            let call_args = quote! { #(#call_args),* };

            quote! {
                #tag => {
                    #deserialize_request
                    let response = self.#method_name(#call_args).await?;
                    let bytes = rmp_serde::to_vec(&response)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                    ::std::result::Result::Ok(::std::option::Option::Some(bytes))
                }
            }
        })
        .collect();

    let wrapper_struct = quote! {
        /// Generated wrapper for the RPC group.
        ///
        /// Owns the group instance and provides dispatch and delegation methods.
        #[doc(hidden)]
        pub struct #wrapper_name {
            __group: #struct_name,
        }

        impl #wrapper_name {
            /// Create a new wrapper with the group instance.
            pub fn new(group: #struct_name) -> Self {
                Self { __group: group }
            }

            /// Get a reference to the group instance.
            pub fn group(&self) -> &#struct_name {
                &self.__group
            }

            /// Dispatch an RPC to this group. Returns `None` if the tag is unknown.
            #[doc(hidden)]
            pub async fn __dispatch(
                &self,
                tag: &str,
                payload: &[u8],
                headers: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<::std::option::Option<::std::vec::Vec<u8>>, #krate::error::ClusterError> {
                let _ = headers;
                match tag {
                    #(#dispatch_arms,)*
                    _ => ::std::result::Result::Ok(::std::option::Option::None),
                }
            }

            // --- RPC delegation methods ---
            #(#wrapper_delegation_methods)*
        }
    };

    // --- Generate access trait ---
    let access_trait = quote! {
        #[doc(hidden)]
        pub trait #access_trait_name {
            fn __rpc_group_wrapper(&self) -> &#wrapper_name;
        }
    };

    // --- Generate methods trait with blanket impl ---
    let blanket_methods: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| rpc.is_trait_visible())
        .map(|rpc| {
            let method_name = &rpc.name;
            let resp_type = &rpc.response_type;
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();
            let param_defs: Vec<_> = param_names
                .iter()
                .zip(param_types.iter())
                .map(|(name, ty)| quote! { #name: #ty })
                .collect();
            quote! {
                async fn #method_name(
                    &self,
                    #(#param_defs),*
                ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                    self.__rpc_group_wrapper().#method_name(#(#param_names),*).await
                }
            }
        })
        .collect();

    let methods_trait = quote! {
        /// Trait providing RPC group methods via blanket implementation.
        ///
        /// Automatically implemented for any type that implements the access trait.
        #[doc(hidden)]
        #[allow(async_fn_in_trait)]
        pub trait #methods_trait_name: #access_trait_name {
            #(#blanket_methods)*
        }

        // Blanket impl: any type that provides the access trait gets the methods.
        impl<T: #access_trait_name> #methods_trait_name for T {}
    };

    // --- Generate client extension trait ---
    // This is like entity trait's ClientExt — adds group RPC methods to the parent entity's client.
    let client_ext_methods: Vec<proc_macro2::TokenStream> = rpcs
        .iter()
        .filter(|rpc| rpc.is_client_visible())
        .map(|rpc| {
            let method_name = &rpc.name;
            let tag = &rpc.tag;
            let resp_type = &rpc.response_type;
            let param_count = rpc.params.len();
            let param_names: Vec<_> = rpc.params.iter().map(|p| &p.name).collect();
            let param_types: Vec<_> = rpc.params.iter().map(|p| &p.ty).collect();
            let param_defs: Vec<_> = param_names
                .iter()
                .zip(param_types.iter())
                .map(|(name, ty)| quote! { #name: &#ty })
                .collect();

            if rpc.uses_persisted_delivery() {
                match param_count {
                    0 => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            self.entity_client()
                                .send_persisted(entity_id, #tag, &(), #krate::schema::Uninterruptible::No)
                                .await
                        }
                    },
                    1 => {
                        let def = &param_defs[0];
                        let name = &param_names[0];
                        quote! {
                            async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                self.entity_client()
                                    .send_persisted(entity_id, #tag, #name, #krate::schema::Uninterruptible::No)
                                    .await
                            }
                        }
                    }
                    _ => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let request = (#(#param_names),*);
                            self.entity_client()
                                .send_persisted(entity_id, #tag, &request, #krate::schema::Uninterruptible::No)
                                .await
                        }
                    },
                }
            } else {
                match param_count {
                    0 => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            self.entity_client().send(entity_id, #tag, &()).await
                        }
                    },
                    1 => {
                        let def = &param_defs[0];
                        let name = &param_names[0];
                        quote! {
                            async fn #method_name(
                                &self,
                                entity_id: &#krate::types::EntityId,
                                #def,
                            ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                                self.entity_client().send(entity_id, #tag, #name).await
                            }
                        }
                    }
                    _ => quote! {
                        async fn #method_name(
                            &self,
                            entity_id: &#krate::types::EntityId,
                            #(#param_defs),*
                        ) -> ::std::result::Result<#resp_type, #krate::error::ClusterError> {
                            let request = (#(#param_names),*);
                            self.entity_client().send(entity_id, #tag, &request).await
                        }
                    },
                }
            }
        })
        .collect();

    let client_ext = quote! {
        #[async_trait::async_trait]
        pub trait #client_ext_name: #krate::entity_client::EntityClientAccessor {
            #(#client_ext_methods)*
        }

        impl<T> #client_ext_name for T where T: #krate::entity_client::EntityClientAccessor {}
    };

    Ok(quote! {
        #view_struct
        #wrapper_struct
        #access_trait
        #methods_trait
        #client_ext
    })
}

// =============================================================================
// Workflow Macros — argument parsing and codegen
// =============================================================================

struct WorkflowStructArgs {
    key: Option<syn::ExprClosure>,
    hash: bool,
    krate: Option<syn::Path>,
}

impl syn::parse::Parse for WorkflowStructArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = WorkflowStructArgs {
            key: None,
            hash: true,
            krate: None,
        };

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;

            match ident.to_string().as_str() {
                "key" => {
                    input.parse::<syn::Token![=]>()?;
                    let expr: syn::Expr = if input.peek(syn::token::Paren) {
                        let content;
                        syn::parenthesized!(content in input);
                        content.parse()?
                    } else {
                        input.parse()?
                    };
                    match expr {
                        syn::Expr::Closure(closure) => args.key = Some(closure),
                        _ => {
                            return Err(syn::Error::new(
                                expr.span(),
                                "key must be a closure, e.g. #[workflow(key = |req| ...)]",
                            ))
                        }
                    }
                }
                "hash" => {
                    input.parse::<syn::Token![=]>()?;
                    let lit: syn::LitBool = input.parse()?;
                    args.hash = lit.value;
                }
                "krate" => {
                    input.parse::<syn::Token![=]>()?;
                    let lit: syn::LitStr = input.parse()?;
                    args.krate = Some(lit.parse()?);
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown workflow attribute: {other}"),
                    ));
                }
            }

            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }

        Ok(args)
    }
}

struct WorkflowImplArgs {
    krate: Option<syn::Path>,
    activity_groups: Vec<syn::Path>,
    /// Optional key extraction closure, e.g. `key = |req| req.order_id.clone()`.
    key: Option<syn::ExprClosure>,
    /// Whether to SHA-256 hash the key. Default `true`.
    hash: bool,
}

impl syn::parse::Parse for WorkflowImplArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = WorkflowImplArgs {
            krate: None,
            activity_groups: Vec::new(),
            key: None,
            hash: true,
        };
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            match ident.to_string().as_str() {
                "krate" => {
                    input.parse::<syn::Token![=]>()?;
                    let lit: syn::LitStr = input.parse()?;
                    args.krate = Some(lit.parse()?);
                }
                "activity_groups" => {
                    let content;
                    syn::parenthesized!(content in input);
                    while !content.is_empty() {
                        let path: syn::Path = content.parse()?;
                        args.activity_groups.push(path);
                        if !content.is_empty() {
                            content.parse::<syn::Token![,]>()?;
                        }
                    }
                }
                "key" => {
                    input.parse::<syn::Token![=]>()?;
                    let expr: syn::Expr = if input.peek(syn::token::Paren) {
                        let content;
                        syn::parenthesized!(content in input);
                        content.parse()?
                    } else {
                        input.parse()?
                    };
                    match expr {
                        syn::Expr::Closure(closure) => args.key = Some(closure),
                        _ => {
                            return Err(syn::Error::new(
                                expr.span(),
                                "key must be a closure, e.g. #[workflow_impl(key = |req| ...)]",
                            ))
                        }
                    }
                }
                "hash" => {
                    input.parse::<syn::Token![=]>()?;
                    let lit: syn::LitBool = input.parse()?;
                    args.hash = lit.value;
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown workflow_impl attribute: {other}"),
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(args)
    }
}

// --- #[workflow] struct-level codegen ---

fn workflow_struct_inner(
    args: WorkflowStructArgs,
    input: syn::ItemStruct,
) -> syn::Result<proc_macro2::TokenStream> {
    let krate = args.krate.unwrap_or_else(default_crate_path);
    let struct_name = &input.ident;
    let entity_name = format!("Workflow/{}", struct_name);

    // Store key/hash config as hidden associated constants for the impl macro to use.
    // The key closure is stored as a hidden method.
    let key_derivation_info = if let Some(_key_closure) = &args.key {
        let hash_val = args.hash;
        quote! {
            #[doc(hidden)]
            fn __workflow_key_closure() -> bool { true }
            #[doc(hidden)]
            fn __workflow_hash() -> bool { #hash_val }
            #[doc(hidden)]
            fn __extract_key<__Req>(req: &__Req) -> ::std::string::String
            where __Req: serde::Serialize,
            {
                // This is a placeholder — the actual key extraction is codegen'd
                // by workflow_impl based on the struct's key attribute.
                let _ = req;
                unreachable!("key extraction is generated by workflow_impl")
            }
        }
    } else {
        quote! {
            #[doc(hidden)]
            fn __workflow_key_closure() -> bool { false }
            #[doc(hidden)]
            fn __workflow_hash() -> bool { true }
        }
    };
    // Store key closure as a hidden item for the impl macro.
    // We stash the key closure info as a doc-hidden type.
    let _ = key_derivation_info;
    let _ = args.key;

    Ok(quote! {
        #input

        #[allow(dead_code)]
        impl #struct_name {
            #[doc(hidden)]
            fn __entity_type(&self) -> #krate::types::EntityType {
                #krate::types::EntityType::new(#entity_name)
            }

            #[doc(hidden)]
            fn __shard_group(&self) -> &str {
                "default"
            }

            #[doc(hidden)]
            fn __shard_group_for(&self, _entity_id: &#krate::types::EntityId) -> &str {
                self.__shard_group()
            }

            #[doc(hidden)]
            fn __max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                ::std::option::Option::None
            }

            #[doc(hidden)]
            fn __mailbox_capacity(&self) -> ::std::option::Option<usize> {
                ::std::option::Option::None
            }

            #[doc(hidden)]
            fn __concurrency(&self) -> ::std::option::Option<usize> {
                ::std::option::Option::None
            }
        }
    })
}

// --- Workflow activity info for codegen ---

struct WorkflowActivityInfo {
    name: syn::Ident,
    #[allow(dead_code)]
    tag: String,
    params: Vec<RpcParam>,
    #[allow(dead_code)]
    response_type: syn::Type,
    persist_key: Option<syn::ExprClosure>,
    original_method: syn::ImplItemFn,
    /// Max retry attempts (from `#[activity(retries = N)]`).
    retries: Option<u32>,
    /// Backoff strategy: `"exponential"` (default) or `"constant"`.
    backoff: Option<String>,
}

struct WorkflowExecuteInfo {
    params: Vec<RpcParam>,
    request_type: syn::Type,
    response_type: syn::Type,
    original_method: syn::ImplItemFn,
}

// --- #[workflow_impl] codegen ---

fn workflow_impl_inner(
    args: WorkflowImplArgs,
    input: syn::ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let krate = args.krate.unwrap_or_else(default_crate_path);
    let self_ty = &input.self_ty;

    let struct_name = match self_ty.as_ref() {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|s| s.ident.clone())
            .ok_or_else(|| syn::Error::new(self_ty.span(), "expected struct name"))?,
        _ => return Err(syn::Error::new(self_ty.span(), "expected struct name")),
    };

    let handler_name = format_ident!("__{}WorkflowHandler", struct_name);
    let client_name = format_ident!("{}Client", struct_name);
    let execute_view_name = format_ident!("__{}ExecuteView", struct_name);
    let activity_view_name = format_ident!("__{}ActivityView", struct_name);
    let entity_name = format!("Workflow/{}", struct_name);
    let has_activity_groups = !args.activity_groups.is_empty();
    let with_groups_name = format_ident!("__{}WithGroups", struct_name);

    // Validate: no #[state], no #[rpc], no #[workflow], no &mut self
    for attr in &input.attrs {
        if attr.path().is_ident("state") {
            return Err(syn::Error::new(
                attr.span(),
                "workflows are stateless; remove #[state(...)]",
            ));
        }
    }

    // Parse methods: find execute, activities, and helpers
    let mut execute_info: Option<WorkflowExecuteInfo> = None;
    let mut activities: Vec<WorkflowActivityInfo> = Vec::new();
    let mut original_methods: Vec<syn::ImplItemFn> = Vec::new();

    for item in &input.items {
        if let syn::ImplItem::Fn(method) = item {
            // Check for forbidden annotations
            for attr in &method.attrs {
                if attr.path().is_ident("state") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "workflows are stateless; remove #[state(...)]",
                    ));
                }
                if attr.path().is_ident("rpc") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "workflows use #[activity], not #[rpc]",
                    ));
                }
                if attr.path().is_ident("workflow") {
                    return Err(syn::Error::new(
                        attr.span(),
                        "workflows have a single execute entry point; use client calls for cross-workflow interaction",
                    ));
                }
            }

            // Check for &mut self
            if let Some(syn::FnArg::Receiver(r)) = method.sig.inputs.first() {
                if r.mutability.is_some() {
                    return Err(syn::Error::new(
                        r.span(),
                        "workflow methods must use &self, not &mut self",
                    ));
                }
            }

            if method.sig.ident == "execute" {
                // Parse execute method
                if execute_info.is_some() {
                    return Err(syn::Error::new(
                        method.sig.span(),
                        "workflow must have exactly one execute method",
                    ));
                }

                if method.sig.asyncness.is_none() {
                    return Err(syn::Error::new(method.sig.span(), "execute must be async"));
                }

                // Parse params (skip &self)
                let mut params = Vec::new();
                for arg in method.sig.inputs.iter().skip(1) {
                    if let syn::FnArg::Typed(pat_type) = arg {
                        let name = match &*pat_type.pat {
                            syn::Pat::Ident(ident) => ident.ident.clone(),
                            _ => {
                                return Err(syn::Error::new(
                                    pat_type.pat.span(),
                                    "execute parameters must be simple identifiers",
                                ))
                            }
                        };
                        params.push(RpcParam {
                            name,
                            ty: (*pat_type.ty).clone(),
                        });
                    }
                }

                if params.len() != 1 {
                    return Err(syn::Error::new(
                        method.sig.span(),
                        "execute must take exactly one request parameter (after &self)",
                    ));
                }

                let request_type = params[0].ty.clone();
                let response_type = extract_result_ok_type(match &method.sig.output {
                    syn::ReturnType::Type(_, ty) => ty,
                    syn::ReturnType::Default => {
                        return Err(syn::Error::new(
                            method.sig.span(),
                            "execute must return Result<T, ClusterError>",
                        ))
                    }
                })?;

                execute_info = Some(WorkflowExecuteInfo {
                    params,
                    request_type,
                    response_type,
                    original_method: method.clone(),
                });
            } else {
                // Check if it's an activity
                let is_activity = method.attrs.iter().any(|a| a.path().is_ident("activity"));

                if is_activity {
                    if method.sig.asyncness.is_none() {
                        return Err(syn::Error::new(
                            method.sig.span(),
                            "#[activity] methods must be async",
                        ));
                    }

                    // Parse activity attributes (key, retries, backoff)
                    let (persist_key, act_retries, act_backoff) = {
                        let mut key = None;
                        let mut retries = None;
                        let mut backoff = None;
                        for attr in &method.attrs {
                            if attr.path().is_ident("activity") {
                                let args = match &attr.meta {
                                    syn::Meta::Path(_) => ActivityAttrArgs {
                                        key: None,
                                        retries: None,
                                        backoff: None,
                                    },
                                    syn::Meta::List(_) => attr.parse_args::<ActivityAttrArgs>()?,
                                    _ => {
                                        return Err(syn::Error::new(
                                            attr.span(),
                                            "expected #[activity] or #[activity(...)]",
                                        ))
                                    }
                                };
                                key = args.key;
                                retries = args.retries;
                                backoff = args.backoff;
                            }
                        }
                        (key, retries, backoff)
                    };

                    // Parse params
                    let mut params = Vec::new();
                    for arg in method.sig.inputs.iter().skip(1) {
                        if let syn::FnArg::Typed(pat_type) = arg {
                            let name = match &*pat_type.pat {
                                syn::Pat::Ident(ident) => ident.ident.clone(),
                                _ => {
                                    return Err(syn::Error::new(
                                        pat_type.pat.span(),
                                        "activity parameters must be simple identifiers",
                                    ))
                                }
                            };
                            params.push(RpcParam {
                                name,
                                ty: (*pat_type.ty).clone(),
                            });
                        }
                    }

                    let response_type = extract_result_ok_type(match &method.sig.output {
                        syn::ReturnType::Type(_, ty) => ty,
                        syn::ReturnType::Default => {
                            return Err(syn::Error::new(
                                method.sig.span(),
                                "#[activity] must return Result<T, ClusterError>",
                            ))
                        }
                    })?;

                    activities.push(WorkflowActivityInfo {
                        name: method.sig.ident.clone(),
                        tag: method.sig.ident.to_string(),
                        params,
                        response_type,
                        persist_key,
                        original_method: method.clone(),
                        retries: act_retries,
                        backoff: act_backoff,
                    });
                }
            }
            original_methods.push(method.clone());
        }
    }

    let execute = execute_info.ok_or_else(|| {
        syn::Error::new(
            input.self_ty.span(),
            "workflow must define an `async fn execute(&self, request: T) -> Result<R, ClusterError>` method",
        )
    })?;

    let request_type = &execute.request_type;
    let response_type = &execute.response_type;

    // --- Process activity groups ---
    #[allow(dead_code)]
    struct ActivityGroupInfo {
        path: syn::Path,
        ident: syn::Ident,
        field: syn::Ident,
        wrapper_ident: syn::Ident,
        wrapper_path: syn::Path,
        access_trait_ident: syn::Ident,
        access_trait_path: syn::Path,
        methods_trait_ident: syn::Ident,
        methods_trait_path: syn::Path,
    }

    let group_infos: Vec<ActivityGroupInfo> = args
        .activity_groups
        .iter()
        .map(|path| {
            let ident = path
                .segments
                .last()
                .map(|s| s.ident.clone())
                .expect("activity group path must have an ident");
            let snake = to_snake(&ident.to_string());
            let field = format_ident!("__group_{}", snake);
            let wrapper_ident = format_ident!("__{}ActivityGroupWrapper", ident);
            let wrapper_path = replace_last_segment(path, wrapper_ident.clone());
            let access_trait_ident = format_ident!("__{}ActivityGroupAccess", ident);
            let access_trait_path = replace_last_segment(path, access_trait_ident.clone());
            let methods_trait_ident = format_ident!("__{}ActivityGroupMethods", ident);
            let methods_trait_path = replace_last_segment(path, methods_trait_ident.clone());
            ActivityGroupInfo {
                path: path.clone(),
                ident,
                field,
                wrapper_ident,
                wrapper_path,
                access_trait_ident,
                access_trait_path,
                methods_trait_ident,
                methods_trait_path,
            }
        })
        .collect();

    // --- Generate execute view methods ---
    // execute goes on the execute view
    let execute_method = &execute.original_method;
    let execute_block = &execute_method.block;
    let execute_output = &execute_method.sig.output;
    let execute_param_name = &execute.params[0].name;
    let execute_param_type = &execute.params[0].ty;
    let execute_attrs: Vec<_> = execute_method
        .attrs
        .iter()
        .filter(|a| {
            !a.path().is_ident("rpc")
                && !a.path().is_ident("workflow")
                && !a.path().is_ident("activity")
        })
        .collect();

    // --- Generate activity methods on the activity view ---
    let mut activity_view_methods = Vec::new();
    for act in &activities {
        let method = &act.original_method;
        let block = &method.block;
        let output = &method.sig.output;
        let name = &act.name;
        let params: Vec<_> = method.sig.inputs.iter().skip(1).collect();
        let attrs: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| {
                !a.path().is_ident("activity")
                    && !a.path().is_ident("public")
                    && !a.path().is_ident("protected")
                    && !a.path().is_ident("private")
            })
            .collect();
        let vis = &method.vis;

        activity_view_methods.push(quote! {
            #(#attrs)*
            #vis async fn #name(&self, #(#params),*) #output
                #block
        });
    }

    // --- Generate helper methods on both views ---
    let mut helper_execute_methods = Vec::new();
    let mut helper_activity_methods = Vec::new();

    for method in &original_methods {
        let name = &method.sig.ident;
        if name == "execute" {
            continue;
        }
        let is_activity = method.attrs.iter().any(|a| a.path().is_ident("activity"));
        if is_activity {
            continue;
        }

        // Unannotated methods are helpers — put them on both views
        let block = &method.block;
        let output = &method.sig.output;
        let params: Vec<_> = method.sig.inputs.iter().skip(1).collect();
        let attrs: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| {
                !a.path().is_ident("rpc")
                    && !a.path().is_ident("workflow")
                    && !a.path().is_ident("activity")
                    && !a.path().is_ident("method")
                    && !a.path().is_ident("public")
                    && !a.path().is_ident("protected")
                    && !a.path().is_ident("private")
            })
            .collect();
        let vis = &method.vis;
        let async_token = if method.sig.asyncness.is_some() {
            quote! { async }
        } else {
            quote! {}
        };

        let method_tokens = quote! {
            #(#attrs)*
            #vis #async_token fn #name(&self, #(#params),*) #output
                #block
        };
        helper_execute_methods.push(method_tokens.clone());
        helper_activity_methods.push(method_tokens);
    }

    // --- Generate activity delegation methods on execute view ---
    // These route through DurableContext::run() for journaling
    let activity_delegations: Vec<proc_macro2::TokenStream> = activities
        .iter()
        .map(|act| {
            let method_name = &act.name;
            let method_name_str = method_name.to_string();
            let method_info = &act.original_method;
            let params: Vec<_> = method_info.sig.inputs.iter().skip(1).collect();
            let param_names: Vec<_> = method_info
                .sig
                .inputs
                .iter()
                .skip(1)
                .filter_map(|arg| {
                    if let syn::FnArg::Typed(pat_type) = arg {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some(&pat_ident.ident);
                        }
                    }
                    None
                })
                .collect();
            let output = &method_info.sig.output;

            let wire_param_names: Vec<_> = act.params.iter().map(|p| &p.name).collect();
            let wire_param_count = wire_param_names.len();

            // Build key-bytes computation
            let key_bytes_code = if let Some(persist_key) = &act.persist_key {
                match wire_param_count {
                    0 => quote! {
                        let __journal_key = (#persist_key)();
                        let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                    1 => {
                        let name = &wire_param_names[0];
                        quote! {
                            let __journal_key = (#persist_key)(#name);
                            let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                                .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                    reason: ::std::format!("failed to serialize journal key: {e}"),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                        }
                    }
                    _ => quote! {
                        let __journal_key = (#persist_key)(#(&#wire_param_names),*);
                        let __journal_key_bytes = rmp_serde::to_vec(&__journal_key)
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                }
            } else {
                match wire_param_count {
                    0 => quote! {
                        let __journal_key_bytes = rmp_serde::to_vec(&()).unwrap_or_default();
                    },
                    1 => {
                        let name = &wire_param_names[0];
                        quote! {
                            let __journal_key_bytes = rmp_serde::to_vec(&#name)
                                .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                    reason: ::std::format!("failed to serialize journal key: {e}"),
                                    source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                                })?;
                        }
                    }
                    _ => quote! {
                        let __journal_key_bytes = rmp_serde::to_vec(&(#(&#wire_param_names),*))
                            .map_err(|e| #krate::error::ClusterError::PersistenceError {
                                reason: ::std::format!("failed to serialize journal key: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                    },
                }
            };

            // Generate the journal body — either single-shot or retry loop
            let max_retries = act.retries.unwrap_or(0);
            let journal_body = if max_retries == 0 {
                // No retry — single shot
                quote! {
                    let __journal_ctx = #krate::__internal::DurableContext::with_journal_storage(
                        ::std::sync::Arc::clone(__engine),
                        self.__handler.ctx.address.entity_type.0.clone(),
                        self.__handler.ctx.address.entity_id.0.clone(),
                        ::std::sync::Arc::clone(__msg_storage),
                        ::std::sync::Arc::clone(__wf_storage),
                    );
                    let __activity_view = #activity_view_name {
                        __handler: self.__handler,
                    };
                    __journal_ctx.run(#method_name_str, &__journal_key_bytes, || {
                        __activity_view.#method_name(#(#param_names),*)
                    }).await
                }
            } else {
                let backoff_str = act.backoff.as_deref().unwrap_or("exponential");
                // Clone params at start of each iteration so owned types survive across retries
                let param_clones: Vec<proc_macro2::TokenStream> = param_names
                    .iter()
                    .map(|name| {
                        let clone_name = format_ident!("__{}_clone", name);
                        quote! { let #clone_name = #name.clone(); }
                    })
                    .collect();
                let cloned_param_names: Vec<syn::Ident> = param_names
                    .iter()
                    .map(|name| format_ident!("__{}_clone", name))
                    .collect();
                quote! {
                    let mut __attempt = 0u32;
                    loop {
                        // Clone params for this attempt (owned types may not be Copy)
                        #(#param_clones)*
                        // Append attempt number to journal key for independent journaling per retry
                        let __retry_key_bytes = {
                            let mut __k = __journal_key_bytes.clone();
                            __k.extend_from_slice(&__attempt.to_le_bytes());
                            __k
                        };
                        let __journal_ctx = #krate::__internal::DurableContext::with_journal_storage(
                            ::std::sync::Arc::clone(__engine),
                            self.__handler.ctx.address.entity_type.0.clone(),
                            self.__handler.ctx.address.entity_id.0.clone(),
                            ::std::sync::Arc::clone(__msg_storage),
                            ::std::sync::Arc::clone(__wf_storage),
                        );
                        let __activity_view = #activity_view_name {
                            __handler: self.__handler,
                        };
                        match __journal_ctx.run(#method_name_str, &__retry_key_bytes, || {
                            __activity_view.#method_name(#(#cloned_param_names),*)
                        }).await {
                            ::std::result::Result::Ok(__val) => {
                                break ::std::result::Result::Ok(__val);
                            }
                            ::std::result::Result::Err(__e) if __attempt < #max_retries => {
                                // Durable sleep between retries
                                let __delay = #krate::__internal::compute_retry_backoff(
                                    __attempt, #backoff_str, 1,
                                );
                                let __sleep_name = ::std::format!(
                                    "{}/retry/{}", #method_name_str, __attempt
                                );
                                __engine.sleep(
                                    &self.__handler.ctx.address.entity_type.0,
                                    &self.__handler.ctx.address.entity_id.0,
                                    &__sleep_name,
                                    __delay,
                                ).await?;
                                __attempt += 1;
                            }
                            ::std::result::Result::Err(__e) => {
                                break ::std::result::Result::Err(__e);
                            }
                        }
                    }
                }
            };

            quote! {
                #[inline]
                async fn #method_name(&self, #(#params),*) #output {
                    if let (
                        ::std::option::Option::Some(__engine),
                        ::std::option::Option::Some(__msg_storage),
                        ::std::option::Option::Some(__wf_storage),
                    ) = (
                        self.__handler.__workflow_engine.as_ref(),
                        self.__handler.__message_storage.as_ref(),
                        self.__handler.__state_storage.as_ref(),
                    ) {
                        #key_bytes_code
                        // Scope the journal key per workflow execution
                        let __journal_key_bytes = {
                            let mut __scoped = ::std::vec::Vec::new();
                            if let ::std::option::Option::Some(__wf_id) = #krate::__internal::WorkflowScope::current() {
                                __scoped.extend_from_slice(&__wf_id.to_le_bytes());
                            }
                            __scoped.extend_from_slice(&__journal_key_bytes);
                            __scoped
                        };
                        #journal_body
                    } else {
                        // No journal — execute directly
                        let __activity_view = #activity_view_name {
                            __handler: self.__handler,
                        };
                        __activity_view.#method_name(#(#param_names),*).await
                    }
                }
            }
        })
        .collect();

    // --- Activity group fields and initialization ---
    let group_handler_fields: Vec<proc_macro2::TokenStream> = group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            quote! {
                #field: #wrapper_path,
            }
        })
        .collect();

    let group_new_params: Vec<proc_macro2::TokenStream> = group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let path = &info.path;
            quote! {
                #field: #path,
            }
        })
        .collect();

    let group_field_inits: Vec<proc_macro2::TokenStream> = group_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            let wrapper_path = &info.wrapper_path;
            quote! {
                #field: #wrapper_path::new(
                    #field,
                    ctx.workflow_engine.clone(),
                    ctx.message_storage.clone(),
                    ctx.state_storage.clone(),
                    ctx.address.entity_type.0.clone(),
                    ctx.address.entity_id.0.clone(),
                ),
            }
        })
        .collect();

    // --- Generate Handler ---
    let handler_def = quote! {
        #[doc(hidden)]
        pub struct #handler_name {
            /// The workflow instance (user struct).
            __workflow: #struct_name,
            /// Entity context.
            #[allow(dead_code)]
            ctx: #krate::entity::EntityContext,
            /// Workflow storage for journal/state.
            __state_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowStorage>>,
            /// Workflow engine for durable primitives.
            __workflow_engine: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowEngine>>,
            /// Message storage for persistence.
            __message_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::MessageStorage>>,
            /// Sharding interface.
            __sharding: ::std::option::Option<::std::sync::Arc<dyn #krate::sharding::Sharding>>,
            /// Entity address.
            __entity_address: #krate::types::EntityAddress,
            #(#group_handler_fields)*
        }

        impl #handler_name {
            #[doc(hidden)]
            pub async fn __new(
                workflow: #struct_name,
                #(#group_new_params)*
                ctx: #krate::entity::EntityContext,
            ) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                let __state_storage = ctx.state_storage.clone();
                let __sharding = ctx.sharding.clone();
                let __entity_address = ctx.address.clone();
                ::std::result::Result::Ok(Self {
                    __workflow: workflow,
                    __workflow_engine: ctx.workflow_engine.clone(),
                    __message_storage: ctx.message_storage.clone(),
                    #(#group_field_inits)*
                    ctx,
                    __state_storage,
                    __sharding,
                    __entity_address,
                })
            }

            /// Durable sleep that survives workflow restarts.
            pub async fn sleep(&self, name: &str, duration: ::std::time::Duration) -> ::std::result::Result<(), #krate::error::ClusterError> {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "sleep() requires a workflow engine".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.sleep(name, duration).await
            }

            /// Wait for an external signal to resolve a typed value.
            pub async fn await_deferred<T, K>(&self, key: K) -> ::std::result::Result<T, #krate::error::ClusterError>
            where
                T: serde::Serialize + serde::de::DeserializeOwned,
                K: #krate::__internal::DeferredKeyLike<T>,
            {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "await_deferred() requires a workflow engine".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.await_deferred(key).await
            }

            /// Resolve a deferred value.
            pub async fn resolve_deferred<T, K>(&self, key: K, value: &T) -> ::std::result::Result<(), #krate::error::ClusterError>
            where
                T: serde::Serialize,
                K: #krate::__internal::DeferredKeyLike<T>,
            {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "resolve_deferred() requires a workflow engine".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.resolve_deferred(key, value).await
            }

            /// Wait for an interrupt signal.
            pub async fn on_interrupt(&self) -> ::std::result::Result<(), #krate::error::ClusterError> {
                let engine = self.__workflow_engine.as_ref().ok_or_else(|| {
                    #krate::error::ClusterError::MalformedMessage {
                        reason: "on_interrupt() requires a workflow engine".into(),
                        source: ::std::option::Option::None,
                    }
                })?;
                let ctx = #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    self.ctx.address.entity_type.0.clone(),
                    self.ctx.address.entity_id.0.clone(),
                );
                ctx.on_interrupt().await
            }

            /// Get the execution ID (= entity ID).
            pub fn execution_id(&self) -> &str {
                &self.__entity_address.entity_id.0
            }

            /// Get the entity ID.
            pub fn entity_id(&self) -> &#krate::types::EntityId {
                &self.__entity_address.entity_id
            }

            /// Get the sharding interface.
            pub fn sharding(&self) -> ::std::option::Option<&::std::sync::Arc<dyn #krate::sharding::Sharding>> {
                self.__sharding.as_ref()
            }

            /// Get the entity address.
            pub fn entity_address(&self) -> &#krate::types::EntityAddress {
                &self.__entity_address
            }
        }
    };

    // --- Generate view structs ---
    let view_structs = quote! {
        /// View struct for execute — provides durable primitives + activity delegation.
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #execute_view_name<'a> {
            __handler: &'a #handler_name,
        }

        /// View struct for activities — provides struct field access only.
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #activity_view_name<'a> {
            __handler: &'a #handler_name,
        }

        // Both views deref to the user's workflow struct
        impl ::std::ops::Deref for #execute_view_name<'_> {
            type Target = #struct_name;
            fn deref(&self) -> &Self::Target {
                &self.__handler.__workflow
            }
        }

        impl ::std::ops::Deref for #activity_view_name<'_> {
            type Target = #struct_name;
            fn deref(&self) -> &Self::Target {
                &self.__handler.__workflow
            }
        }
    };

    // --- Activity group: access trait impls on execute view ---
    let group_access_impls: Vec<proc_macro2::TokenStream> = group_infos
        .iter()
        .map(|info| {
            let access_trait_path = &info.access_trait_path;
            let wrapper_path = &info.wrapper_path;
            let field = &info.field;
            quote! {
                impl #access_trait_path for #execute_view_name<'_> {
                    fn __activity_group_wrapper(&self) -> &#wrapper_path {
                        &self.__handler.#field
                    }
                }
            }
        })
        .collect();

    // --- Activity group: use methods traits ---
    let group_use_methods: Vec<proc_macro2::TokenStream> = group_infos
        .iter()
        .map(|info| {
            let methods_trait_path = &info.methods_trait_path;
            quote! {
                #[allow(unused_imports)]
                use #methods_trait_path as _;
            }
        })
        .collect();

    // --- Execute view: durable primitive delegations ---
    let execute_view_impl = quote! {
        #(#group_use_methods)*
        impl #execute_view_name<'_> {
            /// Durable sleep.
            #[inline]
            async fn sleep(&self, duration: ::std::time::Duration) -> ::std::result::Result<(), #krate::error::ClusterError> {
                // Use a fixed name for workflow sleep — uniqueness comes from WorkflowScope
                self.__handler.sleep("__wf_sleep", duration).await
            }

            /// Wait for an external signal.
            #[inline]
            async fn await_deferred<T, K>(&self, key: K) -> ::std::result::Result<T, #krate::error::ClusterError>
            where
                T: serde::Serialize + serde::de::DeserializeOwned,
                K: #krate::__internal::DeferredKeyLike<T>,
            {
                self.__handler.await_deferred(key).await
            }

            /// Resolve a deferred value.
            #[inline]
            async fn resolve_deferred<T, K>(&self, key: K, value: &T) -> ::std::result::Result<(), #krate::error::ClusterError>
            where
                T: serde::Serialize,
                K: #krate::__internal::DeferredKeyLike<T>,
            {
                self.__handler.resolve_deferred(key, value).await
            }

            /// Wait for interrupt.
            #[inline]
            async fn on_interrupt(&self) -> ::std::result::Result<(), #krate::error::ClusterError> {
                self.__handler.on_interrupt().await
            }

            /// Get the execution ID.
            #[inline]
            fn execution_id(&self) -> &str {
                self.__handler.execution_id()
            }

            /// Get the sharding interface.
            #[inline]
            fn sharding(&self) -> ::std::option::Option<&::std::sync::Arc<dyn #krate::sharding::Sharding>> {
                self.__handler.sharding()
            }

            /// Get a typed client for another workflow or entity.
            ///
            /// The target type `T` must implement `cruster::entity_client::WorkflowClientFactory`.
            /// All types annotated with `#[workflow]` automatically implement this.
            #[inline]
            fn client<T: #krate::entity_client::WorkflowClientFactory>(&self) -> T::Client {
                let sharding = self.__handler.__sharding.clone()
                    .expect("client() requires a sharding interface");
                T::workflow_client(sharding)
            }

            // --- Activity delegation methods (journaled) ---
            #(#activity_delegations)*

            // --- execute method body ---
            #(#execute_attrs)*
            async fn execute(&self, #execute_param_name: #execute_param_type) #execute_output
                #execute_block

            // --- Helper methods ---
            #(#helper_execute_methods)*
        }
    };

    let activity_view_impl = quote! {
        impl #activity_view_name<'_> {
            #(#activity_view_methods)*
            #(#helper_activity_methods)*
        }
    };

    // --- Generate dispatch (single "execute" arm) ---
    let dispatch_impl = quote! {
        #[async_trait::async_trait]
        impl #krate::entity::EntityHandler for #handler_name {
            async fn handle_request(
                &self,
                tag: &str,
                payload: &[u8],
                headers: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<::std::vec::Vec<u8>, #krate::error::ClusterError> {
                #[allow(unused_variables)]
                let headers = headers;
                match tag {
                    "execute" => {
                        let __request: #request_type = rmp_serde::from_slice(payload)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to deserialize workflow request: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })?;
                        let __request_id = headers
                            .get(#krate::__internal::REQUEST_ID_HEADER_KEY)
                            .and_then(|v| v.parse::<i64>().ok())
                            .unwrap_or(0);
                        let (__wf_result, __journal_keys) = #krate::__internal::WorkflowScope::run(__request_id, || async {
                            let __view = #execute_view_name { __handler: self };
                            __view.execute(__request).await
                        }).await;
                        let response = __wf_result?;
                        // Mark journal entries as completed
                        if let ::std::option::Option::Some(ref __wf_storage) = self.__state_storage {
                            for __key in &__journal_keys {
                                let _ = __wf_storage.mark_completed(__key).await;
                            }
                        }
                        rmp_serde::to_vec(&response)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to serialize workflow response: {e}"),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })
                    }
                    _ => ::std::result::Result::Err(
                        #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("unknown workflow tag: {tag}"),
                            source: ::std::option::Option::None,
                        }
                    ),
                }
            }
        }
    };

    // --- Generate Entity trait impl + register ---
    // When activity_groups are present, Entity is implemented on a WithGroups wrapper.
    // When no groups, Entity is on the bare struct (current behavior).
    let (entity_impl, register_impl) = if has_activity_groups {
        let group_struct_fields: Vec<proc_macro2::TokenStream> = group_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                quote! { #field: #path, }
            })
            .collect();

        let group_spawn_args: Vec<proc_macro2::TokenStream> = group_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                quote! { self.#field.clone(), }
            })
            .collect();

        let group_register_params: Vec<proc_macro2::TokenStream> = group_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                quote! { #field: #path, }
            })
            .collect();

        let group_register_field_inits: Vec<proc_macro2::TokenStream> = group_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                quote! { #field, }
            })
            .collect();

        let entity_impl_tokens = quote! {
            /// Generated wrapper that bundles the workflow with its activity groups.
            #[doc(hidden)]
            #[derive(Clone)]
            pub struct #with_groups_name {
                __workflow: #struct_name,
                #(#group_struct_fields)*
            }

            #[async_trait::async_trait]
            impl #krate::entity::Entity for #with_groups_name {
                fn entity_type(&self) -> #krate::types::EntityType {
                    self.__workflow.__entity_type()
                }

                fn shard_group(&self) -> &str {
                    self.__workflow.__shard_group()
                }

                fn shard_group_for(&self, entity_id: &#krate::types::EntityId) -> &str {
                    self.__workflow.__shard_group_for(entity_id)
                }

                fn max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.__workflow.__max_idle_time()
                }

                fn mailbox_capacity(&self) -> ::std::option::Option<usize> {
                    self.__workflow.__mailbox_capacity()
                }

                fn concurrency(&self) -> ::std::option::Option<usize> {
                    self.__workflow.__concurrency()
                }

                async fn spawn(
                    &self,
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn #krate::entity::EntityHandler>,
                    #krate::error::ClusterError,
                > {
                    let handler = #handler_name::__new(
                        self.__workflow.clone(),
                        #(#group_spawn_args)*
                        ctx,
                    ).await?;
                    ::std::result::Result::Ok(::std::boxed::Box::new(handler))
                }
            }
        };

        let register_impl_tokens = quote! {
            impl #struct_name {
                /// Register this workflow with the cluster and return a typed client.
                ///
                /// Each activity group must be provided as a separate parameter.
                pub async fn register(
                    self,
                    sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>,
                    #(#group_register_params)*
                ) -> ::std::result::Result<#client_name, #krate::error::ClusterError> {
                    let bundle = #with_groups_name {
                        __workflow: self,
                        #(#group_register_field_inits)*
                    };
                    sharding.register_entity(::std::sync::Arc::new(bundle)).await?;
                    ::std::result::Result::Ok(#client_name::new(sharding))
                }
            }
        };

        (entity_impl_tokens, register_impl_tokens)
    } else {
        let entity_impl_tokens = quote! {
            #[async_trait::async_trait]
            impl #krate::entity::Entity for #struct_name {
                fn entity_type(&self) -> #krate::types::EntityType {
                    self.__entity_type()
                }

                fn shard_group(&self) -> &str {
                    self.__shard_group()
                }

                fn shard_group_for(&self, entity_id: &#krate::types::EntityId) -> &str {
                    self.__shard_group_for(entity_id)
                }

                fn max_idle_time(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.__max_idle_time()
                }

                fn mailbox_capacity(&self) -> ::std::option::Option<usize> {
                    self.__mailbox_capacity()
                }

                fn concurrency(&self) -> ::std::option::Option<usize> {
                    self.__concurrency()
                }

                async fn spawn(
                    &self,
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn #krate::entity::EntityHandler>,
                    #krate::error::ClusterError,
                > {
                    let handler = #handler_name::__new(self.clone(), ctx).await?;
                    ::std::result::Result::Ok(::std::boxed::Box::new(handler))
                }
            }
        };

        let register_impl_tokens = quote! {
            impl #struct_name {
                /// Register this workflow with the cluster and return a typed client.
                pub async fn register(
                    self,
                    sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>,
                ) -> ::std::result::Result<#client_name, #krate::error::ClusterError> {
                    sharding.register_entity(::std::sync::Arc::new(self)).await?;
                    ::std::result::Result::Ok(#client_name::new(sharding))
                }
            }
        };

        (entity_impl_tokens, register_impl_tokens)
    };

    // --- Generate Client ---
    let struct_name_str = entity_name;
    let client_with_key_name = format_ident!("{}ClientWithKey", struct_name);

    // Generate derive_entity_id based on key/hash configuration
    let derive_entity_id_fn = if let Some(ref key_closure) = args.key {
        if args.hash {
            // Custom key, hashed: extract key via closure, serialize, SHA-256
            quote! {
                fn derive_entity_id(
                    request: &#request_type,
                ) -> ::std::result::Result<#krate::types::EntityId, #krate::error::ClusterError> {
                    let key_value = (#key_closure)(request);
                    let key_bytes = rmp_serde::to_vec(&key_value)
                        .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                            reason: ::std::format!("failed to serialize workflow key: {e}"),
                            source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                        })?;
                    ::std::result::Result::Ok(#krate::types::EntityId::new(
                        #krate::hash::sha256_hex(&key_bytes)
                    ))
                }
            }
        } else {
            // Custom key, no hash: extract key via closure, use ToString directly
            quote! {
                fn derive_entity_id(
                    request: &#request_type,
                ) -> ::std::result::Result<#krate::types::EntityId, #krate::error::ClusterError> {
                    let key_value = (#key_closure)(request);
                    ::std::result::Result::Ok(#krate::types::EntityId::new(
                        key_value.to_string()
                    ))
                }
            }
        }
    } else {
        // Default: serialize entire request and SHA-256 hash
        quote! {
            /// Derive the entity ID from a request (SHA-256 hash of serialized request).
            fn derive_entity_id(
                request: &#request_type,
            ) -> ::std::result::Result<#krate::types::EntityId, #krate::error::ClusterError> {
                let key_bytes = rmp_serde::to_vec(request)
                    .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                        reason: ::std::format!("failed to serialize workflow request: {e}"),
                        source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                    })?;
                ::std::result::Result::Ok(#krate::types::EntityId::new(
                    #krate::hash::sha256_hex(&key_bytes)
                ))
            }
        }
    };

    let client_impl = quote! {
        /// Generated typed client for the standalone workflow.
        pub struct #client_name {
            inner: #krate::entity_client::EntityClient,
        }

        impl #client_name {
            /// Create a new workflow client.
            pub fn new(sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>) -> Self {
                Self {
                    inner: #krate::entity_client::EntityClient::new(
                        sharding,
                        #krate::types::EntityType::new(#struct_name_str),
                    ),
                }
            }

            /// Access the underlying untyped [`EntityClient`].
            pub fn inner(&self) -> &#krate::entity_client::EntityClient {
                &self.inner
            }

            /// Override the idempotency key (hashed with SHA-256).
            ///
            /// Returns a lightweight client view with the key baked in.
            /// Call `.execute()` or `.start()` on the returned view.
            pub fn with_key(&self, key: impl ::std::fmt::Display) -> #client_with_key_name<'_> {
                let key_str = key.to_string();
                let entity_id = #krate::types::EntityId::new(
                    #krate::hash::sha256_hex(key_str.as_bytes())
                );
                #client_with_key_name {
                    inner: &self.inner,
                    entity_id,
                }
            }

            /// Override the idempotency key (raw, no hashing).
            ///
            /// The provided key is used directly as the entity ID.
            /// Returns a lightweight client view with the key baked in.
            pub fn with_key_raw(&self, key: impl ::std::string::ToString) -> #client_with_key_name<'_> {
                #client_with_key_name {
                    inner: &self.inner,
                    entity_id: #krate::types::EntityId::new(key.to_string()),
                }
            }

            #derive_entity_id_fn

            /// Execute the workflow and wait for completion.
            ///
            /// The idempotency key is derived from the request via SHA-256 hash of
            /// the serialized request bytes.
            pub async fn execute(
                &self,
                request: &#request_type,
            ) -> ::std::result::Result<#response_type, #krate::error::ClusterError> {
                let entity_id = Self::derive_entity_id(request)?;
                let key_bytes = entity_id.0.as_bytes().to_vec();
                self.inner.send_persisted_with_key(
                    &entity_id,
                    "execute",
                    request,
                    ::std::option::Option::Some(key_bytes),
                    #krate::schema::Uninterruptible::No,
                ).await
            }

            /// Start the workflow (fire-and-forget) and return the execution ID.
            ///
            /// The idempotency key is derived from the request via SHA-256 hash of
            /// the serialized request bytes. The returned string is the entity ID
            /// (= execution ID) that can be used for later reference.
            pub async fn start(
                &self,
                request: &#request_type,
            ) -> ::std::result::Result<::std::string::String, #krate::error::ClusterError> {
                let entity_id = Self::derive_entity_id(request)?;
                let key_bytes = entity_id.0.as_bytes().to_vec();
                self.inner.notify_persisted_with_key(
                    &entity_id,
                    "execute",
                    request,
                    ::std::option::Option::Some(key_bytes),
                ).await?;
                ::std::result::Result::Ok(entity_id.0)
            }

            /// Poll for the result of a previously-started workflow execution.
            ///
            /// Returns `Ok(Some(result))` if the workflow has completed,
            /// `Ok(None)` if it is still running or no result is available yet.
            /// Returns `Err(...)` if the workflow failed.
            pub async fn poll(
                &self,
                execution_id: &str,
            ) -> ::std::result::Result<::std::option::Option<#response_type>, #krate::error::ClusterError> {
                let entity_id = #krate::types::EntityId::new(execution_id);
                let key_bytes = entity_id.0.as_bytes();
                self.inner.poll_reply::<#response_type>(
                    &entity_id,
                    "execute",
                    key_bytes,
                ).await
            }
        }

        impl #krate::entity_client::EntityClientAccessor for #client_name {
            fn entity_client(&self) -> &#krate::entity_client::EntityClient {
                &self.inner
            }
        }

        /// Lightweight key-override view for the workflow client.
        ///
        /// Created by [`#client_name::with_key`] or [`#client_name::with_key_raw`].
        /// Provides `execute` and `start` using the baked-in key.
        pub struct #client_with_key_name<'a> {
            inner: &'a #krate::entity_client::EntityClient,
            entity_id: #krate::types::EntityId,
        }

        impl #client_with_key_name<'_> {
            /// Execute the workflow and wait for completion using the baked-in key.
            pub async fn execute(
                &self,
                request: &#request_type,
            ) -> ::std::result::Result<#response_type, #krate::error::ClusterError> {
                let key_bytes = self.entity_id.0.as_bytes().to_vec();
                self.inner.send_persisted_with_key(
                    &self.entity_id,
                    "execute",
                    request,
                    ::std::option::Option::Some(key_bytes),
                    #krate::schema::Uninterruptible::No,
                ).await
            }

            /// Start the workflow (fire-and-forget) using the baked-in key.
            ///
            /// Returns the execution ID (= entity ID).
            pub async fn start(
                &self,
                request: &#request_type,
            ) -> ::std::result::Result<::std::string::String, #krate::error::ClusterError> {
                let key_bytes = self.entity_id.0.as_bytes().to_vec();
                self.inner.notify_persisted_with_key(
                    &self.entity_id,
                    "execute",
                    request,
                    ::std::option::Option::Some(key_bytes),
                ).await?;
                ::std::result::Result::Ok(self.entity_id.0.clone())
            }

            /// Poll for the result of the workflow execution using the baked-in key.
            ///
            /// Returns `Ok(Some(result))` if the workflow has completed,
            /// `Ok(None)` if it is still running or no result is available yet.
            /// Returns `Err(...)` if the workflow failed.
            pub async fn poll(
                &self,
            ) -> ::std::result::Result<::std::option::Option<#response_type>, #krate::error::ClusterError> {
                let key_bytes = self.entity_id.0.as_bytes();
                self.inner.poll_reply::<#response_type>(
                    &self.entity_id,
                    "execute",
                    key_bytes,
                ).await
            }
        }
    };

    // --- Generate WorkflowClientFactory impl ---
    let client_factory_impl = quote! {
        impl #krate::entity_client::WorkflowClientFactory for #struct_name {
            type Client = #client_name;

            fn workflow_client(sharding: ::std::sync::Arc<dyn #krate::sharding::Sharding>) -> #client_name {
                #client_name::new(sharding)
            }
        }
    };

    Ok(quote! {
        #handler_def
        #view_structs
        #(#group_access_impls)*
        #execute_view_impl
        #activity_view_impl
        #dispatch_impl
        #entity_impl
        #register_impl
        #client_impl
        #client_factory_impl
    })
}
