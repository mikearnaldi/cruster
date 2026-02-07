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

/// Marks an async method as an RPC handler (read-only operations).
///
/// Use `#[rpc]` for methods that **do not modify state**. These are simple
/// request/response handlers for queries and reads.
///
/// For methods that modify state, use `#[workflow]` instead.
///
/// # Usage
///
/// ```text
/// #[entity_impl]
/// #[state(MyState)]
/// impl MyEntity {
///     #[rpc]
///     async fn get_count(&self) -> Result<i32, ClusterError> {
///         Ok(self.state().count)  // Read-only access
///     }
///
///     #[rpc]
///     async fn get_status(&self) -> Result<Status, ClusterError> {
///         Ok(self.state().status.clone())
///     }
/// }
/// ```
///
/// # Visibility
///
/// By default, `#[rpc]` methods are `#[public]` (externally callable).
/// Combine with `#[protected]` or `#[private]` to restrict access.
///
/// # When to Use
///
/// - **Use `#[rpc]`** for read-only queries that don't modify state
/// - **Use `#[workflow]`** for any operation that calls `state_mut()`
///
/// # Generated Code
///
/// For each `#[rpc]` method, the macro generates:
/// - A dispatch arm in `handle_request` for the method tag
/// - A typed client method with the same signature
#[proc_macro_attribute]
pub fn rpc(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks an async method as a durable workflow (state mutations).
///
/// Use `#[workflow]` for any method that **modifies entity state** via `state_mut()`.
/// Workflows are persisted and can be replayed on restart, ensuring state changes
/// are never lost.
///
/// # Usage
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
///
///     #[workflow]
///     async fn reset(&self) -> Result<(), ClusterError> {
///         let mut state = self.state_mut().await;
///         state.count = 0;
///         Ok(())
///     }
/// }
/// ```
///
/// # When to Use
///
/// - **Any method that calls `state_mut()`** should be `#[workflow]`
/// - State mutations are atomic and persisted
/// - If the process crashes, the workflow will be replayed
///
/// # Complex Workflows with Activities
///
/// For workflows that need to perform external side effects (API calls, emails, etc.),
/// use `#[activity]` methods and call them via `DurableContext`:
///
/// ```text
/// #[workflow]
/// async fn process_order(&self, ctx: &DurableContext, order: Order) -> Result<Receipt, ClusterError> {
///     // State mutation
///     {
///         let mut state = self.state_mut().await;
///         state.orders.push(order.id);
///     }
///     
///     // External side effect (retryable, persisted)
///     ctx.run(|| self.send_confirmation_email(&order)).await?;
///     
///     Ok(Receipt::new(order.id))
/// }
/// ```
///
/// # Visibility
///
/// By default, `#[workflow]` methods are `#[public]` (externally callable).
///
/// # Idempotency Key
///
/// Use `#[workflow(key = |req| ...)]` to deduplicate repeated calls:
///
/// ```text
/// #[workflow(key = |order| order.id.clone())]
/// async fn process_order(&self, order: Order) -> Result<Receipt, ClusterError> {
///     // Duplicate calls with same order.id return cached result
/// }
/// ```
#[proc_macro_attribute]
pub fn workflow(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
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
    let state_persisted = state_info
        .as_ref()
        .map(|info| info.persistent)
        .unwrap_or(false);
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

    let entity_tokens = if is_stateful {
        generate_stateful_entity(
            &krate,
            &struct_name,
            &handler_name,
            &client_name,
            state_type.as_ref().unwrap(),
            state_persisted,
            &traits,
            &rpcs,
            &original_methods,
        )?
    } else {
        generate_stateless_entity(
            &krate,
            &struct_name,
            &handler_name,
            &client_name,
            &traits,
            &rpcs,
            &original_methods,
        )?
    };

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
    let state_persisted = state_info
        .as_ref()
        .map(|info| info.persistent)
        .unwrap_or(false);
    let mut has_init = false;
    let mut init_method: Option<syn::ImplItemFn> = None;
    let mut original_methods = Vec::new();

    for item in &input.items {
        match item {
            syn::ImplItem::Type(type_item) if type_item.ident == "State" => {
                return Err(syn::Error::new(
                    type_item.span(),
                    "use #[state(Type, persistent)] on the impl block instead of `type State`",
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

    if is_stateful && !state_persisted {
        return Err(syn::Error::new(
            input.self_ty.span(),
            "entity trait state must be declared as #[state(Type, persistent)]",
        ));
    }

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

/// Generate code for stateless entities (no `#[state(...)]`).
fn generate_stateless_entity(
    krate: &syn::Path,
    struct_name: &syn::Ident,
    handler_name: &syn::Ident,
    client_name: &syn::Ident,
    traits: &[syn::Path],
    rpcs: &[RpcMethod],
    original_methods: &[syn::ImplItemFn],
) -> syn::Result<proc_macro2::TokenStream> {
    let dispatch_arms = generate_dispatch_arms(krate, rpcs, false, None, false);
    let client_methods = generate_client_methods(krate, rpcs);
    let method_impls = generate_method_impls(original_methods);
    let struct_name_str = struct_name.to_string();
    let has_durable = rpcs.iter().any(|r| r.has_durable_context);

    let trait_infos = trait_infos_from_paths(traits);
    let has_traits = !trait_infos.is_empty();
    let entity_impl = if has_traits {
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
    let durable_field = if has_durable {
        quote! {
            __workflow_engine: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowEngine>>,
        }
    } else {
        quote! {}
    };
    let durable_field_init = if has_durable {
        quote! { __workflow_engine: ctx.workflow_engine.clone(), }
    } else {
        quote! {}
    };
    let trait_dispatch_checks: Vec<proc_macro2::TokenStream> = trait_infos
        .iter()
        .map(|info| {
            let field = &info.field;
            quote! {
                if let ::std::option::Option::Some(ref __trait) = self.#field {
                    if let ::std::option::Option::Some(response) = __trait.__dispatch(tag, payload, headers).await? {
                        return ::std::result::Result::Ok(response);
                    }
                }
            }
        })
        .collect();

    let trait_dispatch_fallback = if has_traits {
        quote! {{
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
        let trait_option_fields = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                let path = &info.path;
                quote! { #field: ::std::option::Option<#path>, }
            })
            .collect::<Vec<_>>();

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

        let trait_field_init_none_tokens = trait_infos
            .iter()
            .map(|info| {
                let field = &info.field;
                quote! { #field: ::std::option::Option::None, }
            })
            .collect::<Vec<_>>();

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

    // Generate register method - different signatures depending on whether traits are used
    let register_impl = if has_traits {
        // With traits: register takes trait dependencies as parameters
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
    } else {
        // Without traits: simple register
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

    Ok(quote! {
        #(#trait_use_tokens)*

        impl #struct_name {
            #(#method_impls)*
        }

        /// Generated handler for the entity.
        #[doc(hidden)]
        pub struct #handler_name {
            entity: #struct_name,
            #[allow(dead_code)]
            ctx: #krate::entity::EntityContext,
            #(#trait_field_defs)*
            #durable_field
        }

        impl #handler_name {
            #[doc(hidden)]
            pub async fn __new(entity: #struct_name, ctx: #krate::entity::EntityContext) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                ::std::result::Result::Ok(Self {
                    entity,
                    #(#trait_field_init_none)*
                    #durable_field_init
                    ctx,
                })
            }

            #[doc(hidden)]
            pub async fn __new_with_traits(
                entity: #struct_name,
                #(#trait_params,)*
                ctx: #krate::entity::EntityContext,
            ) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                #(#trait_state_inits)*
                ::std::result::Result::Ok(Self {
                    entity,
                    #(#trait_field_init_some)*
                    #durable_field_init
                    ctx,
                })
            }
        }

        #[async_trait::async_trait]
        impl #krate::entity::EntityHandler for #handler_name {
            async fn handle_request(
                &self,
                tag: &str,
                payload: &[u8],
                headers: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<::std::vec::Vec<u8>, #krate::error::ClusterError> {
                let _ = headers;
                match tag {
                    #(#dispatch_arms,)*
                    _ => #trait_dispatch_fallback,
                }
            }
        }

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

        #register_impl
        #with_traits_impl
        #entity_impl

        #(#trait_access_impls)*
    })
}

/// Generate code for stateful entities (with `#[state(...)]`).
#[allow(clippy::too_many_arguments)]
fn generate_stateful_entity(
    krate: &syn::Path,
    struct_name: &syn::Ident,
    handler_name: &syn::Ident,
    client_name: &syn::Ident,
    state_type: &syn::Type,
    state_persisted: bool,
    traits: &[syn::Path],
    rpcs: &[RpcMethod],
    original_methods: &[syn::ImplItemFn],
) -> syn::Result<proc_macro2::TokenStream> {
    let trait_infos = trait_infos_from_paths(traits);
    let has_traits = !trait_infos.is_empty();
    let entity_impl = if has_traits {
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
    let save_state_code = if state_persisted {
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
        has_traits && state_persisted,
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

            // Wrapper with activity scope if needed
            if is_activity && state_persisted {
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
    };

    // Generate delegation methods for view structs
    // These let user code call handler methods directly on self instead of self.__handler
    let view_delegation_methods = if state_persisted {
        quote! {
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
        }
    } else {
        quote! {}
    };

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
                    let __result = self.__handler.#method_name(#(#param_names),*).await;
                    // Refresh state from ArcSwap after activity commits
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

    // init goes on the entity struct itself
    let init_method = original_methods
        .iter()
        .find(|m| m.sig.ident == "init")
        .unwrap();
    let init_sig = &init_method.sig;
    let init_block = &init_method.block;
    let init_attrs = &init_method.attrs;
    let init_vis = &init_method.vis;

    let struct_name_str = struct_name.to_string();
    let _state_wrapper_name = format_ident!("{}StateWrapper", struct_name);

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

    let composite_state_defs = if state_persisted && has_traits {
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

    // Generate state loading code for persisted state (entity-only)
    let state_init_code = if state_persisted {
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
        quote! {
            let state: #state_type = entity.init(&ctx)?;
        }
    };

    let state_init_with_traits_code = if has_traits {
        if state_persisted {
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
            quote! {
                let state: #state_type = entity.init(&ctx)?;
                #(#trait_state_inits)*
            }
        }
    } else {
        quote! { #state_init_code }
    };

    // For persisted-state entities, we always need the workflow engine for built-in methods
    // (sleep, await_deferred, resolve_deferred, on_interrupt)
    let has_durable = state_persisted || rpcs.iter().any(|r| r.has_durable_context);

    // For persisted state, the handler needs to store the storage reference
    let handler_storage_field = if state_persisted {
        quote! {
            __state_storage: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowStorage>>,
            __state_key: ::std::string::String,
        }
    } else {
        quote! {}
    };

    let handler_storage_init = if state_persisted {
        quote! {
            let __state_key = ::std::format!(
                "entity/{}/{}/state",
                ctx.address.entity_type.0,
                ctx.address.entity_id.0,
            );
            let __state_storage = ctx.state_storage.clone();
        }
    } else {
        quote! {}
    };

    let handler_storage_fields_init = if state_persisted {
        quote! {
            __state_storage,
            __state_key,
        }
    } else {
        quote! {}
    };

    let durable_field = if has_durable {
        quote! {
            __workflow_engine: ::std::option::Option<::std::sync::Arc<dyn #krate::__internal::WorkflowEngine>>,
        }
    } else {
        quote! {}
    };
    let durable_field_init = if has_durable {
        quote! { __workflow_engine: ctx.workflow_engine.clone(), }
    } else {
        quote! {}
    };

    // Generate built-in durable methods (sleep, await_deferred, resolve_deferred) for persisted-state entities
    // These are now generated as part of the Handler impl block
    let durable_builtin_impls = if state_persisted {
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
    } else {
        quote! {}
    };

    // Generate sharding helper methods for persisted-state entities
    // These are now generated as part of the Handler impl block
    let sharding_builtin_impls = if state_persisted {
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
    } else {
        quote! {}
    };

    // Build the DurableContext for persisted-state entities
    // Note: These wrapper fields are kept for future trait system migration
    let _durable_ctx_wrapper_field = if state_persisted {
        quote! {
            /// Built-in durable context for `sleep()`, `await_deferred()`, `resolve_deferred()`.
            __durable_ctx: ::std::option::Option<#krate::__internal::DurableContext>,
        }
    } else {
        quote! {}
    };

    let _durable_ctx_wrapper_init = if state_persisted {
        quote! {
            let __durable_ctx = ctx.workflow_engine.as_ref().map(|engine| {
                #krate::__internal::DurableContext::new(
                    ::std::sync::Arc::clone(engine),
                    ctx.address.entity_type.0.clone(),
                    ctx.address.entity_id.0.clone(),
                )
            });
        }
    } else {
        quote! {}
    };

    let _durable_ctx_wrapper_field_init = if state_persisted {
        quote! { __durable_ctx, }
    } else {
        quote! {}
    };

    // Build sharding context field for persisted-state entities
    // Note: These wrapper fields are kept for future trait system migration
    let _sharding_ctx_wrapper_field = if state_persisted {
        quote! {
            /// Sharding interface for inter-entity communication and scheduled messages.
            __sharding: ::std::option::Option<::std::sync::Arc<dyn #krate::sharding::Sharding>>,
            /// Entity address for self-referencing in scheduled messages.
            __entity_address: #krate::types::EntityAddress,
        }
    } else {
        quote! {}
    };

    let _sharding_ctx_wrapper_init = if state_persisted {
        quote! {
            let __sharding = ctx.sharding.clone();
            let __entity_address = ctx.address.clone();
        }
    } else {
        quote! {}
    };

    let _sharding_ctx_wrapper_field_init = if state_persisted {
        quote! { __sharding, __entity_address, }
    } else {
        quote! {}
    };

    // Sharding fields on Handler (for the new ArcSwap-based design)
    let sharding_handler_field = if state_persisted {
        quote! {
            /// Sharding interface for inter-entity communication.
            __sharding: ::std::option::Option<::std::sync::Arc<dyn #krate::sharding::Sharding>>,
            /// Entity address for self-referencing.
            __entity_address: #krate::types::EntityAddress,
        }
    } else {
        quote! {}
    };

    // For persisted state, spawn must be async (to load from storage)
    let new_fn = if state_persisted {
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
    } else {
        quote! {
            #[doc(hidden)]
            pub async fn __new(entity: #struct_name, ctx: #krate::entity::EntityContext) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                #state_init_code
                ::std::result::Result::Ok(Self {
                    __state: ::std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(state)),
                    __write_lock: ::std::sync::Arc::new(tokio::sync::Mutex::new(())),
                    __entity: entity,
                    #durable_field_init
                    ctx,
                    #(#trait_field_init_none)*
                })
            }
        }
    };

    let new_with_traits_fn = if has_traits {
        if state_persisted {
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
            quote! {
                #[doc(hidden)]
                pub async fn __new_with_traits(
                    entity: #struct_name,
                    #(#trait_params,)*
                    ctx: #krate::entity::EntityContext,
                ) -> ::std::result::Result<Self, #krate::error::ClusterError> {
                    #state_init_with_traits_code
                    ::std::result::Result::Ok(Self {
                        __state: ::std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(state)),
                        __write_lock: ::std::sync::Arc::new(tokio::sync::Mutex::new(())),
                        __entity: entity,
                        #durable_field_init
                        ctx,
                        #(#trait_field_init_some)*
                    })
                }
            }
        }
    } else {
        quote! {}
    };

    // Generate composite state save method for entities with traits and persistent state
    let save_composite_state_method = if has_traits && state_persisted {
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
            // Save composite state after successful trait dispatch if entity has persistent state
            let save_after_dispatch = if state_persisted {
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

    let trait_dispatch_fallback = if has_traits {
        quote! {{
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

    // Generate register method - different signatures depending on whether traits are used
    let register_impl = if has_traits {
        // With traits: register takes trait dependencies as parameters
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
    } else {
        // Without traits: simple register
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

    Ok(quote! {
        #(#trait_use_tokens)*

        // Emit `init` on the entity struct.
        impl #struct_name {
            #(#init_attrs)*
            #init_vis #init_sig #init_block
        }

        #composite_state_defs

        #with_traits_impl
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
        }

        impl #handler_name {
            #new_fn
            #new_with_traits_fn

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
                let _ = headers;
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

            if stateful {
                // Call method directly on self - method uses state() or state_mut() internally
                quote! {
                    #tag => {
                        #deserialize_request
                        #durable_ctx_code
                        let response = self.#method_name(#call_args).await?;
                        #post_call_save
                        rmp_serde::to_vec(&response)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })
                    }
                }
            } else {
                // Stateless — call directly on entity
                quote! {
                    #tag => {
                        #deserialize_request
                        #durable_ctx_code
                        let response = self.entity.#method_name(#call_args).await?;
                        rmp_serde::to_vec(&response)
                            .map_err(|e| #krate::error::ClusterError::MalformedMessage {
                                reason: ::std::format!("failed to serialize response for '{}': {e}", #tag),
                                source: ::std::option::Option::Some(::std::boxed::Box::new(e)),
                            })
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
            if rpc.kind.is_persisted() {
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

fn generate_method_impls(original_methods: &[syn::ImplItemFn]) -> Vec<proc_macro2::TokenStream> {
    original_methods
        .iter()
        .map(|m| {
            let sig = &m.sig;
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
            quote! {
                #(#attrs)*
                #vis #sig #block
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

            if rpc.kind.is_persisted() {
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
    persistent: bool,
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

        Ok(StateArgs {
            ty,
            persistent: true,
        })
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

fn parse_kind_attr(
    attrs: &[syn::Attribute],
) -> syn::Result<Option<(RpcKind, Option<syn::ExprClosure>)>> {
    let mut kind: Option<RpcKind> = None;
    let mut key: Option<syn::ExprClosure> = None;

    for attr in attrs {
        if attr.path().is_ident("rpc") {
            if kind.is_some() {
                return Err(syn::Error::new(attr.span(), "multiple RPC kind attributes"));
            }
            match &attr.meta {
                syn::Meta::Path(_) => {
                    kind = Some(RpcKind::Rpc);
                }
                _ => {
                    return Err(syn::Error::new(
                        attr.span(),
                        "#[rpc] does not take arguments",
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
                syn::Meta::Path(_) => KeyArgs { key: None },
                syn::Meta::List(_) => attr.parse_args::<KeyArgs>()?,
                syn::Meta::NameValue(_) => {
                    return Err(syn::Error::new(
                        attr.span(),
                        "expected #[activity] or #[activity(key(...))]",
                    ))
                }
            };
            kind = Some(RpcKind::Activity);
            if args.key.is_some() {
                key = args.key;
            }
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

    Ok(kind.map(|kind| (kind, key)))
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

    let (kind, persist_key) = match kind_info {
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
