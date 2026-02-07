## 0.0.15 (2026-02-07)

### Fixes

#### feat: add activity journaling for crash-recovery replay safety

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

## 0.0.14 (2026-02-07)

### Fixes

#### fix: prevent infinite gRPC self-loop in shard routing

When `shard_assignments` marked a shard as owned by the local runner but `owned_shards` had not yet been populated (during the acquire phase window), `send()`, `notify()`, and `interrupt()` would route the message as "remote" via gRPC to the local runner itself. The gRPC handler called back into `sharding.send()`, hitting the same code path, creating an infinite loop that grew memory at ~350MB/s.

Added self-send guards to all three routing methods: when `get_shard_owner_async()` returns the local runner's address, route locally instead of making a gRPC call. Also handles persisted messages correctly in the self-routed path.

Concurrent `acquire_batch`/`refresh_batch` using `FuturesUnordered` with a concurrency limit of 64, reducing 2048-shard acquisition from 30+ seconds to under 5 seconds.

#### fix: resolve stale state in workflows after activity calls

`self.state` in `#[workflow]` and `#[rpc]` methods previously captured a
point-in-time snapshot at method entry via `ArcSwap::load()`. After calling
an activity that mutated state, subsequent reads of `self.state` within the
same workflow would still return pre-activity values.

Introduces `StateRef<S>`, a read-only proxy wrapping `Arc<S>` that implements
`Deref<Target=S>`. The macro-generated activity delegation methods now refresh
the proxy after each activity commits, so `self.state.field` always reflects
the latest committed state. User-facing syntax is unchanged.

## 0.0.13 (2026-02-06)

### Features

- Upgrade sqlx dependency from 0.7 to 0.8, resolving the future-incompatibility warning from sqlx-postgres v0.7.4. This includes security fixes (RUSTSEC-2024-0363), rustls 0.23 upgrade, and various bug fixes.

## 0.0.12 (2026-02-03)

### Fixes

#### test: harden cluster-tests e2e suite and add shard topology coverage

Adds shard allocation/lease tests, per-node health gating, and more resilient
helpers for timing and transient HTTP failures.

## 0.0.11 (2026-02-03)

### Fixes

#### Improved shard distribution balance in rendezvous hashing

Fixes imbalanced shard distribution in rendezvous hashing by switching to a higher-quality hash function.

### Problem

The previous implementation used DJB2 hash with XOR combination, which produced poor distribution across nodes. In production with 3 nodes and 2048 shards, this resulted in up to 11% imbalance (731 vs 655 shards per node).

### Solution

- Added `hash64()` function based on xxHash algorithm with excellent avalanche properties
- Changed `compute_runner_score()` to hash the concatenated key directly instead of XOR-ing separate hashes

### Results (3 nodes, 2048 shards)

| Metric | Before | After |
|--------|--------|-------|
| Distribution | 731, 662, 655 | 689, 683, 676 |
| Max difference | 76 shards | 13 shards |
| Imbalance | 11.1% | 1.9% |

This achieves ~6x better balance with no performance impact.

## 0.0.10 (2026-02-03)

### Features

#### Configurable shard assignment with rendezvous hashing

Implements configurable shard assignment strategies for better distribution and performance.

### New Features

#### ShardAssignmentStrategy enum

Choose between different shard assignment algorithms:

- **Rendezvous** (default): Near-perfect distribution with O(shards × nodes) complexity
- **RendezvousParallel**: Same as Rendezvous but parallelized with Rayon (8x faster for 100+ nodes)
- **ConsistentHash**: Ring-based consistent hashing with configurable vnodes (requires `consistent-hash` feature)

#### Benchmark Results (2048 shards, 100 nodes)

| Strategy | Time |
|----------|------|
| RendezvousParallel | 1.35 ms |
| Rendezvous | 10.9 ms |
| ConsistentHash | 175 ms |

### Configuration

```rust
use cruster::shard_assigner::ShardAssignmentStrategy;
use cruster::config::ShardingConfig;

let config = ShardingConfig {
    assignment_strategy: ShardAssignmentStrategy::RendezvousParallel,
    // ...
};
```

### Feature Flags

- `parallel`: Enables `RendezvousParallel` strategy (requires `rayon`)
- `consistent-hash`: Enables `ConsistentHash` strategy (requires `hashring`)

## 0.0.9 (2026-02-02)

### Fixes

#### fix: optimize etcd shutdown from O(shards) to O(1) by revoking lease

Previously `release_all()` deleted shard lock keys one-by-one with 2048 sequential etcd transactions, taking ~34 seconds. Now we simply revoke the runner's lease, which atomically deletes all attached keys (runner registration + all shard locks) in a single O(1) operation, reducing shutdown to milliseconds.

## 0.0.8 (2026-02-02)

### Fixes

#### fix: reorder shutdown to release shard locks after background tasks stop

Moves `release_all()` and shard clearing to execute AFTER waiting for background tasks to complete. This prevents concurrent etcd operations from the rebalance/refresh loops while the lengthy shard release is in progress, reducing shutdown time from ~32 seconds to ~17 seconds.

## 0.0.7 (2026-02-02)

### Fixes

#### Fix shutdown not cancelling in-flight network calls

- Network calls in background loops (get_runners, release, acquire_batch, refresh_batch) now race against the cancellation token using `tokio::select!`
- Fixed runner_health_loop singleton to use its own cancellation token from SingletonContext instead of ignoring it
- Added cancellation checks throughout check_runner_health including during concurrent health checks

Previously, even with cancellation checks between operations, a slow or hanging network call could block shutdown for the full timeout duration. The runner health singleton was also ignoring its cancellation context entirely.

## 0.0.6 (2026-02-02)

### Fixes

#### Fix shutdown hanging due to background tasks not respecting cancellation

Background loops (shard_acquisition_loop, lock_refresh_loop, storage_poll_loop) now check the cancellation token at key points during their work, not just at sleep boundaries. This ensures shutdown completes promptly instead of hanging while tasks continue making gRPC calls.

## 0.0.5 (2026-02-02)

### Features

- add SQL transaction support for activities
- add SingletonContext for graceful singleton shutdown

### Fixes

- correct visibility modifier documentation in README
- correct entity registration examples in README
- knope release workflow - create branch before PR
- knope config - use release branch and correct variable syntax
- knope config - use git checkout -B for release branch
- knope config - split git add and commit into separate steps
- add graceful_shutdown_at to singleton test
- sync cruster-macros version via workspace dependencies
- include all workspace crates in Cargo.lock versioning
- sync Cargo.lock versions with workspace

#### Fix Cargo.lock versioning for all workspace crates

Configure knope to update all workspace crate versions in Cargo.lock during releases, not just `cruster`. This ensures CI passes with `--locked` flag after version bumps.

## 0.0.4 (2026-02-02)

### Features

- add SQL transaction support for activities
- add SingletonContext for graceful singleton shutdown

### Fixes

- correct visibility modifier documentation in README
- correct entity registration examples in README
- knope release workflow - create branch before PR
- knope config - use release branch and correct variable syntax
- knope config - use git checkout -B for release branch
- knope config - split git add and commit into separate steps
- add graceful_shutdown_at to singleton test
- sync cruster-macros version via workspace dependencies

#### Fix cruster-macros version sync in release workflow

Move `cruster-macros` dependency to workspace dependencies and configure knope to automatically bump its version during releases. This ensures the `cruster-macros` version stays in sync with the workspace version when publishing to crates.io.

- Add `cruster-macros` to `[workspace.dependencies]` in root `Cargo.toml`
- Update `crates/cruster/Cargo.toml` to use `cruster-macros = { workspace = true }`
- Configure `knope.toml` to version the `cruster-macros` dependency in workspace

## 0.0.3 (2026-02-02)

### Features

- add SQL transaction support for activities
- add SingletonContext for graceful singleton shutdown

### Fixes

- correct visibility modifier documentation in README
- correct entity registration examples in README
- knope release workflow - create branch before PR
- knope config - use release branch and correct variable syntax
- knope config - use git checkout -B for release branch
- knope config - split git add and commit into separate steps
- add graceful_shutdown_at to singleton test

#### Add `SingletonContext` to singleton factory functions for graceful shutdown.

Singleton factories now receive a `SingletonContext` which provides opt-in graceful shutdown. By calling `ctx.cancellation()`, the singleton commits to observing the cancellation token and returning when cancelled. The runtime will wait for singletons that opt-in; singletons that don't will be force-cancelled.

**Key behavior:** When a shard is lost (node failure or rebalancing), the runtime now waits for managed singletons to complete gracefully before allowing another node to start a new instance. This ensures the "exactly one singleton" guarantee is maintained during failover.

**Usage:**

```rust
use cruster::singleton::{register_singleton, SingletonContext};

register_singleton(&*sharding, "my-singleton", |ctx: SingletonContext| async move {
    // Opt-in to graceful shutdown
    let cancel = ctx.cancellation();
    
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // Graceful shutdown - clean up resources
                break;
            }
            _ = do_work() => {}
        }
    }
    Ok(())
}).await?;
```

**API:**

- `ctx.cancellation()` - Returns a `CancellationToken` and opts the singleton into graceful shutdown. The runtime will wait for the singleton to return instead of force-cancelling it.
- `ctx.is_cancelled()` - Check if cancellation was requested without opting in to manage it.

**Breaking Change:** Singleton factory closures must now accept a `SingletonContext` parameter. For singletons that don't need graceful shutdown, simply ignore the parameter with `|_ctx|`.

#### Add support for executing arbitrary SQL within activity transactions.

Activities can now execute custom SQL statements that are committed or rolled back together with entity state changes, ensuring atomicity.

**Usage:**

```rust
##[activity]
async fn transfer(&mut self, to: String, amount: i64) -> Result<(), ClusterError> {
    // State mutation (automatically transactional)
    self.state.balance -= amount;
    
    // Execute arbitrary SQL in the same transaction
    if let Some(tx) = ActivityScope::sql_transaction().await {
        tx.execute(
            sqlx::query("INSERT INTO transfers (...) VALUES ($1, $2, $3)")
                .bind(&self.state.entity_id)
                .bind(&to)
                .bind(amount)
        ).await?;
    }
    
    Ok(())
}
```

**API:**

- `ActivityScope::sql_transaction()` - Returns `Option<SqlTransactionHandle>` (requires `sql` feature)
- `SqlTransactionHandle::execute()` - Execute a SQL statement
- `SqlTransactionHandle::fetch_one()` - Fetch exactly one row
- `SqlTransactionHandle::fetch_optional()` - Fetch zero or one row
- `SqlTransactionHandle::fetch_all()` - Fetch all rows

Returns `None` when using non-SQL storage backends (e.g., memory storage for testing).
