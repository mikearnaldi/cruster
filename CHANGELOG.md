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
