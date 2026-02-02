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
