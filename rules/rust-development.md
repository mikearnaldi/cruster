# Rust Development

## Crate Architecture

- **cruster/** — Main framework crate
  - `entity.rs` — Entity trait and EntityHandler
  - `entity_client.rs` — Typed client for entity communication
  - `sharding_impl.rs` — Sharding implementation for distributed entities
  - `transport/grpc.rs` — gRPC-based cluster transport
  - `storage/sql_message.rs` — SQL message storage
  - `storage/sql_workflow.rs` — SQL workflow/journal storage
  - `storage/sql_workflow_engine.rs` — SQL workflow engine
  - `storage/etcd_runner.rs` — etcd runner discovery
- **cruster-macros/** — Procedural macros
  - `#[entity]` / `#[entity_impl]` — Entity definition with RPCs
  - `#[workflow]` / `#[workflow_impl]` — Standalone durable workflows
  - `#[activity_group]` / `#[activity_group_impl]` — Composable activity bundles
  - `#[rpc_group]` / `#[rpc_group_impl]` — Composable RPC bundles

## Entity Pattern (Stateless RPC Handler)

```rust
use cruster::prelude::*;
use sqlx::PgPool;

#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
struct Counter { pool: PgPool }

#[entity_impl]
impl Counter {
    #[rpc(persisted)]  // at-least-once (writes)
    pub async fn increment(&self, req: IncrementRequest) -> Result<i64, ClusterError> {
        let row: (i64,) = sqlx::query_as("INSERT INTO counters ...")
            .bind(&req.entity_id).bind(req.amount)
            .fetch_one(&self.pool).await?;
        Ok(row.0)
    }

    #[rpc]  // best-effort (reads)
    pub async fn get(&self, req: GetRequest) -> Result<i64, ClusterError> { ... }
}
```

## Workflow Pattern (Durable Orchestration)

```rust
#[workflow]
#[derive(Clone)]
struct OrderWorkflow;

#[workflow_impl(key = |req: &OrderRequest| req.order_id.clone(), hash = false)]
impl OrderWorkflow {
    async fn execute(&self, req: OrderRequest) -> Result<OrderResult, ClusterError> {
        self.reserve(req.order_id.clone(), req.count).await?;
        self.charge(req.order_id.clone(), req.amount).await?;
        Ok(OrderResult { status: "done".into() })
    }

    #[activity]
    async fn reserve(&self, order_id: String, count: u32) -> Result<(), ClusterError> {
        sqlx::query("INSERT INTO reservations ...")
            .execute(&self.tx)  // committed atomically with journal
            .await?;
        Ok(())
    }
}
```

## Error Handling

Use `ClusterError` (already in prelude):

```rust
Err(ClusterError::PersistenceError {
    reason: format!("operation failed: {e}"),
    source: Some(Box::new(e)),
})
```
