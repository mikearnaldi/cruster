# Cruster

A Rust framework for building distributed, stateful entity systems with durable workflows.

Cruster provides a distributed actor/entity model with consistent-hashing-based shard assignment, persistent messaging, singleton management, durable workflows, and cron scheduling.

## Features

- **Entity System** - Define stateful actors with automatic persistence and lifecycle management
- **Durable Workflows** - Long-running operations that survive crashes and restarts
- **Activity Journaling** - State mutations with transactional guarantees and replay safety
- **Entity Traits** - Composable behaviors that can be mixed into multiple entities
- **Singletons** - Cluster-wide unique instances with automatic failover
- **Scheduled Tasks** - Cron-based scheduling with distributed coordination
- **gRPC Transport** - Built-in inter-node communication with streaming support
- **Storage** - PostgreSQL for persistence, etcd for cluster formation & health monitoring

## Installation

### Prerequisites

Cruster requires the Protocol Buffers compiler (`protoc`) to be installed for building gRPC support:

```bash
# Debian/Ubuntu
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Windows (with Chocolatey)
choco install protoc
```

### Cargo

Add to your `Cargo.toml`:

```toml
[dependencies]
cruster = "0.1"
cruster-macros = "0.1"
```

## Quick Start

### Defining an Entity

```rust
use cruster::prelude::*;
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use serde::{Deserialize, Serialize};

// Define the entity's state
#[derive(Clone, Serialize, Deserialize, Default)]
struct CounterState {
    count: i32,
}

// Define the entity
#[entity]
#[derive(Clone)]
struct Counter;

#[entity_impl]
#[state(CounterState)]
impl Counter {
    // Initialize state when entity is first created
    fn init(&self, _ctx: &EntityContext) -> Result<CounterState, ClusterError> {
        Ok(CounterState::default())
    }

    // Activity: mutates state, journaled for replay
    #[activity]
    async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
        self.state.count += amount;
        Ok(self.state.count)
    }

    // RPC: read-only, publicly callable
    #[rpc]
    async fn get_count(&self) -> Result<i32, ClusterError> {
        Ok(self.state.count)
    }
}
```

### Using the Entity

```rust
use cruster::testing::TestCluster;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an in-memory test cluster
    let cluster = TestCluster::new().await;
    
    // Register the entity and get a typed client
    let client = cluster.register(Counter).await?;
    
    // Call methods on the entity
    let count = client.call("counter-1", "increment", 5).await?;
    println!("Count: {}", count); // Count: 5
    
    let count = client.call("counter-1", "get_count", ()).await?;
    println!("Count: {}", count); // Count: 5
    
    Ok(())
}
```

## Core Concepts

### Method Types

| Annotation | Purpose | Self Type | Journaled | Callable From |
|------------|---------|-----------|-----------|---------------|
| `#[rpc]` | Read-only operations | `&self` | No | External clients |
| `#[activity]` | State mutations | `&mut self` | Yes | Workflows, other activities |
| `#[workflow]` | Orchestrate activities | `&self` or `&mut self` | Yes | External clients |
| `#[method]` | Private helpers | Any | No | Internal only |

### Visibility Modifiers

- `#[public]` - Callable from generated client (default for `#[rpc]` and `#[workflow]`)
- `#[protected]` - Callable within the entity and by composed traits
- `#[private]` - Callable only within the defining scope (default for `#[activity]`)

### State Persistence

All entity state is automatically persisted. When an entity is evicted from memory (due to idle timeout) and later reactivated, its state is restored from storage.

```rust
#[entity_impl]
#[state(MyState)]  // State is always persisted
impl MyEntity {
    fn init(&self, _ctx: &EntityContext) -> Result<MyState, ClusterError> {
        Ok(MyState::default())
    }
}
```

### Durable Workflows

Workflows orchestrate multiple activities and survive crashes. On restart, workflows replay from their journal, skipping already-completed activities.

```rust
#[entity_impl]
#[state(OrderState)]
impl OrderProcessor {
    #[workflow]
    async fn process_order(&mut self, order_id: String) -> Result<(), ClusterError> {
        // Each activity is journaled - if we crash and restart,
        // completed activities return their cached results
        self.validate_order(&order_id).await?;
        self.charge_payment(&order_id).await?;
        self.ship_order(&order_id).await?;
        self.send_confirmation(&order_id).await?;
        Ok(())
    }

    #[activity]
    async fn validate_order(&mut self, order_id: &str) -> Result<(), ClusterError> {
        // Validation logic...
        Ok(())
    }

    #[activity]
    async fn charge_payment(&mut self, order_id: &str) -> Result<(), ClusterError> {
        // Payment logic - only executed once even on replay
        Ok(())
    }
    
    // ... more activities
}
```

### Idempotency Keys

Activities and workflows are deduplicated by their idempotency key. The default key is `hash(method_name, serialized_params)`.

Custom keys can be specified:

```rust
#[workflow(key(|order_id, _body| order_id))]
async fn send_email(&mut self, order_id: OrderId, body: String) -> Result<(), ClusterError> {
    // Same order_id always returns cached result
    Ok(())
}
```

### Entity Traits

Traits provide reusable behaviors that can be composed into entities:

```rust
// Define a trait
#[entity_trait]
pub trait Auditable {
    #[rpc]
    async fn get_audit_log(&self, limit: usize) -> Result<Vec<AuditEntry>, ClusterError>;
    
    #[activity]
    async fn log_audit(&mut self, action: String, actor: String) -> Result<(), ClusterError>;
}

// Compose into an entity
#[entity_impl(traits(Auditable))]
#[state(MyState)]
impl MyEntity {
    // Entity methods...
}
```

### Singletons

Singletons are cluster-wide unique instances. Only one instance runs at a time, with automatic failover if the hosting node fails.

```rust
use cruster::singleton::singleton;

// Register a singleton using the builder API
singleton("leaderboard", || async {
    // Singleton logic runs on exactly one node
    loop {
        // Process leaderboard updates...
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
})
.register(&*sharding)
.await?;

// Or use the direct function
use cruster::singleton::register_singleton;

register_singleton(&*sharding, "metrics-collector", || async {
    loop {
        collect_metrics().await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}).await?;
```

### Deferreds (Async Coordination)

Deferreds allow workflows to wait for signals from other parts of the system:

```rust
#[entity_impl]
#[state(ApprovalState)]
impl ApprovalWorkflow {
    #[workflow]
    async fn wait_for_approval(&mut self, request_id: String) -> Result<bool, ClusterError> {
        let signal_name = format!("approval/{}", request_id);
        
        // This suspends the workflow until resolved
        let approved: bool = self.await_deferred(&signal_name).await?;
        
        Ok(approved)
    }

    // Called by another workflow or RPC to resolve the deferred
    #[activity]
    async fn approve_request(&mut self, request_id: String) -> Result<(), ClusterError> {
        let signal_name = format!("approval/{}", request_id);
        self.resolve_deferred(&signal_name, &true).await?;
        Ok(())
    }
}
```

### Timers and Sleep

Durable sleep that survives restarts:

```rust
#[workflow]
async fn delayed_notification(&mut self) -> Result<(), ClusterError> {
    // Sleep is persisted - if we restart, we resume from where we left off
    self.sleep(Duration::from_secs(3600)).await?;
    self.send_notification().await?;
    Ok(())
}
```

## Cluster Configuration

### Single Node (Development)

```rust
use cruster::single_runner::SingleRunner;

let runner = SingleRunner::new(postgres_pool).await?;
runner.register(MyEntity).await?;
runner.start().await?;
```

### Multi-Node (Production)

```rust
use cruster::config::ShardingConfig;
use cruster::sharding_impl::ShardingImpl;

let config = ShardingConfig {
    runner_address: "10.0.0.1:9000".parse()?,
    shard_count: 128,
    ..Default::default()
};

let sharding = ShardingImpl::new(
    config,
    postgres_pool,
    etcd_runner_storage,
    grpc_runners,
).await?;

sharding.register_entity(Arc::new(MyEntity)).await?;
sharding.start().await?;
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `shard_count` | 128 | Number of shards per group |
| `max_idle_time` | 60s | Entity eviction timeout |
| `mailbox_capacity` | 1000 | Per-entity message queue size |
| `storage_poll_interval` | 100ms | Message storage polling frequency |
| `storage_message_max_retries` | 3 | Max delivery attempts before dead-letter |

## Storage

### PostgreSQL

Used for:
- Entity state persistence
- Message storage (at-least-once delivery)
- Workflow journals
- Timer and deferred value storage

Required tables are created via migrations in `migrations/`.

### etcd

Used for:
- Runner registration and discovery
- Shard lock acquisition
- Health monitoring
- Leader election

## Testing

### TestCluster

An in-memory cluster for unit tests:

```rust
use cruster::testing::TestCluster;

#[tokio::test]
async fn test_counter() {
    // Basic cluster
    let cluster = TestCluster::new().await;
    
    // With message storage for at-least-once delivery
    let cluster = TestCluster::with_message_storage().await;
    
    // Full workflow support
    let cluster = TestCluster::with_workflow_support().await;
    
    let client = cluster.register(Counter).await.unwrap();
    
    let result = client.call("test-1", "increment", 5).await.unwrap();
    assert_eq!(result, 5);
}
```

## Examples

### cluster-tests

E2E test suite demonstrating all features. Requires PostgreSQL and etcd:

```bash
# Start infrastructure (Postgres and etcd)
docker run -d --name postgres -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=cluster \
  postgres:16

docker run -d --name etcd -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.9 \
  /usr/local/bin/etcd \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-client-urls http://0.0.0.0:2379

# Run the test runner
cd examples/cluster-tests
POSTGRES_URL=postgres://postgres:postgres@localhost/cluster \
ETCD_ENDPOINTS=localhost:2379 \
RUNNER_ADDRESS=localhost:9000 \
cargo run

# In another terminal, run the bash tests:
./tests/e2e.sh
```

### chess-cluster

Distributed chess server demonstrating:
- Player sessions (in-memory state)
- Game state persistence (workflows)
- Matchmaking service (stateless entity)
- Leaderboard singleton
- Move timeouts (scheduled messages)
- Auditable trait composition

**Note:** The HTTP API layer (M3) is not yet implemented. Currently only the entity layer is complete and can be tested via `TestCluster`:

```bash
cd examples/chess-cluster
cargo test
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client                                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Sharding Layer                              │
│  - Consistent hashing for shard assignment                       │
│  - Request routing to correct runner                             │
│  - Storage polling for persisted messages                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│   Runner 1    │  │   Runner 2    │  │   Runner 3    │
│  Shards 0-42  │  │  Shards 43-85 │  │  Shards 86-127│
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                  │                  │
        └──────────────────┴──────────────────┘
                           │
            ┌──────────────┴──────────────┐
            ▼                             ▼
    ┌───────────────┐             ┌───────────────┐
    │   PostgreSQL  │             │     etcd      │
    │  - State      │             │  - Runners    │
    │  - Messages   │             │  - Shard locks│
    │  - Journals   │             │  - Health     │
    └───────────────┘             └───────────────┘
```

## API Reference

See [specs/architecture.md](specs/architecture.md) for detailed API documentation.

## License

MIT
