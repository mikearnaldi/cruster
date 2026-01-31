---
paths: crates/**/*.rs
---
# Rust Development

## Crate Architecture

- **cruster/** - Main framework crate
  - `entity.rs` - Entity trait and EntityHandler
  - `entity_client.rs` - Typed client for entity communication
  - `sharding.rs` - Sharding interface for distributed entities
  - `durable.rs` - Durable workflow engine
  - `state_guard.rs` - Transactional state mutation guards
  - `transport/grpc.rs` - gRPC-based cluster transport
  - `storage/` - Storage backends (memory, SQL, etcd)
- **cruster-macros/** - Procedural macros
  - `#[entity]` - Entity struct definition
  - `#[entity_impl]` - Entity implementation with RPCs/activities
  - `#[entity_trait]` - Composable entity trait
  - `#[entity_trait_impl]` - Trait implementation

## Key Patterns

### Entity Definition

```rust
use cruster::prelude::*;
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Default)]
struct MyState {
    value: i32,
}

#[entity]
#[derive(Clone)]
struct MyEntity;

#[entity_impl]
#[state(MyState)]
impl MyEntity {
    fn init(&self, _ctx: &EntityContext) -> Result<MyState, ClusterError> {
        Ok(MyState::default())
    }

    #[activity]
    async fn mutate(&mut self, delta: i32) -> Result<i32, ClusterError> {
        self.state.value += delta;
        Ok(self.state.value)
    }

    #[rpc]
    async fn get(&self) -> Result<i32, ClusterError> {
        Ok(self.state.value)
    }
}
```

### Entity Traits

```rust
#[entity_trait]
#[derive(Clone)]
struct Auditable;

#[entity_trait_impl]
#[state(AuditLog)]
impl Auditable {
    fn init(&self) -> Result<AuditLog, ClusterError> {
        Ok(AuditLog::default())
    }

    #[activity]
    #[protected]  // Visible to entities using this trait
    async fn log_action(&mut self, action: String) -> Result<(), ClusterError> {
        self.state.entries.push(action);
        Ok(())
    }
}

// Use trait in entity
#[entity_impl(traits(Auditable))]
#[state(MyState)]
impl MyEntity { ... }
```

### Error Handling

Use `ClusterError` for entity errors:

```rust
use cruster::error::ClusterError;

#[rpc]
async fn validate(&self, input: String) -> Result<(), ClusterError> {
    if input.is_empty() {
        return Err(ClusterError::InvalidRequest {
            reason: "input cannot be empty".to_string(),
        });
    }
    Ok(())
}
```

## Testing

```bash
# Run all cruster tests
cargo test -p cruster --lib

# Run specific test
cargo test -p cruster --lib test_name

# Run with output
cargo test -p cruster --lib -- --nocapture
```
