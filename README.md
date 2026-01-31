# Cruster

A Rust framework for building distributed, stateful entity systems with durable workflows.

## Features

- **Entity System**: Define stateful actors with automatic persistence
- **Durable Workflows**: Long-running operations that survive restarts
- **Entity Traits**: Composable behaviors that can be mixed into entities
- **Activity Pattern**: Safe state mutations with transactional guarantees
- **gRPC Transport**: Built-in cluster communication
- **Storage**: PostgreSQL for persistence, etcd for cluster formation & health monitoring

## Quick Start

```rust
use cruster::prelude::*;
use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Default)]
struct CounterState {
    count: i32,
}

#[entity]
#[derive(Clone)]
struct Counter;

#[entity_impl]
#[state(CounterState)]
impl Counter {
    fn init(&self, _ctx: &EntityContext) -> Result<CounterState, ClusterError> {
        Ok(CounterState::default())
    }

    #[activity]
    async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
        self.state.count += amount;
        Ok(self.state.count)
    }

    #[rpc]
    async fn get_count(&self) -> Result<i32, ClusterError> {
        Ok(self.state.count)
    }
}
```

## Method Types

- `#[rpc]` - Read-only operations, publicly callable
- `#[activity]` - State mutations, requires `&mut self`
- `#[workflow]` - Orchestrates activities, publicly callable
- `#[method]` - Private sync helpers

## Examples

- `examples/cluster-tests` - E2E test suite demonstrating all features
- `examples/chess-cluster` - Distributed chess server with matchmaking

## License

MIT
