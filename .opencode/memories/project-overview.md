# Cruster

Cruster is a Rust framework for building distributed, stateful entity systems with durable workflows.

## Git Workflow

**Auto-commit is prohibited.** Do not automatically commit changes. Always wait for explicit user approval before committing.

## Project Structure

```
├── crates/                     # All Rust crates
│   ├── cruster/                # Main framework crate
│   │   ├── src/                # Source code
│   │   ├── migrations/         # SQL migrations
│   │   ├── proto/              # gRPC protobuf definitions
│   │   └── tests/              # Integration tests
│   └── cruster-macros/         # Procedural macros
├── examples/                   # Example applications
│   ├── chess-cluster/          # Distributed chess server
│   └── cluster-tests/          # E2E test suite
└── nix/                        # Nix flake development shells
```

## Key Concepts

- **Entity**: A stateful actor with automatic persistence and lifecycle management
- **Activity**: State-mutating operation with transactional guarantees (`#[activity]` with `&mut self`)
- **Workflow**: Orchestration of activities, publicly callable (`#[workflow]` with `&self`)
- **RPC**: Read-only operation, publicly callable (`#[rpc]` with `&self`)
- **Entity Trait**: Composable behavior that can be mixed into entities

## Method Types

| Attribute    | Self Type    | Visibility | Purpose                          |
| ------------ | ------------ | ---------- | -------------------------------- |
| `#[rpc]`     | `&self`      | Public     | Read-only queries                |
| `#[activity]`| `&mut self`  | Protected  | State mutations (transactional)  |
| `#[workflow]`| `&self`      | Public     | Orchestrate activities           |
| `#[method]`  | `&self`      | Private    | Internal sync helpers            |

## State Access Pattern

In `#[activity]` methods, access state via `self.state`:
```rust
#[activity]
async fn increment(&mut self, amount: i32) -> Result<i32, ClusterError> {
    self.state.count += amount;
    Ok(self.state.count)
}
```

In `#[rpc]` and `#[workflow]` methods, access state via `self.state` (read-only):
```rust
#[rpc]
async fn get_count(&self) -> Result<i32, ClusterError> {
    Ok(self.state.count)
}
```
