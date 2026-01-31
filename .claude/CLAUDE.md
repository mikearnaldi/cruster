# Cruster

Rust framework for distributed, stateful entity systems with durable workflows.

## Critical Constraints

- **No auto-commits** - Wait for explicit user approval
- **State access** - Use `self.state` in activities (`&mut self`), not `state_mut()`
- **Trait activities** - Use `#[protected]` for activities callable by entities using the trait
