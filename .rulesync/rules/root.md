---
description: "Global context for the Cruster project"
root: true
---

# Cruster

Rust framework for distributed, stateful entity systems with durable workflows.

## Reference Implementation

The original Effect cluster code (TypeScript) can be found in `.repos/effect-smol/packages/effect/src/unstable/cluster`.

## Changesets

All changesets before version 0.1 must use **patch** level. No minor or major bumps until 0.1 is released.

## Critical Constraints

- **No auto-commits** - Wait for explicit user approval
- **Trait activities** - Use `#[protected]` for activities callable by entities using the trait
