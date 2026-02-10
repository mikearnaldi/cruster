# Cruster Project Overview

Cruster is a Rust framework for building distributed entity systems with durable workflows.

## Git Workflow

**Auto-commit is prohibited.** Do not automatically commit changes. Always wait for explicit user approval.

## Project Structure

```
crates/
├── cruster/                # Main framework crate
│   ├── src/                # Source code
│   ├── migrations/         # SQL migrations
│   ├── proto/              # gRPC protobuf definitions
│   └── tests/              # Integration tests (testcontainers)
└── cruster-macros/         # Procedural macros
examples/
└── cluster-tests/          # E2E test suite (Docker Compose)
rules/                      # AI assistant rule files
```

## Key Concepts

- **Entity**: Stateless RPC handler with automatic sharding and routing. State is managed by the application via PostgreSQL.
- **RPC**: Entity method — `#[rpc]` (best-effort reads) or `#[rpc(persisted)]` (at-least-once writes)
- **Workflow**: Standalone durable orchestration backed by a hidden entity (`#[workflow]`)
- **Activity**: Journaled side effect within a workflow (`#[activity]`), uses `self.tx` for transactional SQL
- **Activity Group**: Reusable bundle of activities composable across workflows (`#[activity_group]`)
- **RPC Group**: Reusable bundle of RPCs composable across entities (`#[rpc_group]`)
- **Singleton**: Cluster-wide unique task with automatic failover
- **Cron**: Distributed cron job running as a singleton
