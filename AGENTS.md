# Cruster

Rust framework for distributed entity systems with durable workflows.

## Reference Implementation

The original Effect cluster code (TypeScript) is in `.repos/effect-smol/packages/effect/src/unstable/cluster`.

## Rules

Detailed rules are in the `rules/` directory. Reference them as needed based on the task at hand.

| Rule | When to reference |
|------|-------------------|
| [project-overview.md](rules/project-overview.md) | General orientation — project structure, key concepts, git workflow |
| [rust-development.md](rules/rust-development.md) | Writing entities, workflows, activities, RPC groups — crate architecture and code patterns |
| [testing.md](rules/testing.md) | Writing or running tests — unit, integration (testcontainers), E2E (Docker Compose) |
| [build-commands.md](rules/build-commands.md) | Building, linting, running tests — command reference |
| [commit-checklist.md](rules/commit-checklist.md) | Before every commit — required checks (fmt, clippy, tests) |
| [changesets.md](rules/changesets.md) | Creating releases — changeset format, package naming, knope workflow |

### project-overview.md

Read this first for any task. Covers the directory layout (`crates/cruster/`, `crates/cruster-macros/`, `examples/cluster-tests/`), the key domain concepts (entities, workflows, activities, activity groups, RPC groups, singletons, cron), and the git workflow (no auto-commits, wait for user approval). Reference when navigating the codebase, understanding how pieces fit together, or when asked about project structure.

### rust-development.md

Reference when writing or modifying Rust code in `crates/`. Covers the crate architecture (which files own which concerns), the stateless entity pattern (`#[entity]`/`#[entity_impl]` with `#[rpc]`/`#[rpc(persisted)]`), the standalone workflow pattern (`#[workflow]`/`#[workflow_impl]` with `#[activity]` and `self.tx`), activity groups (`#[activity_group]`/`#[activity_group_impl]`), RPC groups (`#[rpc_group]`/`#[rpc_group_impl]`), and error handling with `ClusterError`. Essential reading before writing any entity, workflow, or macro code.

### testing.md

Reference when writing, running, or debugging tests. Covers the full test organization (301 unit tests, 53 SQL integration, 14 macro integration, 17 sharding integration, 3 multi-runner, ~40 E2E bash scripts), test commands, the `testcontainers` PostgreSQL pattern for integration tests, the `test_ctx` helper for spawning entities in tests, the journal cache collision problem (why workflow tests need fresh entity contexts per execution), and how to run E2E tests with Docker Compose.

### build-commands.md

Reference when you need the exact command to build, lint, test, or run any part of the project. Includes unit test commands, integration test commands per suite, and the full E2E test sequence (docker compose up, run scripts, docker compose down).

### commit-checklist.md

Reference before every commit. Contains the required pre-commit checks: `cargo fmt --all && cargo clippy --workspace --all-targets && cargo test -p cruster --lib`. Never skip these.

### changesets.md

Reference when creating a release changeset. Covers the changeset file format, the critical requirement to use `default` (not `cruster`) as the package name, the change type levels (patch/minor/major), examples, and the knope CI workflow (prepare-release PR → merge → publish).

## Critical Constraints

- **No auto-commits** — Wait for explicit user approval
- **`self.tx` in activities** — Transactional SQL; committed with journal on success, rolled back on error
- **`self.pool` in activities** — Read-only pool for queries outside the transaction
- **Persisted RPC dedup** — `#[rpc(persisted)]` deduplicates identical payloads; use `#[rpc]` for non-idempotent operations
- **Changesets** — Use `default` as the package name (not `cruster`); all patch level before 0.1
