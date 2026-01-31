Please also reference the following rules as needed. The list below is provided in TOON format, and `@` stands for the project root directory.

rules[5]:
  - path: @.opencode/memories/build-commands.md
    description: Build and development commands for Cruster
    applyTo[3]: Cargo.toml,Cargo.lock,.cargo/**/*
  - path: @.opencode/memories/project-overview.md
    description: Project overview and git workflow for Cruster
    applyTo[1]: **/*
  - path: @.opencode/memories/rules-maintenance.md
    description: Rule file maintenance and regeneration
    applyTo[2]: .rulesync/**/*,rulesync.jsonc
  - path: @.opencode/memories/rust-development.md
    description: Rust development patterns for cruster crates
    applyTo[1]: crates/**/*.rs
  - path: @.opencode/memories/testing.md
    description: Testing patterns for cruster crates
    applyTo[4]: crates/cruster/tests/**/*,**/*_test.rs,**/tests/**/*,examples/**/tests/**/*

# Cruster

Rust framework for distributed, stateful entity systems with durable workflows.

## Critical Constraints

- **No auto-commits** - Wait for explicit user approval
- **State access** - Use `self.state` in activities (`&mut self`), not `state_mut()`
- **Trait activities** - Use `#[protected]` for activities callable by entities using the trait
