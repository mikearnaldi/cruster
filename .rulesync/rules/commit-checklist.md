---
description: "Pre-commit checklist for Cruster"
globs: ["**/*"]
alwaysApply: true
---

# Pre-Commit Checklist

Before committing any changes, **always** run the following checks in order:

## 1. Format

```bash
cargo fmt --all
```

## 2. Clippy

```bash
cargo clippy --all-features --all-targets -- -D warnings
```

## 3. Build

```bash
cargo build --all-features
```

## 4. Tests

```bash
cargo test --all-features
```

## Quick One-Liner

```bash
cargo fmt --all && cargo clippy --all-features --all-targets -- -D warnings && cargo build --all-features && cargo test --all-features
```

## Important

- Fix any formatting issues before committing
- Fix any clippy warnings before committing  
- Ensure the build succeeds before committing
- Ensure all tests pass before committing
- Never skip these checks
