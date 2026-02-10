# Pre-Commit Checklist

Before committing any changes, **always** run these checks:

```bash
cargo fmt --all && cargo clippy --workspace --all-targets && cargo test -p cruster --lib
```

- Fix formatting issues before committing
- Fix clippy warnings before committing
- Ensure all unit tests pass before committing
- Never skip these checks
