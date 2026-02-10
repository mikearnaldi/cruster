# Build Commands

## Prerequisites

- Rust stable toolchain
- Protocol Buffers compiler (`protoc`) for gRPC code generation
- PostgreSQL (for storage backend)
- etcd (for runner discovery)

## Command Reference

| Task | Command |
|------|---------|
| Build | `cargo build` |
| Build release | `cargo build --release` |
| Format | `cargo fmt --all` |
| Lint | `cargo clippy --workspace --all-targets` |
| Unit tests | `cargo test -p cruster --lib` |
| Integration tests | `cargo test -p cruster --test <name>` |
| E2E tests | See below |

## Testing

```bash
# Unit tests (pure logic, no DB)
cargo test -p cruster --lib

# Integration tests (requires Docker for testcontainers)
cargo test -p cruster --test sql_integration
cargo test -p cruster --test macro_integration
cargo test -p cruster --test sharding_integration
cargo test -p cruster --test multi_runner

# E2E tests (Docker Compose cluster)
docker compose -f examples/cluster-tests/docker-compose.yml up -d --build --wait
CLUSTER_TESTS_URL=http://localhost:8080 ./examples/cluster-tests/tests/e2e.sh
docker compose -f examples/cluster-tests/docker-compose.yml down -v
```
