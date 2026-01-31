# Build Commands

## Prerequisites

- Rust stable toolchain
- Protocol Buffers compiler (`protoc`) for gRPC code generation
- PostgreSQL (optional, for SQL storage backend)
- etcd (optional, for etcd storage backend)

## Command Reference

| Task              | Command                           |
| ----------------- | --------------------------------- |
| Build all         | `cargo build`                     |
| Build release     | `cargo build --release`           |
| Run all tests     | `cargo test`                      |
| Run cruster tests | `cargo test -p cruster --lib`     |
| Run clippy        | `cargo clippy`                    |
| Format code       | `cargo fmt`                       |
| Check formatting  | `cargo fmt --check`               |

## Testing

```bash
# Run all library tests
cargo test -p cruster --lib

# Run chess-cluster example tests
cargo test -p chess-cluster

# Run cluster-tests example tests  
cargo test -p cluster-tests
```

## Features

The `cruster` crate has optional features:

- `sql` - PostgreSQL storage backend (requires `sqlx`)
- `etcd` - etcd storage backend (requires `etcd-client`)

```bash
# Build with SQL support
cargo build -p cruster --features sql

# Build with etcd support
cargo build -p cruster --features etcd

# Build with all features
cargo build -p cruster --features "sql,etcd"
```
