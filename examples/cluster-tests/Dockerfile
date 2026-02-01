# syntax=docker/dockerfile:1.4

# ==============================================================================
# Build stage (for local dev)
# ==============================================================================
FROM rust:1.93-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --package cluster-tests --all-features && \
    cp /app/target/release/cluster-tests /app/cluster-tests-bin

# ==============================================================================
# Runtime stage (for local dev - builds from source)
# Usage: docker build -t cluster-tests:latest ../.. -f Dockerfile
# ==============================================================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/cluster-tests-bin /app/cluster-tests
COPY examples/cluster-tests/tests /app/tests
RUN chmod +x /app/tests/*.sh /app/tests/lib/*.sh

ENV RUST_LOG=info
EXPOSE 8080 9000
ENTRYPOINT ["/app/cluster-tests"]

# ==============================================================================
# CI stage (uses prebuilt binary from context)
# Usage: docker build --target ci -t cluster-tests:latest .
# ==============================================================================
FROM debian:bookworm-slim AS ci

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY cluster-tests /app/cluster-tests
COPY tests /app/tests
RUN chmod +x /app/tests/*.sh /app/tests/lib/*.sh

ENV RUST_LOG=info
EXPOSE 8080 9000
ENTRYPOINT ["/app/cluster-tests"]


