---
description: "Testing patterns for cruster crates"
globs: ["crates/cruster/tests/**/*", "**/*_test.rs", "**/tests/**/*", "examples/**/tests/**/*"]
---

# Testing

## Test Organization

```
crates/cruster/
├── src/
│   └── macro_tests.rs         # Entity macro tests
└── tests/
    ├── ui/                    # Compile-time error tests (trybuild)
    ├── streaming_replay_ordering.rs
    └── storage_retry_exhaustion.rs

examples/
├── chess-cluster/tests/       # Chess example integration tests
└── cluster-tests/tests/       # E2E shell scripts
```

## Running Tests

```bash
# All library tests
cargo test -p cruster --lib

# All tests including integration
cargo test -p cruster

# Chess-cluster example tests
cargo test -p chess-cluster

# Specific test
cargo test -p cruster --lib test_name

# With output
cargo test -p cruster --lib -- --nocapture
```

## Entity Test Patterns

Testing entities with the in-memory test harness:

```rust
use cruster::testing::{TestCluster, test_ctx};
use cruster::durable::MemoryWorkflowStorage;

#[tokio::test]
async fn test_counter_increment() {
    let cluster = TestCluster::new();
    let client = Counter::register(cluster.sharding()).await.unwrap();

    let entity_id = EntityId::new("test-1");
    
    // Call activity via workflow
    let result = client.increment(&entity_id, 5).await.unwrap();
    assert_eq!(result, 5);

    // Verify state persisted
    let count = client.get_count(&entity_id).await.unwrap();
    assert_eq!(count, 5);
}
```

## Testing with Storage

```rust
use cruster::durable::{MemoryWorkflowStorage, WorkflowStorage};
use std::sync::Arc;

#[tokio::test]
async fn test_state_persists_across_restarts() {
    let storage: Arc<dyn WorkflowStorage> = Arc::new(MemoryWorkflowStorage::new());

    // First entity instance
    let entity = Counter;
    let ctx = test_ctx_with_storage("Counter", "c-1", storage.clone());
    let handler = entity.spawn(ctx).await.unwrap();

    // Mutate state
    let payload = rmp_serde::to_vec(&5i32).unwrap();
    handler.handle_request("increment", &payload, &HashMap::new()).await.unwrap();
    drop(handler);

    // Second instance loads persisted state
    let entity = Counter;
    let ctx = test_ctx_with_storage("Counter", "c-1", storage.clone());
    let handler = entity.spawn(ctx).await.unwrap();

    let result = handler.handle_request("get_count", &[], &HashMap::new()).await.unwrap();
    let value: i32 = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(value, 5);
}
```

## Property-Based Testing with Proptest

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn entity_id_roundtrip(id in "[a-z0-9-]{1,64}") {
        let entity_id = EntityId::new(&id);
        assert_eq!(entity_id.as_str(), id);
    }
}
```

## UI Tests (Compile Errors)

UI tests verify that invalid code produces helpful error messages:

```rust
// tests/ui/activity_public_invalid.rs
use cruster::{entity, entity_impl};

#[entity]
struct Bad;

#[entity_impl]
#[state(())]
impl Bad {
    #[activity]
    #[public]  // ERROR: activities cannot be public
    async fn bad(&self) -> Result<(), cruster::error::ClusterError> {
        Ok(())
    }
}
```

```
// tests/ui/activity_public_invalid.stderr
error: #[activity] and #[method] cannot be #[public]
```
