# Testing

## Test Organization

```
crates/cruster/
├── src/
│   └── macro_tests.rs              # Unit tests for macro-generated code
└── tests/
    ├── sql_integration.rs          # 53 SQL storage tests (testcontainers PG)
    ├── macro_integration.rs        # 14 workflow/activity tests (testcontainers PG)
    ├── sharding_integration.rs     # 17 sharding+entity tests (testcontainers PG)
    └── multi_runner.rs             # 3 multi-runner tests (testcontainers PG+etcd)

examples/
└── cluster-tests/tests/            # ~40 E2E bash scripts (Docker Compose)
```

## Running Tests

```bash
# Unit tests (no DB, fast)
cargo test -p cruster --lib

# All integration tests (requires Docker for testcontainers)
cargo test -p cruster --test sql_integration
cargo test -p cruster --test macro_integration
cargo test -p cruster --test sharding_integration
cargo test -p cruster --test multi_runner

# Specific test
cargo test -p cruster --lib test_name
cargo test -p cruster --test macro_integration -- tx_rolls_back

# With output
cargo test -p cruster --lib -- --nocapture
```

## Integration Test Pattern

Integration tests use `testcontainers` to spin up PostgreSQL. The `test_ctx` helper creates an `EntityContext` for spawning entities:

```rust
use cruster::__internal::test_ctx;

#[tokio::test]
async fn my_test() {
    let pool = setup_postgres().await;  // testcontainers
    let ctx = test_ctx(&pool, "MyEntity", "entity-1");
    let handler = MyEntity { pool: pool.clone() }.spawn(ctx).await.unwrap();

    let payload = rmp_serde::to_vec(&MyRequest { ... }).unwrap();
    let result = handler.handle_request("my_method", &payload, &HashMap::new()).await.unwrap();
    let value: MyResponse = rmp_serde::from_slice(&result).unwrap();
}
```

## Workflow Test Pattern

Workflow tests need to spawn via the generated bundle struct and use fresh entity contexts per execution to avoid journal cache collisions:

```rust
let ctx = test_ctx(&pool, "Workflow/MyWorkflow", "exec-1");
let bundle = __MyWorkflowWithGroups { __workflow: MyWorkflow, ... };
let handler = bundle.spawn(ctx).await.unwrap();

// Each workflow execution needs a fresh context with a different entity_id
let ctx2 = test_ctx(&pool, "Workflow/MyWorkflow", "exec-2");
let handler2 = bundle.spawn(ctx2).await.unwrap();
```

## E2E Tests

E2E tests are bash scripts that curl a 3-node Docker Compose cluster:

```bash
docker compose -f examples/cluster-tests/docker-compose.yml up -d --build --wait
CLUSTER_TESTS_URL=http://localhost:8080 ./examples/cluster-tests/tests/e2e.sh
docker compose -f examples/cluster-tests/docker-compose.yml down -v

# Run a single E2E test
CLUSTER_TESTS_URL=http://localhost:8080 ./examples/cluster-tests/tests/test_basic.sh
```
