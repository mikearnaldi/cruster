#!/bin/bash
set -e

SCRIPT_DIR="$(dirname "$0")"

echo "========================================="
echo "Cluster E2E Tests"
echo "========================================="
echo "Target: ${CLUSTER_TESTS_URL:-http://localhost:8080}"
echo ""

# Health check
echo "Checking cluster health..."
if ! curl -sf "${CLUSTER_TESTS_URL:-http://localhost:8080}/health" > /dev/null; then
    echo "FAIL: cluster not healthy"
    exit 1
fi
echo "Cluster healthy"
echo ""

# Per-node health checks when running under docker compose
if [ -n "$DOCKER_COMPOSE_PROJECT" ]; then
    echo "Checking per-node health..."
    for node in node1 node2 node3; do
        if ! docker compose -p "$DOCKER_COMPOSE_PROJECT" exec -T "$node" \
            curl -sf "http://localhost:8080/health" > /dev/null; then
            echo "FAIL: $node not healthy"
            exit 1
        fi
        echo "$node healthy"
    done
    echo ""
fi

# Run all test suites
"$SCRIPT_DIR/test_basic.sh"
echo ""

"$SCRIPT_DIR/test_persistence.sh"
echo ""

"$SCRIPT_DIR/test_workflows.sh"
echo ""

"$SCRIPT_DIR/test_activities.sh"
echo ""

"$SCRIPT_DIR/test_traits.sh"
echo ""

"$SCRIPT_DIR/test_timers.sh"
echo ""

"$SCRIPT_DIR/test_cross_entity.sh"
echo ""

"$SCRIPT_DIR/test_singletons.sh"
echo ""

"$SCRIPT_DIR/test_sql_activity.sh"
echo ""

"$SCRIPT_DIR/test_shard_topology.sh"
echo ""

# Singleton failover test runs last since it kills a node
# Only run if DOCKER_COMPOSE_PROJECT is set
if [ -n "$DOCKER_COMPOSE_PROJECT" ]; then
    "$SCRIPT_DIR/test_singleton_failover.sh"
    echo ""
else
    echo "SKIP: test_singleton_failover.sh (set DOCKER_COMPOSE_PROJECT to enable)"
    echo ""
fi

echo "========================================="
echo "All E2E tests passed!"
echo "========================================="
