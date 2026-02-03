# Shard topology and lease behavior test
set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Shard Topology and Lease Management"

if [ -z "$DOCKER_COMPOSE_PROJECT" ]; then
    echo "SKIP: DOCKER_COMPOSE_PROJECT not set"
    exit 0
fi

ETCD_SERVICE="${ETCD_SERVICE:-etcd}"
LEASE_TTL_SEC="${LEASE_TTL_SEC:-30}"

compose_exec() {
    docker compose -p "$DOCKER_COMPOSE_PROJECT" exec -T "$@"
}

query_node() {
    local node=$1
    local path=$2
    compose_exec "$node" curl -sf "http://localhost:8080$path" 2>/dev/null
}

etcd_shard_keys() {
    compose_exec "$ETCD_SERVICE" etcdctl get --prefix "/cluster-tests/shards/" --keys-only
}

etcd_shard_values() {
    compose_exec "$ETCD_SERVICE" etcdctl get --prefix "/cluster-tests/shards/" --print-value-only
}

total_shards() {
    query_node node1 "/debug/shards" | jq -r '.total_shards'
}

count_non_empty_lines() {
    local count=0
    while IFS= read -r line; do
        if [ -n "$line" ]; then
            count=$((count + 1))
        fi
    done
    echo "$count"
}

assert_total_shards() {
    local expected=$1
    local actual
    actual=$(etcd_shard_keys | count_non_empty_lines)
    assert_eq "$actual" "$expected"
}

assert_known_owners() {
    local owners
    owners=$(etcd_shard_values | sort -u)
    for owner in $owners; do
        case "$owner" in
            node1:9000|node2:9000|node3:9000) ;;
            *) test_fail "unknown shard owner: $owner" ;;
        esac
    done
}

assert_owner_absent() {
    local owner=$1
    if etcd_shard_values | grep -q "$owner"; then
        test_fail "expected shard owner to be absent: $owner"
    fi
}

assert_owner_present() {
    local owner=$1
    if ! etcd_shard_values | grep -q "$owner"; then
        test_fail "expected shard owner to be present: $owner"
    fi
}

TOTAL=$(total_shards)
if [ -z "$TOTAL" ] || [ "$TOTAL" = "null" ]; then
    test_fail "failed to read total_shards"
fi

assert_total_shards "$TOTAL"
assert_known_owners
test_pass "baseline shard ownership looks correct"

docker compose -p "$DOCKER_COMPOSE_PROJECT" stop node2 > /dev/null 2>&1
sleep 5
assert_total_shards "$TOTAL"
assert_owner_absent "node2:9000"
test_pass "shards reallocated after node2 stop"

docker compose -p "$DOCKER_COMPOSE_PROJECT" start node2 > /dev/null 2>&1
sleep 5
assert_total_shards "$TOTAL"
assert_owner_present "node2:9000"
test_pass "node2 reacquired shards after restart"

docker compose -p "$DOCKER_COMPOSE_PROJECT" kill -s SIGKILL node3 > /dev/null 2>&1
sleep $((LEASE_TTL_SEC + 5))
assert_total_shards "$TOTAL"
assert_owner_absent "node3:9000"
test_pass "shards reallocated after node3 hard kill"

docker compose -p "$DOCKER_COMPOSE_PROJECT" start node3 > /dev/null 2>&1
sleep 5
assert_total_shards "$TOTAL"
assert_owner_present "node3:9000"
test_pass "node3 reacquired shards after restart"

echo "All shard topology tests passed"
