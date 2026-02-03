#!/bin/bash

BASE_URL="${CLUSTER_TESTS_URL:-http://localhost:8080}"

CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-2}"
CURL_MAX_TIME="${CURL_MAX_TIME:-10}"
GET_RETRY_COUNT="${GET_RETRY_COUNT:-10}"
GET_RETRY_DELAY_SEC="${GET_RETRY_DELAY_SEC:-0.2}"

curl_base() {
    curl -sf --connect-timeout "$CURL_CONNECT_TIMEOUT" --max-time "$CURL_MAX_TIME" "$@"
}

curl_base_no_fail() {
    curl --connect-timeout "$CURL_CONNECT_TIMEOUT" --max-time "$CURL_MAX_TIME" "$@"
}

retry_get() {
    local path=$1
    local attempt=1
    while [ $attempt -le "$GET_RETRY_COUNT" ]; do
        if curl_base "$BASE_URL$path"; then
            return 0
        fi
        sleep "$GET_RETRY_DELAY_SEC"
        attempt=$((attempt + 1))
    done
    return 1
}

# HTTP helpers
post() { curl_base -X POST "$BASE_URL$1" -H "Content-Type: application/json" -d "$2"; }
get() { retry_get "$1"; }
delete() { curl_base -X DELETE "$BASE_URL$1"; }

now_ms() {
    if command -v python3 >/dev/null 2>&1; then
        python3 -c "import time; print(int(time.time() * 1000))"
    elif command -v python >/dev/null 2>&1; then
        python -c "import time; print(int(time.time() * 1000))"
    else
        date +%s000
    fi
}

# Assertions
assert_eq() {
    if [ "$1" != "$2" ]; then
        echo "FAIL: expected '$2', got '$1'"
        exit 1
    fi
}

assert_contains() {
    if [[ "$1" != *"$2"* ]]; then
        echo "FAIL: expected '$1' to contain '$2'"
        exit 1
    fi
}

# Test lifecycle
test_start() { echo "=== $1 ==="; }
test_pass() { echo "PASS: $1"; }
test_fail() { echo "FAIL: $1"; exit 1; }
