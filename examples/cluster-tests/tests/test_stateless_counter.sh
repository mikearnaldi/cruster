#!/bin/bash
# Test stateless counter entity (pure-RPC, new API pattern)
#
# Tests the new pure-RPC entity pattern where state is managed
# directly in PostgreSQL rather than through framework state.

set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Stateless Counter Operations"

TEST_ID="stateless-$(date +%s)"

# Test increment
RESULT=$(post "/stateless-counter/$TEST_ID/increment" "5")
assert_eq "$RESULT" "5"
test_pass "increment returns new value"

# Test second increment
RESULT=$(post "/stateless-counter/$TEST_ID/increment" "3")
assert_eq "$RESULT" "8"
test_pass "second increment accumulates"

# Test get
RESULT=$(get "/stateless-counter/$TEST_ID")
assert_eq "$RESULT" "8"
test_pass "get returns current value"

# Test decrement
RESULT=$(post "/stateless-counter/$TEST_ID/decrement" "2")
assert_eq "$RESULT" "6"
test_pass "decrement subtracts from value"

# Test reset
post "/stateless-counter/$TEST_ID/reset" "null" > /dev/null
RESULT=$(get "/stateless-counter/$TEST_ID")
assert_eq "$RESULT" "0"
test_pass "reset sets value to zero"

# Test entity independence (different entity IDs don't share state)
TEST_ID2="stateless-other-$(date +%s)"
RESULT=$(post "/stateless-counter/$TEST_ID2/increment" "100")
assert_eq "$RESULT" "100"
RESULT=$(get "/stateless-counter/$TEST_ID")
assert_eq "$RESULT" "0"
test_pass "different entities have independent state"

# Test get on nonexistent entity returns zero
TEST_ID3="stateless-nonexistent-$(date +%s)"
RESULT=$(get "/stateless-counter/$TEST_ID3")
assert_eq "$RESULT" "0"
test_pass "nonexistent entity returns zero"

echo ""
echo "All stateless counter tests passed!"
