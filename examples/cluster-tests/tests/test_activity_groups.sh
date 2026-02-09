#!/bin/bash
set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Activity Group Composition"

ORDER_ID="order-$(date +%s)"

# Process an order using the workflow with composed activity groups
RESULT=$(post "/activity-group/$ORDER_ID/process-order" "{\"item_count\": 3, \"amount\": 9999}")

# Verify the result contains expected fields
assert_contains "$RESULT" "$ORDER_ID"
assert_contains "$RESULT" "reservation_id"
assert_contains "$RESULT" "transaction_id"
assert_contains "$RESULT" "confirmation"
assert_contains "$RESULT" "summary"
test_pass "order workflow completes with activity groups"

# Verify reservation_id format (from Inventory group)
assert_contains "$RESULT" "res-$ORDER_ID-3"
test_pass "inventory group reserve_items activity produced correct reservation"

# Verify transaction_id format (from Payments group)
assert_contains "$RESULT" "tx-$ORDER_ID-9999"
test_pass "payments group charge_payment activity produced correct transaction"

# Verify confirmation format (from Inventory group)
assert_contains "$RESULT" "confirmed-res-$ORDER_ID-3"
test_pass "inventory group confirm_reservation activity produced correct confirmation"

# Verify summary format (local activity)
assert_contains "$RESULT" "order=$ORDER_ID,res=res-$ORDER_ID-3,tx=tx-$ORDER_ID-9999"
test_pass "local summarize activity produced correct summary"

# Check that all steps are recorded in the database via the read entity
STEPS=$(get "/activity-group/$ORDER_ID/order-steps")
assert_contains "$STEPS" "reserve"
assert_contains "$STEPS" "charge"
assert_contains "$STEPS" "confirm"
assert_contains "$STEPS" "summary"
test_pass "all 4 processing steps recorded in database"

# Process a second order to verify independence
ORDER_ID2="order2-$(date +%s)"
RESULT2=$(post "/activity-group/$ORDER_ID2/process-order" "{\"item_count\": 1, \"amount\": 500}")
assert_contains "$RESULT2" "$ORDER_ID2"
assert_contains "$RESULT2" "res-$ORDER_ID2-1"
assert_contains "$RESULT2" "tx-$ORDER_ID2-500"
test_pass "second order is independent from first"

# Verify first order's steps are not mixed with second order
STEPS1=$(get "/activity-group/$ORDER_ID/order-steps")
STEPS2=$(get "/activity-group/$ORDER_ID2/order-steps")

if [[ "$STEPS1" == *"$ORDER_ID2"* ]]; then
    test_fail "first order should not contain second order's data"
fi
if [[ "$STEPS2" == *"$ORDER_ID"* ]] && [[ "$STEPS2" != *"$ORDER_ID2"* ]]; then
    test_fail "second order should not contain first order's data"
fi
test_pass "orders have independent step histories"

echo "All activity group tests passed"
