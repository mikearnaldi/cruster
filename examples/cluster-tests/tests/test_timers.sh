#!/bin/bash
set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Timers"

# Note: schedule_timer is a workflow that blocks until the timer fires.
# This is because workflows are durable and run to completion.
# The test verifies that timers fire after the expected delay and
# are properly recorded.

ID="timer-test-$(date +%s)"
TIMER_ID="t-$(date +%s)"

# Schedule a timer with 500ms delay (this blocks until it fires)
echo "Scheduling timer (will block for 500ms)..."
START=$(now_ms)
post "/timer/$ID/schedule" "{\"timer_id\": \"$TIMER_ID\", \"delay_ms\": 500}" > /dev/null
END=$(now_ms)
ELAPSED=$((END - START))

# Verify timer took at least 500ms (allowing some margin)
if [ "$ELAPSED" -lt 400 ]; then
    test_fail "timer returned too quickly (${ELAPSED}ms < 400ms)"
fi
test_pass "timer blocked for delay duration (${ELAPSED}ms)"

# Check timer has fired
FIRES=$(get "/timer/$ID/fires")
assert_contains "$FIRES" "$TIMER_ID"
test_pass "timer fire recorded"

# Check timer is no longer pending (it completed)
PENDING=$(get "/timer/$ID/pending")
if [[ "$PENDING" == *"$TIMER_ID"* ]]; then
    test_fail "completed timer still appears as pending"
fi
test_pass "completed timer removed from pending"

# Try to cancel a non-existent timer
RESULT=$(post "/timer/$ID/cancel" "{\"timer_id\": \"nonexistent\"}")
assert_eq "$RESULT" "false"
test_pass "cancel of non-existent timer returns false"

# Clear fires and verify
post "/timer/$ID/clear-fires" "{}" > /dev/null
FIRES=$(get "/timer/$ID/fires")
assert_eq "$FIRES" "[]"
test_pass "clear fires works"

# Different entity IDs have independent timer histories
ID2="timer-test-2-$(date +%s)"
TIMER_ID2="t2-$(date +%s)"
post "/timer/$ID2/schedule" "{\"timer_id\": \"$TIMER_ID2\", \"delay_ms\": 100}" > /dev/null

# Original entity shouldn't have the new timer
FIRES_ID1=$(get "/timer/$ID/fires")
if [[ "$FIRES_ID1" == *"$TIMER_ID2"* ]]; then
    test_fail "timer fires not independent between entities"
fi

# New entity should have its timer
FIRES_ID2=$(get "/timer/$ID2/fires")
assert_contains "$FIRES_ID2" "$TIMER_ID2"
test_pass "entity IDs have independent timers"

echo "All timer tests passed"
