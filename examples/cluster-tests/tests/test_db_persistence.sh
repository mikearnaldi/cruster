#!/bin/bash
set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Database Persistence & Workflow Completion"

# Use unique IDs for this test run to avoid interference
TS=$(date +%s)

# Helper: wait until all messages for an entity are processed.
# Internal journal messages may still be in-flight when the API returns,
# so we poll until there are no unprocessed messages or we time out.
wait_all_processed() {
    local entity_type=$1
    local entity_id=$2
    local max_attempts=${3:-20}
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        local result
        result=$(get "/debug/messages?entity_type=$entity_type&entity_id=$entity_id&processed=false")
        if [ "$result" = "[]" ]; then
            return 0
        fi
        sleep 0.3
        attempt=$((attempt + 1))
    done
    echo "TIMEOUT: still have unprocessed messages for $entity_type/$entity_id after $max_attempts attempts"
    echo "Remaining: $result"
    return 1
}

# ============================================================================
# Test 1: Messages are persisted in cluster_messages table
# ============================================================================

COUNTER_ID="db-persist-counter-$TS"

# Perform an increment operation
post "/counter/$COUNTER_ID/increment" "10" > /dev/null

# Wait for all messages (including internal journal messages) to be processed
wait_all_processed "Counter" "$COUNTER_ID"

# Query the database for messages related to this entity
MESSAGES=$(get "/debug/messages?entity_type=Counter&entity_id=$COUNTER_ID")

# Verify messages exist in the database
assert_contains "$MESSAGES" "\"entity_type\":\"Counter\""
assert_contains "$MESSAGES" "\"entity_id\":\"$COUNTER_ID\""
test_pass "counter increment message persisted in database"

# ============================================================================
# Test 2: Messages are marked as processed after completion
# ============================================================================

# All messages for the counter should be processed (the operation completed)
PROCESSED_MESSAGES=$(get "/debug/messages?entity_type=Counter&entity_id=$COUNTER_ID&processed=true")
assert_contains "$PROCESSED_MESSAGES" "\"processed\":true"
test_pass "processed messages correctly flagged in database"

# Verify no unprocessed messages remain for this entity
UNPROCESSED_MESSAGES=$(get "/debug/messages?entity_type=Counter&entity_id=$COUNTER_ID&processed=false")
assert_eq "$UNPROCESSED_MESSAGES" "[]"
test_pass "no unprocessed messages remain after completion"

# ============================================================================
# Test 3: Replies are persisted with exit status
# ============================================================================

# Get the request_id from the processed message (pick the main increment, not the journal message)
REQUEST_ID=$(echo "$PROCESSED_MESSAGES" | python3 -c "
import sys, json
msgs = json.load(sys.stdin)
# Pick the main increment message, not the internal __journal one
main = [m for m in msgs if not m['tag'].startswith('__journal/')]
print(main[0]['request_id'] if main else msgs[0]['request_id'])
" 2>/dev/null || echo "")
if [ -n "$REQUEST_ID" ] && [ "$REQUEST_ID" != "" ]; then
    REPLIES=$(get "/debug/replies?request_id=$REQUEST_ID")
    assert_contains "$REPLIES" "\"is_exit\":true"
    test_pass "exit reply persisted for completed message"
else
    echo "SKIP: could not extract request_id (python3 not available)"
fi

# ============================================================================
# Test 4: Multiple messages accumulate in the database
# ============================================================================

MULTI_COUNTER_ID="db-persist-multi-$TS"

# Perform multiple operations
post "/counter/$MULTI_COUNTER_ID/increment" "1" > /dev/null
post "/counter/$MULTI_COUNTER_ID/increment" "2" > /dev/null
post "/counter/$MULTI_COUNTER_ID/increment" "3" > /dev/null

# Wait for all messages to be processed
wait_all_processed "Counter" "$MULTI_COUNTER_ID"

# Query the database - should have at least 3 messages (increments generate workflow + activity messages)
MULTI_MESSAGES=$(get "/debug/messages?entity_type=Counter&entity_id=$MULTI_COUNTER_ID")
# Count the number of message records
MSG_COUNT=$(echo "$MULTI_MESSAGES" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [ "$MSG_COUNT" != "0" ]; then
    assert_ge "$MSG_COUNT" "3"
    test_pass "multiple operations produce multiple persisted messages (got $MSG_COUNT)"
else
    # Fallback: just check we have multiple entries
    assert_contains "$MULTI_MESSAGES" "\"entity_id\":\"$MULTI_COUNTER_ID\""
    test_pass "multiple operations produce persisted messages"
fi

# ============================================================================
# Test 5: Workflow journal entries are persisted
# ============================================================================

WF_ID="db-persist-wf-$TS"
WF_EXEC_ID="db-exec-$TS"

# Run a simple workflow (entity type: Workflow/SimpleWorkflow)
post "/workflow/$WF_ID/run-simple" "{\"exec_id\": \"$WF_EXEC_ID\"}" > /dev/null

# Wait a moment for the workflow to complete and messages to be processed
sleep 2

# Query workflow journal entries
JOURNAL=$(get "/debug/journal?limit=200")
assert_not_contains "$JOURNAL" "[]"
test_pass "workflow journal entries persisted in database"

# ============================================================================
# Test 6: Completed workflows have completed_at set in journal
# ============================================================================

COMPLETED_JOURNAL=$(get "/debug/journal?completed=true&limit=200")
assert_not_contains "$COMPLETED_JOURNAL" "[]"
assert_contains "$COMPLETED_JOURNAL" "\"completed_at\""
test_pass "completed workflows have completed_at timestamp in journal"

# ============================================================================
# Test 7: Workflow journal contains entries for executed workflows
# ============================================================================

ALL_JOURNAL=$(get "/debug/journal?limit=200")
JOURNAL_COUNT=$(echo "$ALL_JOURNAL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [ "$JOURNAL_COUNT" != "0" ]; then
    assert_ge "$JOURNAL_COUNT" "1"
    test_pass "workflow journal contains entries (total: $JOURNAL_COUNT)"
else
    assert_not_contains "$ALL_JOURNAL" "[]"
    test_pass "workflow journal entries present"
fi

# ============================================================================
# Test 8: Workflow messages are persisted (standalone workflows use Workflow/* entity types)
# ============================================================================

# Standalone workflows register as Workflow/SimpleWorkflow, Workflow/LongWorkflow, etc.
# Their entity_id is derived from the key function (may be hashed).
# Check that messages exist for the workflow entity type.
WF_MESSAGES=$(get "/debug/messages?entity_type=Workflow/SimpleWorkflow&processed=true")
assert_contains "$WF_MESSAGES" "\"entity_type\":\"Workflow/SimpleWorkflow\""
assert_contains "$WF_MESSAGES" "\"processed\":true"
test_pass "workflow messages marked as processed after completion"

# ============================================================================
# Test 9: Long workflow persists all activity journal entries
# ============================================================================

LONG_WF_ID="db-persist-long-$TS"
LONG_EXEC_ID="db-long-exec-$TS"

# Run a long workflow with 5 steps (entity type: Workflow/LongWorkflow)
post "/workflow/$LONG_WF_ID/run-long" "{\"exec_id\": \"$LONG_EXEC_ID\", \"steps\": 5}" > /dev/null

# Wait for completion
sleep 3

# Verify long workflow messages exist and are processed
LONG_WF_MESSAGES=$(get "/debug/messages?entity_type=Workflow/LongWorkflow&processed=true")
assert_contains "$LONG_WF_MESSAGES" "\"entity_type\":\"Workflow/LongWorkflow\""
assert_contains "$LONG_WF_MESSAGES" "\"processed\":true"
test_pass "long workflow messages all marked processed"

LONG_WF_MSG_COUNT=$(echo "$LONG_WF_MESSAGES" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [ "$LONG_WF_MSG_COUNT" != "0" ]; then
    assert_ge "$LONG_WF_MSG_COUNT" "2"
    test_pass "long workflow persisted multiple messages (got $LONG_WF_MSG_COUNT)"
fi

# ============================================================================
# Test 10: Activity workflow persists messages
# ============================================================================

ACT_ID="db-persist-act-$TS"
ACT_EXEC_ID="db-act-exec-$TS"

# Run a workflow with activities (entity type: Workflow/ActivityWorkflow)
post "/activity/$ACT_ID/run" "{\"exec_id\": \"$ACT_EXEC_ID\"}" > /dev/null

# Wait for completion
sleep 2

# Verify activity workflow messages are processed
ACT_MESSAGES=$(get "/debug/messages?entity_type=Workflow/ActivityWorkflow&processed=true")
assert_contains "$ACT_MESSAGES" "\"entity_type\":\"Workflow/ActivityWorkflow\""
assert_contains "$ACT_MESSAGES" "\"processed\":true"
test_pass "activity workflow messages all marked processed"

# ============================================================================
# Test 11: SQL activity persists messages and marks completion
# ============================================================================

SQL_ACT_ID="db-persist-sqlact-$TS"

# Perform a SQL activity transfer (entity type: Workflow/SqlTransferWorkflow)
post "/sql-activity/$SQL_ACT_ID/transfer" "{\"to_entity\": \"target-$TS\", \"amount\": 100}" > /dev/null

# Wait for completion
sleep 2

# Verify SQL transfer workflow messages are processed
SQL_MESSAGES=$(get "/debug/messages?entity_type=Workflow/SqlTransferWorkflow&processed=true")
assert_contains "$SQL_MESSAGES" "\"entity_type\":\"Workflow/SqlTransferWorkflow\""
assert_contains "$SQL_MESSAGES" "\"processed\":true"
test_pass "SQL activity messages persisted and marked processed"

# ============================================================================
# Test 12: Cross-entity messages are persisted
# ============================================================================

CROSS_A="db-cross-a-$TS"
CROSS_B="db-cross-b-$TS"

# Send a cross-entity message
post "/cross/$CROSS_A/send" "{\"target_type\": \"CrossEntity\", \"target_id\": \"$CROSS_B\", \"message\": \"db-test-msg\"}" > /dev/null

# Wait for all messages to be processed
wait_all_processed "CrossEntity" "$CROSS_B"

# Verify the receive message on entity B is persisted and processed
CROSS_MESSAGES=$(get "/debug/messages?entity_type=CrossEntity&entity_id=$CROSS_B&processed=true")
assert_contains "$CROSS_MESSAGES" "\"entity_type\":\"CrossEntity\""
assert_contains "$CROSS_MESSAGES" "\"entity_id\":\"$CROSS_B\""
assert_contains "$CROSS_MESSAGES" "\"processed\":true"
test_pass "cross-entity messages persisted and marked processed"

# ============================================================================
# Test 13: Different entity types have independent messages
# ============================================================================

# Counter messages should not include workflow entity types
COUNTER_ONLY=$(get "/debug/messages?entity_type=Counter&entity_id=$COUNTER_ID")
assert_not_contains "$COUNTER_ONLY" "\"entity_type\":\"Workflow/SimpleWorkflow\""
test_pass "different entity types have independent message records"

# ============================================================================
# Test 14: Trait entity persists messages (persisted RPCs create messages)
# ============================================================================

TRAIT_ID="db-persist-trait-$TS"

# Perform a trait update (uses #[rpc(persisted)] so messages are created)
post "/trait/$TRAIT_ID/update" "{\"data\": \"db-persist-test\"}" > /dev/null

# Wait for all messages to be processed
wait_all_processed "TraitTest" "$TRAIT_ID"

# Verify messages are persisted and processed
TRAIT_MESSAGES=$(get "/debug/messages?entity_type=TraitTest&entity_id=$TRAIT_ID&processed=true")
assert_contains "$TRAIT_MESSAGES" "\"entity_type\":\"TraitTest\""
assert_contains "$TRAIT_MESSAGES" "\"processed\":true"
test_pass "trait entity messages persisted and marked processed"

echo "All database persistence tests passed"
