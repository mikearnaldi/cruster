#!/bin/bash
# Test SQL activity transaction feature
# Tests that arbitrary SQL can be executed within the same transaction as state changes

set -e

SCRIPT_DIR="$(dirname "$0")"
source "$SCRIPT_DIR/lib/helpers.sh"

echo "========================================="
echo "SQL Activity Transaction Tests"
echo "========================================="

# Use a unique entity ID per run to avoid state pollution
ENTITY_ID="sql-e2e-test-$(date +%s)"

# Test 1: Basic transfer (state + SQL in same transaction)
echo ""
echo "Test 1: Basic transfer (state + SQL atomically)"
echo "------------------------------------------------"

# Make a transfer
RESULT=$(post "/sql-activity/$ENTITY_ID/transfer" '{"to_entity": "recipient-1", "amount": 100}')
echo "Transfer result: $RESULT"

# Check state was updated
STATE=$(get "/sql-activity/$ENTITY_ID/state")
echo "State after transfer: $STATE"

TRANSFER_COUNT=$(echo "$STATE" | jq -r '.transfer_count')
TOTAL_TRANSFERRED=$(echo "$STATE" | jq -r '.total_transferred')

if [ "$TRANSFER_COUNT" != "1" ]; then
    echo "FAIL: Expected transfer_count=1, got $TRANSFER_COUNT"
    exit 1
fi

if [ "$TOTAL_TRANSFERRED" != "100" ]; then
    echo "FAIL: Expected total_transferred=100, got $TOTAL_TRANSFERRED"
    exit 1
fi

# Check SQL table was also updated
SQL_COUNT=$(get "/sql-activity/$ENTITY_ID/sql-count")
echo "SQL table count: $SQL_COUNT"

if [ "$SQL_COUNT" != "1" ]; then
    echo "FAIL: Expected SQL count=1, got $SQL_COUNT"
    exit 1
fi

echo "PASS: Basic transfer succeeded"

# Test 2: Multiple transfers
echo ""
echo "Test 2: Multiple transfers"
echo "--------------------------"

post "/sql-activity/$ENTITY_ID/transfer" '{"to_entity": "recipient-2", "amount": 200}' > /dev/null

post "/sql-activity/$ENTITY_ID/transfer" '{"to_entity": "recipient-3", "amount": 300}' > /dev/null

STATE=$(get "/sql-activity/$ENTITY_ID/state")
TRANSFER_COUNT=$(echo "$STATE" | jq -r '.transfer_count')
TOTAL_TRANSFERRED=$(echo "$STATE" | jq -r '.total_transferred')

if [ "$TRANSFER_COUNT" != "3" ]; then
    echo "FAIL: Expected transfer_count=3, got $TRANSFER_COUNT"
    exit 1
fi

if [ "$TOTAL_TRANSFERRED" != "600" ]; then
    echo "FAIL: Expected total_transferred=600, got $TOTAL_TRANSFERRED"
    exit 1
fi

SQL_COUNT=$(get "/sql-activity/$ENTITY_ID/sql-count")
if [ "$SQL_COUNT" != "3" ]; then
    echo "FAIL: Expected SQL count=3, got $SQL_COUNT"
    exit 1
fi

echo "PASS: Multiple transfers succeeded"

# Test 3: Failing transfer (tests rollback)
echo ""
echo "Test 3: Failing transfer (tests rollback)"
echo "------------------------------------------"

# Get state before failing transfer
STATE_BEFORE=$(get "/sql-activity/$ENTITY_ID/state")
TRANSFER_COUNT_BEFORE=$(echo "$STATE_BEFORE" | jq -r '.transfer_count')
SQL_COUNT_BEFORE=$(get "/sql-activity/$ENTITY_ID/sql-count")

echo "State before failing transfer: transfer_count=$TRANSFER_COUNT_BEFORE, sql_count=$SQL_COUNT_BEFORE"

# Attempt failing transfer (should fail and rollback)
HTTP_CODE=$(curl_base_no_fail -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/sql-activity/$ENTITY_ID/failing-transfer" \
    -H "Content-Type: application/json" \
    -d '{"to_entity": "recipient-fail", "amount": 999}')

if [ "$HTTP_CODE" = "200" ]; then
    echo "FAIL: Failing transfer should have returned an error"
    exit 1
fi
echo "Failing transfer returned HTTP $HTTP_CODE (expected error)"

# Check state was NOT updated (rolled back)
STATE_AFTER=$(get "/sql-activity/$ENTITY_ID/state")
TRANSFER_COUNT_AFTER=$(echo "$STATE_AFTER" | jq -r '.transfer_count')
TOTAL_TRANSFERRED_AFTER=$(echo "$STATE_AFTER" | jq -r '.total_transferred')

if [ "$TRANSFER_COUNT_AFTER" != "$TRANSFER_COUNT_BEFORE" ]; then
    echo "FAIL: State should have been rolled back. Expected transfer_count=$TRANSFER_COUNT_BEFORE, got $TRANSFER_COUNT_AFTER"
    exit 1
fi

# Check SQL table was also NOT updated (rolled back)
SQL_COUNT_AFTER=$(get "/sql-activity/$ENTITY_ID/sql-count")

if [ "$SQL_COUNT_AFTER" != "$SQL_COUNT_BEFORE" ]; then
    echo "FAIL: SQL should have been rolled back. Expected sql_count=$SQL_COUNT_BEFORE, got $SQL_COUNT_AFTER"
    exit 1
fi

echo "PASS: Failing transfer correctly rolled back both state and SQL"

# Test 4: Transfer after failed transfer (verify entity still works)
echo ""
echo "Test 4: Transfer after failed transfer"
echo "---------------------------------------"

post "/sql-activity/$ENTITY_ID/transfer" '{"to_entity": "recipient-4", "amount": 400}' > /dev/null

STATE=$(get "/sql-activity/$ENTITY_ID/state")
TRANSFER_COUNT=$(echo "$STATE" | jq -r '.transfer_count')

if [ "$TRANSFER_COUNT" != "4" ]; then
    echo "FAIL: Expected transfer_count=4, got $TRANSFER_COUNT"
    exit 1
fi

SQL_COUNT=$(get "/sql-activity/$ENTITY_ID/sql-count")
if [ "$SQL_COUNT" != "4" ]; then
    echo "FAIL: Expected SQL count=4, got $SQL_COUNT"
    exit 1
fi

echo "PASS: Entity continues to work after failed transfer"

echo ""
echo "========================================="
echo "All SQL Activity tests passed!"
echo "========================================="
