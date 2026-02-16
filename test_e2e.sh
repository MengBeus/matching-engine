#!/bin/bash
# Week 5 Recovery E2E Automated Test Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test configuration
API_URL="http://localhost:8080"
DATA_DIR="./data"
API_BIN="./api"
TEST_SYMBOL="BTC-USDT"
SNAPSHOT_INTERVAL=100

# Counters
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

test_pass() {
    echo -e "${GREEN}✓${NC} $1"
    ((TESTS_PASSED++))
}

test_fail() {
    echo -e "${RED}✗${NC} $1"
    ((TESTS_FAILED++))
}

cleanup() {
    log_info "Cleaning up..."
    if [ ! -z "$API_PID" ]; then
        kill $API_PID 2>/dev/null || true
        wait $API_PID 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Test functions
test_event_persistence() {
    log_info "Test 1: Event Persistence"

    # Send a few orders
    for i in {1..5}; do
        curl -s -X POST $API_URL/v1/orders \
          -H "Content-Type: application/json" \
          -d "{
            \"client_order_id\": \"test-$i\",
            \"account_id\": \"acc-001\",
            \"symbol\": \"$TEST_SYMBOL\",
            \"side\": \"BUY\",
            \"price\": \"50000.00\",
            \"quantity\": \"0.1\",
            \"idempotency_key\": \"idem-test-$i\"
          }" > /dev/null
    done

    sleep 1

    # Check event log exists
    if [ -f "$DATA_DIR/events/$TEST_SYMBOL/events.log" ]; then
        test_pass "Event log file created"

        # Check event count
        EVENT_COUNT=$(wc -l < "$DATA_DIR/events/$TEST_SYMBOL/events.log")
        if [ $EVENT_COUNT -ge 5 ]; then
            test_pass "Events persisted (count: $EVENT_COUNT)"
        else
            test_fail "Insufficient events (expected >= 5, got $EVENT_COUNT)"
        fi

        # Check JSON format
        if cat "$DATA_DIR/events/$TEST_SYMBOL/events.log" | head -1 | jq . > /dev/null 2>&1; then
            test_pass "Event log format valid (JSON Lines)"
        else
            test_fail "Event log format invalid"
        fi
    else
        test_fail "Event log file not created"
    fi
}

test_snapshot_creation() {
    log_info "Test 2: Snapshot Creation"

    # Send enough orders to trigger snapshot
    log_info "Sending $((SNAPSHOT_INTERVAL + 10)) orders to trigger snapshot..."
    for i in $(seq 1 $((SNAPSHOT_INTERVAL + 10))); do
        curl -s -X POST $API_URL/v1/orders \
          -H "Content-Type: application/json" \
          -d "{
            \"client_order_id\": \"snap-test-$i\",
            \"account_id\": \"acc-001\",
            \"symbol\": \"$TEST_SYMBOL\",
            \"side\": \"BUY\",
            \"price\": \"$((50000 + i)).00\",
            \"quantity\": \"0.01\",
            \"idempotency_key\": \"idem-snap-$i\"
          }" > /dev/null

        if [ $((i % 20)) -eq 0 ]; then
            echo -n "."
        fi
    done
    echo ""

    sleep 2

    # Check snapshot exists
    if [ -d "$DATA_DIR/snapshots/$TEST_SYMBOL" ]; then
        SNAPSHOT_COUNT=$(ls "$DATA_DIR/snapshots/$TEST_SYMBOL"/*.json 2>/dev/null | wc -l)
        if [ $SNAPSHOT_COUNT -gt 0 ]; then
            test_pass "Snapshot created (count: $SNAPSHOT_COUNT)"

            # Check snapshot format
            LATEST_SNAPSHOT=$(ls -t "$DATA_DIR/snapshots/$TEST_SYMBOL"/*.json | head -1)
            if cat "$LATEST_SNAPSHOT" | jq . > /dev/null 2>&1; then
                test_pass "Snapshot format valid (JSON)"

                # Check snapshot has required fields
                if cat "$LATEST_SNAPSHOT" | jq -e '.last_sequence' > /dev/null 2>&1; then
                    test_pass "Snapshot contains last_sequence field"
                else
                    test_fail "Snapshot missing last_sequence field"
                fi
            else
                test_fail "Snapshot format invalid"
            fi
        else
            test_fail "No snapshots created"
        fi
    else
        test_fail "Snapshot directory not created"
    fi
}

test_recovery() {
    log_info "Test 3: State Recovery"

    # Get current event count before restart
    EVENT_COUNT_BEFORE=$(wc -l < "$DATA_DIR/events/$TEST_SYMBOL/events.log")
    log_info "Event count before restart: $EVENT_COUNT_BEFORE"

    # Stop service
    log_info "Stopping service..."
    kill $API_PID
    wait $API_PID 2>/dev/null || true
    API_PID=""

    sleep 1

    # Restart service
    log_info "Restarting service..."
    DATA_DIR=$DATA_DIR $API_BIN > service.log 2>&1 &
    API_PID=$!

    sleep 3

    # Check if service started successfully
    if ps -p $API_PID > /dev/null; then
        test_pass "Service restarted successfully"

        # Check recovery log
        if grep -q "Recovering.*symbols" service.log; then
            test_pass "Recovery process initiated"

            if grep -q "Successfully recovered $TEST_SYMBOL" service.log; then
                test_pass "Symbol recovered successfully"
            else
                test_fail "Symbol recovery not confirmed in logs"
            fi
        else
            test_warn "Recovery log not found (might be empty data)"
        fi

        # Send a new order to verify sequence continuity
        RESPONSE=$(curl -s -X POST $API_URL/v1/orders \
          -H "Content-Type: application/json" \
          -d "{
            \"client_order_id\": \"after-restart\",
            \"account_id\": \"acc-001\",
            \"symbol\": \"$TEST_SYMBOL\",
            \"side\": \"BUY\",
            \"price\": \"51000.00\",
            \"quantity\": \"0.1\",
            \"idempotency_key\": \"idem-after-restart\"
          }")

        if echo "$RESPONSE" | jq -e '.data.order_id' > /dev/null 2>&1; then
            test_pass "New order accepted after restart"

            # Check event count increased
            EVENT_COUNT_AFTER=$(wc -l < "$DATA_DIR/events/$TEST_SYMBOL/events.log")
            if [ $EVENT_COUNT_AFTER -gt $EVENT_COUNT_BEFORE ]; then
                test_pass "Event log continues after restart"
            else
                test_fail "Event log not updated after restart"
            fi
        else
            test_fail "Failed to place order after restart"
        fi
    else
        test_fail "Service failed to restart"
    fi
}

# Main test execution
main() {
    echo "=========================================="
    echo "Week 5 Recovery E2E Automated Test"
    echo "=========================================="
    echo ""

    # Step 1: Clean up
    log_info "Step 1: Cleaning up old data..."
    rm -rf $DATA_DIR
    rm -f $API_BIN
    rm -f service.log

    # Step 2: Build
    log_info "Step 2: Building service..."
    if go build -o $API_BIN ./cmd/api; then
        test_pass "Service built successfully"
    else
        test_fail "Service build failed"
        exit 1
    fi

    # Step 3: Start service
    log_info "Step 3: Starting service..."
    DATA_DIR=$DATA_DIR $API_BIN > service.log 2>&1 &
    API_PID=$!

    sleep 2

    if ps -p $API_PID > /dev/null; then
        test_pass "Service started (PID: $API_PID)"
    else
        test_fail "Service failed to start"
        cat service.log
        exit 1
    fi

    # Run tests
    echo ""
    test_event_persistence
    echo ""
    test_snapshot_creation
    echo ""
    test_recovery

    # Summary
    echo ""
    echo "=========================================="
    echo "Test Summary"
    echo "=========================================="
    echo -e "${GREEN}Passed:${NC} $TESTS_PASSED"
    echo -e "${RED}Failed:${NC} $TESTS_FAILED"
    echo ""

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed!${NC}"
        exit 1
    fi
}

# Check dependencies
if ! command -v jq &> /dev/null; then
    log_error "jq is required but not installed. Please install jq first."
    exit 1
fi

if ! command -v curl &> /dev/null; then
    log_error "curl is required but not installed. Please install curl first."
    exit 1
fi

# Run main
main
