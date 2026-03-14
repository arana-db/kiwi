#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[TEST]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }
log_step() { echo -e "${YELLOW}  ->${NC} $1"; }

TESTS_PASSED=0
TESTS_FAILED=0

REDIS_PORTS=(7379 7380 7381)
RAFT_PORTS=(8081 8082 8083)

check_redis_cli() {
    if ! command -v redis-cli &> /dev/null; then
        log_fail "redis-cli not found. Please install redis-tools."
        log_info "On macOS: brew install redis"
        exit 1
    fi
}

check_nodes_running() {
    log_info "Checking if nodes are running..."
    
    for port in ${REDIS_PORTS[*]}; do
        if ! nc -z 127.0.0.1 $port 2>/dev/null; then
            log_fail "Node on port $port is not running"
            log_info "Start cluster first: ./scripts/cluster/start_cluster.sh --init"
            exit 1
        fi
    done
    
    log_pass "All nodes are running"
}

find_leader() {
    for i in 0 1 2; do
        local port=${RAFT_PORTS[$i]}
        local response=$(curl -s http://127.0.0.1:$port/raft/metrics 2>/dev/null)
        
        if echo "$response" | grep -q '"is_leader":true'; then
            LEADER_REDIS_PORT=${REDIS_PORTS[$i]}
            LEADER_INDEX=$((i + 1))
            return 0
        fi
    done
    return 1
}

test_leader_election() {
    log_info "Test 1: Leader Election"
    
    if find_leader; then
        log_step "Leader found on Node $LEADER_INDEX (Redis port: $LEADER_REDIS_PORT)"
        log_pass "Leader election successful"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_fail "No leader found"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

test_write_to_leader() {
    log_info "Test 2: Write to Leader"
    
    local key="test_key_$$"
    local value="test_value_$(date +%s)"
    
    log_step "Writing key='$key' value='$value' to leader (port $LEADER_REDIS_PORT)"
    
    local result=$(redis-cli -p $LEADER_REDIS_PORT SET "$key" "$value" 2>/dev/null)
    
    if [ "$result" == "OK" ]; then
        log_pass "Write to leader successful"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        TEST_KEY="$key"
        TEST_VALUE="$value"
    else
        log_fail "Write to leader failed: $result"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

test_replication() {
    log_info "Test 3: Replication to All Nodes"
    
    if [ -z "$TEST_KEY" ]; then
        log_fail "No test key available (write test may have failed)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return
    fi
    
    sleep 1
    
    local all_match=true
    
    for i in 0 1 2; do
        local port=${REDIS_PORTS[$i]}
        local node_num=$((i + 1))
        log_step "Reading from Node $node_num (port $port)..."
        
        local result=$(redis-cli -p $port GET "$TEST_KEY" 2>/dev/null)
        
        if [ "$result" == "$TEST_VALUE" ]; then
            log_pass "Node $node_num: Value matches ('$result')"
        else
            log_fail "Node $node_num: Value mismatch (expected '$TEST_VALUE', got '$result')"
            all_match=false
        fi
    done
    
    if $all_match; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

test_multiple_writes() {
    log_info "Test 4: Multiple Sequential Writes"
    
    local success_count=0
    local total=5
    
    for i in $(seq 1 $total); do
        local key="multi_test_$$_$i"
        local value="value_$i"
        
        redis-cli -p $LEADER_REDIS_PORT SET "$key" "$value" > /dev/null 2>&1
        
        local follower_port=${REDIS_PORTS[1]}
        local result=$(redis-cli -p $follower_port GET "$key" 2>/dev/null)
        
        if [ "$result" == "$value" ]; then
            success_count=$((success_count + 1))
        fi
    done
    
    if [ $success_count -eq $total ]; then
        log_pass "All $total writes replicated successfully"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        log_fail "Only $success_count/$total writes replicated"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

test_delete() {
    log_info "Test 5: Delete Operation"
    
    local key="delete_test_$$"
    local value="to_be_deleted"
    
    redis-cli -p $LEADER_REDIS_PORT SET "$key" "$value" > /dev/null 2>&1
    sleep 1
    
    log_step "Deleting key '$key' from leader..."
    local del_result=$(redis-cli -p $LEADER_REDIS_PORT DEL "$key" 2>/dev/null)
    
    if [ "$del_result" == "1" ]; then
        log_pass "Delete successful on leader"
        
        sleep 1
        local all_deleted=true
        
        for port in ${REDIS_PORTS[*]}; do
            local result=$(redis-cli -p $port GET "$key" 2>/dev/null)
            if [ -n "$result" ]; then
                log_fail "Key still exists on port $port: '$result'"
                all_deleted=false
            fi
        done
        
        if $all_deleted; then
            log_pass "Key deleted on all nodes"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        else
            TESTS_FAILED=$((TESTS_FAILED + 1))
        fi
    else
        log_fail "Delete failed"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

test_raft_metrics() {
    log_info "Test 6: Raft Metrics API"
    
    local success=true
    
    for i in 0 1 2; do
        local port=${RAFT_PORTS[$i]}
        local node_num=$((i + 1))
        log_step "Checking metrics on Node $node_num..."
        
        local response=$(curl -s http://127.0.0.1:$port/raft/metrics 2>/dev/null)
        
        if echo "$response" | grep -q '"success":true'; then
            local is_leader=$(echo "$response" | grep -o '"is_leader":[^,}]*' | cut -d: -f2)
            log_pass "Node $node_num metrics OK (is_leader: $is_leader)"
        else
            log_fail "Node $node_num metrics failed"
            success=false
        fi
    done
    
    if $success; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

print_summary() {
    echo ""
    echo "=========================================="
    echo "           Test Summary"
    echo "=========================================="
    echo ""
    echo -e "  ${GREEN}Passed:${NC} $TESTS_PASSED"
    echo -e "  ${RED}Failed:${NC} $TESTS_FAILED"
    echo ""
    
    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}Some tests failed. Check logs for details.${NC}"
        echo ""
        echo "Log files:"
        for i in 1 2 3; do
            echo "  - $PROJECT_ROOT/logs/node${i}.log"
        done
        echo ""
        return 1
    fi
}

cleanup() {
    log_info "Cleaning up test keys..."
    for port in ${REDIS_PORTS[*]}; do
        redis-cli -p $port KEYS "test_*" 2>/dev/null | xargs redis-cli -p $port DEL > /dev/null 2>&1 || true
        redis-cli -p $port KEYS "multi_test_*" 2>/dev/null | xargs redis-cli -p $port DEL > /dev/null 2>&1 || true
        redis-cli -p $port KEYS "delete_test_*" 2>/dev/null | xargs redis-cli -p $port DEL > /dev/null 2>&1 || true
    done
}

main() {
    echo ""
    echo "=========================================="
    echo "     Kiwi Raft Cluster Test Suite"
    echo "=========================================="
    echo ""
    
    check_redis_cli
    check_nodes_running
    
    test_leader_election
    test_write_to_leader
    test_replication
    test_multiple_writes
    test_delete
    test_raft_metrics
    
    cleanup
    print_summary
}

main
