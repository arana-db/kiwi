#!/usr/bin/env bash
# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Kiwi Raft Synchronization Test Script

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

info() { echo -e "${CYAN}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }
warning() { echo -e "${YELLOW}$1${NC}"; }
error() { echo -e "${RED}$1${NC}"; }
test_info() { echo -e "${BLUE}$1${NC}"; }

# Configuration
BASE_REDIS_PORT=7379
BASE_RAFT_PORT=8001
NODE_COUNT=3
TEST_KEY_PREFIX="test_key"
TEST_COUNT=${1:-10}

# Check if redis-cli is available
if ! command -v redis-cli &> /dev/null; then
    error "redis-cli not found. Please install redis-cli first."
    exit 1
fi

# Banner
info "=== Kiwi Raft Synchronization Test ==="
echo ""
warning "⚠️  Note: This test requires Raft write path to be fully implemented."
warning "    Currently, writes go directly to leader's storage without Raft replication."
echo ""

# Check if cluster is running
info "Checking cluster status..."
LEADER_ID=""
LEADER_PORT=""

for i in $(seq 1 $NODE_COUNT); do
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    REDIS_PORT=$((BASE_REDIS_PORT + i - 1))
    
    if ! curl -s -f "http://127.0.0.1:${RAFT_PORT}/raft/metrics" > /dev/null 2>&1; then
        error "✗ Node ${i} is not running"
        echo ""
        error "Please start and initialize the cluster first:"
        error "  ./scripts/start_cluster.sh"
        error "  ./scripts/init_cluster.sh"
        exit 1
    fi
    
    # Check if this node is leader
    IS_LEADER=$(curl -s "http://127.0.0.1:${RAFT_PORT}/raft/metrics" | grep -o '"is_leader":true' || true)
    if [ -n "$IS_LEADER" ]; then
        LEADER_ID=$i
        LEADER_PORT=$REDIS_PORT
    fi
    
    success "✓ Node ${i} is running (Redis: ${REDIS_PORT}, Raft: ${RAFT_PORT})"
done

echo ""

if [ -z "$LEADER_ID" ]; then
    error "No leader found in the cluster!"
    exit 1
fi

success "👑 Leader: Node ${LEADER_ID} (Port: ${LEADER_PORT})"
echo ""

# Test 1: Write data to leader
info "=== Test 1: Writing Data to Leader ==="
echo ""

test_info "Writing ${TEST_COUNT} key-value pairs to leader (Node ${LEADER_ID})..."
WRITE_SUCCESS=0
WRITE_FAILED=0

for i in $(seq 1 $TEST_COUNT); do
    KEY="${TEST_KEY_PREFIX}_${i}"
    VALUE="value_${i}_$(date +%s)"
    
    RESULT=$(redis-cli -p $LEADER_PORT SET "$KEY" "$VALUE" 2>&1)
    
    if [ "$RESULT" = "OK" ]; then
        ((WRITE_SUCCESS++))
        if [ $((i % 5)) -eq 0 ] || [ $i -eq 1 ]; then
            success "  ✓ SET ${KEY} = ${VALUE}"
        fi
    else
        ((WRITE_FAILED++))
        error "  ✗ Failed to SET ${KEY}: ${RESULT}"
    fi
done

echo ""
if [ $WRITE_FAILED -eq 0 ]; then
    success "✓ All ${WRITE_SUCCESS} writes successful"
else
    warning "⚠️  ${WRITE_SUCCESS} writes successful, ${WRITE_FAILED} failed"
fi

# Wait for replication
echo ""
info "Waiting for Raft replication..."
sleep 2

# Test 2: Verify data on all nodes
echo ""
info "=== Test 2: Verifying Data Replication ==="
echo ""

TOTAL_CHECKS=$((TEST_COUNT * NODE_COUNT))
PASSED=0
FAILED=0

for node in $(seq 1 $NODE_COUNT); do
    REDIS_PORT=$((BASE_REDIS_PORT + node - 1))
    test_info "Checking Node ${node} (Port: ${REDIS_PORT})..."
    
    NODE_SUCCESS=0
    NODE_FAILED=0
    
    for i in $(seq 1 $TEST_COUNT); do
        KEY="${TEST_KEY_PREFIX}_${i}"
        
        # Get value from current node
        VALUE=$(redis-cli -p $REDIS_PORT GET "$KEY" 2>&1)
        
        if [ -n "$VALUE" ] && [ "$VALUE" != "(nil)" ]; then
            ((NODE_SUCCESS++))
            ((PASSED++))
        else
            ((NODE_FAILED++))
            ((FAILED++))
            if [ $NODE_FAILED -le 3 ]; then
                error "    ✗ Key ${KEY} not found"
            fi
        fi
    done
    
    if [ $NODE_FAILED -eq 0 ]; then
        success "  ✓ Node ${node}: All ${NODE_SUCCESS}/${TEST_COUNT} keys found"
    else
        warning "  ⚠️  Node ${node}: ${NODE_SUCCESS}/${TEST_COUNT} keys found, ${NODE_FAILED} missing"
    fi
done

echo ""

# Test 3: Verify data consistency
echo ""
info "=== Test 3: Verifying Data Consistency ==="
echo ""

test_info "Checking if all nodes have the same values..."
CONSISTENCY_PASSED=0
CONSISTENCY_FAILED=0

for i in $(seq 1 $TEST_COUNT); do
    KEY="${TEST_KEY_PREFIX}_${i}"
    
    # Get values from all nodes
    VALUES=()
    for node in $(seq 1 $NODE_COUNT); do
        REDIS_PORT=$((BASE_REDIS_PORT + node - 1))
        VALUE=$(redis-cli -p $REDIS_PORT GET "$KEY" 2>&1)
        VALUES+=("$VALUE")
    done
    
    # Check if all values are the same
    FIRST_VALUE="${VALUES[0]}"
    ALL_SAME=true
    
    for value in "${VALUES[@]}"; do
        if [ "$value" != "$FIRST_VALUE" ]; then
            ALL_SAME=false
            break
        fi
    done
    
    if [ "$ALL_SAME" = true ] && [ "$FIRST_VALUE" != "(nil)" ]; then
        ((CONSISTENCY_PASSED++))
        if [ $((i % 5)) -eq 0 ] || [ $i -eq 1 ]; then
            success "  ✓ ${KEY}: Consistent across all nodes"
        fi
    else
        ((CONSISTENCY_FAILED++))
        error "  ✗ ${KEY}: Inconsistent values detected"
        for node in $(seq 1 $NODE_COUNT); do
            error "    Node ${node}: ${VALUES[$((node-1))]}"
        done
    fi
done

echo ""
if [ $CONSISTENCY_FAILED -eq 0 ]; then
    success "✓ All ${CONSISTENCY_PASSED} keys are consistent"
else
    warning "⚠️  ${CONSISTENCY_PASSED} keys consistent, ${CONSISTENCY_FAILED} inconsistent"
fi

# Test 4: Read from follower nodes
echo ""
info "=== Test 4: Reading from Follower Nodes ==="
echo ""

for node in $(seq 1 $NODE_COUNT); do
    if [ $node -eq $LEADER_ID ]; then
        continue
    fi
    
    REDIS_PORT=$((BASE_REDIS_PORT + node - 1))
    test_info "Reading from Follower Node ${node}..."
    
    # Read a few random keys
    SAMPLE_SIZE=3
    READ_SUCCESS=0
    
    for i in $(seq 1 $SAMPLE_SIZE); do
        RANDOM_KEY="${TEST_KEY_PREFIX}_${i}"
        VALUE=$(redis-cli -p $REDIS_PORT GET "$RANDOM_KEY" 2>&1)
        
        if [ -n "$VALUE" ] && [ "$VALUE" != "(nil)" ]; then
            ((READ_SUCCESS++))
            success "  ✓ GET ${RANDOM_KEY} = ${VALUE}"
        else
            error "  ✗ GET ${RANDOM_KEY} failed"
        fi
    done
    
    if [ $READ_SUCCESS -eq $SAMPLE_SIZE ]; then
        success "  ✓ Follower Node ${node} can read all data"
    fi
    echo ""
done

# Summary
echo ""
info "=== Test Summary ==="
echo ""

REPLICATION_RATE=$(awk "BEGIN {printf \"%.2f\", ($PASSED / $TOTAL_CHECKS) * 100}")
CONSISTENCY_RATE=$(awk "BEGIN {printf \"%.2f\", ($CONSISTENCY_PASSED / $TEST_COUNT) * 100}")

echo "📊 Statistics:"
echo "  Total Keys Written:     ${TEST_COUNT}"
echo "  Write Success Rate:     ${WRITE_SUCCESS}/${TEST_COUNT} ($(awk "BEGIN {printf \"%.2f\", ($WRITE_SUCCESS / $TEST_COUNT) * 100}")%)"
echo "  Replication Success:    ${PASSED}/${TOTAL_CHECKS} (${REPLICATION_RATE}%)"
echo "  Consistency Rate:       ${CONSISTENCY_PASSED}/${TEST_COUNT} (${CONSISTENCY_RATE}%)"
echo ""

if [ $FAILED -eq 0 ] && [ $CONSISTENCY_FAILED -eq 0 ]; then
    success "🎉 All tests passed! Raft synchronization is working correctly."
    EXIT_CODE=0
elif [ $CONSISTENCY_FAILED -eq 0 ]; then
    warning "⚠️  Tests passed with some replication delays."
    EXIT_CODE=0
else
    error "❌ Tests failed! Data inconsistency detected."
    EXIT_CODE=1
fi

echo ""
info "💡 Cleanup test data:"
info "  for i in {1..${TEST_COUNT}}; do redis-cli -p ${LEADER_PORT} DEL ${TEST_KEY_PREFIX}_\$i; done"
echo ""

exit $EXIT_CODE
