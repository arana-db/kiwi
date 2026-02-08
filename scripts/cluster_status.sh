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

# Kiwi Raft Cluster Status Script

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
leader() { echo -e "${MAGENTA}👑 $1${NC}"; }

# Configuration
BASE_RAFT_PORT=8001
NODE_COUNT=3

# Check if jq is available
if ! command -v jq &> /dev/null; then
    warning "⚠️  jq not found. Install it for better JSON formatting:"
    warning "   brew install jq  (macOS)"
    warning "   apt install jq   (Ubuntu/Debian)"
    echo ""
    USE_JQ=false
else
    USE_JQ=true
fi

# Banner
info "=== Kiwi Raft Cluster Status ==="
echo ""

# Check which nodes are running
info "📊 Node Status:"
echo ""

RUNNING_NODES=()
for i in $(seq 1 $NODE_COUNT); do
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    REDIS_PORT=$((7378 + i))
    
    # Check if node is responding
    if curl -s -f "http://127.0.0.1:${RAFT_PORT}/raft/metrics" > /dev/null 2>&1; then
        success "  ✓ Node ${i} - Running"
        info "    Redis: 127.0.0.1:${REDIS_PORT}"
        info "    Raft:  127.0.0.1:${RAFT_PORT}"
        RUNNING_NODES+=($i)
    else
        error "  ✗ Node ${i} - Not responding"
    fi
done

if [ ${#RUNNING_NODES[@]} -eq 0 ]; then
    echo ""
    error "No nodes are running!"
    echo ""
    info "Start the cluster with:"
    info "  ./scripts/start_cluster.sh"
    exit 1
fi

echo ""
info "🔍 Cluster Information:"
echo ""

# Query leader from first running node
FIRST_NODE=${RUNNING_NODES[0]}
RAFT_PORT=$((BASE_RAFT_PORT + FIRST_NODE - 1))

LEADER_RESPONSE=$(curl -s "http://127.0.0.1:${RAFT_PORT}/raft/leader")

if [ $USE_JQ = true ]; then
    LEADER_ID=$(echo "$LEADER_RESPONSE" | jq -r '.data.leader_id // "none"')
    
    if [ "$LEADER_ID" != "none" ] && [ "$LEADER_ID" != "null" ]; then
        LEADER_RAFT_ADDR=$(echo "$LEADER_RESPONSE" | jq -r '.data.node.raft_addr // "unknown"')
        LEADER_RESP_ADDR=$(echo "$LEADER_RESPONSE" | jq -r '.data.node.resp_addr // "unknown"')
        
        leader "Leader: Node ${LEADER_ID}"
        info "  Raft Address:  ${LEADER_RAFT_ADDR}"
        info "  Redis Address: ${LEADER_RESP_ADDR}"
    else
        warning "  ⚠️  No leader elected yet"
    fi
else
    echo "$LEADER_RESPONSE"
fi

echo ""
info "📈 Node Metrics:"
echo ""

# Get metrics from all running nodes
for i in "${RUNNING_NODES[@]}"; do
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    
    METRICS=$(curl -s "http://127.0.0.1:${RAFT_PORT}/raft/metrics")
    
    if [ $USE_JQ = true ]; then
        IS_LEADER=$(echo "$METRICS" | jq -r '.data.is_leader // false')
        CURRENT_TERM=$(echo "$METRICS" | jq -r '.data.current_term // 0')
        LAST_LOG_INDEX=$(echo "$METRICS" | jq -r '.data.last_log_index // 0')
        LAST_APPLIED=$(echo "$METRICS" | jq -r '.data.last_applied // 0')
        
        if [ "$IS_LEADER" = "true" ]; then
            leader "Node ${i} (LEADER):"
        else
            info "Node ${i}:"
        fi
        
        echo "  Term:            ${CURRENT_TERM}"
        echo "  Last Log Index:  ${LAST_LOG_INDEX}"
        echo "  Last Applied:    ${LAST_APPLIED}"
        echo ""
    else
        info "Node ${i}:"
        echo "$METRICS" | head -n 20
        echo ""
    fi
done

# Show quick commands
info "💡 Quick Commands:"
echo ""
echo "  Check leader:        curl http://127.0.0.1:8001/raft/leader | jq"
echo "  Check node metrics:  curl http://127.0.0.1:8001/raft/metrics | jq"
echo "  Connect to node 1:   redis-cli -p 7379"
echo "  Connect to node 2:   redis-cli -p 7380"
echo "  Connect to node 3:   redis-cli -p 7381"
echo ""
