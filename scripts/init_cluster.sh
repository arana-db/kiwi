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

# Kiwi Raft Cluster Initialization Script

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info() { echo -e "${CYAN}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }
warning() { echo -e "${YELLOW}$1${NC}"; }
error() { echo -e "${RED}$1${NC}"; }

# Configuration
BASE_RAFT_PORT=8001
BASE_REDIS_PORT=7379
NODE_COUNT=${1:-3}
BINDING="127.0.0.1"

# Banner
info "=== Kiwi Raft Cluster Initialization ==="
echo ""

# Check if nodes are running
info "Checking if nodes are running..."
for i in $(seq 1 $NODE_COUNT); do
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    if ! curl -s -f "http://${BINDING}:${RAFT_PORT}/raft/metrics" > /dev/null 2>&1; then
        error "✗ Node ${i} is not running on port ${RAFT_PORT}"
        echo ""
        error "Please start the cluster first:"
        error "  ./scripts/start_cluster.sh --nodes ${NODE_COUNT}"
        exit 1
    fi
    success "✓ Node ${i} is running"
done

echo ""
info "Initializing cluster with ${NODE_COUNT} nodes..."

# Build the JSON payload
NODES_JSON="["
for i in $(seq 1 $NODE_COUNT); do
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    REDIS_PORT=$((BASE_REDIS_PORT + i - 1))
    
    if [ $i -gt 1 ]; then
        NODES_JSON="${NODES_JSON},"
    fi
    
    NODES_JSON="${NODES_JSON}[${i},{\"raft_addr\":\"${BINDING}:${RAFT_PORT}\",\"resp_addr\":\"${BINDING}:${REDIS_PORT}\"}]"
done
NODES_JSON="${NODES_JSON}]"

PAYLOAD="{\"nodes\":${NODES_JSON}}"

# Initialize cluster on node 1
INIT_PORT=$BASE_RAFT_PORT
info "Sending initialization request to node 1..."
echo ""

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD" \
    "http://${BINDING}:${INIT_PORT}/raft/init")

HTTP_CODE=$(echo "$RESPONSE" | tail -n 1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ]; then
    success "✓ Cluster initialized successfully!"
    echo ""
    
    # Wait for leader election
    info "Waiting for leader election..."
    sleep 2
    
    # Check leader
    LEADER_RESPONSE=$(curl -s "http://${BINDING}:${INIT_PORT}/raft/leader")
    
    if command -v jq &> /dev/null; then
        LEADER_ID=$(echo "$LEADER_RESPONSE" | jq -r '.data.leader_id // "none"')
        
        if [ "$LEADER_ID" != "none" ] && [ "$LEADER_ID" != "null" ]; then
            echo ""
            success "👑 Leader elected: Node ${LEADER_ID}"
            
            LEADER_RAFT_ADDR=$(echo "$LEADER_RESPONSE" | jq -r '.data.node.raft_addr')
            LEADER_RESP_ADDR=$(echo "$LEADER_RESPONSE" | jq -r '.data.node.resp_addr')
            
            info "  Raft Address:  ${LEADER_RAFT_ADDR}"
            info "  Redis Address: ${LEADER_RESP_ADDR}"
        else
            warning "⚠️  Leader not yet elected, please wait a moment"
        fi
    else
        echo "$LEADER_RESPONSE"
    fi
    
    echo ""
    success "=== Cluster Ready ==="
    echo ""
    info "Check cluster status:"
    info "  ./scripts/cluster_status.sh"
    echo ""
else
    error "✗ Failed to initialize cluster (HTTP ${HTTP_CODE})"
    echo ""
    error "Response:"
    echo "$BODY"
    exit 1
fi
