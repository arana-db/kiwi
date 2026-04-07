#!/bin/bash
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
#
# Script to start a multi-node Raft cluster for testing Kiwi.
# Each node runs in its own directory with separate storage.
# Logs are written to files, viewable with `tail -f`.
# Uses gRPC for cluster management (`grpcurl` required).
#
# Usage: ./start_node_cluster.sh [OPTIONS] [NODE_COUNT]
#   NODE_COUNT: Number of nodes to start (default: 3, range: 1-9)
#
# Options:
#   -h, --help     Show this help message
#   -v, --verbose  Enable verbose logging (debug level)
#   -q, --quiet    Suppress non-error output
#   -n, --no-test  Skip cluster initialization and testing

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

# Load library functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_lib.sh
source "$SCRIPT_DIR/cluster_lib.sh"

# =============================================================================
# Command Line Parsing
# =============================================================================

NODE_COUNT=$DEFAULT_NODE_COUNT
RUN_TESTS=true

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Usage: $0 [OPTIONS] [NODE_COUNT]"
            echo ""
            echo "Arguments:"
            echo "  NODE_COUNT    Number of nodes to start (default: 3, range: 1-9)"
            echo ""
            echo "Options:"
            echo "  -h, --help    Show this help message"
            echo "  -v, --verbose  Enable verbose logging"
            echo "  -q, --quiet    Suppress non-error output"
            echo "  -n, --no-test  Skip cluster initialization and testing"
            echo ""
            echo "Environment Variables:"
            echo "  LOG_LEVEL     Set log level (0=debug, 1=info, 2=warn, 3=error)"
            echo "  RUST_LOG      Set Rust log level (info, debug, warn, error)"
            echo ""
            echo "Examples:"
            echo "  $0              # Start 3 nodes (default)"
            echo "  $0 5            # Start 5 nodes"
            echo "  $0 -v 3         # Start 3 nodes with verbose logging"
            echo "  $0 -n 3         # Start 3 nodes, skip tests"
            exit 0
            ;;
        -v|--verbose)
            LOG_LEVEL=$LOG_LEVEL_DEBUG
            shift
            ;;
        -q|--quiet)
            LOG_LEVEL=$LOG_LEVEL_ERROR
            shift
            ;;
        -n|--no-test)
            RUN_TESTS=false
            shift
            ;;
        *)
            if [[ $1 =~ ^[0-9]+$ ]]; then
                NODE_COUNT=$1
            else
                log_error "Unknown option: $1"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate node count
validate_node_count "$NODE_COUNT" || exit 1

# Validate binary
validate_binary || exit 1

# Check grpcurl
GRPCURL=$(check_grpcurl) || exit 1

# =============================================================================
# Node Configuration
# =============================================================================

# Generate node configurations
# Format: "node_id:raft_port:resp_port"
NODES=()
for ((i=1; i<=NODE_COUNT; i++)); do
    raft_port=$((RAFT_PORT_BASE + i - 1))
    resp_port=$((RESP_PORT_BASE + i - 1))
    NODES+=("$i:$raft_port:$resp_port")
done

# =============================================================================
# Cleanup Handler
# =============================================================================

# Register cleanup on exit
trap cleanup_cluster EXIT

# Also handle Ctrl+C
trap 'log_info "Interrupted"; cleanup_cluster; exit 130' INT TERM

# =============================================================================
# Cluster Initialization
# =============================================================================

# Clean up any existing cluster
if [ -d "$CLUSTER_BASE_DIR" ]; then
    log_warn "Removing existing cluster directory..."
    cleanup_cluster
fi

# Create cluster base directory
mkdir -p "$CLUSTER_BASE_DIR"
: > "$PIDS_FILE"

# Create all nodes
log_info "Creating $NODE_COUNT nodes..."
for node_info in "${NODES[@]}"; do
    IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
    create_node "$node_id" "$raft_port" "$resp_port" || exit 1
done

# Start all nodes
log_info "Starting all nodes..."
for node_info in "${NODES[@]}"; do
    IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
    start_node "$node_id" || exit 1
done

# Wait for nodes to be ready
log_debug "Waiting for nodes to initialize..."
sleep 3

# =============================================================================
# Cluster Setup via gRPC
# =============================================================================

init_cluster() {
    log_info "Initializing Raft cluster via gRPC..."

    local first_node_raft="127.0.0.1:$RAFT_PORT_BASE"
    local cluster_json
    cluster_json=$(build_cluster_json "${NODES[@]}")

    # Initialize the cluster through node 1
    log_debug "Calling Initialize on node 1..."
    local init_result
    init_result=$(grpc_call "$first_node_raft" "kiwi.raft.v1.RaftAdminService" "Initialize" "$cluster_json" "$GRPCURL")

    if [[ "$init_result" == "error" ]]; then
        log_warn "Direct init failed, trying step-by-step setup..."

        # Add nodes 2..N as learners
        for node_info in "${NODES[@]}"; do
            IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
            if [ "$node_id" -ne 1 ]; then
                log_debug "Adding node $node_id as learner..."
                local learner_data="{\"node_id\": $node_id, \"node\": {\"raft_addr\": \"127.0.0.1:$raft_port\", \"resp_addr\": \"127.0.0.1:$resp_port\"}}"
                grpc_call "$first_node_raft" "kiwi.raft.v1.RaftAdminService" "AddLearner" "$learner_data" "$GRPCURL" > /dev/null
            fi
        done

        # Change membership to include all nodes
        log_debug "Changing cluster membership..."
        local membership_json
        membership_json=$(build_membership_json "${NODES[@]}")
        grpc_call "$first_node_raft" "kiwi.raft.v1.RaftAdminService" "ChangeMembership" "$membership_json" "$GRPCURL" > /dev/null
    fi

    sleep 2
    log_info "Cluster initialization complete"
}

init_cluster

# =============================================================================
# Show Status
# =============================================================================

log_info "Cluster Status:"
for node_info in "${NODES[@]}"; do
    IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
    get_node_status "$node_id" "$raft_port" "$resp_port"
done

# =============================================================================
# Run Tests
# =============================================================================

if [ "$RUN_TESTS" = "true" ]; then
    log_info "Running basic Raft tests..."

    # Find the leader
    local leader_raft=""
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        local metrics
        metrics=$(grpc_call "127.0.0.1:$raft_port" "kiwi.raft.v1.RaftMetricsService" "Metrics" "{}" "$GRPCURL")
        if [[ "$metrics" != "error" ]] && [[ "$metrics" == *"isLeader\": true"* ]]; then
            leader_raft="127.0.0.1:$raft_port"
            log_info "Leader is node $node_id (port $raft_port)"
            break
        fi
    done

    if [ -z "$leader_raft" ]; then
        log_warn "No leader found, skipping tests"
    else
        # Test write
        log_debug "Testing write operation..."
        local write_data='{"binlog": {"db_id": 0, "slot_idx": 0, "entries": [{"cf_idx": 0, "op_type": "Put", "key": "dGVzdF9r", "value": "dGVzdF92"}]}}'
        local write_response
        write_response=$(grpc_call "$leader_raft" "kiwi.raft.v1.RaftClientService" "Write" "$write_data" "$GRPCURL")
        if [[ "$write_response" != "error" ]]; then
            log_info "Write test: OK"
        else
            log_error "Write test: FAILED"
        fi

        # Test read
        log_debug "Testing read operation..."
        local read_response
        read_response=$(grpc_call "$leader_raft" "kiwi.raft.v1.RaftClientService" "Read" '{"key": "dGVzdF9r"}' "$GRPCURL")
        if [[ "$read_response" != "error" ]]; then
            log_info "Read test: OK"
        else
            log_error "Read test: FAILED"
        fi
    fi
fi

# =============================================================================
# Final Output
# =============================================================================

echo ""
log_info "========================================"
log_info "Cluster is running!"
log_info "========================================"
echo ""

for node_info in "${NODES[@]}"; do
    IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
    echo "Node $node_id:"
    echo "  gRPC:   127.0.0.1:$raft_port"
    echo "  RESP:   127.0.0.1:$resp_port"
    echo ""
done

echo "View logs with:"
echo "  tail -f $CLUSTER_BASE_DIR/node1/kiwi.log"
echo ""

echo "gRPC service examples:"
echo "  $GRPCURL -plaintext 127.0.0.1:$RAFT_PORT_BASE kiwi.raft.v1.RaftMetricsService/Leader"
echo "  $GRPCURL -plaintext 127.0.0.1:$RAFT_PORT_BASE kiwi.raft.v1.RaftMetricsService/Metrics"
echo ""

echo -e "${YELLOW}Press Ctrl+C to stop the cluster${NC}"

# Wait indefinitely for user to press Ctrl+C
wait
