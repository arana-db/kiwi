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
# Cluster library functions for Kiwi Raft cluster management.
# This script provides common functions for cluster operations.

# =============================================================================
# Configuration and Constants
# =============================================================================

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# Log levels
readonly LOG_LEVEL_DEBUG=0
readonly LOG_LEVEL_INFO=1
readonly LOG_LEVEL_WARN=2
readonly LOG_LEVEL_ERROR=3

# Default log level (can be overridden by environment variable)
LOG_LEVEL="${LOG_LEVEL:-$LOG_LEVEL_INFO}"

# Base directory for the cluster
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CLUSTER_BASE_DIR="${CLUSTER_BASE_DIR:-$PROJECT_ROOT/cluster_test}"
BINARY_PATH="${BINARY_PATH:-$PROJECT_ROOT/target/debug/kiwi}"

# Base ports
RAFT_PORT_BASE=8081
RESP_PORT_BASE=7379

# Default node count
DEFAULT_NODE_COUNT=3

# PIDS file for tracking PIDs started by this script
PIDS_FILE="$CLUSTER_BASE_DIR/.pids"

# =============================================================================
# Logging Functions
# =============================================================================

log_debug() {
    if [ "$LOG_LEVEL" -le "$LOG_LEVEL_DEBUG" ]; then
        echo -e "${BLUE}[DEBUG]${NC} $*" >&2
    fi
}

log_info() {
    if [ "$LOG_LEVEL" -le "$LOG_LEVEL_INFO" ]; then
        echo -e "${GREEN}[INFO]${NC} $*" >&2
    fi
}

log_warn() {
    if [ "$LOG_LEVEL" -le "$LOG_LEVEL_WARN" ]; then
        echo -e "${YELLOW}[WARN]${NC} $*" >&2
    fi
}

log_error() {
    if [ "$LOG_LEVEL" -le "$LOG_LEVEL_ERROR" ]; then
        echo -e "${RED}[ERROR]${NC} $*" >&2
    fi
}

# =============================================================================
# Utility Functions
# =============================================================================

# Check if a command exists
command_exists() {
    command -v "$1" &> /dev/null
}

# Check if a port is listening
is_port_listening() {
    local host="$1"
    local port="$2"
    nc -z "$host" "$port" 2>/dev/null
}

# Get PID from pid file
get_pid() {
    local pid_file="$1"
    if [ -f "$pid_file" ]; then
        cat "$pid_file"
    fi
}

# Check if a process is running
is_process_running() {
    local pid="$1"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    return 1
}

# Wait for a condition with timeout
wait_for_condition() {
    local condition="$1"
    local timeout="$2"
    local interval="${3:-1}"
    local elapsed=0

    while [ $elapsed -lt "$timeout" ]; do
        if eval "$condition"; then
            return 0
        fi
        sleep "$interval"
        elapsed=$((elapsed + interval))
    done
    return 1
}

# =============================================================================
# Node Management Functions
# =============================================================================

# Create node directory and config
create_node() {
    local node_id=$1
    local raft_port=$2
    local resp_port=$3
    local node_dir="$CLUSTER_BASE_DIR/node$node_id"

    log_debug "Creating node $node_id in $node_dir"

    # Create node directory
    mkdir -p "$node_dir" || {
        log_error "Failed to create node directory: $node_dir"
        return 1
    }

    # Create config file
    cat > "$node_dir/config.toml" << EOF
# Node $node_id configuration
binding = 127.0.0.1
port = $resp_port
network_threads = 1
storage_threads = 2

raft-node-id = $node_id
raft-addr = 127.0.0.1:$raft_port
raft-resp-addr = 127.0.0.1:$resp_port
raft-data-dir = ./raft_data
EOF

    # Copy binary to node directory
    if [ -f "$BINARY_PATH" ]; then
        cp "$BINARY_PATH" "$node_dir/kiwi" || {
            log_error "Failed to copy binary to $node_dir"
            return 1
        }
        chmod +x "$node_dir/kiwi"
    else
        log_error "Binary not found: $BINARY_PATH"
        return 1
    fi

    log_info "Node $node_id created at $node_dir"
    return 0
}

# Start a node
start_node() {
    local node_id=$1
    local node_dir="$CLUSTER_BASE_DIR/node$node_id"
    local log_file="$node_dir/kiwi.log"

    log_debug "Starting node $node_id"

    # Check if node directory exists
    if [ ! -d "$node_dir" ]; then
        log_error "Node directory does not exist: $node_dir"
        return 1
    fi

    # Check if binary exists
    if [ ! -f "$node_dir/kiwi" ]; then
        log_error "Node binary not found: $node_dir/kiwi"
        return 1
    fi

    # Check if already running
    if [ -f "$node_dir/kiwi.pid" ]; then
        local existing_pid=$(cat "$node_dir/kiwi.pid")
        if is_process_running "$existing_pid"; then
            log_warn "Node $node_id is already running (PID: $existing_pid)"
            return 0
        fi
    fi

    # Start node from its own directory
    cd "$node_dir"
    RUST_LOG="${RUST_LOG:-info}" ./kiwi --config config.toml > "$log_file" 2>&1 &
    local pid=$!
    cd - > /dev/null

    # Save PID
    echo "$pid" >> "$PIDS_FILE"
    echo "$pid" > "$node_dir/kiwi.pid"

    log_info "Node $node_id started (PID: $pid)"
    log_debug "Log file: $log_file"

    return 0
}

# Stop a node
stop_node() {
    local node_id=$1
    local node_dir="$CLUSTER_BASE_DIR/node$node_id"
    local force="${2:-false}"

    log_debug "Stopping node $node_id"

    if [ ! -f "$node_dir/kiwi.pid" ]; then
        log_debug "No PID file for node $node_id"
        return 0
    fi

    local pid=$(cat "$node_dir/kiwi.pid")

    if ! is_process_running "$pid"; then
        log_debug "Node $node_id is not running"
        rm -f "$node_dir/kiwi.pid"
        return 0
    fi

    # Send SIGTERM first
    kill "$pid" 2>/dev/null || true
    sleep 1

    # Force kill if requested
    if [ "$force" = "true" ] || ! is_process_running "$pid"; then
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$node_dir/kiwi.pid"
    log_info "Node $node_id stopped"
    return 0
}

# =============================================================================
# Cluster JSON Building Functions
# =============================================================================

# Build cluster initialization JSON
build_cluster_json() {
    local nodes=("$@")
    local json="{\"nodes\": ["
    local first=true

    for node_info in "${nodes[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        if [ "$first" = true ]; then
            first=false
        else
            json+=", "
        fi
        json+="{\"node_id\": $node_id, \"raft_addr\": \"127.0.0.1:$raft_port\", \"resp_addr\": \"127.0.0.1:$resp_port\"}"
    done

    json+="]}"
    echo "$json"
}

# Build membership JSON
build_membership_json() {
    local nodes=("$@")
    local json="{\"members\": ["
    local first=true

    for node_info in "${nodes[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        if [ "$first" = true ]; then
            first=false
        else
            json+=", "
        fi
        json+="{\"node_id\": $node_id, \"raft_addr\": \"127.0.0.1:$raft_port\", \"resp_addr\": \"127.0.0.1:$resp_port\"}"
    done

    json+="], \"retain\": false}"
    echo "$json"
}

# =============================================================================
# gRPC Helper Functions
# =============================================================================

# Find grpcurl executable
find_grpcurl() {
    if command_exists grpcurl; then
        echo "grpcurl"
        return 0
    elif [ -f "$HOME/go/bin/grpcurl" ]; then
        echo "$HOME/go/bin/grpcurl"
        return 0
    elif [ -f "/usr/local/bin/grpcurl" ]; then
        echo "/usr/local/bin/grpcurl"
        return 0
    fi
    return 1
}

# Check if grpcurl is available
check_grpcurl() {
    local grpcurl
    grpcurl=$(find_grpcurl) || {
        log_error "grpcurl is not installed"
        log_info "Install grpcurl:"
        log_info "  go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
        log_info "  or: brew install grpcurl (macOS)"
        return 1
    }
    echo "$grpcurl"
    return 0
}

# Make gRPC call with error handling
grpc_call() {
    local host="$1"
    local service="$2"
    local method="$3"
    local data="$4"
    local grpcurl="$5"

    local result
    result=$($grpcurl -plaintext -d "$data" "$host" "$service/$method" 2>/dev/null) || {
        log_debug "gRPC call failed: $service/$method to $host"
        echo "error"
        return 1
    }
    echo "$result"
    return 0
}

# =============================================================================
# Cleanup Functions
# =============================================================================

# Cleanup cluster
cleanup_cluster() {
    log_info "Cleaning up cluster..."

    # Stop all tracked processes
    if [ -f "$PIDS_FILE" ]; then
        while IFS= read -r pid; do
            if [ -n "$pid" ] && is_process_running "$pid"; then
                log_debug "Stopping PID $pid"
                kill "$pid" 2>/dev/null || true
            fi
        done < "$PIDS_FILE"

        sleep 1

        # Force kill any remaining processes
        while IFS= read -r pid; do
            if [ -n "$pid" ] && is_process_running "$pid"; then
                log_debug "Force killing PID $pid"
                kill -9 "$pid" 2>/dev/null || true
            fi
        done < "$PIDS_FILE"

        rm -f "$PIDS_FILE"
    fi

    # Remove cluster directory
    if [ -d "$CLUSTER_BASE_DIR" ]; then
        rm -rf "$CLUSTER_BASE_DIR"
    fi

    log_info "Cleanup complete"
    return 0
}

# =============================================================================
# Validation Functions
# =============================================================================

# Validate node count
validate_node_count() {
    local count="$1"
    if [ "$count" -lt 1 ] || [ "$count" -gt 9 ]; then
        log_error "Invalid node count: $count (must be between 1 and 9)"
        return 1
    fi
    return 0
}

# Validate binary exists
validate_binary() {
    if [ ! -f "$BINARY_PATH" ]; then
        log_error "Binary not found: $BINARY_PATH"
        log_info "Building the project..."
        if ! (cd "$PROJECT_ROOT" && cargo build --bin kiwi); then
            log_error "Failed to build project"
            return 1
        fi
    fi
    return 0
}

# =============================================================================
# Status Functions
# =============================================================================

# Get node status
get_node_status() {
    local node_id=$1
    local raft_port=$2
    local resp_port=$3

    echo "Node $node_id:"
    echo "  gRPC: 127.0.0.1:$raft_port"
    echo "  RESP: 127.0.0.1:$resp_port"

    # Check process
    local node_dir="$CLUSTER_BASE_DIR/node$node_id"
    if [ -f "$node_dir/kiwi.pid" ]; then
        local pid=$(cat "$node_dir/kiwi.pid")
        if is_process_running "$pid"; then
            echo -e "  ${GREEN}Running (PID: $pid)${NC}"
        else
            echo -e "  ${RED}Not running${NC}"
        fi
    else
        echo -e "  ${RED}No PID file${NC}"
    fi

    # Check ports
    if is_port_listening 127.0.0.1 "$raft_port"; then
        echo -e "  ${GREEN}gRPC listening${NC}"
    else
        echo -e "  ${RED}gRPC not responding${NC}"
    fi

    if is_port_listening 127.0.0.1 "$resp_port"; then
        echo -e "  ${GREEN}RESP listening${NC}"
    else
        echo -e "  ${RED}RESP not responding${NC}"
    fi
}
