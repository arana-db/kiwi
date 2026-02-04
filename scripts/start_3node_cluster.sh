#!/bin/bash
# Copyright (c) 2024-present, arana-db Community. All rights reserved.
#
# Script to start a 3-node Raft cluster for testing Kiwi
# Each node runs in its own directory with separate storage
# Logs are written to files, viewable with tail -f

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base directory for the cluster
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CLUSTER_BASE_DIR="$PROJECT_ROOT/cluster_test"
BINARY_PATH="$PROJECT_ROOT/target/debug/kiwi"

# Check if binary exists
if [ ! -f "$BINARY_PATH" ]; then
    echo -e "${RED}Error: Binary not found at $BINARY_PATH${NC}"
    echo -e "${YELLOW}Building the project...${NC}"
    cd "$PROJECT_ROOT" && cargo build --bin kiwi
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to build project${NC}"
        exit 1
    fi
fi

# Node configurations
# Format: "node_id:raft_port:resp_port"
# Using 7379-7381 to avoid conflicts with default Redis 6379
NODES=(
    "1:8081:7379"
    "2:8082:7380"
    "3:8083:7381"
)

# PIDs for tracking
PIDS=()

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}Stopping cluster...${NC}"

    # Kill all node processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done

    sleep 1

    # Force kill any remaining kiwi processes
    pkill -9 kiwi 2>/dev/null || true

    # Remove cluster directory
    rm -rf "$CLUSTER_BASE_DIR"
    rm -rf "$PROJECT_ROOT/db"

    echo -e "${GREEN}Cleanup complete${NC}"
}

# Register cleanup on Ctrl+C
trap cleanup INT TERM

# Function to create node directory and config
create_node() {
    local node_id=$1
    local raft_port=$2
    local resp_port=$3
    local node_dir="$CLUSTER_BASE_DIR/node$node_id"

    echo -e "${BLUE}Creating node $node_id...${NC}"

    # Create node directory
    mkdir -p "$node_dir"

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
    cp "$BINARY_PATH" "$node_dir/kiwi"
    chmod +x "$node_dir/kiwi"

    echo -e "${GREEN}Node $node_id created at $node_dir${NC}"
}

# Function to start a node
start_node() {
    local node_id=$1
    local node_dir="$CLUSTER_BASE_DIR/node$node_id"
    local log_file="$node_dir/kiwi.log"

    echo -e "${BLUE}Starting node $node_id...${NC}"

    # Start node from its own directory (so ./db is per-node)
    cd "$node_dir"
    RUST_LOG=info ./kiwi --config config.toml > "$log_file" 2>&1 &
    local pid=$!
    cd - > /dev/null

    PIDS+=("$pid")
    echo "$pid" > "$node_dir/kiwi.pid"

    echo -e "${GREEN}Node $node_id started (PID: $pid)${NC}"
    echo -e "${YELLOW}  Log: $log_file${NC}"
}

# Function to initialize the cluster
init_cluster() {
    echo -e "\n${YELLOW}Waiting for nodes to initialize...${NC}"
    sleep 3

    echo -e "${YELLOW}Initializing Raft cluster...${NC}"

    # Get the first node's Raft address
    local first_node_raft="127.0.0.1:8081"

    # Initialize the cluster through node 1
    echo -e "${BLUE}Initializing cluster via node 1...${NC}"
    local init_result=$(curl --noproxy "*" -s -X POST "http://$first_node_raft/raft/init" \
        -H "Content-Type: application/json" \
        -d '{"nodes": [[1, {"raft_addr": "http://127.0.0.1:8081", "resp_addr": "127.0.0.1:7379"}]]}')

    echo "  Init result: $init_result"

    # Add other nodes as learners
    echo -e "${BLUE}Adding node 2 as learner...${NC}"
    local learner2_result=$(curl --noproxy "*" -s -X POST "http://$first_node_raft/raft/add_learner" \
        -H "Content-Type: application/json" \
        -d '{"node_id": 2, "node": {"raft_addr": "http://127.0.0.1:8082", "resp_addr": "127.0.0.1:7380"}}')
    echo "  Add learner 2 result: $learner2_result"

    echo -e "${BLUE}Adding node 3 as learner...${NC}"
    local learner3_result=$(curl --noproxy "*" -s -X POST "http://$first_node_raft/raft/add_learner" \
        -H "Content-Type: application/json" \
        -d '{"node_id": 3, "node": {"raft_addr": "http://127.0.0.1:8083", "resp_addr": "127.0.0.1:7381"}}')
    echo "  Add learner 3 result: $learner3_result"

    # Change membership to include all nodes
    echo -e "${BLUE}Changing cluster membership...${NC}"
    local membership_result=$(curl --noproxy "*" -s -X POST "http://$first_node_raft/raft/change_membership" \
        -H "Content-Type: application/json" \
        -d '{"members": [[1, {"raft_addr": "http://127.0.0.1:8081", "resp_addr": "127.0.0.1:7379"}], [2, {"raft_addr": "http://127.0.0.1:8082", "resp_addr": "127.0.0.1:7380"}], [3, {"raft_addr": "http://127.0.0.1:8083", "resp_addr": "127.0.0.1:7381"}]], "retain": false}')
    echo "  Membership result: $membership_result"

    sleep 2

    echo -e "${GREEN}Cluster initialization complete${NC}"
}

# Function to show cluster status
show_status() {
    echo -e "\n${YELLOW}Cluster Status:${NC}"

    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"

        echo -e "\n${BLUE}Node $node_id:${NC}"

        # Check if process is running
        local node_dir="$CLUSTER_BASE_DIR/node$node_id"
        if [ -f "$node_dir/kiwi.pid" ]; then
            local pid=$(cat "$node_dir/kiwi.pid")
            if kill -0 "$pid" 2>/dev/null; then
                echo -e "  ${GREEN}✓ Running (PID: $pid)${NC}"
            else
                echo -e "  ${RED}✗ Not running${NC}"
            fi
        fi

        # Check if port is listening
        if nc -z 127.0.0.1 "$raft_port" 2>/dev/null; then
            echo -e "  ${GREEN}✓ Raft API listening on port $raft_port${NC}"
        else
            echo -e "  ${RED}✗ Raft API not responding on port $raft_port${NC}"
        fi

        if nc -z 127.0.0.1 "$resp_port" 2>/dev/null; then
            echo -e "  ${GREEN}✓ RESP listening on port $resp_port${NC}"
        else
            echo -e "  ${RED}✗ RESP not responding on port $resp_port${NC}"
        fi

        # Check leader
        local leader=$(curl --noproxy "*" -s "http://127.0.0.1:$raft_port/raft/leader" 2>/dev/null || echo "error")
        if [ "$leader" != "error" ] && [ "$leader" != "null" ]; then
            echo -e "  Leader info: $leader"
        fi
    done
}

# Function to run tests
run_tests() {
    echo -e "\n${YELLOW}Running basic Raft tests...${NC}"

    # Find the leader
    local leader_raft=""
    local leader_id=""

    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        local leader_check=$(curl --noproxy "*" -s "http://127.0.0.1:$raft_port/raft/leader" 2>/dev/null || echo "error")
        if [ "$leader_check" != "error" ] && [ "$leader_check" != "null" ]; then
            leader_raft="127.0.0.1:$raft_port"
            leader_id="$node_id"
            echo -e "${GREEN}Leader is node $node_id (port $raft_port)${NC}"
            break
        fi
    done

    if [ -z "$leader_raft" ]; then
        echo -e "${RED}No leader found, skipping tests${NC}"
        return
    fi

    # Test write operation
    echo -e "${BLUE}Testing write operation via leader ($leader_raft)...${NC}"
    local write_response=$(curl --noproxy "*" -s -X POST "http://$leader_raft/raft/write" \
        -H "Content-Type: application/json" \
        -d '{"binlog": {"db_id": 0, "slot_idx": 0, "entries": [{"cf_idx": 0, "op_type": "Put", "key": [116,101,115,116,95,107], "value": [116,101,115,116,95,118]}]}}' 2>/dev/null || echo "error")

    if [ "$write_response" != "error" ]; then
        echo -e "${GREEN}Write response: $write_response${NC}"
    else
        echo -e "${RED}Write failed${NC}"
    fi

    # Test read operation
    echo -e "${BLUE}Testing read operation...${NC}"
    local read_response=$(curl --noproxy "*" -s -X POST "http://$leader_raft/raft/read" \
        -H "Content-Type: application/json" \
        -d '{"key": [116,101,115,116,95,107]}' 2>/dev/null || echo "error")

    if [ "$read_response" != "error" ]; then
        echo -e "${GREEN}Read response: $read_response${NC}"
    else
        echo -e "${RED}Read failed${NC}"
    fi
}

# Main execution
main() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Kiwi 3-Node Raft Cluster Starter${NC}"
    echo -e "${GREEN}========================================${NC}"

    # Clean up any existing cluster
    if [ -d "$CLUSTER_BASE_DIR" ]; then
        echo -e "${YELLOW}Removing existing cluster directory...${NC}"
        rm -rf "$CLUSTER_BASE_DIR"
    fi

    # Kill any kiwi processes
    pkill -9 kiwi 2>/dev/null || true

    # Clean up project root db directory
    rm -rf "$PROJECT_ROOT/db"

    sleep 1

    # Create cluster base directory
    mkdir -p "$CLUSTER_BASE_DIR"

    # Create all nodes
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        create_node "$node_id" "$raft_port" "$resp_port"
    done

    # Start all nodes
    echo -e "\n${YELLOW}Starting all nodes...${NC}"
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        start_node "$node_id"
    done

    # Wait for nodes to be ready
    sleep 3

    # Initialize the cluster
    init_cluster

    # Show cluster status
    show_status

    # Run tests
    run_tests

    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Cluster is running!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "${YELLOW}Nodes:${NC}"
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        local node_dir="$CLUSTER_BASE_DIR/node$node_id"
        echo -e "  ${BLUE}Node $node_id:${NC}"
        echo -e "    Raft API: http://127.0.0.1:$raft_port"
        echo -e "    RESP:    127.0.0.1:$resp_port"
        echo -e "    Log:     $node_dir/kiwi.log"
        echo -e "    Data:    $node_dir"
    done

    echo -e "\n${YELLOW}View logs with:${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node1/kiwi.log${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node2/kiwi.log${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node3/kiwi.log${NC}"
    echo -e "\n${YELLOW}Or view all at once:${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node*/kiwi.log${NC}"
    echo -e "\n${YELLOW}Press Ctrl+C to stop the cluster${NC}"

    # Keep script running
    echo -e "\n${GREEN}Monitoring cluster...${NC}"
    while true; do
        sleep 5
        # Check if all nodes are still running
        local all_running=true
        for pid in "${PIDS[@]}"; do
            if ! kill -0 "$pid" 2>/dev/null; then
                all_running=false
                break
            fi
        done
        if [ "$all_running" = "false" ]; then
            echo -e "\n${RED}A node has stopped, cleaning up...${NC}"
            cleanup
            exit 1
        fi
    done
}

# Run main
main
