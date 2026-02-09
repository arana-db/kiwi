#!/bin/bash
# Copyright (c) 2024-present, arana-db Community. All rights reserved.
#
# Script to start a multi-node Raft cluster for testing Kiwi
# Each node runs in its own directory with separate storage
# Logs are written to files, viewable with tail -f
# Uses gRPC for cluster management (grpcurl required)
#
# Usage: ./start_3node_cluster.sh [NODE_COUNT]
#   NODE_COUNT: Number of nodes to start (default: 3, range: 1-9)

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

# Default node count
NODE_COUNT=3

# Base ports
RAFT_PORT_BASE=8081
RESP_PORT_BASE=7379

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Usage: $0 [NODE_COUNT]"
            echo ""
            echo "Arguments:"
            echo "  NODE_COUNT    Number of nodes to start (default: 3, range: 1-9)"
            echo ""
            echo "Options:"
            echo "  -h, --help    Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0        # Start 3 nodes (default)"
            echo "  $0 5      # Start 5 nodes"
            echo "  $0 1      # Start 1 node (single node mode)"
            exit 0
            ;;
        *)
            if [[ $1 =~ ^[0-9]+$ ]] && [ $1 -ge 1 ] && [ $1 -le 9 ]; then
                NODE_COUNT=$1
            else
                echo -e "${RED}Error: Invalid node count '$1'${NC}"
                echo -e "${YELLOW}Node count must be between 1 and 9${NC}"
                echo "Use -h or --help for usage information"
                exit 1
            fi
            shift
            ;;
    esac
done

# Check if grpcurl is installed
GRPCURL=""
find_grpcurl() {
    # Try to find grpcurl in various locations
    if command -v grpcurl &> /dev/null; then
        GRPCURL="grpcurl"
    elif [ -f "$HOME/go/bin/grpcurl" ]; then
        GRPCURL="$HOME/go/bin/grpcurl"
    elif [ -f "/usr/local/bin/grpcurl" ]; then
        GRPCURL="/usr/local/bin/grpcurl"
    else
        echo -e "${RED}Error: grpcurl is not installed${NC}"
        echo -e "${YELLOW}Install grpcurl:${NC}"
        echo "  go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
        echo "  or: brew install grpcurl (macOS)"
        echo -e "\n${YELLOW}Then make sure \$HOME/go/bin is in your PATH:${NC}"
        echo "  export PATH=\"\$PATH:\$HOME/go/bin\""
        exit 1
    fi
}

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

# Find grpcurl
find_grpcurl

# Dynamically generate node configurations
# Format: "node_id:raft_port:resp_port"
NODES=()
for ((i=1; i<=NODE_COUNT; i++)); do
    raft_port=$((RAFT_PORT_BASE + i - 1))
    resp_port=$((RESP_PORT_BASE + i - 1))
    NODES+=("$i:$raft_port:$resp_port")
done

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

# Function to build the cluster initialization JSON
build_cluster_json() {
    local json="{\"nodes\": ["
    local first=true

    for node_info in "${NODES[@]}"; do
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

# Function to build membership JSON
build_membership_json() {
    local json="{\"members\": ["
    local first=true

    for node_info in "${NODES[@]}"; do
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

# Function to initialize the cluster using gRPC
init_cluster() {
    echo -e "\n${YELLOW}Waiting for nodes to initialize...${NC}"
    sleep 3

    echo -e "${YELLOW}Initializing Raft cluster via gRPC...${NC}"

    # Get the first node's Raft address
    local first_node_raft="127.0.0.1:$RAFT_PORT_BASE"

    # Build cluster JSON dynamically
    local cluster_json=$(build_cluster_json)

    # Initialize the cluster through node 1 using gRPC
    echo -e "${BLUE}Initializing cluster via node 1...${NC}"
    local init_result=$(echo "$cluster_json" | $GRPCURL -plaintext -d @ "$first_node_raft" raft_proto.RaftAdminService/Initialize 2>/dev/null || echo "error")

    echo "  Init result: $init_result"

    # If init failed, try adding nodes step by step
    if [[ "$init_result" == *"error"* ]] || [[ "$init_result" == *"failed"* ]]; then
        echo -e "${YELLOW}Direct init may have failed, trying step-by-step setup...${NC}"

        # Add nodes 2..N as learners
        for node_info in "${NODES[@]}"; do
            IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
            if [ $node_id -ne 1 ]; then
                echo -e "${BLUE}Adding node $node_id as learner...${NC}"
                local learner_result=$($GRPCURL -plaintext -d '{
                    "node_id": '$node_id',
                    "node": {"raft_addr": "127.0.0.1:'$raft_port'", "resp_addr": "127.0.0.1:'$resp_port'"}
                }' "$first_node_raft" raft_proto.RaftAdminService/AddLearner 2>/dev/null || echo "error")
                echo "  Add learner $node_id result: $learner_result"
            fi
        done

        # Change membership to include all nodes
        echo -e "${BLUE}Changing cluster membership...${NC}"
        local membership_json=$(build_membership_json)
        local membership_result=$(echo "$membership_json" | $GRPCURL -plaintext -d @ "$first_node_raft" raft_proto.RaftAdminService/ChangeMembership 2>/dev/null || echo "error")
        echo "  Membership result: $membership_result"
    fi

    sleep 2

    echo -e "${GREEN}Cluster initialization complete${NC}"
}

# Function to show cluster status using gRPC
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
            echo -e "  ${GREEN}✓ gRPC listening on port $raft_port${NC}"
        else
            echo -e "  ${RED}✗ gRPC not responding on port $raft_port${NC}"
        fi

        if nc -z 127.0.0.1 "$resp_port" 2>/dev/null; then
            echo -e "  ${GREEN}✓ RESP listening on port $resp_port${NC}"
        else
            echo -e "  ${RED}✗ RESP not responding on port $resp_port${NC}"
        fi

        # Check leader via gRPC
        local leader=$($GRPCURL -plaintext "127.0.0.1:$raft_port" raft_proto.RaftMetricsService/Leader 2>/dev/null || echo "error")
        if [ "$leader" != "error" ] && [ "$leader" != "null" ]; then
            echo -e "  Leader info: $leader"
        fi

        # Check metrics via gRPC
        local metrics=$($GRPCURL -plaintext "127.0.0.1:$raft_port" raft_proto.RaftMetricsService/Metrics 2>/dev/null || echo "error")
        if [ "$metrics" != "error" ] && [ "$metrics" != "null" ]; then
            echo -e "  Metrics: $metrics"
        fi
    done
}

# Function to run tests using gRPC
run_tests() {
    echo -e "\n${YELLOW}Running basic Raft tests...${NC}"

    # Find the leader
    local leader_raft=""
    local leader_id=""

    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id raft_port resp_port <<< "$node_info"
        local leader_check=$($GRPCURL -plaintext "127.0.0.1:$raft_port" raft_proto.RaftMetricsService/Metrics 2>/dev/null || echo "error")
        if [ "$leader_check" != "error" ] && [[ "$leader_check" == *"isLeader"* ]]; then
            # Check if this node is the leader (isLeader: true)
            if [[ "$leader_check" == *"isLeader\": true"* ]]; then
                leader_raft="127.0.0.1:$raft_port"
                leader_id="$node_id"
                echo -e "${GREEN}Leader is node $node_id (port $raft_port)${NC}"
                break
            fi
        fi
    done

    if [ -z "$leader_raft" ]; then
        echo -e "${RED}No leader found, skipping tests${NC}"
        return
    fi

    # Test write operation via gRPC
    echo -e "${BLUE}Testing write operation via leader ($leader_raft)...${NC}"
    local write_response=$($GRPCURL -plaintext -d '{
        "binlog": {
            "db_id": 0,
            "slot_idx": 0,
            "entries": [
                {
                    "cf_idx": 0,
                    "op_type": "Put",
                    "key": "dGVzdF9r",
                    "value": "dGVzdF92"
                }
            ]
        }
    }' "$leader_raft" raft_proto.RaftClientService/Write 2>/dev/null || echo "error")

    if [ "$write_response" != "error" ]; then
        echo -e "${GREEN}Write response: $write_response${NC}"
    else
        echo -e "${RED}Write failed${NC}"
    fi

    # Test read operation via gRPC
    echo -e "${BLUE}Testing read operation...${NC}"
    local read_response=$($GRPCURL -plaintext -d '{
        "key": "dGVzdF9r"
    }' "$leader_raft" raft_proto.RaftClientService/Read 2>/dev/null || echo "error")

    if [ "$read_response" != "error" ]; then
        echo -e "${GREEN}Read response: $read_response${NC}"
    else
        echo -e "${RED}Read failed${NC}"
    fi
}

# Main execution
main() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Kiwi ${NODE_COUNT}-Node Raft Cluster${NC}"
    echo -e "${GREEN}  (using gRPC for cluster management)${NC}"
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
        echo -e "    gRPC:   127.0.0.1:$raft_port"
        echo -e "    RESP:   127.0.0.1:$resp_port"
        echo -e "    Log:    $node_dir/kiwi.log"
        echo -e "    Data:   $node_dir"
    done

    echo -e "\n${YELLOW}View logs with:${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node1/kiwi.log${NC}"
    for ((i=2; i<=NODE_COUNT; i++)); do
        echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node${i}/kiwi.log${NC}"
    done
    echo -e "\n${YELLOW}Or view all at once:${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node*/kiwi.log${NC}"

    echo -e "\n${YELLOW}gRPC service examples:${NC}"
    echo -e "  ${BLUE}$GRPCURL -plaintext 127.0.0.1:$RAFT_PORT_BASE raft_proto.RaftMetricsService/Leader${NC}"
    echo -e "  ${BLUE}$GRPCURL -plaintext 127.0.0.1:$RAFT_PORT_BASE raft_proto.RaftMetricsService/Metrics${NC}"
    echo -e "  ${BLUE}$GRPCURL -plaintext 127.0.0.1:$RAFT_PORT_BASE raft_proto.RaftMetricsService/Members${NC}"
    echo -e "  ${BLUE}$GRPCURL -plaintext 127.0.0.1:$RAFT_PORT_BASE list${NC}"

    echo -e "\n${YELLOW}Press Ctrl+C to stop the cluster${NC}"

    # Wait indefinitely for user to press Ctrl+C
    wait
}

# Run main
main
