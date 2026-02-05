#!/bin/bash
# Copyright (c) 2024-present, arana-db Community. All rights reserved.
#
# Script to start a 3-node Raft cluster for testing Kiwi
# Each node runs in its own directory with separate storage
# Logs are written to files, viewable with tail -f
# Uses gRPC for cluster management (grpcurl required)

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

# Check if grpcurl is installed
GRPCURL=""
PROTOSET=""
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

# Generate proto descriptor set for grpcurl
generate_protoset() {
    local proto_dir="$PROJECT_ROOT/src/raft/proto"
    local protoset_file="/tmp/raft.desc"

    if [ ! -f "$protoset_file" ]; then
        echo -e "${YELLOW}Generating proto descriptor set...${NC}"
        if ! command -v protoc &> /dev/null; then
            echo -e "${RED}Error: protoc is not installed${NC}"
            echo -e "${YELLOW}Install protoc (protobuf compiler):${NC}"
            echo "  apt install protobuf-compiler  # Ubuntu/Debian"
            echo "  brew install protobuf           # macOS"
            exit 1
        fi

        (cd "$proto_dir" && protoc --descriptor_set_out="$protoset_file" \
            --include_imports --proto_path=. \
            types.proto raft.proto admin.proto client.proto 2>/dev/null)

        if [ $? -ne 0 ]; then
            echo -e "${RED}Failed to generate proto descriptor${NC}"
            exit 1
        fi
        echo -e "${GREEN}Proto descriptor generated: $protoset_file${NC}"
    fi

    PROTOSET="-protoset $protoset_file"
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

# Generate proto descriptor set
generate_protoset

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

# Function to initialize the cluster using gRPC
init_cluster() {
    echo -e "\n${YELLOW}Waiting for nodes to initialize...${NC}"
    sleep 3

    echo -e "${YELLOW}Initializing Raft cluster via gRPC...${NC}"

    # Get the first node's Raft address
    local first_node_raft="127.0.0.1:8081"

    # Initialize the cluster through node 1 using gRPC
    echo -e "${BLUE}Initializing cluster via node 1...${NC}"
    local init_result=$($GRPCURL $PROTOSET -plaintext -d '{
        "nodes": [
            {"node_id": 1, "raft_addr": "127.0.0.1:8081", "resp_addr": "127.0.0.1:7379"},
            {"node_id": 2, "raft_addr": "127.0.0.1:8082", "resp_addr": "127.0.0.1:7380"},
            {"node_id": 3, "raft_addr": "127.0.0.1:8083", "resp_addr": "127.0.0.1:7381"}
        ]
    }' "$first_node_raft" raft_proto.RaftAdminService/Initialize 2>/dev/null || echo "error")

    echo "  Init result: $init_result"

    # If init failed, try adding nodes step by step
    if [[ "$init_result" == *"error"* ]] || [[ "$init_result" == *"failed"* ]]; then
        echo -e "${YELLOW}Direct init may have failed, trying step-by-step setup...${NC}"

        # Try adding node 2 as learner
        echo -e "${BLUE}Adding node 2 as learner...${NC}"
        local learner2_result=$($GRPCURL $PROTOSET -plaintext -d '{
            "node_id": 2,
            "node": {"raft_addr": "127.0.0.1:8082", "resp_addr": "127.0.0.1:7380"}
        }' "$first_node_raft" raft_proto.RaftAdminService/AddLearner 2>/dev/null || echo "error")
        echo "  Add learner 2 result: $learner2_result"

        # Try adding node 3 as learner
        echo -e "${BLUE}Adding node 3 as learner...${NC}"
        local learner3_result=$($GRPCURL $PROTOSET -plaintext -d '{
            "node_id": 3,
            "node": {"raft_addr": "127.0.0.1:8083", "resp_addr": "127.0.0.1:7381"}
        }' "$first_node_raft" raft_proto.RaftAdminService/AddLearner 2>/dev/null || echo "error")
        echo "  Add learner 3 result: $learner3_result"

        # Change membership to include all nodes
        echo -e "${BLUE}Changing cluster membership...${NC}"
        local membership_result=$($GRPCURL $PROTOSET -plaintext -d '{
            "members": [
                {"node_id": 1, "raft_addr": "127.0.0.1:8081", "resp_addr": "127.0.0.1:7379"},
                {"node_id": 2, "raft_addr": "127.0.0.1:8082", "resp_addr": "127.0.0.1:7380"},
                {"node_id": 3, "raft_addr": "127.0.0.1:8083", "resp_addr": "127.0.0.1:7381"}
            ],
            "retain": false
        }' "$first_node_raft" raft_proto.RaftAdminService/ChangeMembership 2>/dev/null || echo "error")
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
        local leader=$($GRPCURL $PROTOSET -plaintext "127.0.0.1:$raft_port" raft_proto.RaftMetricsService/Leader 2>/dev/null || echo "error")
        if [ "$leader" != "error" ] && [ "$leader" != "null" ]; then
            echo -e "  Leader info: $leader"
        fi

        # Check metrics via gRPC
        local metrics=$($GRPCURL $PROTOSET -plaintext "127.0.0.1:$raft_port" raft_proto.RaftMetricsService/Metrics 2>/dev/null || echo "error")
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
        local leader_check=$($GRPCURL $PROTOSET -plaintext "127.0.0.1:$raft_port" raft_proto.RaftMetricsService/Metrics 2>/dev/null || echo "error")
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
    local write_response=$($GRPCURL $PROTOSET -plaintext -d '{
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
    local read_response=$($GRPCURL $PROTOSET -plaintext -d '{
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
    echo -e "${GREEN}  Kiwi 3-Node Raft Cluster Starter${NC}"
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
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node2/kiwi.log${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node3/kiwi.log${NC}"
    echo -e "\n${YELLOW}Or view all at once:${NC}"
    echo -e "  ${BLUE}tail -f $CLUSTER_BASE_DIR/node*/kiwi.log${NC}"

    echo -e "\n${YELLOW}gRPC service examples:${NC}"
    echo -e "  ${BLUE}$GRPCURL $PROTOSET -plaintext 127.0.0.1:8081 raft_proto.RaftMetricsService/Leader${NC}"
    echo -e "  ${BLUE}$GRPCURL $PROTOSET -plaintext 127.0.0.1:8081 raft_proto.RaftMetricsService/Metrics${NC}"
    echo -e "  ${BLUE}$GRPCURL $PROTOSET -plaintext 127.0.0.1:8081 raft_proto.RaftMetricsService/Members${NC}"
    echo -e "  ${BLUE}$GRPCURL $PROTOSET -plaintext 127.0.0.1:8081 list${NC}"

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
