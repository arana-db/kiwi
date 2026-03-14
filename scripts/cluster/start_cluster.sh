#!/bin/bash
#
# Kiwi 3-Node Raft Cluster Startup Script
# Usage: ./start_cluster.sh [--init]
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_DIR="$SCRIPT_DIR"
LOG_DIR="$PROJECT_ROOT/logs"
PID_DIR="$PROJECT_ROOT/.pids"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

NODE_REDIS_PORTS=(7379 7380 7381)
NODE_RAFT_PORTS=(8081 8082 8083)

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

cleanup_old_data() {
    log_info "Cleaning up old Raft data..."
    rm -rf "$PROJECT_ROOT/raft_data"
    rm -rf "$PROJECT_ROOT/db"
    rm -rf "$LOG_DIR"
    rm -rf "$PID_DIR"
}

create_directories() {
    log_info "Creating directories..."
    mkdir -p "$LOG_DIR"
    mkdir -p "$PID_DIR"
    for i in 1 2 3; do
        mkdir -p "$PROJECT_ROOT/raft_data/node$i"
        mkdir -p "$PROJECT_ROOT/db/node$i"
    done
}

build_binary() {
    log_info "Building Kiwi binary..."
    cd "$PROJECT_ROOT"
    cargo build --release --bin kiwi 2>&1 | tail -5
    if [ ! -f "$PROJECT_ROOT/target/release/kiwi" ]; then
        log_error "Build failed!"
        exit 1
    fi
    log_success "Build complete"
}

start_node() {
    local node_id=$1
    local config_file="$CONFIG_DIR/node${node_id}.conf"
    local log_file="$LOG_DIR/node${node_id}.log"
    local pid_file="$PID_DIR/node${node_id}.pid"
    
    log_info "Starting Node $node_id..."
    
    cd "$PROJECT_ROOT"
    RUST_LOG=info ./target/release/kiwi --config "$config_file" > "$log_file" 2>&1 &
    local pid=$!
    echo $pid > "$pid_file"
    
    log_success "Node $node_id started (PID: $pid, Log: $log_file)"
}

wait_for_ports() {
    log_info "Waiting for nodes to be ready..."
    sleep 3
    
    for i in 0 1 2; do
        local redis_port=${NODE_REDIS_PORTS[$i]}
        local raft_port=${NODE_RAFT_PORTS[$i]}
        local node_num=$((i + 1))
        
        for j in {1..10}; do
            if nc -z 127.0.0.1 $redis_port 2>/dev/null; then
                log_success "Node $node_num Redis port $redis_port is ready"
                break
            fi
            sleep 1
        done
        
        for j in {1..10}; do
            if nc -z 127.0.0.1 $raft_port 2>/dev/null; then
                log_success "Node $node_num Raft port $raft_port is ready"
                break
            fi
            sleep 1
        done
    done
}

init_cluster() {
    log_info "Initializing Raft cluster..."
    
    local init_response=$(curl -s -X POST http://127.0.0.1:8081/raft/init \
        -H "Content-Type: application/json" \
        -d '{
            "nodes": [
                [1, {"raft_addr": "127.0.0.1:8081", "resp_addr": "127.0.0.1:7379"}],
                [2, {"raft_addr": "127.0.0.1:8082", "resp_addr": "127.0.0.1:7380"}],
                [3, {"raft_addr": "127.0.0.1:8083", "resp_addr": "127.0.0.1:7381"}]
            ]
        }')
    
    if echo "$init_response" | grep -q '"success":true'; then
        log_success "Cluster initialized successfully"
    else
        log_warn "Cluster initialization response: $init_response"
    fi
    
    log_info "Waiting for leader election (5 seconds)..."
    sleep 5
}

check_leader() {
    log_info "Checking leader status..."
    
    for i in 0 1 2; do
        local raft_port=${NODE_RAFT_PORTS[$i]}
        local node_num=$((i + 1))
        local response=$(curl -s http://127.0.0.1:$raft_port/raft/leader 2>/dev/null)
        
        if echo "$response" | grep -q '"leader_id"'; then
            local leader_id=$(echo "$response" | grep -o '"leader_id":[0-9]*' | cut -d: -f2)
            log_info "Node $node_num sees leader: $leader_id"
        fi
    done
}

print_status() {
    echo ""
    echo "=========================================="
    echo "       Kiwi 3-Node Raft Cluster"
    echo "=========================================="
    echo ""
    echo "Nodes:"
    for i in 0 1 2; do
        local redis_port=${NODE_REDIS_PORTS[$i]}
        local raft_port=${NODE_RAFT_PORTS[$i]}
        local node_num=$((i + 1))
        local pid=$(cat "$PID_DIR/node${node_num}.pid" 2>/dev/null || echo "N/A")
        echo "  Node $node_num:"
        echo "    - Redis:  127.0.0.1:$redis_port"
        echo "    - Raft:   127.0.0.1:$raft_port"
        echo "    - PID:    $pid"
        echo "    - Log:    $LOG_DIR/node${node_num}.log"
    done
    echo ""
    echo "Commands:"
    echo "  Test cluster:    ./scripts/cluster/test_cluster.sh"
    echo "  Stop cluster:    ./scripts/cluster/stop_cluster.sh"
    echo "  Check logs:      tail -f $LOG_DIR/node1.log"
    echo ""
}

main() {
    echo ""
    echo "=========================================="
    echo "   Starting Kiwi 3-Node Raft Cluster"
    echo "=========================================="
    echo ""
    
    if [ "$1" == "--clean" ]; then
        cleanup_old_data
    fi
    
    create_directories
    build_binary
    
    for i in 1 2 3; do
        start_node $i
    done
    
    wait_for_ports
    
    if [ "$1" == "--init" ] || [ "$1" == "--clean" ]; then
        init_cluster
    fi
    
    check_leader
    print_status
}

main "$@"
