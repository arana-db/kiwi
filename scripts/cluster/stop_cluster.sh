#!/bin/bash
#
# Kiwi Raft Cluster Stop Script
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PID_DIR="$PROJECT_ROOT/.pids"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

stop_node() {
    local node_id=$1
    local pid_file="$PID_DIR/node${node_id}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            log_info "Stopping Node $node_id (PID: $pid)..."
            kill $pid 2>/dev/null || true
            
            # Wait for graceful shutdown
            for i in {1..10}; do
                if ! kill -0 $pid 2>/dev/null; then
                    break
                fi
                sleep 1
            done
            
            # Force kill if still running
            if kill -0 $pid 2>/dev/null; then
                log_warn "Force killing Node $node_id..."
                kill -9 $pid 2>/dev/null || true
            fi
            
            log_success "Node $node_id stopped"
        else
            log_warn "Node $node_id (PID: $pid) is not running"
        fi
        rm -f "$pid_file"
    else
        log_warn "No PID file for Node $node_id"
    fi
}

echo ""
echo "=========================================="
echo "     Stopping Kiwi Raft Cluster"
echo "=========================================="
echo ""

for i in 1 2 3; do
    stop_node $i
done

log_success "All nodes stopped"
echo ""
