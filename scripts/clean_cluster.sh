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

# Kiwi Raft Cluster Cleanup Script

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

# Parse arguments
FORCE=false
STOP_CLUSTER=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--force)
            FORCE=true
            shift
            ;;
        --stop)
            STOP_CLUSTER=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Clean up Kiwi Raft cluster data and configuration files"
            echo ""
            echo "Options:"
            echo "  -f, --force    Skip confirmation prompt"
            echo "  --stop         Stop cluster before cleaning"
            echo "  --help         Show this help message"
            echo ""
            echo "This will remove:"
            echo "  - Cluster configuration files (cluster_node*.conf)"
            echo "  - Raft data directories (raft_data_*)"
            echo "  - Database directories (db_node*)"
            echo "  - Cluster logs (cluster_logs/)"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Banner
info "=== Kiwi Raft Cluster Cleanup ==="
echo ""

# Check if cluster is running
RUNNING_PROCESSES=$(pgrep -f "kiwi.*cluster_node" 2>/dev/null || true)

if [ -n "$RUNNING_PROCESSES" ]; then
    warning "⚠️  Cluster is currently running!"
    echo ""
    
    if [ "$STOP_CLUSTER" = true ]; then
        info "Stopping cluster first..."
        ./scripts/stop_cluster.sh
        echo ""
    else
        error "Please stop the cluster first:"
        error "  ./scripts/stop_cluster.sh"
        echo ""
        error "Or use --stop flag to stop automatically:"
        error "  $0 --stop"
        exit 1
    fi
fi

# List files to be deleted
info "Files and directories to be removed:"
echo ""

ITEMS_TO_DELETE=()
TOTAL_SIZE=0

# Check cluster config files
if ls cluster_node*.conf 1> /dev/null 2>&1; then
    COUNT=$(ls cluster_node*.conf 2>/dev/null | wc -l | tr -d ' ')
    echo "  📄 Cluster config files: ${COUNT} files"
    ITEMS_TO_DELETE+=("cluster_node*.conf")
fi

# Check raft data directories
if ls -d raft_data_* 1> /dev/null 2>&1; then
    COUNT=$(ls -d raft_data_* 2>/dev/null | wc -l | tr -d ' ')
    SIZE=$(du -sh raft_data_* 2>/dev/null | awk '{sum+=$1} END {print sum}' || echo "0")
    echo "  📁 Raft data directories: ${COUNT} directories"
    ITEMS_TO_DELETE+=("raft_data_*")
fi

# Check database directories
if ls -d db_node* 1> /dev/null 2>&1; then
    COUNT=$(ls -d db_node* 2>/dev/null | wc -l | tr -d ' ')
    SIZE=$(du -sh db_node* 2>/dev/null | awk '{s+=$1} END {print s}' || echo "0")
    echo "  📁 Database directories: ${COUNT} directories"
    ITEMS_TO_DELETE+=("db_node*")
fi

# Check cluster logs
if [ -d "cluster_logs" ]; then
    LOG_COUNT=$(ls cluster_logs/*.log 2>/dev/null | wc -l | tr -d ' ')
    LOG_SIZE=$(du -sh cluster_logs 2>/dev/null | awk '{print $1}' || echo "0")
    echo "  📋 Cluster logs: ${LOG_COUNT} log files (${LOG_SIZE})"
    ITEMS_TO_DELETE+=("cluster_logs")
fi

# Check if there's anything to delete
if [ ${#ITEMS_TO_DELETE[@]} -eq 0 ]; then
    success "✓ No cluster data found. Nothing to clean."
    exit 0
fi

echo ""

# Confirmation prompt
if [ "$FORCE" = false ]; then
    warning "⚠️  This will permanently delete all cluster data!"
    echo ""
    read -p "Are you sure you want to continue? (yes/no) " -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        info "Cleanup cancelled."
        exit 0
    fi
fi

# Perform cleanup
info "Cleaning up..."
echo ""

CLEANED=0
FAILED=0

for pattern in "${ITEMS_TO_DELETE[@]}"; do
    if rm -rf $pattern 2>/dev/null; then
        success "  ✓ Removed: ${pattern}"
        ((CLEANED++))
    else
        error "  ✗ Failed to remove: ${pattern}"
        ((FAILED++))
    fi
done

echo ""

if [ $FAILED -eq 0 ]; then
    success "=== Cleanup Complete ==="
    echo ""
    success "✓ Successfully removed ${CLEANED} item(s)"
    echo ""
    info "To start a fresh cluster:"
    info "  ./scripts/start_cluster.sh"
    info "  ./scripts/init_cluster.sh"
else
    warning "=== Cleanup Completed with Errors ==="
    echo ""
    warning "⚠️  ${CLEANED} item(s) removed, ${FAILED} failed"
    echo ""
    info "You may need to manually remove some files with:"
    info "  sudo rm -rf raft_data_* db_node* cluster_logs cluster_node*.conf"
fi
