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

# Kiwi Raft Cluster Stop Script

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

LOG_DIR="./cluster_logs"
PID_FILE="${LOG_DIR}/cluster.pids"

info "=== Stopping Kiwi Raft Cluster ==="
echo ""

# Try to read PIDs from file
if [ -f ${PID_FILE} ]; then
    PIDS=$(cat ${PID_FILE})
    info "Found PID file with processes: ${PIDS}"
    
    for PID in ${PIDS}; do
        if kill -0 ${PID} 2>/dev/null; then
            info "Stopping process ${PID}..."
            kill ${PID}
            success "  ✓ Sent SIGTERM to ${PID}"
        else
            warning "  ⚠ Process ${PID} not running"
        fi
    done
    
    # Wait for processes to stop
    sleep 2
    
    # Force kill if still running
    for PID in ${PIDS}; do
        if kill -0 ${PID} 2>/dev/null; then
            warning "  Force killing ${PID}..."
            kill -9 ${PID}
        fi
    done
    
    rm -f ${PID_FILE}
else
    warning "No PID file found, searching for kiwi processes..."
    
    # Find and kill kiwi server processes
    PIDS=$(pgrep -f "kiwi.*server.*cluster_node" || true)
    
    if [ -z "$PIDS" ]; then
        info "No kiwi cluster processes found"
    else
        info "Found processes: ${PIDS}"
        for PID in ${PIDS}; do
            info "Stopping process ${PID}..."
            kill ${PID}
            success "  ✓ Sent SIGTERM to ${PID}"
        done
        
        sleep 2
        
        # Force kill if still running
        for PID in ${PIDS}; do
            if kill -0 ${PID} 2>/dev/null; then
                warning "  Force killing ${PID}..."
                kill -9 ${PID}
            fi
        done
    fi
fi

echo ""
success "=== Cluster Stopped ==="
echo ""
info "To clean up data and logs, run:"
info "  rm -rf raft_data_* cluster_logs cluster_node*.conf"
