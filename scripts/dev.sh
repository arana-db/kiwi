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

# Kiwi Development Helper Script for Linux/macOS

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Functions
info() { echo -e "${CYAN}$1${NC}"; }
success() { echo -e "${GREEN}$1${NC}"; }
warning() { echo -e "${YELLOW}$1${NC}"; }
error() { echo -e "${RED}$1${NC}"; }

# Default command
COMMAND=${1:-check}
PROFILE=""
VERBOSE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --release)
            PROFILE="--release"
            shift
            ;;
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        *)
            COMMAND=$1
            shift
            ;;
    esac
done

# Banner
info "=== Kiwi Development Tool ==="
echo ""

# Check if this is first-time use (only for build/run commands)
SETUP_MARKER=".kiwi_setup_done"
if [ "$COMMAND" = "build" ] || [ "$COMMAND" = "run" ]; then
    if [ ! -f "$SETUP_MARKER" ]; then
        echo ""
        warning "⚠️  First-time setup recommended for optimal performance!"
        echo ""
        echo "Run this command to install sccache and cargo-watch:"
        info "  ./scripts/quick_setup.sh"
        echo ""
        echo "Or continue without setup (you can run it later)."
        echo ""
        read -p "Run quick setup now? (y/n) " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            ./scripts/quick_setup.sh
            if [ $? -eq 0 ]; then
                touch "$SETUP_MARKER"
                success "✓ Setup complete! Continuing with build..."
                echo ""
            fi
        else
            info "Skipping setup. You can run './scripts/quick_setup.sh' anytime."
            # Create marker to not ask again
            touch "$SETUP_MARKER"
            echo ""
        fi
    fi
fi

# Set environment variables for faster builds
export CARGO_BUILD_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Check and setup sccache if available
if command -v sccache &> /dev/null; then
    export RUSTC_WRAPPER=sccache
    # sccache doesn't support incremental compilation, so disable it
    export CARGO_INCREMENTAL=0
    # Silently start sccache server if not running
    sccache --start-server &> /dev/null || true
else
    # Enable incremental compilation when not using sccache
    export CARGO_INCREMENTAL=1
    if [ "$COMMAND" = "build" ] || [ "$COMMAND" = "run" ]; then
        warning "⚡ Tip: Install sccache for faster builds: cargo install sccache"
        warning "   Or run: ./scripts/quick_setup.sh"
    fi
fi

case $COMMAND in
    check)
        info "Running cargo check (fast syntax check)..."
        cargo check $PROFILE $VERBOSE
        if [ $? -eq 0 ]; then
            success "✓ Check passed!"
        fi
        ;;
    
    build)
        info "Building Kiwi..."
        
        # Check if librocksdb-sys is cached
        TARGET_DIR="target/debug"
        if [ "$PROFILE" = "--release" ]; then
            TARGET_DIR="target/release"
        fi
        
        if ls $TARGET_DIR/deps/*rocksdb*.rlib 1> /dev/null 2>&1; then
            success "✓ Using cached librocksdb-sys"
        else
            warning "⚠ librocksdb-sys will be compiled (this may take a while)..."
        fi
        
        START_TIME=$(date +%s)
        cargo build $PROFILE $VERBOSE
        END_TIME=$(date +%s)
        BUILD_TIME=$((END_TIME - START_TIME))
        
        if [ $? -eq 0 ]; then
            success "✓ Build completed in ${BUILD_TIME} seconds"
        fi
        ;;
    
    run)
        info "Running Kiwi..."
        export RUST_LOG=debug
        cargo run $PROFILE $VERBOSE
        ;;
    
    test)
        info "Running tests..."
        cargo test $PROFILE $VERBOSE
        ;;
    
    clean)
        warning "Cleaning build artifacts..."
        cargo clean
        success "✓ Clean complete"
        ;;
    
    watch)
        info "Starting cargo-watch..."
        info "This will automatically check your code on file changes"
        info "Press Ctrl+C to stop"
        echo ""
        
        # Check if cargo-watch is installed
        if ! command -v cargo-watch &> /dev/null; then
            warning "cargo-watch not found."
            echo ""
            read -p "Install cargo-watch now? (y/n) " -n 1 -r
            echo ""
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                info "Installing cargo-watch..."
                cargo install cargo-watch
                success "✓ cargo-watch installed"
            else
                error "cargo-watch is required for watch mode"
                exit 1
            fi
        fi
        
        cargo watch -x check -x "test --lib"
        ;;
    
    stats)
        info "Build Statistics:"
        echo ""
        
        # Check target directory size
        if [ -d "target" ]; then
            TARGET_SIZE=$(du -sh target 2>/dev/null | cut -f1)
            info "Target directory size: $TARGET_SIZE"
        fi
        
        # Check if sccache is available
        if command -v sccache &> /dev/null; then
            echo ""
            info "sccache statistics:"
            sccache --show-stats
        else
            echo ""
            warning "sccache not installed. Install it for faster builds:"
            warning "  cargo install sccache"
            warning "  Then configure in ~/.cargo/config.toml:"
            warning "  [build]"
            warning "  rustc-wrapper = \"sccache\""
        fi
        
        # Show last build times
        echo ""
        info "Recent builds:"
        if [ -d "target/debug" ]; then
            DEBUG_TIME=$(stat -c %y target/debug 2>/dev/null || stat -f "%Sm" target/debug 2>/dev/null)
            info "  Debug:   $DEBUG_TIME"
        fi
        if [ -d "target/release" ]; then
            RELEASE_TIME=$(stat -c %y target/release 2>/dev/null || stat -f "%Sm" target/release 2>/dev/null)
            info "  Release: $RELEASE_TIME"
        fi
        ;;
    
    *)
        error "Unknown command: $COMMAND"
        echo ""
        echo "Available commands:"
        echo "  check  - Quick syntax check"
        echo "  build  - Build the project"
        echo "  run    - Build and run"
        echo "  test   - Run tests"
        echo "  clean  - Clean build artifacts"
        echo "  watch  - Auto-check on file changes"
        echo "  stats  - Show build statistics"
        echo ""
        echo "Options:"
        echo "  --release  - Use release profile"
        echo "  -v         - Verbose output"
        exit 1
        ;;
esac
