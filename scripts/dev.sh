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
DEBUG_MODE=""

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
        --debug)
            DEBUG_MODE="true"
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
if [ "$COMMAND" = "build" ] || [ "$COMMAND" = "run" ]; then
    # Check if sccache is available, if not prompt to install
    if ! command -v sccache &> /dev/null; then
        echo ""
        warning "⚠️  sccache not detected for optimal performance!"
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
                success "✓ Setup complete! Continuing with build..."
                echo ""
            fi
        else
            info "Skipping setup. You can run './scripts/quick_setup.sh' anytime."
            echo ""
        fi
    fi
fi

# Set environment variables for faster builds
export CARGO_BUILD_JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Debug mode configuration
if [ -n "$DEBUG_MODE" ]; then
    info "🐛 Debug mode enabled - using Cargo_debug.toml"
    # Set environment variables for debugging
    export RUSTFLAGS="-g -Zmacro-backtrace"
    export CARGO_INCREMENTAL=1
    # Disable sccache for debug builds to ensure debug symbols
    unset RUSTC_WRAPPER
    # Use debug config if available
    if [ -f "Cargo_debug.toml" ]; then
        CARGO_CONFIG_FLAG="--config Cargo_debug.toml"
        info "📋 Using Cargo_debug.toml for maximum debug symbols"
    else
        warning "⚠️ Cargo_debug.toml not found, using default debug settings"
    fi
else
    # Check and setup sccache if available (normal builds)
    if command -v sccache &> /dev/null; then
        export RUSTC_WRAPPER=sccache
        # sccache doesn't support incremental compilation, so disable it
        export CARGO_INCREMENTAL=0
        unset CARGO_CACHE_RUSTC_INFO
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
    CARGO_CONFIG_FLAG=""
fi

case $COMMAND in
    check)
        info "Running cargo check (fast syntax check)..."
        cargo check $PROFILE $CARGO_CONFIG_FLAG $VERBOSE
        if [ $? -eq 0 ]; then
            success "✓ Check passed!"
        fi
        ;;
    
    build)
        if [ -n "$DEBUG_MODE" ]; then
            info "Building Kiwi in DEBUG mode..."
        else
            info "Building Kiwi..."
        fi

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
        cargo build $PROFILE $CARGO_CONFIG_FLAG $VERBOSE
        END_TIME=$(date +%s)
        BUILD_TIME=$((END_TIME - START_TIME))
        
        if [ $? -eq 0 ]; then
            if [ -n "$DEBUG_MODE" ]; then
                success "✓ Debug build completed in ${BUILD_TIME} seconds"
                echo ""
                info "🐛 Debug binary ready at: target/debug/kiwi"
                info "🔍 GDB command: gdb -ex 'break main' -ex 'run' target/debug/kiwi"
                info "🔍 Or use: rust-gdb target/debug/kiwi"
            else
                success "✓ Build completed in ${BUILD_TIME} seconds"
            fi
        fi
        ;;
    
    run)
        if [ -n "$DEBUG_MODE" ]; then
            info "Running Kiwi in DEBUG mode..."
        else
            info "Running Kiwi..."
        fi
        export RUST_LOG=debug
        cargo run $PROFILE $CARGO_CONFIG_FLAG $VERBOSE
        ;;
    
    test)
        info "Running tests..."
        cargo test $PROFILE $CARGO_CONFIG_FLAG $VERBOSE
        ;;

    gdb)
        info "Starting GDB debug session..."
        # First build with debug symbols if not already built
        if [ ! -f "target/debug/kiwi" ]; then
            warning "Debug binary not found, building first..."
            ./scripts/dev.sh build --debug
        fi

        if [ -f "target/debug/kiwi" ]; then
            success "✓ Starting GDB with debug symbols"
            if command -v rust-gdb &> /dev/null; then
                info "Using rust-gdb for better Rust debugging..."
                rust-gdb -ex 'break main' -ex 'run' target/debug/kiwi
            else
                info "Using standard gdb..."
                gdb -ex 'set debug-file-directory ./target/debug/deps' -ex 'break main' -ex 'run' target/debug/kiwi
            fi
        else
            error "Debug binary not found. Run './scripts/dev.sh build --debug' first."
            exit 1
        fi
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
        echo "  gdb    - Build and start GDB"
        echo "  clean  - Clean build artifacts"
        echo "  watch  - Auto-check on file changes"
        echo "  stats  - Show build statistics"
        echo ""
        echo "Options:"
        echo "  --debug    - Enable full debug symbols (slower build, better debugging)"
        echo "  --release  - Use release profile"
        echo "  -v         - Verbose output"
        echo ""
        echo "Debug examples:"
        echo "  ./scripts/dev.sh build --debug     # Build with debug symbols"
        echo "  ./scripts/dev.sh run --debug       # Run with debug symbols"
        echo "  ./scripts/dev.sh gdb               # Build and start GDB"
        exit 1
        ;;
esac
