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

# Setup sccache for faster Rust compilation

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}=== Setting up sccache ===${NC}"
echo ""

# Check if sccache is already installed
if command -v sccache &> /dev/null; then
    echo -e "${GREEN}✓ sccache is already installed${NC}"
else
    echo -e "${YELLOW}Installing sccache...${NC}"
    cargo install sccache
fi

# Configure Cargo to use sccache
CARGO_CONFIG_DIR="$HOME/.cargo"
CARGO_CONFIG_FILE="$CARGO_CONFIG_DIR/config.toml"

echo ""
echo -e "${CYAN}Configuring Cargo to use sccache...${NC}"

# Create .cargo directory if it doesn't exist
mkdir -p "$CARGO_CONFIG_DIR"

# Check if config already has sccache
if [ -f "$CARGO_CONFIG_FILE" ] && grep -q "rustc-wrapper.*sccache" "$CARGO_CONFIG_FILE"; then
    echo -e "${GREEN}✓ sccache is already configured in Cargo${NC}"
else
    # Add sccache configuration
    echo "" >> "$CARGO_CONFIG_FILE"
    echo "[build]" >> "$CARGO_CONFIG_FILE"
    echo 'rustc-wrapper = "sccache"' >> "$CARGO_CONFIG_FILE"
    echo -e "${GREEN}✓ Added sccache configuration to $CARGO_CONFIG_FILE${NC}"
fi

echo ""
echo -e "${GREEN}=== Setup complete! ===${NC}"
echo ""
echo "You can check sccache statistics with:"
echo "  sccache --show-stats"
echo ""
echo "To clear the cache:"
echo "  sccache --zero-stats"
