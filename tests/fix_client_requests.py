#!/usr/bin/env python3

# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

# Read the file
with open('src/raft/src/integration_tests.rs', 'r') as f:
    content = f.read()

# Pattern to match ClientRequest constructions missing consistency_level
pattern = r'(ClientRequest\s*\{\s*id:\s*(\d+|[a-zA-Z_][a-zA-Z0-9_]*(?:\s*\+\s*\d+)?),\s*command:\s*RedisCommand\s*\{[^}]*\},\s*\})'

def replace_client_request(match):
    full_match = match.group(1)
    id_part = match.group(2)
    
    # Check if it already has consistency_level
    if 'consistency_level' in full_match:
        return full_match
    
    # Add RequestId wrapper if it's just a number
    if id_part.isdigit() or ('+' in id_part and any(c.isdigit() for c in id_part)):
        id_replacement = f'RequestId({id_part})'
    else:
        id_replacement = f'RequestId({id_part})'
    
    # Replace the id and add consistency_level
    new_match = full_match.replace(f'id: {id_part}', f'id: {id_replacement}')
    new_match = new_match.replace('},', '},\n                consistency_level: ConsistencyLevel::Linearizable,')
    
    return new_match

# Apply the replacement
new_content = re.sub(pattern, replace_client_request, content, flags=re.MULTILINE | re.DOTALL)

# Write back
with open('src/raft/src/integration_tests.rs', 'w') as f:
    f.write(new_content)

print("Fixed ClientRequest constructions")