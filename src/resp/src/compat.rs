// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Defines how Boolean values should be converted when encoding to older RESP versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BooleanMode {
    /// Convert Boolean to Integer: true → :1, false → :0
    Integer,
    /// Convert Boolean to Simple String: true → +OK, false → +ERR
    SimpleString,
}

/// Defines how Double values should be converted when encoding to older RESP versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DoubleMode {
    /// Convert Double to Bulk String: 3.14 → $4\r\n3.14\r\n
    BulkString,
    /// Convert Double to Integer if whole number: 2.0 → :2, 2.5 → BulkString
    IntegerIfWhole,
}

/// Defines how Map values should be converted when encoding to older RESP versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapMode {
    /// Convert Map to flat Array: Map{k1:v1,k2:v2} → *4\r\nk1\r\nv1\r\nk2\r\nv2\r\n
    FlatArray,
    /// Convert Map to Array of pairs: Map{k1:v1,k2:v2} → *2\r\n*2\r\nk1\r\nv1\r\n*2\r\nk2\r\nv2\r\n
    ArrayOfPairs,
}

/// Configuration for converting RESP3 types to older RESP versions.
/// 
/// This policy defines how RESP3-specific types (Boolean, Double, Map, etc.)
/// should be represented when encoding to RESP1 or RESP2 protocols.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DownlevelPolicy {
    /// How to convert Boolean values
    pub boolean_mode: BooleanMode,
    /// How to convert Double values
    pub double_mode: DoubleMode,
    /// How to convert Map values
    pub map_mode: MapMode,
}

impl Default for DownlevelPolicy {
    /// Creates a default downlevel policy with conservative conversion settings.
    /// 
    /// Default settings:
    /// - Boolean → Integer (true → :1, false → :0)
    /// - Double → BulkString (3.14 → $4\r\n3.14\r\n)
    /// - Map → FlatArray (Map{k:v} → *2\r\nk\r\nv\r\n)
    fn default() -> Self {
        Self {
            boolean_mode: BooleanMode::Integer,
            double_mode: DoubleMode::BulkString,
            map_mode: MapMode::FlatArray,
        }
    }
}
