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
use serde::{Deserialize, Deserializer, de};
use std::collections::HashMap;

use crate::error::MemoryParseError;
pub fn deserialize_bool_from_yes_no<'de, D>(deserializer: D) -> Result<bool, D::Error>
where D: Deserializer<'de> {
    let s: String = Deserialize::deserialize(deserializer)?;
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" | "on" => Ok(true),
        "no" | "false" | "0" | "off" => Ok(false),
        _ => Err(de::Error::custom(
            "expected one of: yes, no, true, false, 1, 0, on, off",
        )),
    }
}

pub fn deserialize_memory<'de, D>(deserializer: D) -> Result<u64, D::Error>
where D: Deserializer<'de> {
    let s: String = Deserialize::deserialize(deserializer)?;
    match parse_memory(s.as_str()) {
        Ok(num) => Ok(num),
        Err(e) => Err(de::Error::custom(e)),
    }
}

pub fn parse_memory(input: &str) -> Result<u64, MemoryParseError> {
    let cleaned_input = input.trim().replace(',', "").to_uppercase();

    let num_end = cleaned_input
        .rfind(|c: char| c.is_ascii_digit())
        .map(|pos| pos + 1)
        .unwrap_or(0);

    let (num_str, unit_str) = cleaned_input.split_at(num_end);
    let unit_str = unit_str.trim();

    if num_str.is_empty() {
        return Err(MemoryParseError::InvalidFormat {
            raw: input.to_string(),
        });
    }

    let num_value: u64 = num_str
        .parse()
        .map_err(|_e| MemoryParseError::InvalidNumber { source: _e })?;

    let multiplier: u64 = match unit_str {
        "" | "B" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024u64.pow(2),
        "G" | "GB" => 1024u64.pow(3),
        "T" | "TB" => 1024u64.pow(4),
        _ => {
            return Err(MemoryParseError::UnknownUnit {
                unit: unit_str.to_string(),
            });
        }
    };

    match num_value.checked_mul(multiplier) {
        Some(bytes) => Ok(bytes),
        None => Err(MemoryParseError::OutOfRange {
            raw: input.to_string(),
        }),
    }
}

/// Parse Redis-style configuration file content
/// Supports comments starting with # and empty lines
pub fn parse_redis_config(content: &str) -> Result<HashMap<String, String>, String> {
    let mut config = HashMap::new();
    
    for (line_num, line) in content.lines().enumerate() {
        let line = line.trim();
        
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        // Parse key-value pairs
        if let Some((key, value)) = parse_config_line(line) {
            config.insert(key, value);
        } else {
            return Err(format!("Invalid configuration line {}: {}", line_num + 1, line));
        }
    }
    
    Ok(config)
}

/// Parse a single configuration line
/// Supports both "key value" and "key=value" formats
fn parse_config_line(line: &str) -> Option<(String, String)> {
    // Try "key=value" format first
    if let Some(pos) = line.find('=') {
        let key = line[..pos].trim().to_string();
        let value = line[pos + 1..].trim().to_string();
        return Some((key, value));
    }
    
    // Try "key value" format
    if let Some(pos) = line.find(' ') {
        let key = line[..pos].trim().to_string();
        let value = line[pos..].trim().to_string();
        if !value.is_empty() {
            return Some((key, value));
        }
    }
    
    None
}

/// Convert Redis-style configuration to INI format for compatibility
pub fn redis_config_to_ini(content: &str) -> Result<String, String> {
    let config = parse_redis_config(content)?;
    let mut ini_content = String::new();
    
    for (key, value) in config {
        ini_content.push_str(&format!("{} = {}\n", key, value));
    }
    
    Ok(ini_content)
}
