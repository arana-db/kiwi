/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::error::MemoryParseError;
use serde::{de, Deserialize, Deserializer};
pub fn deserialize_bool_from_yes_no<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
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
where
    D: Deserializer<'de>,
{
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
        .map_err(|e| MemoryParseError::InvalidNumber { source: e })?;

    let multiplier: u64 = match unit_str {
        "" | "B" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024u64.pow(2),
        "G" | "GB" => 1024u64.pow(3),
        "T" | "TB" => 1024u64.pow(4),
        _ => {
            return Err(MemoryParseError::UnknownUnit {
                unit: unit_str.to_string(),
            })
        }
    };

    match num_value.checked_mul(multiplier) {
        Some(bytes) => Ok(bytes),
        None => Err(MemoryParseError::OutOfRange {
            raw: input.to_string(),
        }),
    }
}
