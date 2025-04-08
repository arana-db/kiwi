// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//  of patent rights can be found in the PATENTS file in the same directory.

use std::fmt;

/// Data type enumeration, corresponding to the DataType in C++ version
/// TODO: remove allow dead code
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// String type
    String,
    /// Hash table type
    Hash,
    /// List type
    List,
    /// Set type
    Set,
    /// Sorted set type
    Zset,
    /// All types
    All,
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl DataType {
    /// Convert data type to character
    pub fn to_char(self) -> char {
        match self {
            DataType::String => 'k',
            DataType::Hash => 'h',
            DataType::List => 'l',
            DataType::Set => 's',
            DataType::Zset => 'z',
            DataType::All => 'a',
        }
    }

    /// Convert data type to tag string
    pub fn to_tag(self) -> &'static str {
        match self {
            DataType::String => "k",
            DataType::Hash => "h",
            DataType::List => "l",
            DataType::Set => "s",
            DataType::Zset => "z",
            DataType::All => "a",
        }
    }

    /// Convert character to data type
    pub fn from_char(c: char) -> Option<Self> {
        match c {
            'k' => Some(DataType::String),
            'h' => Some(DataType::Hash),
            'l' => Some(DataType::List),
            's' => Some(DataType::Set),
            'z' => Some(DataType::Zset),
            'a' => Some(DataType::All),
            _ => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            DataType::String => "string",
            DataType::Hash => "hash",
            DataType::List => "list",
            DataType::Set => "set",
            DataType::Zset => "zset",
            DataType::All => "all",
        };
        write!(f, "{}", s)
    }
}

/// Convert data type to tag string
/// TODO: remove allow dead code
#[allow(dead_code)]
pub fn data_type_to_tag(data_type: DataType) -> &'static str {
    match data_type {
        DataType::String => "k",
        DataType::Hash => "h",
        DataType::List => "l",
        DataType::Set => "s",
        DataType::Zset => "z",
        DataType::All => "a",
    }
}

/// Encoding type enumeration, corresponding to the EncodeType in C++ version
/// TODO: remove allow dead code
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeType {
    /// String encoding
    String,
    /// Hash table encoding
    Hash,
    /// List encoding
    List,
    /// Set encoding
    Set,
    /// Sorted set encoding
    Zset,
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl EncodeType {
    /// Convert encoding type to character
    pub fn to_char(self) -> char {
        match self {
            EncodeType::String => 'k',
            EncodeType::Hash => 'h',
            EncodeType::List => 'l',
            EncodeType::Set => 's',
            EncodeType::Zset => 'z',
        }
    }

    /// Convert character to encoding type
    pub fn from_char(c: char) -> Option<Self> {
        match c {
            'k' => Some(EncodeType::String),
            'h' => Some(EncodeType::Hash),
            'l' => Some(EncodeType::List),
            's' => Some(EncodeType::Set),
            'z' => Some(EncodeType::Zset),
            _ => None,
        }
    }
}

/// Value type enumeration, corresponding to the ValueType in C++ version
/// TODO: remove allow dead code
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    /// Deletion type
    Deletion,
    /// Value type
    Value,
    /// Merge type
    Merge,
    /// Other type
    Other,
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl ValueType {
    /// Convert value type to u8
    pub fn to_u8(self) -> u8 {
        match self {
            ValueType::Deletion => 0,
            ValueType::Value => 1,
            ValueType::Merge => 2,
            ValueType::Other => 3,
        }
    }

    /// Convert u8 to value type
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(ValueType::Deletion),
            1 => Some(ValueType::Value),
            2 => Some(ValueType::Merge),
            3 => Some(ValueType::Other),
            _ => None,
        }
    }
}

/// Internal value structure for storing Redis data
/// TODO: remove allow dead code
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InternalValue {
    /// The type of data stored
    data_type: DataType,
    /// The actual value bytes
    value: Vec<u8>,
    /// Version for concurrency control
    version: u64,
    /// Timestamp for expiration
    timestamp: u64,
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl InternalValue {
    /// Create a new internal value
    pub fn new(data_type: DataType, value: &[u8]) -> Self {
        InternalValue {
            data_type,
            value: value.to_vec(),
            version: 0,
            timestamp: 0,
        }
    }

    /// Set the size of the collection
    /// TODO: implement
    pub fn set_size(&mut self, _size: u64) {
        // In a real implementation, this would encode the size into the value
        // For now, we just implement the method to fix compilation errors
    }

    /// Get the data type
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// Get the value bytes
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Get the version
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Set the version
    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    /// Get the timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Set the timestamp
    pub fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    /// Set the expiration time
    pub fn set_etime(&mut self, etime: u64) {
        self.timestamp = etime;
    }

    /// Set relative timestamp
    pub fn set_relative_timestamp(&mut self, ttl: u64) -> Result<(), &'static str> {
        if ttl > 0 {
            self.timestamp = ttl;
        }
        Ok(())
    }

    /// Encode the value into bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&self.value);
        encoded
    }
}

/// Parsed internal value structure
/// TODO: remove allow dead code
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ParsedInternalValue {
    /// The type of data stored
    data_type: DataType,
    /// The parsed value
    value: String,
    /// Version for concurrency control
    version: u64,
    /// Timestamp for expiration
    timestamp: u64,
    /// Size of the collection (for List, Hash, Set, ZSet)
    size: u64,
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl ParsedInternalValue {
    /// Create a new parsed internal value
    pub fn new(data_type: DataType, value: String) -> Self {
        ParsedInternalValue {
            data_type,
            value,
            version: 0,
            timestamp: 0,
            size: 0,
        }
    }

    /// Get the size of the collection
    pub fn get_size(&self) -> Option<u64> {
        Some(self.size)
    }

    /// Set the size of the collection
    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    /// Get the size directly
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Encode the parsed internal value
    pub fn encode(&self) -> Vec<u8> {
        // Simple implementation - in a real scenario this would properly serialize all fields
        self.value.as_bytes().to_vec()
    }

    /// Get the data type
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// Get the value as string
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Get the version
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Set the version
    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    /// Get the timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Set the timestamp
    pub fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    /// Check if the value is expired
    pub fn is_expired(&self, now: u64) -> bool {
        self.timestamp > 0 && now >= self.timestamp
    }

    /// Get the expiration time
    pub fn etime(&self) -> u64 {
        self.timestamp
    }

    /// Get the user value
    pub fn user_value(&self) -> &[u8] {
        self.value.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_conversion() {
        // Test DataType conversion
        assert_eq!(DataType::String.to_char(), 'k');
        assert_eq!(DataType::Hash.to_char(), 'h');
        assert_eq!(DataType::List.to_char(), 'l');
        assert_eq!(DataType::Set.to_char(), 's');
        assert_eq!(DataType::Zset.to_char(), 'z');
        assert_eq!(DataType::All.to_char(), 'a');

        assert_eq!(DataType::from_char('k'), Some(DataType::String));
        assert_eq!(DataType::from_char('h'), Some(DataType::Hash));
        assert_eq!(DataType::from_char('l'), Some(DataType::List));
        assert_eq!(DataType::from_char('s'), Some(DataType::Set));
        assert_eq!(DataType::from_char('z'), Some(DataType::Zset));
        assert_eq!(DataType::from_char('a'), Some(DataType::All));
        assert_eq!(DataType::from_char('x'), None);

        assert_eq!(DataType::String.to_string(), "string");
        assert_eq!(DataType::Hash.to_string(), "hash");
        assert_eq!(DataType::List.to_string(), "list");
        assert_eq!(DataType::Set.to_string(), "set");
        assert_eq!(DataType::Zset.to_string(), "zset");
        assert_eq!(DataType::All.to_string(), "all");
    }

    #[test]
    fn test_encode_type_conversion() {
        // Test EncodeType conversion
        assert_eq!(EncodeType::String.to_char(), 'k');
        assert_eq!(EncodeType::Hash.to_char(), 'h');
        assert_eq!(EncodeType::List.to_char(), 'l');
        assert_eq!(EncodeType::Set.to_char(), 's');
        assert_eq!(EncodeType::Zset.to_char(), 'z');

        assert_eq!(EncodeType::from_char('k'), Some(EncodeType::String));
        assert_eq!(EncodeType::from_char('h'), Some(EncodeType::Hash));
        assert_eq!(EncodeType::from_char('l'), Some(EncodeType::List));
        assert_eq!(EncodeType::from_char('s'), Some(EncodeType::Set));
        assert_eq!(EncodeType::from_char('z'), Some(EncodeType::Zset));
        assert_eq!(EncodeType::from_char('x'), None);
    }

    #[test]
    fn test_value_type_conversion() {
        // Test ValueType conversion
        assert_eq!(ValueType::Deletion.to_u8(), 0);
        assert_eq!(ValueType::Value.to_u8(), 1);
        assert_eq!(ValueType::Merge.to_u8(), 2);
        assert_eq!(ValueType::Other.to_u8(), 3);

        assert_eq!(ValueType::from_u8(0), Some(ValueType::Deletion));
        assert_eq!(ValueType::from_u8(1), Some(ValueType::Value));
        assert_eq!(ValueType::from_u8(2), Some(ValueType::Merge));
        assert_eq!(ValueType::from_u8(3), Some(ValueType::Other));
        assert_eq!(ValueType::from_u8(4), None);
    }
}
