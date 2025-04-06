//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Redis HyperLogLog operations implementation
//! This module provides HyperLogLog operations for Redis storage

use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashSet;
use rocksdb::{WriteBatch, WriteOptions, ReadOptions};

use crate::{Result, StorageError};
use crate::format::{DataType, InternalValue, ParsedInternalValue};
use crate::redis::Redis;
use crate::lock::ScopeRecordLock;

// Constants for HyperLogLog implementation
const HLL_P: u8 = 14; // The precision (number of bits used for register index)
const HLL_REGISTERS: usize = 1 << HLL_P; // Number of registers
const HLL_P_MASK: u64 = (1 << HLL_P) - 1; // Mask to get the register index
const HLL_DENSE_SIZE: usize = 6 * HLL_REGISTERS / 8; // Size of dense representation

impl Redis {
    /// Add elements to a HyperLogLog data structure
    pub fn pfadd(&self, key: &[u8], elements: &[&[u8]], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        if elements.is_empty() {
            *ret = 0;
            return Ok(());
        }
        
        // Create lock for the key
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), key);
        
        // Try to get the existing meta value
        let read_options = ReadOptions::default();
        let mut registers = vec![0u8; HLL_DENSE_SIZE];
        let mut updated = false;
        
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let mut parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if it's the right type
                if parsed_meta.data_type() != DataType::HyperLogLog {
                    if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                        // Treat as not found if expired
                        // Initialize new HyperLogLog
                        for element in elements {
                            if self.hll_add_to_registers(&mut registers, element) {
                                updated = true;
                            }
                        }
                    } else {
                        return Err(StorageError::InvalidFormat(format!("Wrong type for key: {}", String::from_utf8_lossy(key))));
                    }
                } else if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                    // Initialize new HyperLogLog if expired
                    for element in elements {
                        if self.hll_add_to_registers(&mut registers, element) {
                            updated = true;
                        }
                    }
                } else {
                    // Get existing registers
                    registers = parsed_meta.user_value().to_vec();
                    
                    // Add elements to existing HyperLogLog
                    for element in elements {
                        if self.hll_add_to_registers(&mut registers, element) {
                            updated = true;
                        }
                    }
                }
            },
            None => {
                // Initialize new HyperLogLog
                for element in elements {
                    if self.hll_add_to_registers(&mut registers, element) {
                        updated = true;
                    }
                }
            }
        }
        
        // Update the value if any register was modified
        if updated {
            let mut internal_value = InternalValue::new(DataType::HyperLogLog, &registers);
            let encoded_value = internal_value.encode();
            db.put_opt(key, &encoded_value, &self.default_write_options)?;
            *ret = 1;
        } else {
            *ret = 0;
        }
        
        Ok(())
    }
    
    /// Count the approximate number of unique elements in HyperLogLog data structures
    pub fn pfcount(&self, keys: &[&[u8]], ret: &mut i64) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        if keys.is_empty() {
            *ret = 0;
            return Ok(());
        }
        
        // If there's only one key, just return its cardinality
        if keys.len() == 1 {
            let read_options = ReadOptions::default();
            match db.get_opt(keys[0], &read_options)? {
                Some(meta_value) => {
                    // Parse the meta value
                    let parsed_meta = ParsedInternalValue::new(&meta_value);
                    
                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::HyperLogLog {
                        return Err(StorageError::InvalidFormat(format!("Wrong type for key: {}", String::from_utf8_lossy(keys[0]))));
                    }
                    
                    // Check if expired
                    if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                        *ret = 0;
                        return Ok(());
                    }
                    
                    // Get registers and calculate cardinality
                    let registers = parsed_meta.user_value();
                    *ret = self.hll_count_registers(registers) as i64;
                },
                None => {
                    *ret = 0;
                }
            }
            return Ok(());
        }
        
        // For multiple keys, merge the registers
        let mut merged_registers = vec![0u8; HLL_DENSE_SIZE];
        let mut found = false;
        
        for &key in keys {
            let read_options = ReadOptions::default();
            match db.get_opt(key, &read_options)? {
                Some(meta_value) => {
                    // Parse the meta value
                    let parsed_meta = ParsedInternalValue::new(&meta_value);
                    
                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::HyperLogLog {
                        return Err(StorageError::InvalidFormat(format!("Wrong type for key: {}", String::from_utf8_lossy(key))));
                    }
                    
                    // Check if expired
                    if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                        continue;
                    }
                    
                    // Merge registers
                    let registers = parsed_meta.user_value();
                    self.hll_merge_registers(&mut merged_registers, registers);
                    found = true;
                },
                None => {
                    // Skip non-existent keys
                }
            }
        }
        
        if found {
            *ret = self.hll_count_registers(&merged_registers) as i64;
        } else {
            *ret = 0;
        }
        
        Ok(())
    }
    
    /// Merge multiple HyperLogLog values into a single one
    pub fn pfmerge(&self, destination: &[u8], sources: &[&[u8]]) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        if sources.is_empty() {
            return Ok(());
        }
        
        // Create lock for the destination key
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), destination);
        
        // Initialize merged registers
        let mut merged_registers = vec![0u8; HLL_DENSE_SIZE];
        let mut found = false;
        
        // Merge all source HyperLogLogs
        for &source in sources {
            let read_options = ReadOptions::default();
            match db.get_opt(source, &read_options)? {
                Some(meta_value) => {
                    // Parse the meta value
                    let parsed_meta = ParsedInternalValue::new(&meta_value);
                    
                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::HyperLogLog {
                        continue; // Skip wrong type keys
                    }
                    
                    // Check if expired
                    if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                        continue;
                    }
                    
                    // Merge registers
                    let registers = parsed_meta.user_value();
                    self.hll_merge_registers(&mut merged_registers, registers);
                    found = true;
                },
                None => {
                    // Skip non-existent keys
                }
            }
        }
        
        // If any valid source was found, update the destination
        if found {
            let mut internal_value = InternalValue::new(DataType::HyperLogLog, &merged_registers);
            let encoded_value = internal_value.encode();
            db.put_opt(destination, &encoded_value, &self.default_write_options)?;
        }
        
        Ok(())
    }
    
    /// Add an element to HyperLogLog registers
    fn hll_add_to_registers(&self, registers: &mut [u8], element: &[u8]) -> bool {
        // Hash the element
        let hash = self.hll_hash(element);
        
        // Extract the register index (using the first P bits)
        let index = (hash & HLL_P_MASK) as usize;
        
        // Count the number of leading zeros plus one in the remaining bits
        let count_zeros = (hash >> HLL_P).leading_zeros() + 1;
        
        // Each register uses 6 bits (values from 1 to 64)
        let register_value = count_zeros.min(63) as u8;
        
        // Calculate byte position and bit offset
        let byte_pos = (index * 6) / 8;
        let bit_offset = (index * 6) % 8;
        
        // Get current value
        let current_value = if bit_offset <= 2 {
            // Value is contained in a single byte
            (registers[byte_pos] >> bit_offset) & 0x3F
        } else {
            // Value is split between two bytes
            let first_part = registers[byte_pos] >> bit_offset;
            let second_part = registers[byte_pos + 1] << (8 - bit_offset);
            (first_part | second_part) & 0x3F
        };
        
        // If new value is greater, update the register
        if register_value > current_value {
            // Clear the old value
            if bit_offset <= 2 {
                registers[byte_pos] &= !(0x3F << bit_offset);
                registers[byte_pos] |= register_value << bit_offset;
            } else {
                registers[byte_pos] &= !(0xFF << bit_offset);
                registers[byte_pos] |= register_value << bit_offset;
                registers[byte_pos + 1] &= !(0x3F >> (8 - bit_offset));
                registers[byte_pos + 1] |= register_value >> (8 - bit_offset);
            }
            return true;
        }
        
        false
    }
    
    /// Merge two HyperLogLog register sets
    fn hll_merge_registers(&self, dest: &mut [u8], src: &[u8]) {
        for i in 0..HLL_REGISTERS {
            // Calculate byte position and bit offset
            let byte_pos = (i * 6) / 8;
            let bit_offset = (i * 6) % 8;
            
            // Get values from both register sets
            let src_value = if bit_offset <= 2 {
                (src[byte_pos] >> bit_offset) & 0x3F
            } else {
                let first_part = src[byte_pos] >> bit_offset;
                let second_part = src[byte_pos + 1] << (8 - bit_offset);
                (first_part | second_part) & 0x3F
            };
            
            let dest_value = if bit_offset <= 2 {
                (dest[byte_pos] >> bit_offset) & 0x3F
            } else {
                let first_part = dest[byte_pos] >> bit_offset;
                let second_part = dest[byte_pos + 1] << (8 - bit_offset);
                (first_part | second_part) & 0x3F
            };
            
            // If source value is greater, update destination
            if src_value > dest_value {
                if bit_offset <= 2 {
                    dest[byte_pos] &= !(0x3F << bit_offset);
                    dest[byte_pos] |= src_value << bit_offset;
                } else {
                    dest[byte_pos] &= !(0xFF << bit_offset);
                    dest[byte_pos] |= src_value << bit_offset;
                    dest[byte_pos + 1] &= !(0x3F >> (8 - bit_offset));
                    dest[byte_pos + 1] |= src_value >> (8 - bit_offset);
                }
            }
        }
    }
    
    /// Calculate cardinality from HyperLogLog registers
    fn hll_count_registers(&self, registers: &[u8]) -> u64 {
        let mut sum = 0.0;
        let mut zero_regs = 0;
        
        for i in 0..HLL_REGISTERS {
            // Calculate byte position and bit offset
            let byte_pos = (i * 6) / 8;
            let bit_offset = (i * 6) % 8;
            
            // Get register value
            let value = if bit_offset <= 2 {
                (registers[byte_pos] >> bit_offset) & 0x3F
            } else {
                let first_part = registers[byte_pos] >> bit_offset;
                let second_part = registers[byte_pos + 1] << (8 - bit_offset);
                (first_part | second_part) & 0x3F
            };
            
            sum += 1.0 / (1u64 << value) as f64;
            if value == 0 {
                zero_regs += 1;
            }
        }
        
        // Apply HyperLogLog algorithm
        let alpha = match HLL_P {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / (HLL_REGISTERS as f64)),
        };
        
        let mut estimate = alpha * (HLL_REGISTERS as f64).powi(2) / sum;
        
        // Apply corrections for small and large cardinalities
        if estimate <= 2.5 * (HLL_REGISTERS as f64) && zero_regs > 0 {
            // Small range correction
            estimate = (HLL_REGISTERS as f64) * (HLL_REGISTERS as f64 / zero_regs as f64).ln();
        } else if estimate > 2u64.pow(32) as f64 / 30.0 {
            // Large range correction
            estimate = -2u64.pow(32) as f64 * (1.0 - estimate / 2u64.pow(32) as f64).ln();
        }
        
        estimate.round() as u64
    }
    
    /// Hash function for HyperLogLog
    fn hll_hash(&self, data: &[u8]) -> u64 {
        // MurmurHash64A implementation
        const SEED: u64 = 0x5F3759DF;
        const M: u64 = 0xc6a4a7935bd1e995;
        const R: u32 = 47;
        
        let mut h = SEED ^ ((data.len() as u64) * M);
        
        // Process 8 bytes at a time
        let chunks = data.chunks_exact(8);
        let remainder = chunks.remainder();
        
        for chunk in chunks {
            let mut k = u64::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7]]);
            
            k = k.wrapping_mul(M);
            k ^= k >> R;
            k = k.wrapping_mul(M);
            
            h ^= k;
            h = h.wrapping_mul(M);
        }
        
        // Process remaining bytes
        if !remainder.is_empty() {
            let mut bytes = [0u8; 8];
            for (i, &byte) in remainder.iter().enumerate() {
                bytes[i] = byte;
            }
            let k = u64::from_le_bytes(bytes);
            
            h ^= k;
            h = h.wrapping_mul(M);
        }
        
        // Finalization
        h ^= h >> R;
        h = h.wrapping_mul(M);
        h ^= h >> R;
        
        h
    }
}