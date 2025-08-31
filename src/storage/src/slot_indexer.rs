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

use crc16::{ARC, State};

pub const SLOT_INDEXER_INSTANCE_NUM: usize = 3;

/// Manage slots to rocksdb indexes
#[derive(Debug)]
pub struct SlotIndexer {
    // Number of instances
    instance_num: usize,
}

/// NOTE: default instance number is 3.
impl Default for SlotIndexer {
    fn default() -> Self {
        Self {
            instance_num: SLOT_INDEXER_INSTANCE_NUM,
        }
    }
}

impl SlotIndexer {
    /// Create a new SlotIndexer with a defined instance number.
    pub fn new(instance_num: usize) -> Self {
        assert!(
            instance_num > 0,
            "Instance number must be greater than zero."
        );
        Self { instance_num }
    }

    /// Calculate the instance ID from given slot ID.
    pub fn get_instance_id(&self, slot_id: usize) -> usize {
        slot_id % self.instance_num
    }

    /// Placeholder for re-sharding slots functionality.
    pub fn reshard_slots(&self, _slots: Vec<usize>) {
        // TODO: Implement the logic for re-sharding slots.
        // When we implement this method, remove the underscore.
        // Don't forget add unit test.
        unimplemented!("Resharding logic not implemented yet.");
    }
}

/// Map key to slot ID using CRC16-ARC (compatible with Redis Cluster hash slot)
pub fn key_to_slot_id(key: &[u8]) -> usize {
    State::<ARC>::calculate(key) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_slot_indexer() {
        let indexer = SlotIndexer::new(5);
        assert_eq!(indexer.instance_num, 5);
    }

    #[test]
    #[should_panic(expected = "Instance number must be greater than zero.")]
    fn test_create_slot_indexer_with_zero() {
        // This should panic
        SlotIndexer::new(0);
    }

    #[test]
    fn test_get_instance_id() {
        let indexer = SlotIndexer::new(10);
        assert_eq!(indexer.get_instance_id(25), 5);
        assert_eq!(indexer.get_instance_id(10), 0);
        assert_eq!(indexer.get_instance_id(8), 8);
        assert_eq!(indexer.get_instance_id(15), 5);
    }
}
