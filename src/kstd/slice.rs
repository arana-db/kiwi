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

// Simulate rocksdb's Slice
#[derive(Clone, Debug)]
pub struct Slice {
    data: *const u8,
    size: usize,
}

impl Default for Slice {
    fn default() -> Self {
        Self::new(std::ptr::null(), 0)
    }
}

impl Slice {
    // Creates a slice referring to d[0,n-1].
    pub fn new(data: *const u8, size: usize) -> Self {
        Slice { data, size }
    }

    // Creates a slice referring to the contents of a string.
    pub fn new_with_str(s: &str) -> Self {
        Slice {
            data: s.as_ptr(),
            size: s.len(),
        }
    }

    // Returns a pointer to the beginning of the referenced data.
    pub fn data(&self) -> *const u8 {
        self.data
    }

    // Returns the length (in bytes) of the referenced data.
    pub fn size(&self) -> usize {
        self.size
    }

    // Returns true if the length of the referenced data is zero.
    pub fn empty(&self) -> bool {
        self.size == 0
    }

    // Returns the ith byte in the referenced data.
    // REQUIRES: n < size()
    pub fn at(&self, n: usize) -> u8 {
        assert!(n < self.size, "Index out of bounds");
        unsafe { *self.data.add(n) }
    }

    // Changes this slice to refer to an empty array.
    pub fn clear(&mut self) {
        self.data = std::ptr::null();
        self.size = 0;
    }

    // Returns a string that contains the copy of the referenced data.
    pub fn to_string(&self, hex: bool) -> String {
        if self.data.is_null() {
            return String::new();
        }
        let slice = unsafe { std::slice::from_raw_parts(self.data, self.size) };
        if hex {
            slice
                .iter()
                .fold(String::with_capacity(slice.len() * 2), |mut acc, byte| {
                    use std::fmt::Write;
                    write!(&mut acc, "{:02X}", byte).unwrap();
                    acc
                })
        } else {
            String::from_utf8_lossy(slice).into_owned()
        }
    }

    // Returns a u8 slice containing the copy of the referenced data.
    pub fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() || self.size == 0 {
            return &[];
        }
        // Safely convert the raw pointer and size into a slice.
        unsafe { std::slice::from_raw_parts(self.data, self.size) }
    }

    pub fn count_byte(&self, byte: u8) -> usize {
        if self.data.is_null() {
            return 0;
        }
        unsafe {
            std::slice::from_raw_parts(self.data, self.size)
                .iter()
                .filter(|&&x| x == byte)
                .count()
        }
    }
}
