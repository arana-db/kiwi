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
    pub fn as_string(&self, hex: bool) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let slice = Slice::default();
        assert!(slice.empty(), "Newly created slice should be empty");
        assert_eq!(
            slice.size(),
            0,
            "Size of newly created slice should be zero"
        );
        assert_eq!(
            slice.data(),
            std::ptr::null(),
            "Data pointer should be null for empty slice"
        );
    }

    #[test]
    fn test_new() {
        let data = [1, 2, 3, 4, 5];
        let slice = Slice::new(data.as_ptr(), data.len());
        assert_eq!(
            slice.size(),
            data.len(),
            "Size should match the length of the data"
        );
        assert_eq!(
            unsafe { *slice.data() },
            data[0],
            "First element should match"
        );
    }

    #[test]
    fn test_new_with_string() {
        let s = "hello";
        let slice = Slice::new_with_str(s);
        assert_eq!(
            slice.size(),
            s.len(),
            "Size should match the length of the string"
        );
        let result_str = slice.as_string(false);
        assert_eq!(result_str, s, "The strings should match");
    }

    #[test]
    fn test_at() {
        let data = [10, 20, 30];
        let slice = Slice::new(data.as_ptr(), data.len());
        assert_eq!(slice.at(0), 10, "First element should be 10");
        assert_eq!(slice.at(1), 20, "Second element should be 20");
        assert_eq!(slice.at(2), 30, "Third element should be 30");
    }

    #[test]
    #[should_panic(expected = "Index out of bounds")]
    fn test_at_out_of_bounds() {
        let data = [10, 20, 30];
        let slice = Slice::new(data.as_ptr(), data.len());
        slice.at(3); // This should panic
    }

    #[test]
    fn test_clear() {
        let data = [1, 2, 3];
        let mut slice = Slice::new(data.as_ptr(), data.len());
        slice.clear();
        assert!(slice.empty(), "Slice should be empty after clear");
        assert_eq!(slice.size(), 0, "Size should be zero after clear");
        assert_eq!(
            slice.data(),
            std::ptr::null(),
            "Data pointer should be null after clear"
        );
    }

    #[test]
    fn test_to_string() {
        let s = "hello";
        let slice = Slice::new_with_str(s);
        assert_eq!(
            slice.as_string(false),
            s,
            "String conversion should match original"
        );
        let hex_str = slice.as_string(true);
        assert_eq!(hex_str, "68656C6C6F", "Hex string should match");
    }
}
