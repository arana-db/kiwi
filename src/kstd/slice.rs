// Simulate rocksdb's Slice
#[derive(Clone, Debug)]
pub struct Slice {
    data: *const u8,
    size: usize,
}

impl Slice {
    // Creates an empty slice.
    pub fn new() -> Self {
        Slice {
            data: std::ptr::null(),
            size: 0,
        }
    }

    // Creates a slice referring to d[0,n-1].
    pub fn new_with_parts(data: *const u8, size: usize) -> Self {
        Slice { data, size }
    }

    // Creates a slice referring to the contents of a string.
    pub fn new_with_string(s: &str) -> Self {
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
            return slice.iter().map(|byte| format!("{:02X}", byte)).collect();
        } else {
            return String::from_utf8_lossy(slice).into_owned();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let slice = Slice::new();
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
    fn test_new_with_parts() {
        let data = [1, 2, 3, 4, 5];
        let slice = Slice::new_with_parts(data.as_ptr(), data.len());
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
        let slice = Slice::new_with_string(s);
        assert_eq!(
            slice.size(),
            s.len(),
            "Size should match the length of the string"
        );
        let result_str = slice.to_string(false);
        assert_eq!(result_str, s, "The strings should match");
    }

    #[test]
    fn test_at() {
        let data = [10, 20, 30];
        let slice = Slice::new_with_parts(data.as_ptr(), data.len());
        assert_eq!(slice.at(0), 10, "First element should be 10");
        assert_eq!(slice.at(1), 20, "Second element should be 20");
        assert_eq!(slice.at(2), 30, "Third element should be 30");
    }

    #[test]
    #[should_panic(expected = "Index out of bounds")]
    fn test_at_out_of_bounds() {
        let data = [10, 20, 30];
        let slice = Slice::new_with_parts(data.as_ptr(), data.len());
        slice.at(3); // This should panic
    }

    #[test]
    fn test_clear() {
        let data = [1, 2, 3];
        let mut slice = Slice::new_with_parts(data.as_ptr(), data.len());
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
        let slice = Slice::new_with_string(s);
        assert_eq!(
            slice.to_string(false),
            s,
            "String conversion should match original"
        );
        let hex_str = slice.to_string(true);
        assert_eq!(hex_str, "68656C6C6F", "Hex string should match");
    }
}
