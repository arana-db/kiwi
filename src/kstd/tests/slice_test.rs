use crate::kstd::slice::Slice;

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
    let result_str = slice.to_string(false);
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
        slice.to_string(false),
        s,
        "String conversion should match original"
    );
    let hex_str = slice.to_string(true);
    assert_eq!(hex_str, "68656C6C6F", "Hex string should match");
}
