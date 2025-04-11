use crate::kstd::slice::Slice;
use crate::storage::storage_define::*;

#[test]
fn test_encode_user_key_no_zero() {
    let user_key = Slice::new_with_str("testkey");
    let mut encoded: Vec<u8> = vec![0; user_key.size() + ENCODED_KEY_DELIM_SIZE];
    // Create a raw pointer for the encoded buffer
    let dst = encoded.as_mut_ptr();
    let new_ptr = encode_user_key(&user_key, dst, 0);

    // Expect the encoded key to be the same as user_key with the delimiter appended at the end
    let expected_encoded = "testkey\u{0000}\u{0000}".as_bytes();
    assert_eq!(encoded, expected_encoded);
    // Check that the new_ptr correctly points at the end of the encoded data
    let offset = new_ptr as usize - dst as usize;
    assert_eq!(offset, expected_encoded.len());
}

#[test]
fn test_encode_user_key_with_zero() {
    let user_key = Slice::new_with_str("test\u{0000}key");
    let mut encoded: Vec<u8> = vec![0; user_key.size() + 1 + ENCODED_KEY_DELIM_SIZE];
    let dst = encoded.as_mut_ptr();
    let new_ptr = encode_user_key(&user_key, dst, 1);

    // Expect the encoded key to replace \0 with ENCODED_TRANSFORM_CHARACTER
    let expected_encoded = "test\u{0000}\u{0001}key\u{0000}\u{0000}".as_bytes();
    assert_eq!(encoded, expected_encoded);
    let offset = new_ptr as usize - dst as usize;
    assert_eq!(offset, expected_encoded.len());
}

#[test]
fn test_decode_user_key() {
    let encoded = "test\u{0000}\u{0001}key\u{0000}\u{0000}".as_bytes();
    let mut user_key = Vec::new();
    let ptr_len = encoded.len();
    let ret_ptr = decode_user_key(encoded.as_ptr(), ptr_len, &mut user_key);
    let expected_user_key = "test\u{0000}key".as_bytes();
    assert_eq!(user_key, expected_user_key);
    let offset = unsafe { ret_ptr.offset_from(encoded.as_ptr()) };
    assert_eq!(offset, ptr_len as isize);
}

#[test]
fn test_encode_and_decode_user_key() {
    let original_user_key = Slice::new_with_str("example\u{0000}key\u{0000}value");
    let nzero = original_user_key.count_byte(NEED_TRANSFORM_CHARACTER as u8);
    // Allocate a buffer large enough to hold the encoded key
    let mut encoded: Vec<u8> = vec![
        0;
        original_user_key.size()
            + nzero * (ENCODED_TRANSFORM_CHARACTER.len() - 1)
            + ENCODED_KEY_DELIM_SIZE
    ];
    let dst = encoded.as_mut_ptr();
    // Encode the user key
    encode_user_key(&original_user_key, dst, nzero);
    // Prepare a buffer for the decoded key
    let mut decoded_user_key = Vec::new();
    // Decode the user key
    decode_user_key(encoded.as_ptr(), encoded.len(), &mut decoded_user_key);
    // Assert that the decoded key is the same as the original
    assert_eq!(decoded_user_key, original_user_key.as_bytes());
}
