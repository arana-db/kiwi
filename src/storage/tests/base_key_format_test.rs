use crate::kstd::slice::Slice;
use crate::storage::base_key_format::*;
use crate::storage::storage_define::{
    ENCODED_KEY_DELIM_SIZE, PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH,
};

#[test]
fn test_encode_and_decode() {
    let test_key = Slice::new_with_str("test_key");

    let mut base_key = BaseKey::new(&test_key);
    let encoded_data = base_key.encode();

    // there is no zero, so no need to transform.
    // reserve1 + key.size + delim.size + reserve2
    assert_eq!(
        encoded_data.len(),
        PREFIX_RESERVE_LENGTH + test_key.size() + ENCODED_KEY_DELIM_SIZE + SUFFIX_RESERVE_LENGTH
    );

    println!("Encoded data: {:?}", encoded_data);

    let additional_data: Vec<u8> = encoded_data.iter().copied().collect();
    let encoded_str = std::str::from_utf8(&additional_data).unwrap();

    let decode_key = ParsedBaseKey::new(encoded_str.as_bytes());

    assert_eq!(decode_key.key(), test_key.as_bytes());
}
