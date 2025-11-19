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

pub const PREFIX_RESERVE_LENGTH: usize = 8;
pub const VERSION_LENGTH: usize = 8;
// const SCORE_LENGTH: usize = 8;
pub const SUFFIX_RESERVE_LENGTH: usize = 16;
// const LIST_VALUE_INDEX_LENGTH: usize = 16;

// used to store a fixed-size value for the Type field.
pub const TYPE_LENGTH: usize = 1;
pub const TIMESTAMP_LENGTH: usize = 8;

// TODO: maybe we can change \u{0000} to \0,
// it will be more readable.
pub const NEED_TRANSFORM_CHARACTER: char = '\x00';
const ENCODED_TRANSFORM_CHARACTER: &str = "\x00\x01";
const ENCODED_KEY_DELIM: &str = "\x00\x00";
pub const ENCODED_KEY_DELIM_SIZE: usize = 2;

pub const STRING_VALUE_SUFFIXLENGTH: usize = 2 * TIMESTAMP_LENGTH + SUFFIX_RESERVE_LENGTH;
pub const BASE_META_VALUE_COUNT_LENGTH: usize = 8;
/// type(1B) + len(8B) + version(8B) + reserve(16B) + cdata(8B) + timestamp(8B)
pub const BASE_META_VALUE_LENGTH: usize = TYPE_LENGTH
    + BASE_META_VALUE_COUNT_LENGTH
    + VERSION_LENGTH
    + SUFFIX_RESERVE_LENGTH
    + 2 * TIMESTAMP_LENGTH;

use bytes::{BufMut, BytesMut};
use snafu::ensure;

use crate::error::{InvalidFormatSnafu, Result};

pub fn encode_user_key(user_key: &[u8], dst: &mut BytesMut) -> Result<()> {
    let mut start_pos = 0;
    for (i, &byte) in user_key.iter().enumerate() {
        if byte == NEED_TRANSFORM_CHARACTER as u8 {
            if i > start_pos {
                dst.put_slice(&user_key[start_pos..i]);
            }
            dst.put_slice(ENCODED_TRANSFORM_CHARACTER.as_bytes());
            start_pos = i + 1;
        }
    }

    if start_pos < user_key.len() {
        dst.put_slice(&user_key[start_pos..]);
    }

    dst.put_slice(ENCODED_KEY_DELIM.as_bytes());

    Ok(())
}

pub fn decode_user_key(encoded_key_part: &[u8], user_key: &mut BytesMut) -> Result<()> {
    let mut zero_ahead = false;
    let mut delim_found = false;

    ensure!(
        encoded_key_part.len() >= ENCODED_KEY_DELIM_SIZE,
        InvalidFormatSnafu {
            message: "Encoded key part too short".to_string()
        }
    );

    for &byte in encoded_key_part.iter() {
        match byte {
            0x00 => {
                if zero_ahead {
                    delim_found = true;
                    break;
                }
                zero_ahead = true;
            }
            0x01 => {
                if zero_ahead {
                    user_key.put_u8(0x00);
                    zero_ahead = false;
                } else {
                    user_key.put_u8(byte);
                }
            }
            _ => {
                ensure!(!zero_ahead, InvalidFormatSnafu {
                    message:
                        "Invalid encoding sequence: single zero followed by non-one/non-zero byte"
                            .to_string()
                });
                user_key.put_u8(byte);
                zero_ahead = false;
            }
        }
    }

    ensure!(
        delim_found,
        InvalidFormatSnafu {
            message: "Encoded key delimiter not found or key ends unexpectedly".to_string()
        }
    );

    Ok(())
}

pub fn seek_userkey_delim(data: &[u8]) -> usize {
    let mut zero_ahead = false;
    for (i, &byte) in data.iter().enumerate() {
        if byte == 0x00 && zero_ahead {
            return i + 1;
        }
        zero_ahead = byte == 0x00;
    }
    data.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn test_encode_user_key_no_zero() {
        let user_key = b"testkey";
        let mut encoded = BytesMut::new();
        assert!(encode_user_key(user_key, &mut encoded).is_ok());

        let expected_encoded = b"testkey\x00\x00";
        assert_eq!(encoded.as_ref(), expected_encoded);
    }

    #[test]
    fn test_encode_user_key_with_zero() {
        let user_key = b"test\x00key";
        let mut encoded = BytesMut::new();
        assert!(encode_user_key(user_key, &mut encoded).is_ok());

        let expected_encoded = b"test\x00\x01key\x00\x00";
        assert_eq!(encoded.as_ref(), expected_encoded);
    }

    #[test]
    fn test_decode_user_key_bytesmut() {
        let encoded = b"test\x00\x01key\x00\x00";
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);

        assert!(result.is_ok());
        let expected_user_key = b"test\x00key";
        assert_eq!(user_key.as_ref(), expected_user_key);
    }

    #[test]
    fn test_decode_user_key_no_zero_bytesmut() {
        let encoded = b"testkey\x00\x00";
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);

        assert!(result.is_ok());
        let expected_user_key = b"testkey";
        assert_eq!(user_key.as_ref(), expected_user_key);
    }

    #[test]
    fn test_decode_user_key_empty_bytesmut() {
        let encoded = b"\x00\x00";
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);

        assert!(result.is_ok());
        let expected_user_key = b"";
        assert_eq!(user_key.as_ref(), expected_user_key);
    }

    #[test]
    fn test_decode_user_key_only_zero_bytesmut() {
        let encoded = b"\x00\x01\x00\x00";
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);

        assert!(result.is_ok());
        let expected_user_key = b"\x00";
        assert_eq!(user_key.as_ref(), expected_user_key);
    }

    #[test]
    fn test_decode_user_key_invalid_format_short_bytesmut() {
        let encoded = b"\x00"; // Too short
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), Error::InvalidFormat { .. }));
    }

    #[test]
    fn test_decode_user_key_invalid_format_no_delim_bytesmut() {
        let encoded = b"testkey"; // No delimiter
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), Error::InvalidFormat { .. }));
    }

    #[test]
    fn test_decode_user_key_invalid_format_single_zero_end_bytesmut() {
        let encoded = b"testkey\x00"; // Ends with single zero
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), Error::InvalidFormat { .. }));
    }

    #[test]
    fn test_decode_user_key_invalid_sequence_bytesmut() {
        let encoded = b"test\x00\x02key\x00\x00"; // Invalid sequence \x00\x02
        let mut user_key = BytesMut::new();
        let result = decode_user_key(encoded, &mut user_key);
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), Error::InvalidFormat { .. }));
    }

    #[test]
    fn test_encode_and_decode_user_key_bytesmut() {
        let original_user_key = b"example\x00key\x00value";
        let mut encoded = BytesMut::new();
        encode_user_key(original_user_key, &mut encoded).expect("Encoding failed");

        let mut decoded_user_key = BytesMut::new();
        decode_user_key(&encoded, &mut decoded_user_key).expect("Decoding failed");

        assert_eq!(decoded_user_key.as_ref(), original_user_key);
    }
}
