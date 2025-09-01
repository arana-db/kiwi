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

/// TODO: remove allow dead code
#[allow(dead_code)]
pub trait FixedInt: Copy {
    /// convert self to little-endian bytes
    fn to_le_bytes(self) -> Vec<u8>;
    /// create from little-endian bytes
    fn from_le_bytes(bytes: &[u8]) -> Self;
    /// size of self in bytes
    fn byte_size() -> usize {
        size_of::<Self>()
    }
}

macro_rules! impl_fixed_int_32 {
    ($($t:ty),*) => {
        $(
            impl FixedInt for $t {
                fn to_le_bytes(self) -> Vec<u8> {
                    (self as u32).to_le_bytes().to_vec()
                }

                fn from_le_bytes(bytes: &[u8]) -> Self {
                    let arr: [u8; 4] = bytes.try_into().unwrap();
                    u32::from_le_bytes(arr) as Self
                }
            }
        )*
    }
}

macro_rules! impl_fixed_int_64 {
    ($($t:ty),*) => {
        $(
            impl FixedInt for $t {
                fn to_le_bytes(self) -> Vec<u8> {
                    (self as u64).to_le_bytes().to_vec()
                }

                fn from_le_bytes(bytes: &[u8]) -> Self {
                    let arr: [u8; 8] = bytes.try_into().unwrap();
                    u64::from_le_bytes(arr) as Self
                }
            }
        )*
    }
}

impl_fixed_int_32!(i32, u32);
impl_fixed_int_64!(i64, u64);

/// encode a fixed-width int into a byte buffer
/// TODO: remove allow dead code
#[allow(dead_code)]
#[inline]
pub fn encode_fixed<T: FixedInt>(buf: &mut [u8], value: T) {
    let size = T::byte_size();
    assert!(buf.len() >= size, "buffer too small for fixed int");
    buf[..size].copy_from_slice(&value.to_le_bytes());
}

/// decode a fixed-width int from a byte buffer
/// TODO: remove allow dead code
#[allow(dead_code)]
#[inline]
pub fn decode_fixed<T: FixedInt>(buf: &[u8]) -> T {
    let size = T::byte_size();
    assert!(buf.len() >= size, "buffer too small for fixed int");
    T::from_le_bytes(&buf[..size])
}

#[cfg(test)]
mod tests {

    use crate::coding::{decode_fixed, encode_fixed};

    #[test]
    fn test_encode_decode_u32_fixed() {
        let mut buf = [0u8; 4];
        let original = 0x12345678u32;

        encode_fixed(&mut buf, original);
        let decoded = decode_fixed(&buf);

        assert_eq!(
            original, decoded,
            "Value should be the same after encode/decode cycle"
        );
    }

    #[test]
    fn test_encode_decode_u64_fixed() {
        let mut buf = [0u8; 8];
        let original = 0x1234567890ABCDEFu64;

        encode_fixed(&mut buf, original);
        let decoded = decode_fixed(&buf);

        assert_eq!(
            original, decoded,
            "Value should be the same after encode/decode cycle"
        );
    }

    #[test]
    fn test_edge_cases_fixed32() {
        let mut buf = [0u8; 4];

        encode_fixed(&mut buf, 0);
        assert_eq!(
            decode_fixed::<u32>(&buf),
            0,
            "Minimum u32 value should encode/decode correctly"
        );

        encode_fixed(&mut buf, u32::MAX);
        assert_eq!(
            decode_fixed::<u32>(&buf),
            u32::MAX,
            "Maximum u32 value should encode/decode correctly"
        );

        let powers = [1u32, 2, 4, 16, 256, 65536, 16777216];
        for &power in &powers {
            encode_fixed(&mut buf, power);
            assert_eq!(
                decode_fixed::<u32>(&buf),
                power,
                "Power of 2 value {} should encode/decode correctly",
                power
            );
        }

        encode_fixed::<u32>(&mut buf, 0xAAAAAAAA);
        assert_eq!(
            decode_fixed::<u32>(&buf),
            0xAAAAAAAA,
            "Alternating bits should encode/decode correctly"
        );

        encode_fixed(&mut buf, 0x55555555);
        assert_eq!(
            decode_fixed::<u32>(&buf),
            0x55555555,
            "Alternating bits should encode/decode correctly"
        );
    }

    #[test]
    fn test_edge_cases_fixed64() {
        let mut buf = [0u8; 8];

        encode_fixed(&mut buf, 0);
        assert_eq!(
            decode_fixed::<u64>(&buf),
            0,
            "Minimum u64 value should encode/decode correctly"
        );

        encode_fixed(&mut buf, u64::MAX);
        assert_eq!(
            decode_fixed::<u64>(&buf),
            u64::MAX,
            "Maximum u64 value should encode/decode correctly"
        );

        let powers = [
            1u64,
            2,
            4,
            16,
            256,
            65536,
            1u64 << 32,
            1u64 << 48,
            1u64 << 63,
        ];
        for &power in &powers {
            encode_fixed(&mut buf, power);
            assert_eq!(
                decode_fixed::<u64>(&buf),
                power,
                "Power of 2 value {} should encode/decode correctly",
                power
            );
        }

        encode_fixed::<u64>(&mut buf, 0xAAAAAAAAAAAAAAAA);
        assert_eq!(
            decode_fixed::<u64>(&buf),
            0xAAAAAAAAAAAAAAAA,
            "Alternating bits should encode/decode correctly"
        );

        encode_fixed::<u64>(&mut buf, 0x5555555555555555);
        assert_eq!(
            decode_fixed::<u64>(&buf),
            0x5555555555555555,
            "Alternating bits should encode/decode correctly"
        );

        let patterns = [
            0x00000000FFFFFFFF, // Upper half all zeros, lower half all ones
            0xFFFFFFFF00000000, // Upper half all ones, lower half all zeros
            0x00FF00FF00FF00FF, // Alternating bytes of zeros and ones
            0xFF00FF00FF00FF00, // Alternating bytes of ones and zeros
        ];

        for &pattern in &patterns {
            encode_fixed(&mut buf, pattern);
            assert_eq!(
                decode_fixed::<u64>(&buf),
                pattern,
                "Byte pattern {:X} should encode/decode correctly",
                pattern
            );
        }
    }

    #[test]
    fn test_byte_order_fixed32() {
        let value = 0x01020304u32;
        let mut buf = [0u8; 4];

        encode_fixed(&mut buf, value);

        if cfg!(target_endian = "little") {
            assert_eq!(buf[0], 0x04);
            assert_eq!(buf[1], 0x03);
            assert_eq!(buf[2], 0x02);
            assert_eq!(buf[3], 0x01);
        } else {
            assert_eq!(buf[0], 0x01);
            assert_eq!(buf[1], 0x02);
            assert_eq!(buf[2], 0x03);
            assert_eq!(buf[3], 0x04);
        }

        assert_eq!(decode_fixed::<u32>(&buf), value);
    }

    #[test]
    fn test_byte_order_fixed64() {
        let value = 0x0102030405060708u64;
        let mut buf = [0u8; 8];

        encode_fixed(&mut buf, value);

        if cfg!(target_endian = "little") {
            assert_eq!(buf[0], 0x08);
            assert_eq!(buf[1], 0x07);
            assert_eq!(buf[2], 0x06);
            assert_eq!(buf[3], 0x05);
            assert_eq!(buf[4], 0x04);
            assert_eq!(buf[5], 0x03);
            assert_eq!(buf[6], 0x02);
            assert_eq!(buf[7], 0x01);
        } else {
            assert_eq!(buf[0], 0x01);
            assert_eq!(buf[1], 0x02);
            assert_eq!(buf[2], 0x03);
            assert_eq!(buf[3], 0x04);
            assert_eq!(buf[4], 0x05);
            assert_eq!(buf[5], 0x06);
            assert_eq!(buf[6], 0x07);
            assert_eq!(buf[7], 0x08);
        }

        assert_eq!(decode_fixed::<u64>(&buf), value);
    }

    #[test]
    fn test_round_trip_random_values_fixed32() {
        let mut buf = [0u8; 4];

        let values = [
            0x12345678, 0xFEDCBA98, 0x01234567, 0x89ABCDEF, 0x55AA55AA, 0xA55AA55A, 0xFFFFFFFF,
            0x00000000, 0x80000000, 0x7FFFFFFF,
        ];

        for &value in &values {
            encode_fixed(&mut buf, value);
            let decoded = decode_fixed::<u32>(&buf);
            assert_eq!(value, decoded, "Round trip of 0x{:X} failed", value);
        }
    }

    #[test]
    fn test_round_trip_random_values_fixed64() {
        let mut buf = [0u8; 8];

        let values = [
            0x0123456789ABCDEF,
            0xFEDCBA9876543210,
            0x0102030405060708,
            0xF0E0D0C0B0A09080,
            0x55AA55AA55AA55AA,
            0xA55AA55AA55AA55A,
            0xFFFFFFFFFFFFFFFF,
            0x0000000000000000,
            0x8000000000000000,
            0x7FFFFFFFFFFFFFFF,
        ];

        for &value in &values {
            encode_fixed::<u64>(&mut buf, value);
            let decoded = decode_fixed(&buf);
            assert_eq!(value, decoded, "Round trip of 0x{:X} failed", value);
        }
    }
}
