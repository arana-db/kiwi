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

//! Encoding and decoding utilities for storage engine

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use std::mem;

pub trait Encoder {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decoder: Sized {
    fn decode(bytes: &[u8]) -> Option<Self>;
}

pub fn encode_fixed_64(dst: &mut [u8], value: u64) {
    LittleEndian::write_u64(dst, value);
}

pub fn encode_fixed_32(dst: &mut [u8], value: u32) {
    LittleEndian::write_u32(dst, value);
}

pub fn encode_fixed_16(dst: &mut [u8], value: u16) {
    LittleEndian::write_u16(dst, value);
}

pub fn decode_fixed_64(ptr: &[u8]) -> u64 {
    LittleEndian::read_u64(ptr)
}

pub fn decode_fixed_32(ptr: &[u8]) -> u32 {
    LittleEndian::read_u32(ptr)
}

pub fn decode_fixed_16(ptr: &[u8]) -> u16 {
    LittleEndian::read_u16(ptr)
}

pub fn put_fixed_64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(&value.to_le_bytes());
}

pub fn put_fixed_32(dst: &mut Vec<u8>, value: u32) {
    dst.extend_from_slice(&value.to_le_bytes());
}

pub fn put_fixed_16(dst: &mut Vec<u8>, value: u16) {
    dst.extend_from_slice(&value.to_le_bytes());
}

pub fn encode_var_64(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 128 {
        dst.push((v as u8) | 128);
        v >>= 7;
    }
    dst.push(v as u8);
}

pub fn encode_var_32(dst: &mut Vec<u8>, mut v: u32) {
    while v >= 128 {
        dst.push((v as u8) | 128);
        v >>= 7;
    }
    dst.push(v as u8);
}

pub fn get_var_int_len(v: u64) -> usize {
    let mut len = 1;
    let mut val = v;
    while val >= 128 {
        len += 1;
        val >>= 7;
    }
    len
}

pub fn get_length_prefixed_slice(input: &[u8]) -> Option<(&[u8], &[u8])> {
    if input.is_empty() {
        return None;
    }
    let (val, offset) = get_varint_32(input)?;
    let val = val as usize;
    if offset + val > input.len() {
        return None;
    }
    Some((&input[offset..offset + val], &input[offset + val..]))
}

pub fn get_varint_32(input: &[u8]) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    let mut offset = 0;

    loop {
        if offset >= input.len() {
            return None;
        }
        let byte = input[offset];
        offset += 1;
        result |= ((byte & 127) as u32) << shift;
        if byte & 128 == 0 {
            break;
        }
        shift += 7;
        if shift >= 32 {
            return None;
        }
    }

    Some((result, offset))
}

pub fn get_varint_64(input: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    let mut offset = 0;

    loop {
        if offset >= input.len() {
            return None;
        }
        let byte = input[offset];
        offset += 1;
        result |= ((byte & 127) as u64) << shift;
        if byte & 128 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }

    Some((result, offset))
}
