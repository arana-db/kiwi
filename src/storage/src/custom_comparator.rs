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

use std::{cmp::Ordering, ffi::CString};

use crate::{
    coding::decode_fixed,
    storage_define::{PREFIX_RESERVE_LENGTH, VERSION_LENGTH, seek_userkey_delim},
};

pub fn lists_data_key_comparator_name() -> CString {
    CString::new("floyd.ListsDataKeyComparator").unwrap()
}

pub fn zsets_score_key_comparator_name() -> CString {
    CString::new("floyd.ZSetsScoreKeyComparator").unwrap()
}

/// ## ListsDataKey format
/// ```text
/// | reserve1 | key | version | index | reserve2 |
/// |    8B    |     |    8B   |  8B   |   16B    |
/// ```
///
/// ## Order
/// - Compare by `key`
/// - If equal, compare `version` (numeric asc)
/// - If equal, compare `index` (numeric asc)
#[inline(always)]
pub fn lists_data_key_compare(a: &[u8], b: &[u8]) -> Ordering {
    assert!(!a.is_empty() && !b.is_empty());

    let a_size = a.len();
    let b_size = b.len();

    if a_size <= PREFIX_RESERVE_LENGTH || b_size <= PREFIX_RESERVE_LENGTH {
        return a.cmp(b);
    }

    let a_ptr = &a[PREFIX_RESERVE_LENGTH..];
    let b_ptr = &b[PREFIX_RESERVE_LENGTH..];

    let a_userkey_end = seek_userkey_delim(a_ptr);
    let b_userkey_end = seek_userkey_delim(b_ptr);

    let a_prefix = &a[..PREFIX_RESERVE_LENGTH + a_userkey_end];
    let b_prefix = &b[..PREFIX_RESERVE_LENGTH + b_userkey_end];

    match a_prefix.cmp(b_prefix) {
        Ordering::Equal => {}
        other => return other,
    }

    if PREFIX_RESERVE_LENGTH + a_userkey_end == a_size
        && PREFIX_RESERVE_LENGTH + b_userkey_end == b_size
    {
        return Ordering::Equal;
    } else if PREFIX_RESERVE_LENGTH + a_userkey_end == a_size {
        return Ordering::Less;
    } else if PREFIX_RESERVE_LENGTH + b_userkey_end == b_size {
        return Ordering::Greater;
    }

    let a_version_start = PREFIX_RESERVE_LENGTH + a_userkey_end;
    let b_version_start = PREFIX_RESERVE_LENGTH + b_userkey_end;

    if a_version_start + VERSION_LENGTH > a_size || b_version_start + VERSION_LENGTH > b_size {
        return a.cmp(b);
    }

    let version_a = decode_fixed::<u64>(&a[a_version_start..]);
    let version_b = decode_fixed::<u64>(&b[b_version_start..]);

    match version_a.cmp(&version_b) {
        Ordering::Equal => {}
        other => return other,
    }

    let a_index_start = a_version_start + VERSION_LENGTH;
    let b_index_start = b_version_start + VERSION_LENGTH;

    if a_index_start + VERSION_LENGTH > a_size || b_index_start + VERSION_LENGTH > b_size {
        return a.cmp(b);
    }

    let index_a = decode_fixed::<u64>(&a[a_index_start..]);
    let index_b = decode_fixed::<u64>(&b[b_index_start..]);
    index_a.cmp(&index_b)
}

/// ## ZSetsScoreKey format
/// ```text
/// | reserve1 | key | version |  score  |  member  | reserve2 |
/// |   8B     | ... |   8B    |   8B    |   ...    |   16B    |
/// ```
///
/// ## Order
/// - Compare by `key`
/// - If equal, compare `version` (numeric asc)
/// - If equal, compare `score` (numeric asc, f64)
/// - If equal, compare `member` (bytewise asc)
///
/// ## Notes
/// - `version` and `score` are little-endian; custom comparator enforces numeric order.
#[inline(always)]
pub fn zsets_score_key_compare(a: &[u8], b: &[u8]) -> Ordering {
    assert!(a.len() > PREFIX_RESERVE_LENGTH);
    assert!(b.len() > PREFIX_RESERVE_LENGTH);

    let a_size = a.len();
    let b_size = b.len();

    // skip prefix reserve
    let a_ptr = &a[PREFIX_RESERVE_LENGTH..];
    let b_ptr = &b[PREFIX_RESERVE_LENGTH..];

    // seek userkey delim
    let a_userkey_end = seek_userkey_delim(a_ptr);
    let b_userkey_end = seek_userkey_delim(b_ptr);

    // compare userkey prefix
    let a_prefix = &a[PREFIX_RESERVE_LENGTH..PREFIX_RESERVE_LENGTH + a_userkey_end];
    let b_prefix = &b[PREFIX_RESERVE_LENGTH..PREFIX_RESERVE_LENGTH + b_userkey_end];

    match a_prefix.cmp(b_prefix) {
        Ordering::Equal => {}
        other => return other,
    }

    // compare version
    let a_version_start = PREFIX_RESERVE_LENGTH + a_userkey_end;
    let b_version_start = PREFIX_RESERVE_LENGTH + b_userkey_end;

    if a_version_start + VERSION_LENGTH > a_size || b_version_start + VERSION_LENGTH > b_size {
        return a.cmp(b);
    }

    let version_a = decode_fixed::<u64>(&a[a_version_start..]);
    let version_b = decode_fixed::<u64>(&b[b_version_start..]);

    match version_a.cmp(&version_b) {
        Ordering::Equal => {}
        other => return other,
    }

    // compare score
    let a_score_start = a_version_start + VERSION_LENGTH;
    let b_score_start = b_version_start + VERSION_LENGTH;

    if a_score_start + VERSION_LENGTH > a_size || b_score_start + VERSION_LENGTH > b_size {
        return a.cmp(b);
    }

    let score_a = f64::from_bits(decode_fixed::<u64>(&a[a_score_start..]));
    let score_b = f64::from_bits(decode_fixed::<u64>(&b[b_score_start..]));

    match score_a.partial_cmp(&score_b) {
        Some(ordering) => {
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        None => {
            // handle NaN consistently: define NaN > non-NaN, and double-NaN treated as equal score
            if score_a.is_nan() && !score_b.is_nan() {
                return Ordering::Greater;
            } else if !score_a.is_nan() && score_b.is_nan() {
                return Ordering::Less;
            }
            // both NaN: fall through to compare member (do not return Equal here)
        }
    }

    // compare rest (member and reserve)
    let a_rest_start = a_score_start + VERSION_LENGTH;
    let b_rest_start = b_score_start + VERSION_LENGTH;

    let a_rest = &a[a_rest_start..];
    let b_rest = &b[b_rest_start..];

    a_rest.cmp(b_rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lists_data_key_format::ListsDataKey;

    #[test]
    fn lists_compare_prefix_then_version_then_index() {
        // same key, version diff
        let a = ListsDataKey::new(b"kiwi", 1, 10).encode().unwrap();
        let b = ListsDataKey::new(b"kiwi", 2, 10).encode().unwrap();
        assert_eq!(lists_data_key_compare(&a, &b), Ordering::Less);

        // same key + version, index diff
        let a = ListsDataKey::new(b"kiwi", 2, 5).encode().unwrap();
        let b = ListsDataKey::new(b"kiwi", 2, 6).encode().unwrap();
        assert_eq!(lists_data_key_compare(&a, &b), Ordering::Less);

        // prefix diff
        let a = ListsDataKey::new(b"kiwi", 2, 5).encode().unwrap();
        let b = ListsDataKey::new(b"kiwi2", 2, 5).encode().unwrap();
        assert!(matches!(
            lists_data_key_compare(&a, &b),
            Ordering::Less | Ordering::Greater
        ));
    }
}
