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

/// Returns the comparator name for ListsDataKey.
///
/// This name is registered with RocksDB and must remain consistent
/// to ensure database compatibility.
pub fn lists_data_key_comparator_name() -> CString {
    CString::new("floyd.ListsDataKeyComparator").unwrap()
}

/// Returns the comparator name for ZSetsScoreKey.
///
/// This name is registered with RocksDB and must remain consistent
/// to ensure database compatibility.
pub fn zsets_score_key_comparator_name() -> CString {
    CString::new("floyd.ZSetsScoreKeyComparator").unwrap()
}

/// Custom comparator for ListsDataKey.
///
/// ## ListsDataKey format
/// ```text
/// | reserve1 | key | version | index | reserve2 |
/// |    8B    |     |    8B   |  8B   |   16B    |
/// ```
///
/// ## Ordering Logic
///
/// Keys are compared in the following order:
/// 1. **User key** (bytewise comparison, including the encoded delimiter)
/// 2. **Version** (numeric ascending, little-endian u64)
/// 3. **Index** (numeric ascending, little-endian u64)
///
/// ## Purpose
///
/// This ensures that list elements for the same key and version are
/// ordered by their index, enabling efficient range scans for list operations.
///
/// ## Safety
///
/// This function asserts that both input slices are non-empty.
/// Passing empty slices will cause a panic.
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

/// Custom comparator for ZSetsScoreKey.
///
/// ## ZSetsScoreKey format
/// ```text
/// | reserve1 | key | version |  score  |  member  | reserve2 |
/// |   8B     | ... |   8B    |   8B    |   ...    |   16B    |
/// ```
///
/// ## Ordering Logic
///
/// Keys are compared in the following order:
/// 1. **User key** (bytewise comparison, including the encoded delimiter)
/// 2. **Version** (numeric ascending, little-endian u64)
/// 3. **Score** (numeric ascending, f64 with special NaN handling)
/// 4. **Member** (bytewise comparison) and **reserve2**
///
/// ## Special Score Handling
///
/// - **NaN values**: Defined as greater than all non-NaN values (including infinity)
///   - When both scores are NaN, fall through to compare by member
/// - **Positive/Negative Zero**: `-0.0` and `+0.0` are treated as equal
/// - **Infinity**: `NEG_INFINITY < finite values < INFINITY < NaN`
///
/// ## Purpose
///
/// This comparator ensures that:
/// - Members in a sorted set are ordered by score (ascending)
/// - Members with the same score are ordered lexicographically
/// - Different versions of the same key are kept separate
///
/// ## Notes
///
/// - Both `version` and `score` are stored in little-endian format
/// - The score is stored as the raw u64 bits of an IEEE 754 double
/// - Custom comparator enforces numeric ordering despite little-endian storage
///
/// ## Safety
///
/// This function asserts that both input slices are longer than `PREFIX_RESERVE_LENGTH`.
/// Violating this will cause a panic.
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
    use crate::zset_score_key_format::ZSetsScoreKey;

    // ========== ListsDataKey 测试 ==========

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

    // ========== ZSetsScoreKey 基础排序测试 ==========

    #[test]
    fn test_zsets_score_key_compare_by_key() {
        // 不同 key，其他相同
        let a = ZSetsScoreKey::new(b"key1", 1, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key2", 1, 1.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_zsets_score_key_compare_by_version() {
        // 相同 key，不同 version
        let a = ZSetsScoreKey::new(b"key", 1, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 2, 1.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        let a = ZSetsScoreKey::new(b"key", 10, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 5, 1.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Greater);
    }

    #[test]
    fn test_zsets_score_key_compare_by_score() {
        // 相同 key 和 version，不同 score
        let a = ZSetsScoreKey::new(b"key", 1, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, 2.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        // 负数 score
        let a = ZSetsScoreKey::new(b"key", 1, -5.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, -2.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        // 正负 score
        let a = ZSetsScoreKey::new(b"key", 1, -1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, 1.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_zsets_score_key_compare_by_member() {
        // 相同 key、version、score，不同 member
        let a = ZSetsScoreKey::new(b"key", 1, 1.0, b"alice").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, 1.0, b"bob").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        let a = ZSetsScoreKey::new(b"key", 1, 1.0, b"zebra").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, 1.0, b"apple").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Greater);
    }

    #[test]
    fn test_zsets_score_key_compare_equal() {
        // 完全相同
        let a = ZSetsScoreKey::new(b"key", 1, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, 1.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Equal);
    }

    // ========== 特殊值排序测试 ==========

    #[test]
    fn test_zsets_score_key_compare_infinity() {
        // 正无穷 vs 有限值
        let a = ZSetsScoreKey::new(b"key", 1, 100.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::INFINITY, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        // 负无穷 vs 有限值
        let a = ZSetsScoreKey::new(b"key", 1, f64::NEG_INFINITY, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, -100.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        // 正无穷 vs 负无穷
        let a = ZSetsScoreKey::new(b"key", 1, f64::NEG_INFINITY, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::INFINITY, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        // 两个正无穷，比较 member
        let a = ZSetsScoreKey::new(b"key", 1, f64::INFINITY, b"alice").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::INFINITY, b"bob").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_zsets_score_key_compare_positive_negative_zero() {
        // 0.0 vs -0.0 应该相等（数值上）
        let a = ZSetsScoreKey::new(b"key", 1, 0.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, -0.0, b"member").encode().unwrap();
        
        // 注意：0.0 和 -0.0 的 to_bits() 不同，所以编码不同
        // 但在比较时，f64::partial_cmp 会认为它们相等
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Equal);
    }

    // ========== NaN 处理测试 ==========

    #[test]
    fn test_zsets_score_key_compare_nan_vs_number() {
        // NaN > 非NaN
        let a = ZSetsScoreKey::new(b"key", 1, 100.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        let a = ZSetsScoreKey::new(b"key", 1, -100.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_zsets_score_key_compare_nan_vs_infinity() {
        // NaN > 正无穷
        let a = ZSetsScoreKey::new(b"key", 1, f64::INFINITY, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        // NaN > 负无穷
        let a = ZSetsScoreKey::new(b"key", 1, f64::NEG_INFINITY, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_zsets_score_key_compare_nan_vs_nan() {
        // 两个 NaN，比较 member
        let a = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"alice").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"bob").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        let a = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"zebra").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"apple").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Greater);

        // 相同 NaN 和 member
        let a = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, f64::NAN, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Equal);
    }

    // ========== 综合排序测试 ==========

    #[test]
    fn test_zsets_score_key_compare_sorting_order() {
        // 模拟多个 score 的排序
        let mut keys = vec![
            ZSetsScoreKey::new(b"zset", 1, 3.0, b"c").encode().unwrap(),
            ZSetsScoreKey::new(b"zset", 1, 1.0, b"a").encode().unwrap(),
            ZSetsScoreKey::new(b"zset", 1, 2.0, b"b").encode().unwrap(),
            ZSetsScoreKey::new(b"zset", 1, 1.0, b"z").encode().unwrap(), // 相同 score，不同 member
            ZSetsScoreKey::new(b"zset", 1, f64::NEG_INFINITY, b"neg_inf").encode().unwrap(),
            ZSetsScoreKey::new(b"zset", 1, f64::INFINITY, b"pos_inf").encode().unwrap(),
            ZSetsScoreKey::new(b"zset", 1, f64::NAN, b"nan").encode().unwrap(),
        ];

        keys.sort_by(|a, b| zsets_score_key_compare(a, b));

        // 验证排序顺序：-inf < 1.0 < 2.0 < 3.0 < +inf < NaN
        // 对于相同 score，按 member 字典序
        let parsed_keys: Vec<_> = keys.iter()
            .map(|k| crate::zset_score_key_format::ParsedZSetsScoreKey::new(k).unwrap())
            .collect();

        assert!(parsed_keys[0].score().is_infinite() && parsed_keys[0].score().is_sign_negative());
        assert_eq!(parsed_keys[1].score(), 1.0);
        assert_eq!(parsed_keys[1].member(), b"a"); // member "a" < "z"
        assert_eq!(parsed_keys[2].score(), 1.0);
        assert_eq!(parsed_keys[2].member(), b"z");
        assert_eq!(parsed_keys[3].score(), 2.0);
        assert_eq!(parsed_keys[4].score(), 3.0);
        assert!(parsed_keys[5].score().is_infinite() && parsed_keys[5].score().is_sign_positive());
        assert!(parsed_keys[6].score().is_nan());
    }

    #[test]
    fn test_zsets_score_key_compare_different_keys() {
        // 不同 key 的排序
        let a = ZSetsScoreKey::new(b"aaa", 1, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"bbb", 1, 1.0, b"member").encode().unwrap();
        let c = ZSetsScoreKey::new(b"ccc", 1, 1.0, b"member").encode().unwrap();

        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
        assert_eq!(zsets_score_key_compare(&b, &c), Ordering::Less);
        assert_eq!(zsets_score_key_compare(&a, &c), Ordering::Less);
    }

    #[test]
    fn test_zsets_score_key_compare_version_priority() {
        // version 的优先级高于 score
        let a = ZSetsScoreKey::new(b"key", 1, 100.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 2, 1.0, b"member").encode().unwrap();
        
        // 即使 a 的 score 更大，但 version 更小，所以 a < b
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_zsets_score_key_compare_empty_members() {
        // 空 member
        let a = ZSetsScoreKey::new(b"key", 1, 1.0, b"").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, 1.0, b"a").encode().unwrap();
        
        // 空 member < 非空member
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        // 两个空 member
        let a = ZSetsScoreKey::new(b"key", 1, 1.0, b"").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", 1, 1.0, b"").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Equal);
    }

    #[test]
    fn test_zsets_score_key_compare_boundary_versions() {
        // 边界 version 值
        let a = ZSetsScoreKey::new(b"key", 0, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", u64::MAX, 1.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);

        let a = ZSetsScoreKey::new(b"key", u64::MAX - 1, 1.0, b"member").encode().unwrap();
        let b = ZSetsScoreKey::new(b"key", u64::MAX, 1.0, b"member").encode().unwrap();
        
        assert_eq!(zsets_score_key_compare(&a, &b), Ordering::Less);
    }
}
