//! Integration tests for ZSet score key format and comparator
//!
//! These tests verify the behavior of ZSet score keys in realistic scenarios,
//! using the custom comparator to ensure correct sorting behavior.
//!
//! # Test Categories
//!
//! - Format Versioning: Verify format version markers (BE vs LE)
//! - Custom Comparator: Verify sort order using the custom comparator
//! - Range Queries: Simulate ZRANGE operations with proper comparator logic
//! - Edge Cases: Test boundary conditions and special values

use storage::zset_score_key_format::{ZSetsScoreKey, ParsedZSetsScoreKey};
use storage::custom_comparator::zsets_score_key_compare;
use std::cmp::Ordering;

/// Verify that encoded keys using custom comparator sort correctly
#[test]
fn test_score_sorting_with_comparator() {
    let key = b\"myzset\";
    let version = 1u64;

    // Create keys with different scores
    let keys: Vec<(f64, &[u8])> = vec![
        (f64::NEG_INFINITY, b\"min\"),
        (-100.0, b\"neg100\"),
        (-1.0, b\"neg1\"),
        (0.0, b\"zero\"),
        (1.0, b\"one\"),
        (100.0, b\"pos100\"),
        (f64::INFINITY, b\"max\"),
    ];

    let mut encoded_keys: Vec<_> = keys
        .iter()
        .map(|(score, member)| {
            ZSetsScoreKey::new(key, version, *score, member)
                .encode()
                .expect(\"encode failed\")
        })
        .collect();

    // Verify that the keys are in sorted order using the comparator
    for i in 0..encoded_keys.len() - 1 {
        assert_eq!(
            zsets_score_key_compare(&encoded_keys[i], &encoded_keys[i + 1]),
            Ordering::Less,
            \"Key at index {} should be less than key at index {}\",
            i,
            i + 1
        );
    }
}

/// Verify format version marker is set correctly
#[test]
fn test_format_version_marker() {
    use storage::zset_score_key_format::{FORMAT_VERSION_LE, is_little_endian_format};

    let key = b\"test\";
    let score_key = ZSetsScoreKey::new(key, 1, 1.0, b\"member\");
    let encoded = score_key.encode().expect(\"encode failed\");

    // Parse the encoded key
    let parsed = ParsedZSetsScoreKey::new(&encoded).expect(\"parse failed\");

    // Verify format version marker in reserve1[0]
    assert_eq!(
        parsed.reserve1[0],
        FORMAT_VERSION_LE,
        \"reserve1[0] should be marked as LE format\"
    );

    // Verify is_little_endian_format detects it correctly
    assert!(
        is_little_endian_format(&parsed.reserve1),
        \"Key should be detected as LE format\"
    );
}

/// Test range query behavior with negative scores
#[test]
fn test_range_query_negative_scores() {
    let key = b\"scores\";
    let version = 1u64;

    // Create range boundaries
    let range_start = ZSetsScoreKey::new(key, version, -10.0, b\"\")
        .encode_seek_key()
        .expect(\"encode failed\");
    let range_end = ZSetsScoreKey::new(key, version, 10.0, b\"\")
        .encode_seek_key()
        .expect(\"encode failed\");

    // Create test items within and outside range
    let items = vec![
        (-20.0, b\"outside_low\"),
        (-10.0, b\"boundary_low\"),
        (-5.0, b\"inside\"),
        (0.0, b\"center\"),
        (5.0, b\"inside\"),
        (10.0, b\"boundary_high\"),
        (20.0, b\"outside_high\"),
    ];

    let encoded_items: Vec<_> = items
        .iter()
        .map(|(score, member)| {
            ZSetsScoreKey::new(key, version, *score, member)
                .encode()
                .expect(\"encode failed\")
        })
        .collect();

    // Count items that fall within range using comparator
    let in_range = encoded_items
        .iter()
        .filter(|item| {
            zsets_score_key_compare(item, &range_start) != Ordering::Less
                && zsets_score_key_compare(item, &range_end) != Ordering::Greater
        })
        .count();

    // Should include items at indices 1, 2, 3, 4, 5 (5 items total)
    assert_eq!(
        in_range, 5,
        \"Range query should include 5 items between -10.0 and 10.0\"
    );
}

/// Test handling of NaN in comparisons (should sort after all finite and infinite values)
#[test]
fn test_nan_sorting_position() {
    let key = b\"test\";
    let version = 1u64;

    // Create keys with various special values
    let special_values = vec![
        f64::NEG_INFINITY,
        -1.0,
        0.0,
        1.0,
        f64::INFINITY,
        f64::NAN,
    ];

    let mut encoded_keys: Vec<_> = special_values
        .iter()
        .map(|score| {
            ZSetsScoreKey::new(key, version, *score, b\"member\")
                .encode()
                .expect(\"encode failed\")
        })
        .collect();

    // Verify order: all non-NaN values come before NaN
    let nan_key = encoded_keys.pop().unwrap(); // Last one is NaN
    for key in &encoded_keys {
        assert_eq!(
            zsets_score_key_compare(key, &nan_key),
            Ordering::Less,
            \"All non-NaN values should sort before NaN\"
        );
    }
}

/// Test backward compatibility: verify that old BE format keys can be detected and migrated
#[test]
fn test_backward_compatibility_detection() {
    use storage::zset_score_key_format::{FORMAT_VERSION_BE, is_big_endian_format, is_little_endian_format};

    // Simulate old format key with BE marker
    let mut old_reserve1 = [0u8; 8];
    old_reserve1[0] = FORMAT_VERSION_BE;

    // Verify detection functions work correctly
    assert!(is_big_endian_format(&old_reserve1), \"Should detect BE format\");
    assert!(!is_little_endian_format(&old_reserve1), \"Should not detect as LE\");

    // New format with LE marker
    let mut new_reserve1 = [0u8; 8];
    new_reserve1[0] = storage::zset_score_key_format::FORMAT_VERSION_LE;

    assert!(!is_big_endian_format(&new_reserve1), \"Should not detect as BE\");
    assert!(is_little_endian_format(&new_reserve1), \"Should detect LE format\");
}

/// Test seek key usage in range queries
#[test]
fn test_seek_key_for_range_boundaries() {
    let key = b\"zset\";
    let version = 1u64;

    // Create full keys and corresponding seek keys
    let score_values = vec![0.5, 1.5, 2.5];
    
    let full_keys: Vec<_> = score_values
        .iter()
        .map(|score| {
            ZSetsScoreKey::new(key, version, *score, b\"member\")
                .encode()
                .expect(\"encode failed\")
        })
        .collect();

    let seek_keys: Vec<_> = score_values
        .iter()
        .map(|score| {
            ZSetsScoreKey::new(key, version, *score, b\"\")
                .encode_seek_key()
                .expect(\"encode_seek_key failed\")
        })
        .collect();

    // Seek keys should have same ordering as full keys for the score field
    for i in 0..seek_keys.len() - 1 {
        assert_eq!(
            zsets_score_key_compare(&seek_keys[i], &seek_keys[i + 1]),
            Ordering::Less,
            \"Seek keys should maintain score ordering\"
        );
    }

    // Seek keys should be shorter than full keys (no member or reserve2)
    for (seek, full) in seek_keys.iter().zip(&full_keys) {
        assert!(
            seek.len() < full.len(),
            \"Seek key should be shorter than full key\"
        );
    }
}

/// Test migration scenario: simulate reading keys in mixed formats
#[test]
fn test_mixed_format_handling() {
    // This test documents the migration process:
    // 1. Old BE format keys are read and detected
    // 2. New LE format keys are identified as already migrated
    // 3. Both types coexist during migration

    let key = b\"data\";
    let version = 1u64;
    let score = 42.0;
    let member = b\"member\";

    // Create new format key
    let new_format_key = ZSetsScoreKey::new(key, version, score, member)
        .encode()
        .expect(\"encode failed\");

    // Verify it has LE marker
    let parsed = ParsedZSetsScoreKey::new(&new_format_key).expect(\"parse failed\");
    assert!(
        storage::zset_score_key_format::is_little_endian_format(&parsed.reserve1),
        \"New key should have LE format marker\"
    );

    // Simulate old BE format (would be detected and converted in actual migration)
    // This is just to document the format difference
    assert_eq!(
        parsed.version, version,
        \"Version should be correctly decoded in LE format\"
    );
}

/// Test empty key and member edge cases
#[test]
fn test_empty_key_and_member() {
    let score_key = ZSetsScoreKey::new(b\"\", 0, 0.0, b\"\");
    let encoded = score_key.encode().expect(\"encode failed\");

    let parsed = ParsedZSetsScoreKey::new(&encoded).expect(\"parse failed\");
    assert_eq!(parsed.key(), b\"\");
    assert_eq!(parsed.member(), b\"\");
    assert_eq!(parsed.version(), 0);
    assert_eq!(parsed.score(), 0.0);
}

/// Test comparator with null bytes in keys and members
#[test]
fn test_null_bytes_in_keys() {
    let key_with_nulls = b\"key\\x00with\\x00nulls\";
    let member_with_nulls = b\"mem\\x00ber\";

    let score_key = ZSetsScoreKey::new(key_with_nulls, 1, 1.0, member_with_nulls);
    let encoded = score_key.encode().expect(\"encode failed\");

    let parsed = ParsedZSetsScoreKey::new(&encoded).expect(\"parse failed\");
    assert_eq!(parsed.key(), key_with_nulls);
    assert_eq!(parsed.member(), member_with_nulls);

    // Verify it can still be compared
    let another_key = ZSetsScoreKey::new(key_with_nulls, 2, 2.0, member_with_nulls)
        .encode()
        .expect(\"encode failed\");

    assert_eq!(
        zsets_score_key_compare(&encoded, &another_key),
        Ordering::Less,
        \"Keys with null bytes should still be comparable\"
    );
}
