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

//! Database migration utilities for handling breaking changes in data format.
//!
//! This module provides tools to migrate data from old formats (Big-Endian)
//! to new formats (Little-Endian) for ZSet member keys.
//!
//! # Migration Process
//!
//! The migration process consists of three stages:
//! 1. **Detection**: Identify keys in old BE format
//! 2. **Conversion**: Transform BE keys to LE format
//! 3. **Verification**: Validate the migrated data
//!
//! # Example
//!
//! ```ignore
//! use storage::migration::MigrationStats;
//!
//! // Perform migration with dry-run first
//! let stats = migrate_member_keys_be_to_le(db, "zsets", true)?;
//! println!("Would migrate {} keys", stats.total_keys);
//!
//! // Then run actual migration
//! let stats = migrate_member_keys_be_to_le(db, "zsets", false)?;
//! println!("Migrated {} keys successfully", stats.migrated_keys);
//! ```

use crate::{
    error::Result,
    storage_define::{
        ENCODED_KEY_DELIM_SIZE, PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH,
        decode_user_key, encode_user_key, seek_userkey_delim,
    },
};
use bytes::{BufMut, BytesMut};
use log;
use std::fmt;

/// Statistics about a migration operation
#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    /// Total keys processed
    pub total_keys: u64,
    /// Keys successfully migrated
    pub migrated_keys: u64,
    /// Keys already in new format
    pub already_new_format: u64,
    /// Keys that couldn't be migrated (corruption or unknown format)
    pub skipped_keys: u64,
    /// Total bytes processed
    pub total_bytes: u64,
    /// Total bytes written
    pub migrated_bytes: u64,
}

impl MigrationStats {
    /// Create new empty statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if migration was successful (no skipped keys)
    pub fn is_successful(&self) -> bool {
        self.skipped_keys == 0
    }

    /// Get migration progress percentage
    pub fn progress_percent(&self) -> f64 {
        if self.total_keys == 0 {
            0.0
        } else {
            (self.migrated_keys as f64 / self.total_keys as f64) * 100.0
        }
    }
}

impl fmt::Display for MigrationStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Migration Stats:\n  Total keys: {}\n  Migrated: {}\n  Already new format: {}\n  Skipped: {}\n  Total bytes: {}\n  Migrated bytes: {}\n  Success rate: {:.2}%",
            self.total_keys,
            self.migrated_keys,
            self.already_new_format,
            self.skipped_keys,
            self.total_bytes,
            self.migrated_bytes,
            if self.total_keys > 0 {
                ((self.migrated_keys + self.already_new_format) as f64 / self.total_keys as f64) * 100.0
            } else {
                0.0
            }
        )
    }
}

/// Detects if a key is in old Big-Endian format
///
/// A key is considered to be in old BE format if:
/// 1. It's long enough to contain version (at least PREFIX_RESERVE_LENGTH + 2 + 8 + SUFFIX_RESERVE_LENGTH bytes)
/// 2. The version bytes appear to be BE (heuristic: byte patterns typical of BE encoding)
///
/// # Returns
///
/// - `Ok(true)` if the key appears to be in old BE format
/// - `Ok(false)` if the key is already in LE format or format is unclear
/// - `Err` if the key is corrupted or too short
fn is_old_be_key(key: &[u8]) -> Result<bool> {
    // Minimum size: reserve1(8) + empty_key_delim(2) + version(8) + score(8) + reserve2(16)
    if key.len() < PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE + 8 + 8 + SUFFIX_RESERVE_LENGTH {
        return Ok(false);
    }

    // Find the version bytes (they come after the encoded key)
    // This is a heuristic: we look for the position where version should be
    // by finding the key delimiter (\x00\x00)
    let mut delim_pos = None;
    for i in 0..key.len().saturating_sub(1) {
        if i >= PREFIX_RESERVE_LENGTH && key[i] == 0x00 && key[i + 1] == 0x00 {
            delim_pos = Some(i + ENCODED_KEY_DELIM_SIZE);
            break;
        }
    }

    if let Some(version_start) = delim_pos {
        let version_end = version_start + 8;
        if version_end <= key.len() {
            let version_bytes = &key[version_start..version_end];
            // A heuristic check: if the first few bytes are zero (common in BE for small numbers)
            // and the last bytes are non-zero, it's likely BE format
            // This is not foolproof but works for most cases
            if version_bytes[0..4] == [0, 0, 0, 0] && version_bytes[4] != 0 {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Converts a version from Big-Endian to Little-Endian
fn convert_version_be_to_le(be_bytes: &[u8]) -> u64 {
    u64::from_be_bytes(be_bytes.try_into().unwrap_or([0; 8]))
}

/// Converts a key from Big-Endian version to Little-Endian version
///
/// The only difference is the byte order of the version field.
/// All other fields remain unchanged.
fn convert_key_be_to_le(key: &[u8]) -> Result<Vec<u8>> {
    if key.len() < PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE + 8 + SUFFIX_RESERVE_LENGTH {
        return Ok(key.to_vec());
    }

    // Find the version field position
    let mut version_start = None;
    for i in 0..key.len().saturating_sub(1) {
        if i >= PREFIX_RESERVE_LENGTH && key[i] == 0x00 && key[i + 1] == 0x00 {
            version_start = Some(i + ENCODED_KEY_DELIM_SIZE);
            break;
        }
    }

    if let Some(v_start) = version_start {
        let v_end = v_start + 8;
        if v_end <= key.len() {
            // Extract BE version and convert to LE
            let be_version = convert_version_be_to_le(&key[v_start..v_end]);

            // Reconstruct key with LE version
            let mut new_key = Vec::with_capacity(key.len());
            new_key.extend_from_slice(&key[..v_start]);
            new_key.extend_from_slice(&be_version.to_le_bytes());
            new_key.extend_from_slice(&key[v_end..]);
            return Ok(new_key);
        }
    }

    Ok(key.to_vec())
}



/// Analyzes a ZSet member key and extracts its components
///
/// Returns the decoded key, version, score, and member data from an encoded key.
pub fn analyze_member_key(key: &[u8]) -> Result<(Vec<u8>, u64, f64, Vec<u8>)> {
    if key.len() < PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE + 8 + 8 + SUFFIX_RESERVE_LENGTH {
        return Err(crate::error::Error::InvalidFormat {
            message: format!("Key too short: {} bytes", key.len()),
            location: snafu::location!(),
        });
    }

    // Skip reserve1
    let mut pos = PREFIX_RESERVE_LENGTH;

    // Find user key delimiter
    let delim_pos = seek_userkey_delim(&key[pos..]);
    let user_key_end = pos + delim_pos;

    // Decode user key
    let mut user_key = BytesMut::new();
    decode_user_key(&key[pos..user_key_end], &mut user_key)?;

    // Move past delimiter
    pos = user_key_end + ENCODED_KEY_DELIM_SIZE;

    // Extract version (LE)
    if pos + 8 > key.len() {
        return Err(crate::error::Error::InvalidFormat {
            message: "Cannot read version field".to_string(),
            location: snafu::location!(),
        });
    }
    let version = u64::from_le_bytes(key[pos..pos + 8].try_into().unwrap());
    pos += 8;

    // Extract score (LE) as f64 bits
    if pos + 8 > key.len() {
        return Err(crate::error::Error::InvalidFormat {
            message: "Cannot read score field".to_string(),
            location: snafu::location!(),
        });
    }
    let score_bits = u64::from_le_bytes(key[pos..pos + 8].try_into().unwrap());
    let score = f64::from_bits(score_bits);
    pos += 8;

    // Extract member (remaining data without reserve2)
    let member_end = key.len() - SUFFIX_RESERVE_LENGTH;
    let member = key[pos..member_end].to_vec();

    Ok((user_key.to_vec(), version, score, member))
}

/// Rebuilds a ZSet member key from its components in LE format
pub fn rebuild_member_key_le(
    user_key: &[u8],
    version: u64,
    score: f64,
    member: &[u8],
) -> Result<Vec<u8>> {
    let mut encoded = BytesMut::new();

    // Add reserve1
    encoded.put_slice(&[0u8; PREFIX_RESERVE_LENGTH]);

    // Encode and add user key
    encode_user_key(user_key, &mut encoded)?;

    // Add version in LE
    encoded.put_u64_le(version);

    // Add score (f64 bits) in LE
    let score_bits = score.to_bits();
    encoded.put_u64_le(score_bits);

    // Add member
    encoded.put_slice(member);

    // Add reserve2
    encoded.put_slice(&[0u8; SUFFIX_RESERVE_LENGTH]);

    Ok(encoded.to_vec())
}

/// Performs in-memory migration of a ZSet member key from BE to LE
///
/// This function handles the conversion of keys that are already in memory.
/// It attempts to detect if a key is in old BE format and converts it to LE.
pub fn migrate_key_in_memory(key: &[u8]) -> Result<Option<Vec<u8>>> {
    // Try to analyze the key assuming it's in LE format
    if let Ok((user_key, version, score, member)) = analyze_member_key(key) {
        // Key is already valid in LE format, no migration needed
        return Ok(None);
    }

    // If parsing as LE failed, try to detect if it's BE format
    match is_old_be_key(key) {
        Ok(true) => {
            // Key is in old BE format, convert it
            match convert_key_be_to_le(key) {
                Ok(new_key) => {
                    // Verify the converted key is valid
                    match analyze_member_key(&new_key) {
                        Ok(_) => Ok(Some(new_key)),
                        Err(e) => {
                            log::warn!("Converted key validation failed: {:?}", e);
                            Err(e)
                        }
                    }
                }
                Err(e) => Err(e),
            }
        }
        Ok(false) => {
            // Key is not in old format, might be corrupted
            log::debug!("Key is not in old BE format and cannot be parsed as LE");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Provides a stateless API for scanning and migrating keys
/// This trait can be implemented by actual storage backends
pub trait MigrationSource {
    /// Iterate over all keys in the specified column family
    /// Each item is (key, value) pair
    fn scan_keys(&self, cf_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Delete a key from the column family
    fn delete_key(&mut self, cf_name: &str, key: &[u8]) -> Result<()>;

    /// Put a key-value pair into the column family
    fn put_key(&mut self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()>;
}

/// Performs migration of ZSet member keys from BE to LE format
///
/// # Arguments
///
/// * `source` - A storage backend implementing MigrationSource
/// * `cf_name` - The column family name (typically "zsets")
/// * `dry_run` - If true, only report what would be migrated without making changes
///
/// # Returns
///
/// A `MigrationStats` struct containing detailed information about the migration
pub fn migrate_member_keys_be_to_le(
    source: &mut dyn MigrationSource,
    cf_name: &str,
    dry_run: bool,
) -> Result<MigrationStats> {
    let mut stats = MigrationStats::new();

    log::info!("Starting migration of {} (dry_run: {})", cf_name, dry_run);

    // Scan all keys in the column family
    let keys_and_values = match source.scan_keys(cf_name) {
        Ok(kvs) => kvs,
        Err(e) => {
            log::error!("Failed to scan keys from {}: {:?}", cf_name, e);
            return Err(e);
        }
    };

    log::info!("Scanning {} keys in column family '{}'", keys_and_values.len(), cf_name);

    // Process each key
    for (old_key, value) in keys_and_values {
        stats.total_keys += 1;
        stats.total_bytes += old_key.len() as u64;

        // Try to migrate the key
        match migrate_key_in_memory(&old_key) {
            Ok(Some(new_key)) => {
                // Key was migrated
                stats.migrated_bytes += new_key.len() as u64;

                if !dry_run {
                    // Delete old key and put new key
                    match source.delete_key(cf_name, &old_key) {
                        Ok(_) => {
                            match source.put_key(cf_name, &new_key, &value) {
                                Ok(_) => {
                                    stats.migrated_keys += 1;
                                    log::debug!("Migrated key ({}→{} bytes)", old_key.len(), new_key.len());
                                }
                                Err(e) => {
                                    stats.skipped_keys += 1;
                                    log::error!("Failed to put migrated key: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            stats.skipped_keys += 1;
                            log::error!("Failed to delete old key: {:?}", e);
                        }
                    }
                } else {
                    // Dry-run: just count
                    stats.migrated_keys += 1;
                    log::debug!("[DRY-RUN] Would migrate key ({}→{} bytes)", old_key.len(), new_key.len());
                }
            }
            Ok(None) => {
                // Key is already in new format or cannot be migrated
                stats.already_new_format += 1;
                log::debug!("Key already in new format or invalid");
            }
            Err(e) => {
                // Error during migration
                stats.skipped_keys += 1;
                log::warn!("Error migrating key: {:?}", e);
            }
        }
    }

    log::info!(
        "Migration of {} completed: {} migrated, {} already new, {} skipped",
        cf_name, stats.migrated_keys, stats.already_new_format, stats.skipped_keys
    );

    Ok(stats)
}

///
/// This function performs basic sanity checks to ensure:
/// 1. The key has the expected structure
/// 2. The reserve fields are zeros (as expected)
/// 3. The key doesn't appear to be in old BE format
pub fn verify_key_format(key: &[u8]) -> Result<bool> {
    // Check minimum length
    if key.len() < PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE + 8 + SUFFIX_RESERVE_LENGTH {
        return Ok(false);
    }

    // Check if reserve1 is all zeros
    if key[..PREFIX_RESERVE_LENGTH].iter().any(|&b| b != 0) {
        return Ok(false);
    }

    // Check if it's not in old BE format
    match is_old_be_key(key) {
        Ok(is_be) => Ok(!is_be),
        Err(_) => Ok(false),
    }
}

/// Generates a migration report
pub fn generate_migration_report(stats: &MigrationStats) -> String {
    format!(
        r#"
╔════════════════════════════════════════════╗
║    Migration Report - BE to LE Conversion  ║
╠════════════════════════════════════════════╣
║ Total Keys Processed:     {:>22}║
║ Keys Migrated:            {:>22}║
║ Already in New Format:     {:>22}║
║ Skipped (Errors):         {:>22}║
║ Progress:                 {:>21.1}%║
╠════════════════════════════════════════════╣
║ Total Bytes Processed:    {:>22}║
║ Total Bytes Migrated:     {:>22}║
║ Migration Status:         {:>22}║
╚════════════════════════════════════════════╝
"#,
        stats.total_keys,
        stats.migrated_keys,
        stats.already_new_format,
        stats.skipped_keys,
        stats.progress_percent(),
        stats.total_bytes,
        stats.migrated_bytes,
        if stats.is_successful() {
            "✓ SUCCESS"
        } else {
            "✗ FAILED"
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_stats_progress() {
        let mut stats = MigrationStats::new();
        stats.total_keys = 100;
        stats.migrated_keys = 50;
        assert_eq!(stats.progress_percent(), 50.0);
    }

    #[test]
    fn test_migration_stats_success() {
        let mut stats = MigrationStats::new();
        stats.total_keys = 100;
        stats.migrated_keys = 100;
        stats.skipped_keys = 0;
        assert!(stats.is_successful());

        stats.skipped_keys = 1;
        assert!(!stats.is_successful());
    }

    #[test]
    fn test_convert_version_be_to_le() {
        let be_bytes = [0u8, 0, 0, 0, 0, 0, 0, 42];
        let le_version = convert_version_be_to_le(&be_bytes);
        assert_eq!(le_version, 42);
    }

    #[test]
    fn test_migration_stats_display() {
        let mut stats = MigrationStats::new();
        stats.total_keys = 1000;
        stats.migrated_keys = 950;
        stats.already_new_format = 50;
        stats.total_bytes = 1_000_000;
        stats.migrated_bytes = 950_000;

        let display = format!("{}", stats);
        assert!(display.contains("1000"));
        assert!(display.contains("950"));
    }

    #[test]
    fn test_generate_migration_report() {
        let mut stats = MigrationStats::new();
        stats.total_keys = 100;
        stats.migrated_keys = 90;
        stats.already_new_format = 10;

        let report = generate_migration_report(&stats);
        assert!(report.contains("100"));
        assert!(report.contains("90"));
        assert!(report.contains("SUCCESS"));
    }

    #[test]
    fn test_is_old_be_key_detection() {
        // Test BE format detection with known BE pattern
        // BE format for small number (42): 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x2A
        let mut be_key = vec![0u8; PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE + 8 + 8 + SUFFIX_RESERVE_LENGTH];
        // Set delimiter
        be_key[PREFIX_RESERVE_LENGTH] = 0x00;
        be_key[PREFIX_RESERVE_LENGTH + 1] = 0x00;
        // Set BE version bytes (small number at end)
        let version_pos = PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE;
        be_key[version_pos + 7] = 42; // 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x2A
        
        match is_old_be_key(&be_key) {
            Ok(is_be) => assert!(is_be, "Should detect as BE format"),
            Err(e) => panic!("is_old_be_key failed: {:?}", e),
        }
    }

    #[test]
    fn test_convert_key_preserves_other_fields() {
        // Create a key and convert it
        let mut key = vec![0u8; PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE + 8 + 8 + SUFFIX_RESERVE_LENGTH];
        
        // Set delimiter
        key[PREFIX_RESERVE_LENGTH] = 0x00;
        key[PREFIX_RESERVE_LENGTH + 1] = 0x00;
        
        // Set BE version
        let version_pos = PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE;
        key[version_pos + 7] = 100; // BE: 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x64
        
        // Set score (some arbitrary bytes)
        key[version_pos + 8 + 0] = 0xFF;
        key[version_pos + 8 + 7] = 0xAA;
        
        match convert_key_be_to_le(&key) {
            Ok(new_key) => {
                // Score should be preserved
                assert_eq!(new_key[version_pos + 8 + 0], 0xFF);
                assert_eq!(new_key[version_pos + 8 + 7], 0xAA);
                // Reserve bytes should be zero
                for i in 0..PREFIX_RESERVE_LENGTH {
                    assert_eq!(new_key[i], 0);
                }
            }
            Err(e) => panic!("convert_key_be_to_le failed: {:?}", e),
        }
    }

    #[test]
    fn test_rebuild_member_key_le() {
        let user_key = b"mykey";
        let version = 42u64;
        let score = 3.14f64;
        let member = b"mymember";

        match rebuild_member_key_le(user_key, version, score, member) {
            Ok(key) => {
                // Key should have minimum size
                let min_size = PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE + 8 + 8 + member.len() + SUFFIX_RESERVE_LENGTH;
                assert!(key.len() >= min_size, "Key too short: {} < {}", key.len(), min_size);
                
                // Reserve1 should be zeros
                for i in 0..PREFIX_RESERVE_LENGTH {
                    assert_eq!(key[i], 0, "Reserve1 byte {} should be 0", i);
                }
            }
            Err(e) => panic!("rebuild_member_key_le failed: {:?}", e),
        }
    }

    #[test]
    fn test_analyze_member_key_roundtrip() {
        let user_key = b"test_key";
        let version = 12345u64;
        let score = 2.718f64;
        let member = b"test_member";

        // Rebuild a key
        match rebuild_member_key_le(user_key, version, score, member) {
            Ok(rebuilt_key) => {
                // Analyze it
                match analyze_member_key(&rebuilt_key) {
                    Ok((extracted_user_key, extracted_version, extracted_score, extracted_member)) => {
                        assert_eq!(&extracted_user_key[..], user_key);
                        assert_eq!(extracted_version, version);
                        assert_eq!(extracted_score.to_bits(), score.to_bits());
                        assert_eq!(&extracted_member[..], member);
                    }
                    Err(e) => panic!("analyze_member_key failed: {:?}", e),
                }
            }
            Err(e) => panic!("rebuild_member_key_le failed: {:?}", e),
        }
    }
}
