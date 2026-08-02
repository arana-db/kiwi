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

use serde::{Deserialize, Serialize};

/// Hard upper bound for `max_dimension`.
pub const MAX_VECTOR_DIMENSION: u32 = 65536;

/// Configuration for the Vector Set feature (VADD/VSIM/... commands).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfig {
    /// Whether vector commands are enabled at all
    pub enabled: bool,
    /// Allow vector commands in cluster mode. Defaults to false: vector
    /// commands are deterministically rejected while `raft` is configured,
    /// until the Raft apply-correctness contract (PR0) lands.
    pub cluster_enabled: bool,
    /// Maximum accepted vector dimension
    pub max_dimension: u32,
    /// Maximum number of neighbors a query may request
    pub max_k: usize,
    /// Maximum byte size of a single element payload
    pub max_element_bytes: usize,
    /// Maximum byte size of a single vector blob
    pub max_vector_bytes: usize,
    /// Maximum number of flat (brute-force) queries running concurrently
    pub max_concurrent_flat_queries: usize,
    /// Timeout for a single flat query in milliseconds
    pub flat_query_timeout_ms: u64,
    /// How many scanned entries between cancellation checks
    pub flat_cancel_check_interval: usize,
    /// Maximum entries a flat scan may visit
    pub flat_scan_max_entries: u64,
    /// Maximum bytes a flat scan may read
    pub flat_scan_max_bytes: u64,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cluster_enabled: false,
            max_dimension: 4096,
            max_k: 1000,
            max_element_bytes: 1048576,
            max_vector_bytes: 16777216,
            max_concurrent_flat_queries: 4,
            flat_query_timeout_ms: 5000,
            flat_cancel_check_interval: 256,
            flat_scan_max_entries: 1000000,
            flat_scan_max_bytes: 1073741824,
        }
    }
}

impl VectorConfig {
    /// Validate the configuration parameters
    pub fn validate(&self) -> Result<(), String> {
        if self.max_dimension == 0 {
            return Err("vector.max_dimension must be greater than 0".to_string());
        }

        if self.max_dimension > MAX_VECTOR_DIMENSION {
            return Err(format!(
                "vector.max_dimension must be <= {MAX_VECTOR_DIMENSION}"
            ));
        }

        if self.max_k == 0 {
            return Err("vector.max_k must be greater than 0".to_string());
        }

        if self.max_concurrent_flat_queries == 0 {
            return Err("vector.max_concurrent_flat_queries must be greater than 0".to_string());
        }

        if self.flat_cancel_check_interval == 0 {
            return Err("vector.flat_cancel_check_interval must be greater than 0".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = VectorConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.enabled);
        assert_eq!(4096, config.max_dimension);
        assert_eq!(1000, config.max_k);
        assert_eq!(1048576, config.max_element_bytes);
        assert_eq!(16777216, config.max_vector_bytes);
        assert_eq!(4, config.max_concurrent_flat_queries);
        assert_eq!(5000, config.flat_query_timeout_ms);
        assert_eq!(256, config.flat_cancel_check_interval);
        assert_eq!(1000000, config.flat_scan_max_entries);
        assert_eq!(1073741824, config.flat_scan_max_bytes);
    }

    #[test]
    fn max_dimension_bounds_are_enforced() {
        assert!(
            VectorConfig {
                max_dimension: 0,
                ..Default::default()
            }
            .validate()
            .is_err()
        );

        assert!(
            VectorConfig {
                max_dimension: MAX_VECTOR_DIMENSION + 1,
                ..Default::default()
            }
            .validate()
            .is_err()
        );

        assert!(
            VectorConfig {
                max_dimension: MAX_VECTOR_DIMENSION,
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn zero_max_k_is_rejected() {
        let config = VectorConfig {
            max_k: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn zero_concurrent_flat_queries_is_rejected() {
        let config = VectorConfig {
            max_concurrent_flat_queries: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn zero_cancel_check_interval_is_rejected() {
        let config = VectorConfig {
            flat_cancel_check_interval: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
