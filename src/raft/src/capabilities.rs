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

//! Node capability advertisement for rolling upgrades (Discussion #331 §19).
//!
//! This binary advertises only storage formats it can actually open and
//! snapshot. Vector commands remain fail-closed in cluster mode; no mutation
//! capability or dormant feature-enable contract is advertised here.

use crate::raft_proto::GetNodeCapabilitiesRequest;
use crate::raft_proto::raft_admin_service_client::RaftAdminServiceClient;

/// Vector set data can be stored in VectorDataCF with the v1 codec.
pub const CAP_VECTOR_SET_STORAGE_V1: &str = "vector_set_storage_v1";
/// Raft snapshot metadata carries the v2 storage schema description.
pub const CAP_SNAPSHOT_SCHEMA_V2: &str = "snapshot_schema_v2";

/// Capabilities supported by this binary.
pub fn node_capabilities() -> Vec<String> {
    [CAP_VECTOR_SET_STORAGE_V1, CAP_SNAPSHOT_SCHEMA_V2]
        .iter()
        .map(|capability| capability.to_string())
        .collect()
}

/// Fetch one node's advertised capabilities via the admin gRPC service.
pub async fn fetch_node_capabilities(addr: &str) -> Result<Vec<String>, String> {
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|error| format!("invalid admin address: {error}"))?;
    let mut client = RaftAdminServiceClient::connect(endpoint)
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;
    let response = client
        .get_node_capabilities(GetNodeCapabilitiesRequest {})
        .await
        .map_err(|error| format!("GetNodeCapabilities failed: {error}"))?
        .into_inner();
    Ok(response.capabilities)
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_source_has_no_dormant_vector_raft_enablement_contract() {
        let source = include_str!("capabilities.rs");
        assert!(!source.contains(concat!("vector_set_raft_", "mutation_v1")));
        assert!(!source.contains(concat!("REQUIRED_VECTOR_SET_", "CAPABILITIES")));
        assert_eq!(
            node_capabilities(),
            vec![
                CAP_VECTOR_SET_STORAGE_V1.to_string(),
                CAP_SNAPSHOT_SCHEMA_V2.to_string(),
            ]
        );
    }
}
