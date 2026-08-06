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

//! Node capability advertisement and cluster-wide capability checks for
//! rolling upgrades (Discussion #331 §19).
//!
//! Rolling upgrades deploy in two phases: first a binary that supports the new
//! vector-set storage / Raft mutation / snapshot schema paths runs with
//! `vector.enabled=false`; the feature is enabled only after every voting
//! member advertises all [`REQUIRED_VECTOR_SET_CAPABILITIES`]. Each node
//! reports its own list through the `GetNodeCapabilities` admin RPC;
//! [`check_cluster_capabilities`] queries a given member address list and
//! summarizes whether the whole set is ready.

use std::fmt;

use crate::raft_proto::GetNodeCapabilitiesRequest;
use crate::raft_proto::raft_admin_service_client::RaftAdminServiceClient;

/// Vector set data can be stored in VectorDataCF with the v1 codec.
pub const CAP_VECTOR_SET_STORAGE_V1: &str = "vector_set_storage_v1";
/// Vector set mutations can be applied through the Raft binlog path.
pub const CAP_VECTOR_SET_RAFT_MUTATION_V1: &str = "vector_set_raft_mutation_v1";
/// Raft snapshot metadata carries the v2 storage schema description.
pub const CAP_SNAPSHOT_SCHEMA_V2: &str = "snapshot_schema_v2";

/// Capabilities every voting member must advertise before vector sets may be
/// enabled cluster-wide.
pub const REQUIRED_VECTOR_SET_CAPABILITIES: [&str; 3] = [
    CAP_VECTOR_SET_STORAGE_V1,
    CAP_VECTOR_SET_RAFT_MUTATION_V1,
    CAP_SNAPSHOT_SCHEMA_V2,
];

/// Capabilities supported by this binary.
pub fn node_capabilities() -> Vec<String> {
    [CAP_VECTOR_SET_STORAGE_V1, CAP_SNAPSHOT_SCHEMA_V2]
        .iter()
        .map(|capability| capability.to_string())
        .collect()
}

/// Capabilities reported by one cluster member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberCapabilities {
    pub addr: String,
    pub capabilities: Vec<String>,
}

/// Reasons a cluster-wide capability check failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityCheckError {
    pub failures: Vec<String>,
}

impl fmt::Display for CapabilityCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "capability check failed: {}", self.failures.join("; "))
    }
}

impl std::error::Error for CapabilityCheckError {}

/// Required capabilities missing from one member's advertised list.
pub fn missing_capabilities(capabilities: &[String]) -> Vec<&'static str> {
    REQUIRED_VECTOR_SET_CAPABILITIES
        .iter()
        .filter(|required| !capabilities.iter().any(|held| held == *required))
        .copied()
        .collect()
}

/// Summarize already-fetched voting-member capabilities: ok only when at
/// least one voter and every listed voter advertise every required capability.
///
/// This is intentionally reserved for a future voter-membership and
/// feature-enable epoch closure; current cluster startup and member join do
/// not call it because this binary does not advertise Raft vector mutations.
pub fn summarize_capabilities(members: &[MemberCapabilities]) -> Result<(), CapabilityCheckError> {
    if members.is_empty() {
        return Err(CapabilityCheckError {
            failures: vec!["no voting members were provided".to_string()],
        });
    }
    let failures: Vec<String> = members
        .iter()
        .filter_map(|member| {
            let missing = missing_capabilities(&member.capabilities);
            (!missing.is_empty()).then(|| {
                format!(
                    "{} is missing capabilities: {}",
                    member.addr,
                    missing.join(", ")
                )
            })
        })
        .collect();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(CapabilityCheckError { failures })
    }
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

/// Query every given member address and verify that all of them advertise all
/// [`REQUIRED_VECTOR_SET_CAPABILITIES`]. Returns the fetched per-member lists
/// on success.
pub async fn check_cluster_capabilities(
    addrs: &[String],
) -> Result<Vec<MemberCapabilities>, CapabilityCheckError> {
    let mut members = Vec::with_capacity(addrs.len());
    let mut failures = Vec::new();
    for addr in addrs {
        match fetch_node_capabilities(addr).await {
            Ok(capabilities) => members.push(MemberCapabilities {
                addr: addr.clone(),
                capabilities,
            }),
            Err(error) => failures.push(format!("{addr}: {error}")),
        }
    }
    if let Err(summary) = summarize_capabilities(&members) {
        failures.extend(summary.failures);
    }
    if failures.is_empty() {
        Ok(members)
    } else {
        Err(CapabilityCheckError { failures })
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn member(addr: &str, capabilities: &[&str]) -> MemberCapabilities {
        MemberCapabilities {
            addr: addr.to_string(),
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
        }
    }

    #[test]
    fn node_capabilities_do_not_advertise_unimplemented_raft_mutations() {
        let capabilities = node_capabilities();
        assert!(capabilities.contains(&CAP_VECTOR_SET_STORAGE_V1.to_string()));
        assert!(capabilities.contains(&CAP_SNAPSHOT_SCHEMA_V2.to_string()));
        assert!(!capabilities.contains(&CAP_VECTOR_SET_RAFT_MUTATION_V1.to_string()));
        assert_eq!(
            missing_capabilities(&capabilities),
            vec![CAP_VECTOR_SET_RAFT_MUTATION_V1]
        );
    }

    #[test]
    fn summarize_rejects_cluster_of_current_binaries() {
        let members = vec![MemberCapabilities {
            addr: "127.0.0.1:7401".to_string(),
            capabilities: node_capabilities(),
        }];
        let error = summarize_capabilities(&members).unwrap_err();
        assert!(error.failures[0].contains(CAP_VECTOR_SET_RAFT_MUTATION_V1));
    }

    #[test]
    fn summarize_rejects_an_empty_member_set() {
        let error = summarize_capabilities(&[]).unwrap_err();
        assert!(
            error
                .failures
                .iter()
                .any(|failure| failure.contains("no voting members"))
        );
    }

    #[test]
    fn summarize_accepts_full_cluster() {
        let all = REQUIRED_VECTOR_SET_CAPABILITIES
            .iter()
            .map(|capability| capability.to_string())
            .collect::<Vec<_>>();
        let members = vec![
            MemberCapabilities {
                addr: "127.0.0.1:7401".to_string(),
                capabilities: all.clone(),
            },
            MemberCapabilities {
                addr: "127.0.0.1:7402".to_string(),
                capabilities: all,
            },
        ];
        assert!(summarize_capabilities(&members).is_ok());
    }

    #[test]
    fn summarize_rejects_member_missing_capability() {
        let members = vec![
            member("127.0.0.1:7401", &REQUIRED_VECTOR_SET_CAPABILITIES),
            member(
                "127.0.0.1:7402",
                &[CAP_VECTOR_SET_STORAGE_V1, CAP_VECTOR_SET_RAFT_MUTATION_V1],
            ),
        ];
        let error = summarize_capabilities(&members).unwrap_err();
        assert_eq!(error.failures.len(), 1);
        assert!(error.failures[0].contains("127.0.0.1:7402"));
        assert!(error.failures[0].contains(CAP_SNAPSHOT_SCHEMA_V2));
        assert!(error.to_string().contains("capability check failed"));
    }

    #[test]
    fn summarize_rejects_all_members_missing_capabilities() {
        let members = vec![
            member("127.0.0.1:7401", &[CAP_VECTOR_SET_STORAGE_V1]),
            member("127.0.0.1:7402", &[]),
        ];
        let error = summarize_capabilities(&members).unwrap_err();
        assert_eq!(error.failures.len(), 2);
    }

    #[test]
    fn missing_capabilities_reports_exact_set() {
        let missing = missing_capabilities(&[CAP_VECTOR_SET_STORAGE_V1.to_string()]);
        assert_eq!(
            missing,
            vec![CAP_VECTOR_SET_RAFT_MUTATION_V1, CAP_SNAPSHOT_SCHEMA_V2]
        );
    }
}
