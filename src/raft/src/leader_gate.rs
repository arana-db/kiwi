// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Lightweight leadership view used by the network layer to gate writes.
///
/// Lets `net` decide whether to accept a write locally or reply `-MOVED`,
/// without depending on the full Raft machinery.
pub trait LeaderGate: Send + Sync {
    /// Whether this node is currently the Raft leader.
    fn is_leader(&self) -> bool;

    /// Current leader's RESP address for client redirect, if known.
    fn leader_resp_addr(&self) -> Option<String>;
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysLeader;
    impl LeaderGate for AlwaysLeader {
        fn is_leader(&self) -> bool {
            true
        }
        fn leader_resp_addr(&self) -> Option<String> {
            None
        }
    }

    #[test]
    fn test_always_leader_gate() {
        let g = AlwaysLeader;
        assert!(g.is_leader());
        assert!(g.leader_resp_addr().is_none());
    }
}
