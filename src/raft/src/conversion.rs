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

// 类型转换模块：Proto 类型 ↔ OpenRaft 类型
use crate::raft_proto as proto;
use conf::raft_type::{Binlog, KiwiNode, KiwiTypeConfig};
use openraft::CommittedLeaderId;
use openraft::LeaderId;
use openraft::LogId;
use openraft::Vote;
use openraft::entry::{Entry, EntryPayload};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use std::convert::TryInto;

// 辅助函数：创建 LeaderId、LogId 和 Vote
pub fn proto_to_leader_id(lid: &Option<&proto::LeaderId>) -> LeaderId<u64> {
    match lid {
        Some(l) => LeaderId::new(l.term, l.node_id.as_ref().map(|n| n.id).unwrap_or(0)),
        None => LeaderId::default(),
    }
}

pub fn leader_id_to_proto(lid: &LeaderId<u64>) -> proto::LeaderId {
    proto::LeaderId {
        term: lid.term,
        node_id: Some(proto::NodeId { id: lid.node_id }),
    }
}

pub fn proto_to_committed_leader_id(lid: &Option<&proto::LeaderId>) -> CommittedLeaderId<u64> {
    match lid {
        Some(l) => CommittedLeaderId::new(l.term, l.node_id.as_ref().map(|n| n.id).unwrap_or(0)),
        None => CommittedLeaderId::default(),
    }
}

pub fn proto_to_log_id(lid: &Option<&proto::LogId>) -> Option<LogId<u64>> {
    lid.map(|l| LogId {
        leader_id: proto_to_committed_leader_id(&l.leader_id.as_ref()),
        index: l.index,
    })
}

pub fn log_id_to_proto(lid: &LogId<u64>) -> proto::LogId {
    proto::LogId {
        leader_id: Some(proto::LeaderId {
            term: lid.leader_id.term,
            node_id: Some(proto::NodeId {
                id: lid.leader_id.node_id,
            }),
        }),
        index: lid.index,
    }
}

pub fn log_id_option_to_proto(lid: &Option<LogId<u64>>) -> Option<proto::LogId> {
    lid.as_ref().map(log_id_to_proto)
}

pub fn proto_to_vote(vote: &Option<&proto::Vote>) -> Vote<u64> {
    match vote {
        Some(v) => {
            let term = v.leader_id.as_ref().map(|l| l.term).unwrap_or(0);
            let node_id = v
                .leader_id
                .as_ref()
                .and_then(|l| l.node_id.as_ref().map(|n| n.id))
                .unwrap_or(0);
            if v.committed {
                Vote::new_committed(term, node_id)
            } else {
                Vote::new(term, node_id)
            }
        }
        None => Vote::default(),
    }
}

pub fn vote_to_proto(vote: &Vote<u64>) -> proto::Vote {
    proto::Vote {
        leader_id: Some(leader_id_to_proto(&vote.leader_id)),
        committed: vote.committed,
    }
}

// Proto → OpenRaft 转换 (gRPC Server 使用)
// ----- VoteRequest -----

impl TryInto<VoteRequest<u64>> for &proto::VoteRequest {
    type Error = tonic::Status;

    fn try_into(self) -> Result<VoteRequest<u64>, Self::Error> {
        Ok(VoteRequest {
            vote: proto_to_vote(&self.vote.as_ref()),
            last_log_id: proto_to_log_id(&self.last_log_id.as_ref()),
        })
    }
}

// ----- AppendEntriesRequest -----

impl TryInto<AppendEntriesRequest<KiwiTypeConfig>> for &proto::AppendEntriesRequest {
    type Error = tonic::Status;

    fn try_into(self) -> Result<AppendEntriesRequest<KiwiTypeConfig>, Self::Error> {
        let vote = proto_to_vote(&self.vote.as_ref());
        let prev_log_id = proto_to_log_id(&self.prev_log_id.as_ref());

        // 转换 entries
        let mut entries = Vec::new();
        for proto_entry in &self.entries {
            match proto_entry.try_into() {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    return Err(tonic::Status::invalid_argument(format!(
                        "invalid entry: {}",
                        e
                    )));
                }
            }
        }

        let leader_commit = proto_to_log_id(&self.leader_commit.as_ref());

        Ok(AppendEntriesRequest {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        })
    }
}

// 转换单个 Entry (Proto → OpenRaft)
impl TryInto<Entry<KiwiTypeConfig>> for &proto::Entry {
    type Error = tonic::Status;

    fn try_into(self) -> Result<Entry<KiwiTypeConfig>, Self::Error> {
        let log_id = proto_to_log_id(&self.log_id.as_ref())
            .ok_or_else(|| tonic::Status::invalid_argument("missing log_id in entry"))?;

        let payload = match &self.payload {
            Some(p) => match &p.payload {
                Some(proto::entry_payload::Payload::Blank(_)) => EntryPayload::Blank,
                Some(proto::entry_payload::Payload::Normal(normal)) => {
                    // 反序列化 Binlog
                    match bincode::deserialize::<Binlog>(&normal.data) {
                        Ok(binlog) => EntryPayload::Normal(binlog),
                        Err(e) => {
                            return Err(tonic::Status::invalid_argument(format!(
                                "failed to deserialize binlog: {}",
                                e
                            )));
                        }
                    }
                }
                Some(proto::entry_payload::Payload::Membership(membership)) => {
                    // 转换 Membership
                    use openraft::Membership;
                    use std::collections::BTreeSet;

                    // Membership::new 的参数是 Vec<BTreeSet<NodeId>> 和 nodes
                    // 单个配置是 vec![BTreeSet]
                    let node_ids: BTreeSet<u64> = membership.node_ids.iter().copied().collect();

                    // 构建 BTreeMap<u64, KiwiNode>
                    let mut nodes = std::collections::BTreeMap::new();
                    for node_config in &membership.nodes {
                        let kiwi_node = KiwiNode {
                            raft_addr: node_config.raft_addr.clone(),
                            resp_addr: node_config.resp_addr.clone(),
                        };
                        nodes.insert(node_config.node_id, kiwi_node);
                    }

                    // Membership::new(configs, nodes) 其中 configs: Vec<BTreeSet<NID>>
                    EntryPayload::Membership(Membership::new(vec![node_ids], nodes))
                }
                None => return Err(tonic::Status::invalid_argument("empty payload")),
            },
            None => return Err(tonic::Status::invalid_argument("empty payload")),
        };

        Ok(Entry { log_id, payload })
    }
}

// OpenRaft → Proto 转换 (gRPC Server 响应使用)
// ----- VoteResponse -----

impl From<VoteResponse<u64>> for proto::VoteResponse {
    fn from(resp: VoteResponse<u64>) -> Self {
        proto::VoteResponse {
            vote: Some(vote_to_proto(&resp.vote)),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.as_ref().map(log_id_to_proto),
        }
    }
}

// ----- AppendEntriesResponse -----
// 注意：OpenRaft 的 AppendEntriesResponse 是 enum (Success/PartialSuccess)
// 简化版本：只返回 success 布尔值

impl From<AppendEntriesResponse<u64>> for proto::AppendEntriesResponse {
    fn from(resp: AppendEntriesResponse<u64>) -> Self {
        let (success, result, higher_vote) = match resp {
            AppendEntriesResponse::Success | AppendEntriesResponse::PartialSuccess(_) => {
                (true, proto::AppendEntriesResult::Success as i32, None)
            }
            AppendEntriesResponse::Conflict => {
                (false, proto::AppendEntriesResult::Conflict as i32, None)
            }
            AppendEntriesResponse::HigherVote(vote) => (
                false,
                proto::AppendEntriesResult::HigherVote as i32,
                Some(vote_to_proto(&vote)),
            ),
        };

        proto::AppendEntriesResponse {
            success,
            result,
            higher_vote,
        }
    }
}

// OpenRaft → Proto 转换 (gRPC Client 请求使用)

// ----- VoteRequest -----

impl From<VoteRequest<u64>> for proto::VoteRequest {
    fn from(req: VoteRequest<u64>) -> Self {
        proto::VoteRequest {
            vote: Some(vote_to_proto(&req.vote)),
            last_log_id: req.last_log_id.as_ref().map(log_id_to_proto),
        }
    }
}

// ----- AppendEntriesRequest -----

impl TryFrom<AppendEntriesRequest<KiwiTypeConfig>> for proto::AppendEntriesRequest {
    type Error = tonic::Status;

    fn try_from(req: AppendEntriesRequest<KiwiTypeConfig>) -> Result<Self, Self::Error> {
        let vote = Some(vote_to_proto(&req.vote));
        let prev_log_id = log_id_option_to_proto(&req.prev_log_id);

        // 转换 entries，记录转换失败的条目而非静默丢弃
        let mut entries = Vec::new();
        let mut errors = Vec::new();
        for (idx, e) in req.entries.into_iter().enumerate() {
            match e.try_into() {
                Ok(entry) => entries.push(entry),
                Err(e) => errors.push(format!("entry[{}]: {}", idx, e)),
            }
        }
        if !errors.is_empty() {
            return Err(tonic::Status::invalid_argument(format!(
                "failed to convert {} entries: {}",
                errors.len(),
                errors.join("; ")
            )));
        }

        let leader_commit = log_id_option_to_proto(&req.leader_commit);

        Ok(proto::AppendEntriesRequest {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        })
    }
}

// 转换单个 Entry (OpenRaft → Proto)
impl TryInto<proto::Entry> for Entry<KiwiTypeConfig> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<proto::Entry, Self::Error> {
        let log_id = Some(log_id_to_proto(&self.log_id));

        let payload = match self.payload {
            EntryPayload::Blank => Some(proto::EntryPayload {
                payload: Some(proto::entry_payload::Payload::Blank(proto::BlankPayload {})),
            }),
            EntryPayload::Normal(binlog) => {
                // 序列化 Binlog
                let data = bincode::serialize(&binlog)?;
                Some(proto::EntryPayload {
                    payload: Some(proto::entry_payload::Payload::Normal(
                        proto::NormalPayload { data },
                    )),
                })
            }
            EntryPayload::Membership(membership) => {
                // 转换 Membership
                // 获取 voter_ids (所有配置中的节点 ID)
                let mut all_node_ids = std::collections::BTreeSet::new();
                for config in membership.get_joint_config() {
                    for node_id in config {
                        all_node_ids.insert(*node_id);
                    }
                }
                let node_ids: Vec<u64> = all_node_ids.iter().copied().collect();

                let nodes: Vec<proto::NodeConfig> = membership
                    .nodes()
                    .map(|(id, node)| proto::NodeConfig {
                        node_id: *id,
                        raft_addr: node.raft_addr.clone(),
                        resp_addr: node.resp_addr.clone(),
                    })
                    .collect();

                Some(proto::EntryPayload {
                    payload: Some(proto::entry_payload::Payload::Membership(
                        proto::Membership { node_ids, nodes },
                    )),
                })
            }
        };

        Ok(proto::Entry { log_id, payload })
    }
}

// Proto → OpenRaft 转换 (gRPC Client 响应使用)
// ----- VoteResponse -----

impl TryInto<VoteResponse<u64>> for &proto::VoteResponse {
    type Error = tonic::Status;

    fn try_into(self) -> Result<VoteResponse<u64>, Self::Error> {
        Ok(VoteResponse {
            vote: proto_to_vote(&self.vote.as_ref()),
            vote_granted: self.vote_granted,
            last_log_id: proto_to_log_id(&self.last_log_id.as_ref()),
        })
    }
}

// ----- AppendEntriesResponse -----
// Proto 的 success: true 映射为 Success，false 也映射为 Success（简化处理）
// 生产环境应该根据情况返回 Success 或错误

impl TryInto<AppendEntriesResponse<u64>> for &proto::AppendEntriesResponse {
    type Error = tonic::Status;

    fn try_into(self) -> Result<AppendEntriesResponse<u64>, Self::Error> {
        match proto::AppendEntriesResult::try_from(self.result)
            .unwrap_or(proto::AppendEntriesResult::Unspecified)
        {
            proto::AppendEntriesResult::Success => Ok(AppendEntriesResponse::Success),
            proto::AppendEntriesResult::Conflict => Ok(AppendEntriesResponse::Conflict),
            proto::AppendEntriesResult::HigherVote => {
                let higher_vote = self
                    .higher_vote
                    .as_ref()
                    .ok_or_else(|| tonic::Status::invalid_argument("missing higher_vote"))?;
                Ok(AppendEntriesResponse::HigherVote(proto_to_vote(&Some(
                    higher_vote,
                ))))
            }
            proto::AppendEntriesResult::Unspecified => {
                if self.success {
                    Ok(AppendEntriesResponse::Success)
                } else if let Some(higher_vote) = self.higher_vote.as_ref() {
                    Ok(AppendEntriesResponse::HigherVote(proto_to_vote(&Some(
                        higher_vote,
                    ))))
                } else {
                    Ok(AppendEntriesResponse::Conflict)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_conflict_maps_to_conflict_response() {
        let proto = proto::AppendEntriesResponse {
            success: false,
            result: proto::AppendEntriesResult::Conflict as i32,
            higher_vote: None,
        };

        let resp: AppendEntriesResponse<u64> = (&proto).try_into().unwrap();
        assert!(matches!(resp, AppendEntriesResponse::Conflict));
    }

    #[test]
    fn proto_higher_vote_maps_to_higher_vote_response() {
        let higher_vote = Vote::new_committed(3, 9);
        let proto = proto::AppendEntriesResponse {
            success: false,
            result: proto::AppendEntriesResult::HigherVote as i32,
            higher_vote: Some(vote_to_proto(&higher_vote)),
        };

        let resp: AppendEntriesResponse<u64> = (&proto).try_into().unwrap();
        match resp {
            AppendEntriesResponse::HigherVote(vote) => {
                assert_eq!(vote.leader_id.term, 3);
                assert_eq!(vote.leader_id.node_id, 9);
                assert!(vote.committed);
            }
            other => panic!("expected HigherVote, got {other:?}"),
        }
    }

    #[test]
    fn openraft_higher_vote_preserves_proto_discriminator() {
        let proto: proto::AppendEntriesResponse =
            AppendEntriesResponse::HigherVote(Vote::new(7, 11)).into();

        assert!(!proto.success);
        assert_eq!(proto.result, proto::AppendEntriesResult::HigherVote as i32);
        assert!(proto.higher_vote.is_some());
    }

    #[test]
    fn openraft_success_preserves_proto_success() {
        let proto: proto::AppendEntriesResponse = AppendEntriesResponse::Success.into();

        assert!(proto.success);
        assert_eq!(proto.result, proto::AppendEntriesResult::Success as i32);
        assert!(proto.higher_vote.is_none());
    }
}
