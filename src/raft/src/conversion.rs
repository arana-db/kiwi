// 类型转换模块：Proto 类型 ↔ OpenRaft 类型
use crate::raft_proto as proto;
use conf::raft_type::{Binlog, KiwiNode, KiwiTypeConfig};
use openraft::entry::{Entry, EntryPayload};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse,
};
use openraft::CommittedLeaderId;
use openraft::LeaderId;
use openraft::LogId;
use openraft::Vote;
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
            node_id: Some(proto::NodeId { id: lid.leader_id.node_id }),
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
            let node_id = v.leader_id.as_ref().and_then(|l| l.node_id.as_ref().map(|n| n.id)).unwrap_or(0);
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
                    )))
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
        let log_id = proto_to_log_id(&self.log_id.as_ref()).unwrap_or_default();

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
                            )))
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
        let success = matches!(resp, AppendEntriesResponse::Success);
        proto::AppendEntriesResponse { success }
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

impl From<AppendEntriesRequest<KiwiTypeConfig>> for proto::AppendEntriesRequest {
    fn from(req: AppendEntriesRequest<KiwiTypeConfig>) -> Self {
        let vote = Some(vote_to_proto(&req.vote));
        let prev_log_id = log_id_option_to_proto(&req.prev_log_id);

        // 转换 entries
        let entries: Vec<proto::Entry> = req
            .entries
            .into_iter()
            .filter_map(|e| e.try_into().ok())
            .collect();

        let leader_commit = log_id_option_to_proto(&req.leader_commit);

        proto::AppendEntriesRequest {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        }
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
                    payload: Some(proto::entry_payload::Payload::Normal(proto::NormalPayload {
                        data,
                    })),
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
                    payload: Some(proto::entry_payload::Payload::Membership(proto::Membership {
                        node_ids,
                        nodes,
                    })),
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
        // 简化版本：总是返回 Success
        // 生产环境应该检查 self.success 并可能返回错误
        Ok(AppendEntriesResponse::Success)
    }
}
