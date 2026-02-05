// RaftCoreService 实现 - Raft 协议核心服务
// 用途：节点间通信，实现 Raft 共识算法

use conf::raft_type::KiwiTypeConfig;
use openraft::Raft;
use std::pin::Pin;
use tokio_stream::Stream;

// 导入 proto 生成的类型
use crate::raft_proto::{
    raft_core_service_server::{RaftCoreService, RaftCoreServiceServer},
    VoteRequest, VoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    Entry as ProtoEntry,
    entry_payload,
};
use tonic::{Request, Response, Status};

// 定义流式响应类型
type StreamAppendStream = Pin<Box<dyn Stream<Item = Result<AppendEntriesResponse, Status>> + Send + 'static>>;

/// Raft 核心服务实现
pub struct RaftCoreServiceImpl {
    raft: Raft<KiwiTypeConfig>,
}

impl RaftCoreServiceImpl {
    pub fn new(raft: Raft<KiwiTypeConfig>) -> Self {
        Self { raft }
    }
}

/// 创建 RaftCoreService 服务器
pub fn create_core_service(raft: Raft<KiwiTypeConfig>) -> RaftCoreServiceServer<RaftCoreServiceImpl> {
    RaftCoreServiceServer::new(RaftCoreServiceImpl::new(raft))
}

#[tonic::async_trait]
impl RaftCoreService for RaftCoreServiceImpl {
    type StreamAppendStream = StreamAppendStream;

    /// 请求投票 RPC - 选举阶段使用
    async fn vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        use crate::conversion::{proto_to_vote, proto_to_log_id, vote_to_proto, log_id_option_to_proto};
        use openraft::raft::VoteRequest as OpenRaftVoteRequest;

        // Proto → OpenRaft
        let proto_req = request.into_inner();
        let vote = proto_to_vote(&proto_req.vote.as_ref());
        let last_log_id = proto_to_log_id(&proto_req.last_log_id.as_ref());

        let raft_req = OpenRaftVoteRequest {
            vote,
            last_log_id,
        };

        // 调用 OpenRaft
        let raft_resp = self.raft.vote(raft_req).await.map_err(|e| {
            Status::internal(format!("Raft vote error: {}", e))
        })?;

        // OpenRaft → Proto
        let proto_resp = VoteResponse {
            vote: Some(vote_to_proto(&raft_resp.vote)),
            vote_granted: raft_resp.vote_granted,
            last_log_id: log_id_option_to_proto(&raft_resp.last_log_id),
        };

        Ok(Response::new(proto_resp))
    }

    /// 追加日志条目 RPC - 日志复制使用
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        use crate::conversion::{proto_to_vote, proto_to_log_id};
        use openraft::raft::AppendEntriesResponse as OpenRaftAppendEntriesResponse;

        // Proto → OpenRaft
        let proto_req = request.into_inner();
        let vote = proto_to_vote(&proto_req.vote.as_ref());
        let prev_log_id = proto_to_log_id(&proto_req.prev_log_id.as_ref());

        // 转换 entries
        let mut entries = Vec::new();
        for proto_entry in &proto_req.entries {
            let entry = convert_proto_entry(proto_entry)?;
            entries.push(entry);
        }

        let leader_commit = proto_to_log_id(&proto_req.leader_commit.as_ref());

        let raft_req = openraft::raft::AppendEntriesRequest {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        };

        // 调用 OpenRaft
        let raft_resp = self.raft.append_entries(raft_req).await.map_err(|e| {
            Status::internal(format!("Raft append_entries error: {}", e))
        })?;

        // OpenRaft → Proto
        let success = matches!(raft_resp, OpenRaftAppendEntriesResponse::Success);
        Ok(Response::new(AppendEntriesResponse { success }))
    }

    /// 流式追加日志 RPC - pipeline 复制
    async fn stream_append(
        &self,
        _request: Request<tonic::Streaming<AppendEntriesRequest>>,
    ) -> Result<Response<Self::StreamAppendStream>, Status> {
        // TODO: 实现 pipeline 复制
        Err(Status::unimplemented("StreamAppend not implemented yet"))
    }

    /// 安装快照 RPC
    async fn install_snapshot(
        &self,
        _request: Request<tonic::Streaming<InstallSnapshotRequest>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        // TODO: 实现快照安装
        Err(Status::unimplemented("InstallSnapshot not implemented yet"))
    }
}

/// 转换 Proto Entry 到 OpenRaft Entry
fn convert_proto_entry(proto_entry: &ProtoEntry) -> Result<openraft::entry::Entry<KiwiTypeConfig>, Status> {
    use conf::raft_type::Binlog;
    use crate::conversion::proto_to_log_id;
    use openraft::entry::{Entry, EntryPayload};
    use openraft::Membership;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    let log_id = proto_to_log_id(&proto_entry.log_id.as_ref()).unwrap_or_default();

    let payload = match &proto_entry.payload {
        Some(p) => match &p.payload {
            Some(entry_payload::Payload::Blank(_)) => EntryPayload::Blank,
            Some(entry_payload::Payload::Normal(normal)) => {
                // 反序列化 Binlog
                match bincode::deserialize::<Binlog>(&normal.data) {
                    Ok(binlog) => EntryPayload::Normal(binlog),
                    Err(e) => {
                        return Err(Status::invalid_argument(format!(
                            "failed to deserialize binlog: {}",
                            e
                        )))
                    }
                }
            }
            Some(entry_payload::Payload::Membership(membership)) => {
                // 转换 Membership
                let node_ids: BTreeSet<u64> = membership.node_ids.iter().copied().collect();

                // 构建 BTreeMap<u64, KiwiNode>
                let mut nodes = BTreeMap::new();
                for node_config in &membership.nodes {
                    use conf::raft_type::KiwiNode;
                    let kiwi_node = KiwiNode {
                        raft_addr: node_config.raft_addr.clone(),
                        resp_addr: node_config.resp_addr.clone(),
                    };
                    nodes.insert(node_config.node_id, kiwi_node);
                }

                EntryPayload::Membership(Membership::new(vec![node_ids], nodes))
            }
            None => return Err(Status::invalid_argument("empty payload")),
        },
        None => return Err(Status::invalid_argument("empty payload")),
    };

    Ok(Entry { log_id, payload })
}
