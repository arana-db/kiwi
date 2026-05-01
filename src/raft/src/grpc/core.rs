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

// RaftCoreService 实现 - Raft 协议核心服务
// 用途：节点间通信，实现 Raft 共识算法

use conf::raft_type::KiwiTypeConfig;
use openraft::Raft;
use std::pin::Pin;
use tokio_stream::Stream;

// 导入 proto 生成的类型
use crate::raft_proto::{
    AppendEntriesRequest, AppendEntriesResponse, AppendEntriesResult, Entry as ProtoEntry,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse, entry_payload,
    raft_core_service_server::{RaftCoreService, RaftCoreServiceServer},
};
use tonic::{Request, Response, Status};

// 定义流式响应类型
type StreamAppendStream =
    Pin<Box<dyn Stream<Item = Result<AppendEntriesResponse, Status>> + Send + 'static>>;

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
pub fn create_core_service(
    raft: Raft<KiwiTypeConfig>,
) -> RaftCoreServiceServer<RaftCoreServiceImpl> {
    RaftCoreServiceServer::new(RaftCoreServiceImpl::new(raft))
}

#[tonic::async_trait]
impl RaftCoreService for RaftCoreServiceImpl {
    type StreamAppendStream = StreamAppendStream;

    /// 请求投票 RPC - 选举阶段使用
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        use crate::conversion::{
            log_id_option_to_proto, proto_to_log_id, proto_to_vote, vote_to_proto,
        };
        use openraft::raft::VoteRequest as OpenRaftVoteRequest;

        // Proto → OpenRaft
        let proto_req = request.into_inner();
        let vote = proto_to_vote(&proto_req.vote.as_ref());
        let last_log_id = proto_to_log_id(&proto_req.last_log_id.as_ref());

        let raft_req = OpenRaftVoteRequest { vote, last_log_id };

        // 调用 OpenRaft
        let raft_resp = self
            .raft
            .vote(raft_req)
            .await
            .map_err(|e| Status::internal(format!("Raft vote error: {}", e)))?;

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
        use crate::conversion::{proto_to_log_id, proto_to_vote, vote_to_proto};
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
        let raft_resp = self
            .raft
            .append_entries(raft_req)
            .await
            .map_err(|e| Status::internal(format!("Raft append_entries error: {}", e)))?;

        // OpenRaft → Proto
        // 这些都应该返回 success=false，而不是 gRPC 错误
        match raft_resp {
            OpenRaftAppendEntriesResponse::Success => Ok(Response::new(AppendEntriesResponse {
                success: true,
                result: AppendEntriesResult::Success as i32,
                higher_vote: None,
            })),
            OpenRaftAppendEntriesResponse::PartialSuccess(_) => {
                // PartialSuccess 表示部分成功，对客户端来说算成功
                Ok(Response::new(AppendEntriesResponse {
                    success: true,
                    result: AppendEntriesResult::Success as i32,
                    higher_vote: None,
                }))
            }
            OpenRaftAppendEntriesResponse::Conflict => {
                // Conflict 是正常的协议响应：日志不匹配
                Ok(Response::new(AppendEntriesResponse {
                    success: false,
                    result: AppendEntriesResult::Conflict as i32,
                    higher_vote: None,
                }))
            }
            OpenRaftAppendEntriesResponse::HigherVote(vote) => {
                // HigherVote 是正常的协议响应：对方有更高的 term 或 vote
                Ok(Response::new(AppendEntriesResponse {
                    success: false,
                    result: AppendEntriesResult::HigherVote as i32,
                    higher_vote: Some(vote_to_proto(&vote)),
                }))
            }
        }
    }

    /// 流式追加日志 RPC - pipeline 复制
    ///
    /// # 当前状态
    /// 此 RPC 尚未实现，返回 `Status::unimplemented`。
    /// openraft 在某些场景下会依赖 stream_append 做 pipeline 复制。
    /// 如果这个 RPC 不工作，节点间日志复制会回退到同步方式，影响性能。
    /// 将在后续版本中实现流水线复制优化。
    async fn stream_append(
        &self,
        _request: Request<tonic::Streaming<AppendEntriesRequest>>,
    ) -> Result<Response<Self::StreamAppendStream>, Status> {
        Err(Status::unimplemented(
            "StreamAppend RPC is not implemented yet: nodes will use synchronous log replication",
        ))
    }

    /// 安装快照 RPC
    ///
    /// # 当前状态 - 临时 workaround
    /// 此 RPC 尚未实现。当前实现消费整个流然后返回 `failed_precondition` 错误。
    ///
    /// ## 已知问题
    /// - 客户端会接收到 `failed_precondition` 错误，看起来像服务端不支持此 RPC
    /// - 但实际上服务端接收了所有数据只是丢弃了
    /// - 如果将来实现了快照安装，这个 workaround 会被删除
    async fn install_snapshot(
        &self,
        request: Request<tonic::Streaming<InstallSnapshotRequest>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        // FIXME: 实现快照安装
        // 当前故意丢弃所有快照分片，将来实现时必须删除此 workaround
        let mut stream = request.into_inner();
        while let Some(_chunk) = stream.message().await? {
            // 丢弃所有快照分片
        }

        Err(Status::failed_precondition(
            "InstallSnapshot RPC is not implemented yet: received snapshot data was discarded",
        ))
    }
}

/// 转换 Proto Entry 到 OpenRaft Entry
fn convert_proto_entry(
    proto_entry: &ProtoEntry,
) -> Result<openraft::entry::Entry<KiwiTypeConfig>, Status> {
    use crate::conversion::proto_to_log_id;
    use conf::raft_type::Binlog;
    use openraft::Membership;
    use openraft::entry::{Entry, EntryPayload};
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    let log_id = proto_to_log_id(&proto_entry.log_id.as_ref())
        .ok_or_else(|| Status::invalid_argument("missing log_id in entry"))?;

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
                        )));
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
