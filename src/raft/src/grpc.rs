// gRPC 服务实现，用于处理 Raft 协议的 RPC 调用
use conf::raft_type::KiwiTypeConfig;
use openraft::Raft;

// 导入 proto 生成的类型，简化代码
use crate::raft_proto::{VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse};
use crate::raft_proto::raft_service_server::RaftService;

// 定义响应流类型用于双向流式 RPC
type AppendEntriesResponseStream = std::pin::Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<AppendEntriesResponse, tonic::Status>>
            + Send
            + 'static,
    >,
>;

pub struct RaftServiceImpl {
    raft: Raft<KiwiTypeConfig>,
}

impl RaftServiceImpl {
    pub fn new(raft: Raft<KiwiTypeConfig>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    // 定义 StreamAppendStream 关联类型
    type StreamAppendStream = AppendEntriesResponseStream;

    // Implement gRPC methods here
    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        // Proto → OpenRaft (使用 TryInto trait)
        let proto_req = request.into_inner();
        let raft_req = (&proto_req).try_into()?;

        // 调用 OpenRaft
        let raft_resp = self.raft.vote(raft_req).await.map_err(|e| {
            tonic::Status::internal(format!("Raft vote error: {}", e))
        })?;

        // OpenRaft → Proto (使用 From trait)
        let proto_resp = VoteResponse::from(raft_resp);
        Ok(tonic::Response::new(proto_resp))
    }

    async fn append_entries(
        &self,
        _request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        // Handle append entries request
        unimplemented!()
    }

    async fn install_snapshot(
        &self,
        _request: tonic::Request<tonic::Streaming<InstallSnapshotRequest>>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        // Handle install snapshot request
        unimplemented!()
    }

    async fn stream_append(
        &self,
        _request: tonic::Request<tonic::Streaming<AppendEntriesRequest>>,
    ) -> Result<tonic::Response<Self::StreamAppendStream>, tonic::Status> {
        // Handle stream append request
        unimplemented!()
    }
}