use conf::raft_type::KiwiTypeConfig;
use openraft::Raft;

use crate::raft_proto::raft_service_server::RaftService;
// 导入 proto 生成的类型，简化代码
use crate::raft_proto::{VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse};

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
        // Handle vote request
        unimplemented!()
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        // Handle append entries request
        unimplemented!()
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<InstallSnapshotRequest>>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        // Handle install snapshot request
        unimplemented!()
    }

    async fn stream_append(
        &self,
        request: tonic::Request<tonic::Streaming<AppendEntriesRequest>>,
    ) -> Result<tonic::Response<Self::StreamAppendStream>, tonic::Status> {
        // Handle stream append request
        unimplemented!()
    }
}