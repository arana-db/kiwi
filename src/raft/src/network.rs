use conf::raft_type::{KiwiNode, KiwiTypeConfig};
use openraft::error::{NetworkError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::io;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::raft_proto::raft_core_service_client::RaftCoreServiceClient;
use tonic::transport::Channel;
use tonic::Request as TonicRequest;

// 类型别名，简化 RaftNetwork 的返回类型
type NodeId = <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId;
type Node = KiwiNode;
type RPCErr = openraft::error::RPCError<NodeId, Node, RaftError<NodeId>>;
type RPCErrSnapshot = openraft::error::RPCError<
    NodeId,
    Node,
    RaftError<NodeId, openraft::error::InstallSnapshotError>,
>;

pub struct KiwiNetworkFactory {
    // NodeId -> raft address
    node_addrs: Arc<RwLock<HashMap<NodeId, String>>>,
    // The current node id
    node_id: NodeId,
}

impl KiwiNetworkFactory {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            node_id,
        }
    }
}

impl RaftNetworkFactory<KiwiTypeConfig> for KiwiNetworkFactory {
    type Network = KiwiNetwork;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        // Get or create gRPC client for the target node
        let addr = node.raft_addr.clone();

        
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .expect("Invalid gRPC endpoint")
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .connect_lazy();
        let client = RaftCoreServiceClient::new(endpoint);

        KiwiNetwork {
            target_id: target,
            client,
            target_addr: addr,
        }
    }
}

pub struct KiwiNetwork {
    target_id: u64,
    client: RaftCoreServiceClient<Channel>,
    target_addr: String,
}

// Impl the RaftNetwork trait for KiwiNetwork according to openraft requirements
impl RaftNetwork<KiwiTypeConfig> for KiwiNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<KiwiTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCErr> {
        // OpenRaft → Proto
        let proto_req: crate::raft_proto::AppendEntriesRequest = rpc.into();

        // 调用 gRPC
        let response = self
            .client
            .append_entries(TonicRequest::new(proto_req))
            .await
            .map_err(|e| {
                RPCErr::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("gRPC error: {}", e),
                )))
            })?;

        let proto_resp = response.into_inner();

        // Proto → OpenRaft
        if proto_resp.success {
            Ok(openraft::raft::AppendEntriesResponse::Success)
        } else {
            Err(RPCErr::Network(NetworkError::new(&io::Error::new(
                io::ErrorKind::Other,
                "AppendEntries failed",
            ))))
        }
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<KiwiTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCErrSnapshot> {
        Err(RPCErrSnapshot::Network(NetworkError::new(&io::Error::new(
            io::ErrorKind::Unsupported,
            "Install snapshot not implemented",
        ))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCErr> {
        // OpenRaft → Proto
        let proto_req: crate::raft_proto::VoteRequest = rpc.into();

        // 调用 gRPC
        let response = self
            .client
            .vote(TonicRequest::new(proto_req))
            .await
            .map_err(|e| {
                RPCErr::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("gRPC error: {}", e),
                )))
            })?;

        let proto_resp = response.into_inner();

        // Proto → OpenRaft
        use crate::conversion;
        (&proto_resp).try_into().map_err(|e| {
            RPCErr::Network(NetworkError::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to convert vote response: {}", e),
            )))
        })
    }
}
