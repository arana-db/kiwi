use conf::raft_type::KiwiTypeConfig;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use reqwest::Client;
use std::io;

pub struct KiwiNetworkFactory {
    client: Client,
}

impl KiwiNetworkFactory {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }
}

impl RaftNetworkFactory<KiwiTypeConfig> for KiwiNetworkFactory {
    type Network = KiwiNetwork;

    async fn new_client(
        &mut self,
        _target: <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
        node: &<KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
    ) -> Self::Network {
        KiwiNetwork {
            client: self.client.clone(),
            target_addr: node.raft_addr.clone(),
        }
    }
}

pub struct KiwiNetwork {
    client: Client,
    target_addr: String,
}

impl KiwiNetwork {
    async fn post<Req, Res>(
        &self,
        endpoint: &str,
        req: &Req,
    ) -> Result<Res, RPCError<KiwiTypeConfig>>
    where
        Req: serde::Serialize,
        Res: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.target_addr, endpoint);

        let resp = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(RPCError::Network(NetworkError::new(&io::Error::new(
                io::ErrorKind::Other,
                format!("HTTP error {}: {}", status, body),
            ))));
        }

        resp.json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

impl RaftNetwork<KiwiTypeConfig> for KiwiNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<KiwiTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        AppendEntriesResponse<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        RPCError<
            <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
            <KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        // post() returns RPCError<NodeId, Node, Infallible>
        // Trait requires RPCError<NodeId, Node, RaftError<NodeId>>
        // Since RaftError<NodeId> defaults to RaftError<NodeId, Infallible>,
        // we use a map to convert the error type appropriately
        self.post("/raft/append", &rpc)
            .await
            .map_err(|e| e.with_raft_error::<openraft::error::Infallible>())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<KiwiTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        RPCError<
            <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
            <KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<
                <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
                openraft::error::InstallSnapshotError,
            >,
        >,
    > {
        Err(RPCError::Network(NetworkError::new(&io::Error::new(
            io::ErrorKind::Unsupported,
            "Install snapshot not implemented",
        )))
        .map_err(|e| match e {
            RPCError::Network(ne) => RPCError::Network(ne),
            RPCError::Timeout(_) => RPCError::Timeout(_),
            RPCError::Unreachable(_) => RPCError::Unreachable(_),
            RPCError::PayloadTooLarge(_) => RPCError::PayloadTooLarge(_),
            RPCError::RemoteError(re) => RPCError::RemoteError(re),
        }))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        VoteResponse<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        RPCError<
            <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId,
            <KiwiTypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<<KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        self.post("/raft/vote", &rpc)
            .await
            .map_err(|e| e.with_raft_error::<openraft::error::Infallible>())
    }
}
