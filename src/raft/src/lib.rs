pub mod api;
pub mod log_store;
pub mod network;
pub mod node;
pub mod state_machine;
pub mod grpc;
pub mod raft_proto {
    tonic::include_proto!("raft_proto"); 
}
