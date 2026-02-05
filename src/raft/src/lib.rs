pub mod log_store;
pub mod network;
pub mod node;
pub mod state_machine;
pub mod grpc;
pub mod conversion;
pub mod raft_proto {
    tonic::include_proto!("raft_proto"); 

     pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("raft_proto_descriptor");
}
