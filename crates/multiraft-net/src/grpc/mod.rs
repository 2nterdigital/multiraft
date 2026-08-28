//! Cross-process tonic/gRPC transport for Multi-Raft.
//!
//! One unary [`RaftService::call`](proto::raft_service_server::RaftService) RPC
//! carries `group_id` + path + UTF-8 JSON payload (same as in-process encode/decode).

mod channel_pool;
pub mod router;
pub mod server;

pub mod proto {
    tonic::include_proto!("multiraft");
}

pub mod node_rpc {
    tonic::include_proto!("multiraft.node_rpc");
}

pub use channel_pool::GrpcPeerChannelError;
pub use channel_pool::GrpcPeerChannelPool;
pub use router::GrpcRouter;
pub use server::GrpcServer;
