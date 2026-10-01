//! Cross-process tonic/gRPC transport for Multi-Raft.
//!
//! One unary [`RaftService::call`](proto::raft_service_server::RaftService) RPC
//! carries `group_id` + path + bincode payload (native Raft request/response types).

mod channel_pool;
pub mod router;
pub mod server;

// Generated tonic signatures return their required concrete Status.
#[allow(clippy::result_large_err)]
pub mod proto {
    tonic::include_proto!("multiraft");
}

#[allow(clippy::result_large_err)]
pub mod node_rpc {
    tonic::include_proto!("multiraft.node_rpc");
}

pub use channel_pool::GrpcPeerChannelError;
pub use channel_pool::{GrpcPeerChannelLease, GrpcPeerChannelPool};
pub use router::GrpcRouter;
pub use server::GrpcServer;
