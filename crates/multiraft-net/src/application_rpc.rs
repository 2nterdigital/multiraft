//! Owned, business-neutral application RPC. Separate service from native Raft RPC.
//! Calls carry opaque operation IDs/bytes/metadata, with one deadline and no retry.
mod error;
mod metadata;
mod operation;
mod owner;
mod wire;

pub use error::{RpcDispatch, RpcError, RpcErrorKind, RpcHandlerError, RpcPhase};
pub use metadata::RpcMetadata;
pub use owner::{ApplicationRpcHandle, ApplicationRpcOwner};

use multiraft_core::NodeId;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::time::Instant;

/// Protobuf-encoded request/reply outer cap, including field tags and lengths.
pub const APPLICATION_RPC_OUTER_BYTES: usize = 270_336;
/// Finite generic transport admission, independent of application/business gates.
pub const DEFAULT_RPC_MAX_INFLIGHT: usize = 16_384;

/// Listener inputs. Different address catalogs must use different pool instances.
pub struct ApplicationRpcConfig {
    pub local_node_id: NodeId,
    pub listen_address: SocketAddr,
    pub peers: Vec<(NodeId, SocketAddr)>,
    pub max_inflight: usize,
    /// At most eight ASCII keys; names and values are never interpreted by the library.
    pub metadata_keys: BTreeSet<String>,
}
impl ApplicationRpcConfig {
    pub fn new(
        local_node_id: NodeId,
        listen_address: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
    ) -> Self {
        Self {
            local_node_id,
            listen_address,
            peers,
            max_inflight: DEFAULT_RPC_MAX_INFLIGHT,
            metadata_keys: BTreeSet::new(),
        }
    }
}
/// Opaque service/method numbers and application bytes. Debug omits content.
#[derive(Clone, PartialEq, Eq)]
pub struct RpcCall {
    pub service_id: u32,
    pub method_id: u32,
    pub payload: Vec<u8>,
}
impl RpcCall {
    pub fn new(service_id: u32, method_id: u32, payload: Vec<u8>) -> Self {
        Self {
            service_id,
            method_id,
            payload,
        }
    }
}
impl fmt::Debug for RpcCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcCall")
            .field("service_id", &self.service_id)
            .field("method_id", &self.method_id)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}
#[derive(Clone, PartialEq, Eq)]
pub struct RpcReply {
    pub payload: Vec<u8>,
}
impl RpcReply {
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }
}
impl fmt::Debug for RpcReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcReply")
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}
/// Request budget/context. Remote Instants are derived from the remaining grpc-timeout.
#[derive(Clone, Debug)]
pub struct RpcContext {
    pub deadline: Instant,
    pub metadata: RpcMetadata,
}
impl RpcContext {
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            metadata: RpcMetadata::default(),
        }
    }
}
pub type RpcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RpcReply, RpcHandlerError>> + Send + 'a>>;
/// Synchronous entry and returned Future must be bounded. No detached business work.
pub trait ApplicationRpcHandler: Send + Sync + 'static {
    fn call(&self, context: RpcContext, call: RpcCall) -> RpcFuture<'_>;
}
