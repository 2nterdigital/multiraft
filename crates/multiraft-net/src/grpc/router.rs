//! Native RPC adapter over shared generation-aware channels; Raft owns retries.
use crate::encode;
use crate::grpc::proto::{raft_service_client::RaftServiceClient, RaftRequest};
use crate::grpc::{GrpcPeerChannelError, GrpcPeerChannelPool};
use crate::standby_throttle::StandbyThrottle;
use multiraft_core::{typ, ClusterConfig, GroupId, NodeId, TypeConfig};
use openraft::alias::SnapshotOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::OptionalSend;
use openraft_multi::GroupRouter;
use std::{fmt, future::Future, net::SocketAddr, sync::Arc};

mod snapshot_send;
use snapshot_send::SnapshotSender;

#[derive(Debug)]
enum SendCause {
    Channel(GrpcPeerChannelError),
    Rpc(Box<tonic::Status>),
    Native(typ::RaftError),
    InvalidResponse(bincode::Error),
    Closed,
    Deadline { dispatched: bool },
}
#[derive(Debug)]
struct SendFailure {
    peer: NodeId,
    group: GroupId,
    method: &'static str,
    cause: SendCause,
}
impl fmt::Display for SendFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "native RPC peer {} Group {} {} failed ({})",
            self.peer,
            self.group,
            self.method,
            self.category()
        )
    }
}
impl std::error::Error for SendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            SendCause::Channel(error) => Some(error),
            SendCause::Rpc(error) => Some(error.as_ref()),
            SendCause::Native(error) => Some(error),
            SendCause::InvalidResponse(error) => Some(error.as_ref()),
            SendCause::Closed | SendCause::Deadline { .. } => None,
        }
    }
}
impl SendFailure {
    fn category(&self) -> &'static str {
        match self.cause {
            SendCause::Channel(_) => "connect",
            SendCause::Rpc(_) => "rpc",
            SendCause::Native(_) => "native",
            SendCause::InvalidResponse(_) => "invalid_response",
            SendCause::Closed => "closed",
            SendCause::Deadline { dispatched: false } => "deadline_before_dispatch",
            SendCause::Deadline { dispatched: true } => "deadline_unconfirmed",
        }
    }
}

/// Context captured by sends. It deliberately owns no snapshot task registry,
/// avoiding a registry -> task -> router -> registry cycle.
#[derive(Clone)]
struct RpcTransport {
    channels: GrpcPeerChannelPool,
    throttle: StandbyThrottle,
    snapshot_cap: usize,
}
impl RpcTransport {
    async fn send<Req, Resp>(
        &self,
        peer: NodeId,
        group: GroupId,
        method: &'static str,
        req: Req,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Resp, Unreachable<TypeConfig>>
    where
        Req: serde::Serialize,
        Result<Resp, typ::RaftError>: serde::de::DeserializeOwned,
    {
        let failure = |cause| {
            Unreachable::new(&SendFailure {
                peer,
                group,
                method,
                cause,
            })
        };
        let mut stopping = self.channels.stopping();
        if *stopping.borrow_and_update() {
            return Err(failure(SendCause::Closed));
        }
        let send = async {
            let _standby_permit = with_deadline(deadline, false, async {
                Ok(self.throttle.before_send(peer).await)
            })
            .await
            .map_err(failure)?;
            let lease = with_deadline(deadline, false, async {
                self.channels.lease(peer).await.map_err(SendCause::Channel)
            })
            .await
            .map_err(failure)?;
            if deadline.is_some_and(|at| tokio::time::Instant::now() >= at) {
                return Err(failure(SendCause::Deadline { dispatched: false }));
            }
            let mut client = RaftServiceClient::new(lease.channel());
            if method == "/raft/snapshot" {
                client = client
                    .max_encoding_message_size(self.snapshot_cap + 1024 * 1024)
                    .max_decoding_message_size(self.snapshot_cap + 1024 * 1024);
            }
            let result = with_deadline(deadline, true, async {
                let payload = encode(&req);
                if deadline.is_some_and(|at| tokio::time::Instant::now() >= at) {
                    return Err(SendCause::Deadline { dispatched: false });
                }
                let response = client
                    .call(RaftRequest {
                        group_id: group,
                        path: method.to_owned(),
                        payload,
                    })
                    .await
                    .map_err(|error| SendCause::Rpc(Box::new(error)))?;
                let result = bincode::deserialize::<Result<Resp, typ::RaftError>>(
                    &response.into_inner().payload,
                )
                .map_err(SendCause::InvalidResponse)?;
                result.map_err(SendCause::Native)
            })
            .await;
            result.map_err(|cause| {
                // Native Stopped responses can come from still-open accepted old
                // connections. The next native attempt must get a fresh generation.
                let generation = lease.generation();
                let discarded = self.channels.invalidate(&lease);
                let source = SendFailure {
                    peer,
                    group,
                    method,
                    cause,
                };
                tracing::debug!(target: "multiraft::transport", peer, group, method,
                    generation, discarded, cause = source.category(), "native peer request failed");
                Unreachable::new(&source)
            })
        };
        tokio::select! {
            biased;
            _ = stopping.changed() => Err(failure(SendCause::Closed)),
            result = send => result,
        }
    }
}

/// Every stage shares the one caller/native deadline; no phase obtains a new TTL.
async fn with_deadline<T>(
    deadline: Option<tokio::time::Instant>,
    dispatched: bool,
    future: impl Future<Output = Result<T, SendCause>>,
) -> Result<T, SendCause> {
    match deadline {
        Some(at) => tokio::time::timeout_at(at, future)
            .await
            .map_err(|_| SendCause::Deadline { dispatched })?,
        None => future.await,
    }
}

/// Shared node transport: all Groups reuse one channel generation per peer and
/// one owned, bounded outbound snapshot slot. Native backoff remains 500 ms.
#[derive(Clone)]
pub struct GrpcRouter {
    self_id: NodeId,
    transport: RpcTransport,
    snapshot_sender: Arc<SnapshotSender>,
}
impl GrpcRouter {
    pub fn new(peers: Vec<(NodeId, SocketAddr)>, self_id: NodeId) -> Self {
        Self::with_throttle(peers, self_id, StandbyThrottle::default())
    }
    pub fn with_throttle(
        peers: Vec<(NodeId, SocketAddr)>,
        self_id: NodeId,
        throttle: StandbyThrottle,
    ) -> Self {
        Self {
            self_id,
            transport: RpcTransport {
                channels: GrpcPeerChannelPool::new(peers),
                throttle,
                snapshot_cap: 64 * 1024 * 1024,
            },
            snapshot_sender: Arc::new(SnapshotSender::default()),
        }
    }
    pub fn from_config(config: &ClusterConfig) -> Self {
        let mut router = Self::with_throttle(
            config.peers.clone(),
            config.node_id,
            StandbyThrottle::from_config(config),
        );
        router.transport.snapshot_cap = config.max_snapshot_bytes;
        router
    }
    pub fn self_id(&self) -> NodeId {
        self.self_id
    }
    pub fn throttle(&self) -> &StandbyThrottle {
        &self.transport.throttle
    }
    pub fn unique_peer_links(&self) -> usize {
        self.transport.channels.unique_peer_links()
    }

    /// Fence transport intake and pending connects. An already-dispatched send
    /// interrupted by closing remains unconfirmed; closing does not retract it.
    pub fn close(&self) {
        self.snapshot_sender.close();
        self.transport.channels.close();
    }
    pub fn abort(&self) {
        self.close();
        self.snapshot_sender.abort();
    }
    /// Join owned sends after fencing. Group owners stop native work separately.
    pub async fn join(&self) -> anyhow::Result<()> {
        self.close();
        let result = self.snapshot_sender.join().await;
        self.transport.channels.shutdown().await;
        result
    }
}
impl GroupRouter<TypeConfig, GroupId> for GrpcRouter {
    type SnapshotData = typ::SnapshotData;
    async fn append_entries(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.transport
            .send(target, group_id, "/raft/append", rpc, None)
            .await
            .map_err(RPCError::Unreachable)
    }
    async fn vote(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.transport
            .send(target, group_id, "/raft/vote", rpc, None)
            .await
            .map_err(RPCError::Unreachable)
    }
    async fn full_snapshot(
        &self,
        target: NodeId,
        group_id: GroupId,
        vote: typ::Vote,
        snapshot: SnapshotOf<TypeConfig, typ::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let sending = self.snapshot_sender.send(
            self.transport.clone(),
            target,
            group_id,
            vote,
            snapshot,
            option,
        );
        // Native stream cancellation closes only its waiter. The node-owned send
        // retains the permit and its original deadline until actual work ends.
        tokio::pin!(sending, cancel);
        tokio::select! {
            biased;
            reason = &mut cancel => Err(StreamingError::Closed(reason)),
            result = &mut sending => result,
        }
    }
    async fn transfer_leader(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.transport
            .send(target, group_id, "/raft/transfer_leader", rpc, None)
            .await
            .map_err(RPCError::Unreachable)
    }
    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(
            std::time::Duration::from_millis(500),
        )))
    }
}

#[cfg(test)]
#[path = "../../tests/native_transport_owner/mod.rs"]
mod ownership_tests;
