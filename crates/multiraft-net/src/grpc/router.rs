//! Outbound gRPC [`GroupRouter`](openraft_multi::GroupRouter) with per-peer channel cache.

use std::fmt;
use std::future::Future;
use std::net::SocketAddr;

use openraft::alias::SnapshotOf;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::Backoff;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::OptionalSend;
use openraft_multi::GroupRouter;

use crate::encode;
use crate::grpc::proto::raft_service_client::RaftServiceClient;
use crate::grpc::proto::RaftRequest;
use crate::grpc::GrpcPeerChannelPool;
use crate::standby_throttle::StandbyThrottle;
use multiraft_core::typ;
use multiraft_core::typ::RaftError;
use multiraft_core::ClusterConfig;
use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_core::TypeConfig;

#[derive(Debug)]
struct GrpcError(String);

impl fmt::Display for GrpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GrpcError {}

/// Shared outbound gRPC router: one tonic [`Channel`] per peer node.
#[derive(Clone)]
pub struct GrpcRouter {
    self_id: NodeId,
    channels: GrpcPeerChannelPool,
    throttle: StandbyThrottle,
    snapshot_cap: usize,
    snapshot_slots: std::sync::Arc<tokio::sync::Semaphore>,
}

impl GrpcRouter {
    /// Build a router for `self_id` using `peers` (including self; self is skipped outbound).
    pub fn new(peers: Vec<(NodeId, SocketAddr)>, self_id: NodeId) -> Self {
        Self::with_throttle(peers, self_id, StandbyThrottle::default())
    }

    /// Build with a preconfigured standby throttle (from [`ClusterConfig`]).
    pub fn with_throttle(
        peers: Vec<(NodeId, SocketAddr)>,
        self_id: NodeId,
        throttle: StandbyThrottle,
    ) -> Self {
        Self {
            self_id,
            channels: GrpcPeerChannelPool::new(peers),
            throttle,
            snapshot_cap: 64 * 1024 * 1024,
            snapshot_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    /// Build from cluster config (seeds standby throttle).
    pub fn from_config(config: &ClusterConfig) -> Self {
        let throttle = StandbyThrottle::from_config(config);
        let mut router = Self::with_throttle(config.peers.clone(), config.node_id, throttle);
        router.snapshot_cap = config.max_snapshot_bytes;
        router
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// Standby replication throttle for outbound RPCs.
    pub fn throttle(&self) -> &StandbyThrottle {
        &self.throttle
    }

    /// Distinct peer channels created (O(nodes), not O(groups)).
    pub fn unique_peer_links(&self) -> usize {
        self.channels.unique_peer_links()
    }

    async fn send<Req, Resp>(
        &self,
        to_node: NodeId,
        to_group: GroupId,
        path: &str,
        req: Req,
    ) -> Result<Resp, Unreachable<TypeConfig>>
    where
        Req: serde::Serialize,
        Result<Resp, RaftError>: serde::de::DeserializeOwned,
    {
        let _standby_permit = self.throttle.before_send(to_node).await;

        let channel = self
            .channels
            .channel(to_node)
            .await
            .map_err(|error| Unreachable::new(&GrpcError(error.to_string())))?;
        let mut client = RaftServiceClient::new(channel);
        if path == "/raft/snapshot" {
            client = client
                .max_encoding_message_size(self.snapshot_cap + 1024 * 1024)
                .max_decoding_message_size(self.snapshot_cap + 1024 * 1024);
        }

        let encoded_req = encode(&req);
        tracing::debug!(
            to_node,
            to_group,
            path,
            req_bytes = encoded_req.len(),
            "grpc send"
        );

        let response = client
            .call(RaftRequest {
                group_id: to_group,
                path: path.to_string(),
                payload: encoded_req,
            })
            .await
            .map_err(|e| Unreachable::new(&GrpcError(format!("rpc to {to_node}: {e}"))))?;

        let resp_bytes = response.into_inner().payload;
        tracing::debug!(
            to_node,
            to_group,
            path,
            resp_bytes = resp_bytes.len(),
            "grpc resp"
        );

        let res = bincode::deserialize::<Result<Resp, RaftError>>(&resp_bytes)
            .map_err(|_| Unreachable::new(&GrpcError("invalid native RPC response".into())))?;
        res.map_err(|e| Unreachable::new(&GrpcError(e.to_string())))
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
        self.send(target, group_id, "/raft/append", rpc)
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
        self.send(target, group_id, "/raft/vote", rpc)
            .await
            .map_err(RPCError::Unreachable)
    }

    async fn full_snapshot(
        &self,
        target: NodeId,
        group_id: GroupId,
        vote: typ::Vote,
        snapshot: SnapshotOf<TypeConfig, typ::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let permit = self
            .snapshot_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                StreamingError::Unreachable(Unreachable::new(&GrpcError(
                    "native snapshot send busy".into(),
                )))
            })?;
        if self.snapshot_cap == 0
            || self.snapshot_cap > 64 * 1024 * 1024
            || snapshot.snapshot.get_ref().len() > self.snapshot_cap
        {
            return Err(StreamingError::Unreachable(Unreachable::new(&GrpcError(
                "native snapshot size limit".into(),
            ))));
        }
        let data: Vec<u8> = snapshot.snapshot.into_inner();
        let wire_size = bincode::serialized_size(&(vote, &snapshot.meta, &data)).map_err(|_| {
            StreamingError::Unreachable(Unreachable::new(&GrpcError(
                "invalid snapshot metadata".into(),
            )))
        })?;
        if wire_size > (self.snapshot_cap + 1024 * 1024) as u64
            || wire_size.saturating_sub(data.len() as u64 + 8) > 1024 * 1024 - 1024
        {
            return Err(StreamingError::Unreachable(Unreachable::new(&GrpcError(
                "native snapshot wire limit".into(),
            ))));
        }
        let router = self.clone();
        let ttl = option.hard_ttl();
        tokio::spawn(async move {
            let _permit = permit;
            tokio::time::timeout(
                ttl,
                router.send(
                    target,
                    group_id,
                    "/raft/snapshot",
                    (vote, snapshot.meta, data),
                ),
            )
            .await
            .map_err(|_| {
                Unreachable::new(&GrpcError(
                    "native snapshot RPC timeout; outcome unconfirmed".into(),
                ))
            })?
        })
        .await
        .map_err(|_| {
            StreamingError::Unreachable(Unreachable::new(&GrpcError(
                "native snapshot send task stopped".into(),
            )))
        })?
        .map_err(StreamingError::Unreachable)
    }

    async fn transfer_leader(
        &self,
        target: NodeId,
        group_id: GroupId,
        req: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.send(target, group_id, "/raft/transfer_leader", req)
            .await
            .map_err(RPCError::Unreachable)
    }

    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(
            std::time::Duration::from_millis(500),
        )))
    }
}
