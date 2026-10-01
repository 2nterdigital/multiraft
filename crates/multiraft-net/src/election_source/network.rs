//! Typed outbound boundaries over the existing transport implementation.
use super::*;
use crate::group_observation::normalize_log_id;
use futures::Stream;
use multiraft_core::TypeConfig;
use openraft::{
    alias::SnapshotOf,
    base::{BoxFuture, BoxStream},
    error::{RPCError, ReplicationClosed, StreamingError},
    network::{
        Backoff, NetAppend, NetBackoff, NetSnapshot, NetStreamAppend, NetTransferLeader, NetVote,
        RPCOption, RaftNetworkFactory,
    },
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, StreamAppendResult,
        TransferLeaderError, TransferLeaderRequest, TransferLeaderResponse, VoteRequest,
        VoteResponse,
    },
    vote::RaftLeaderId,
    OptionalSend,
};
use std::future::Future;

pub(crate) fn vote(value: &multiraft_core::typ::Vote) -> VoteObservation {
    VoteObservation::new(
        value.leader_id().term(),
        *value.leader_id().node_id(),
        value.committed,
    )
}
pub(crate) struct ObservedFactory<F> {
    inner: F,
    group: GroupId,
    source: Option<Arc<SourceHub>>,
}
impl<F> ObservedFactory<F> {
    pub(crate) fn new(inner: F, group: GroupId, source: Option<Arc<SourceHub>>) -> Self {
        Self {
            inner,
            group,
            source,
        }
    }
}
pub(crate) struct ObservedNetwork<N> {
    inner: N,
    group: GroupId,
    target: NodeId,
    source: Option<Arc<SourceHub>>,
}
impl<F: RaftNetworkFactory<TypeConfig>> RaftNetworkFactory<TypeConfig> for ObservedFactory<F> {
    type Network = ObservedNetwork<F::Network>;
    async fn new_client(&mut self, target: NodeId, node: &openraft::BasicNode) -> Self::Network {
        ObservedNetwork {
            inner: self.inner.new_client(target, node).await,
            group: self.group,
            target,
            source: self.source.clone(),
        }
    }
}
impl<N: NetAppend<TypeConfig>> NetAppend<TypeConfig> for ObservedNetwork<N> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.inner.append_entries(rpc, option).await
    }
}
impl<N: NetVote<TypeConfig>> NetVote<TypeConfig> for ObservedNetwork<N> {
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let attempt = self.source.as_ref().map(|hub| {
            hub.begin(
                self.group,
                ElectionSourceEvent::VoteRpcStarted {
                    target: self.target,
                    request: VoteRpcRequest {
                        vote: vote(&rpc.vote),
                        last_log_id: rpc.last_log_id.as_ref().map(normalize_log_id),
                        leadership_transfer: rpc.leadership_transfer,
                    },
                },
            )
        });
        let result = self.inner.vote(rpc, option).await;
        if let Some(attempt) = attempt {
            attempt.finish(ElectionSourceEvent::VoteRpcFinished {
                target: self.target,
                response: match &result {
                    Ok(response) => SourceFact::Known(VoteRpcResponse {
                        vote: vote(&response.vote),
                        last_log_id: response.last_log_id.as_ref().map(normalize_log_id),
                        vote_granted: response.vote_granted,
                        native_consumed: SourceFact::Unknown(SourceUnknown::NotObserved),
                    }),
                    Err(error) => {
                        SourceFact::Unknown(SourceUnknown::RpcFailure(rpc_failure(error)))
                    }
                },
            });
        }
        result
    }
    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        // Preserve the underlying alpha30 behavior; never label a synthetic
        // default grant as an observed remote PreVote.
        self.inner.pre_vote(rpc, option).await
    }
}
impl<N: NetSnapshot<TypeConfig>> NetSnapshot<TypeConfig> for ObservedNetwork<N> {
    type SnapshotData = N::SnapshotData;
    async fn full_snapshot(
        &mut self,
        vote: multiraft_core::typ::Vote,
        snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        self.inner
            .full_snapshot(vote, snapshot, cancel, option)
            .await
    }
}
impl<N: NetStreamAppend<TypeConfig>> NetStreamAppend<TypeConfig> for ObservedNetwork<N> {
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> BoxFuture<
        's,
        Result<
            BoxStream<'s, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
            RPCError<TypeConfig>,
        >,
    >
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
    {
        self.inner.stream_append(input, option)
    }
}
impl<N: NetTransferLeader<TypeConfig>> NetTransferLeader<TypeConfig> for ObservedNetwork<N> {
    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        let attempt = self.source.as_ref().map(|hub| {
            hub.begin(
                self.group,
                ElectionSourceEvent::TransferRpcStarted {
                    target: self.target,
                    request: TransferRpcRequest {
                        from_vote: vote(req.from_leader()),
                        to_node_id: *req.to_node_id(),
                        required_log_id: req.last_log_id().map(normalize_log_id),
                    },
                },
            )
        });
        let result = self.inner.transfer_leader(req, option).await;
        if let Some(attempt) = attempt {
            let projection = match &result {
                Ok(Ok(())) => SourceFact::Known(TransferRpcResult::Accepted),
                Ok(Err(TransferLeaderError::VoteChanged { expected, actual })) => {
                    SourceFact::Known(TransferRpcResult::VoteChanged {
                        expected: vote(expected),
                        actual: vote(actual),
                    })
                }
                Ok(Err(TransferLeaderError::LogNotFlushed { expected, actual })) => {
                    SourceFact::Known(TransferRpcResult::LogNotFlushed {
                        expected: expected.as_ref().map(normalize_log_id),
                        actual: actual.as_ref().map(normalize_log_id),
                    })
                }
                Err(error) => SourceFact::Unknown(SourceUnknown::RpcFailure(rpc_failure(error))),
            };
            attempt.finish(ElectionSourceEvent::TransferRpcFinished {
                target: self.target,
                result: projection,
            });
        }
        result
    }
}
impl<N: NetBackoff<TypeConfig>> NetBackoff<TypeConfig> for ObservedNetwork<N> {
    fn backoff(&self) -> Option<Backoff> {
        self.inner.backoff()
    }
}

fn rpc_failure(error: &RPCError<TypeConfig>) -> SourceRpcFailure {
    match error {
        RPCError::Timeout(_) => SourceRpcFailure::Timeout,
        RPCError::Unreachable(_) => SourceRpcFailure::Unreachable,
        RPCError::Network(_) => SourceRpcFailure::Network,
        RPCError::RemoteError(_) => SourceRpcFailure::Remote,
    }
}
