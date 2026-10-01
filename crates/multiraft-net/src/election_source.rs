//! Bounded source facts, not campaign inference or consensus authority.
//!
//! Public alpha30 point reads and RPC boundaries are explicitly distinguished
//! from the optional source-native campaign/timer/consumed-quorum observer.
mod buffer;
mod native;
mod network;
mod state;

pub(crate) use buffer::SourceHub;
pub use buffer::{ElectionSource, ElectionSourceConfig, ElectionSourceReceiver};
pub(crate) use native::NativeSourceObserver;
pub use native::{
    AutomaticElectionDecision, CampaignOrigin, CampaignPhase, NativeElectionFact,
    NativeElectionKind, NativeElectionTiming, VoteRequestDisposition, VoteResponseDisposition,
};
pub(crate) use network::ObservedFactory;
pub(crate) use state::read_state;

use crate::{GroupServerState, MembershipObservation, ObservedLogId, VoteObservation};
use multiraft_core::{GroupId, InitializeDisposition, NodeId};
use std::{sync::Arc, time::Duration};

/// A missing fact never means zero, false, or successful native processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceFact<T> {
    Known(T),
    Unknown(SourceUnknown),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceUnknown {
    NativeNotExposed,
    PublicPointNotExposed,
    NotObserved,
    MembershipLimit,
    RequestFailed,
    RpcFailure(SourceRpcFailure),
    Cancelled,
}

/// Typed transport failure category, without arbitrary error text or addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceRpcFailure {
    Timeout,
    Unreachable,
    Network,
    Remote,
}

/// Capability declaration for the exact observer implementation, including gaps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionSourceCapabilities {
    pub native_origin: SourceFact<()>,
    pub native_round: SourceFact<()>,
    pub actual_random_timeout: SourceFact<()>,
    pub lease_enabled_and_duration: SourceFact<()>,
    pub greater_log: SourceFact<()>,
    pub native_consumed_quorum: SourceFact<()>,
    /// Point reads are serialized in RaftCore, not a per-renewal event stream.
    pub vote_last_modified_point: bool,
    /// Outbound RPC facts do not prove receipt/consumption by RaftCore.
    pub outbound_vote_rpc: bool,
    /// Whether the capability is limited to public points rather than native events.
    pub state_points_only: bool,
}
impl Default for ElectionSourceCapabilities {
    fn default() -> Self {
        Self {
            native_origin: SourceFact::Known(()),
            native_round: SourceFact::Known(()),
            actual_random_timeout: SourceFact::Known(()),
            lease_enabled_and_duration: SourceFact::Known(()),
            greater_log: SourceFact::Known(()),
            native_consumed_quorum: SourceFact::Known(()),
            vote_last_modified_point: true,
            outbound_vote_rpc: true,
            state_points_only: false,
        }
    }
}

/// A local monotonic sample. Ages cannot be compared as cross-node timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionStatePoint {
    /// Exact source clock offset captured inside the serialized native callback.
    pub state_sample_elapsed: Duration,
    pub vote: VoteObservation,
    pub vote_last_modified_age: Option<Duration>,
    pub role: GroupServerState,
    pub effective_membership: SourceFact<MembershipObservation>,
    pub committed_membership: SourceFact<MembershipObservation>,
    pub local_committed: Option<ObservedLogId>,
    pub cluster_committed: Option<ObservedLogId>,
    pub actual_random_timeout: SourceFact<Duration>,
    pub lease_enabled: SourceFact<bool>,
    pub lease_duration: SourceFact<Duration>,
    pub greater_log: SourceFact<bool>,
    /// Separate latest metrics sample: committed AppendEntries ACK quorum age.
    /// This is not a RequestVote quorum or an authority token.
    pub committed_append_quorum_ack_age: Option<Duration>,
    pub ack_metrics_sample_elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRpcRequest {
    pub vote: VoteObservation,
    pub last_log_id: Option<ObservedLogId>,
    pub leadership_transfer: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRpcResponse {
    pub vote: VoteObservation,
    pub last_log_id: Option<ObservedLogId>,
    pub vote_granted: bool,
    pub native_consumed: SourceFact<bool>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferRpcRequest {
    pub from_vote: VoteObservation,
    pub to_node_id: NodeId,
    pub required_log_id: Option<ObservedLogId>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferRpcResult {
    Accepted,
    VoteChanged {
        expected: VoteObservation,
        actual: VoteObservation,
    },
    LogNotFlushed {
        expected: Option<ObservedLogId>,
        actual: Option<ObservedLogId>,
    },
}

/// Actual source boundaries. No variant is inferred from a term/role delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionSourceEvent {
    Native {
        event: NativeElectionFact,
    },
    Attached {
        capabilities: ElectionSourceCapabilities,
    },
    InitializeStarted,
    InitializeFinished {
        disposition: InitializeDisposition,
    },
    VoteRpcStarted {
        target: NodeId,
        request: VoteRpcRequest,
    },
    VoteRpcFinished {
        target: NodeId,
        response: SourceFact<VoteRpcResponse>,
    },
    TransferRpcStarted {
        target: NodeId,
        request: TransferRpcRequest,
    },
    TransferRpcFinished {
        target: NodeId,
        result: SourceFact<TransferRpcResult>,
    },
    StateRequested,
    StatePoint {
        point: Box<ElectionStatePoint>,
    },
    StateRequestFailed,
    /// Drop after dispatch; this does not retract a request already sent.
    AttemptCancelled,
    /// Native tasks, transport and FSM release have actually completed.
    Closed,
}

/// Identities are supplied by the runtime consumer and bounded on construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionSourceRecord {
    pub run_id: Arc<str>,
    pub boot_id: Arc<str>,
    pub local_node_id: NodeId,
    pub group_id: Option<GroupId>,
    pub sequence: u64,
    pub local_elapsed: Duration,
    /// Node-local source operation identity; not a native campaign round.
    pub attempt_id: Option<u64>,
    pub native_round: SourceFact<u64>,
    pub event: ElectionSourceEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionSourceStatus {
    pub attached_node: Option<NodeId>,
    pub capacity: usize,
    pub retained: usize,
    pub last_sequence: u64,
    /// Records evicted globally from the bounded buffer; not receiver loss.
    pub evicted: u64,
    /// Native callbacks observed, including those lost to nonblocking contention.
    pub native_received: u64,
    /// Native callbacks not admitted because the bounded ingress was full/closed.
    pub native_dropped: u64,
    /// Accepted native metadata waiting for ordinary ring-owner admission.
    pub native_pending: usize,
    pub active_attempts: usize,
    pub receivers: usize,
    pub closed: bool,
    pub capabilities: ElectionSourceCapabilities,
}

/// One read from the stream. Lag is reported before the next retained record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionSourceRead {
    NativeDropped {
        total: u64,
        new_since_previous: u64,
    },
    Record(Box<ElectionSourceRecord>),
    Lagged {
        first_missing: u64,
        last_missing: u64,
    },
    Closed,
}
