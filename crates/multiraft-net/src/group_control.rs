//! Normative public group-control contracts. Native behavior is feature-owned.
use crate::{GroupServerState, MembershipObservation, ObservedLogId, VoteObservation};
use multiraft_core::{GroupId, NativeFailure, NodeId, ReadIndexFailure};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::Duration,
};
pub(crate) mod logging;
mod precheck;
mod sampling;
mod transfer;
pub use sampling::read_group_control_sample;
pub use transfer::classify_group_control_layout;
pub(crate) use transfer::submit_group_control_transfer;
#[cfg(test)]
use transfer::submit_transfer_from_sample;
#[cfg(test)]
mod tests;

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupControlSample {
    pub group_id: GroupId,
    pub local_node_id: NodeId,
    pub leader_id: NodeId,
    pub server_state: GroupServerState,
    pub flushed_vote: VoteObservation,
    pub effective_membership: MembershipObservation,
    pub committed_membership: MembershipObservation,
    pub read_log_id: Option<ObservedLogId>,
    pub local_committed: Option<ObservedLogId>,
    pub target_qualifications: BTreeMap<NodeId, TargetQualification>,
    pub sample_age: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetQualification {
    Source,
    Qualified {
        ack_age: Duration,
        matched: ObservedLogId,
    },
    NotVoter,
    MissingAck,
    AckTooOld {
        age: Duration,
        max: Duration,
    },
    MissingLocalCommitted,
    MissingMatched,
    Lagging {
        matched: ObservedLogId,
        required: ObservedLogId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupControlRequestEcho {
    pub group_id: GroupId,
    pub source: NodeId,
    pub target: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupControlPreconditions {
    pub group_id: GroupId,
    pub expected_source: NodeId,
    pub observed_vote: VoteObservation,
    pub effective_membership: MembershipObservation,
    pub committed_membership: MembershipObservation,
    pub target: NodeId,
}

impl GroupControlPreconditions {
    pub fn echo(&self) -> GroupControlRequestEcho {
        GroupControlRequestEcho {
            group_id: self.group_id,
            source: self.expected_source,
            target: self.target,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipChangeKind {
    Effective,
    Committed,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupControlPrecheckError {
    GroupChanged {
        expected: GroupId,
        actual: GroupId,
    },
    SourceChanged {
        expected: NodeId,
        actual: NodeId,
    },
    VoteChanged {
        expected: VoteObservation,
        actual: VoteObservation,
    },
    MembershipChanged {
        kind: MembershipChangeKind,
        expected: Box<MembershipObservation>,
        actual: Box<MembershipObservation>,
    },
    TargetNotVoter {
        target: NodeId,
    },
    TargetIsSource {
        target: NodeId,
    },
    TargetNotRecentlyAcked {
        target: NodeId,
        qualification: TargetQualification,
    },
    TargetProgressUnavailable {
        target: NodeId,
        qualification: TargetQualification,
    },
    TargetLagging {
        target: NodeId,
        matched: ObservedLogId,
        required: ObservedLogId,
    },
}

impl fmt::Display for GroupControlPrecheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupChanged { expected, actual } => {
                write!(
                    formatter,
                    "group changed before trigger: {expected} != {actual}"
                )
            }
            Self::SourceChanged { expected, actual } => {
                write!(
                    formatter,
                    "source changed before trigger: {expected} != {actual}"
                )
            }
            Self::VoteChanged { expected, actual } => {
                write!(
                    formatter,
                    "vote changed before trigger: {expected:?} != {actual:?}"
                )
            }
            Self::MembershipChanged { kind, .. } => {
                write!(formatter, "{kind:?} membership changed before trigger")
            }
            Self::TargetNotVoter { target } => {
                write!(formatter, "target {target} is not a fixed voter")
            }
            Self::TargetIsSource { target } => {
                write!(formatter, "target {target} is the current source")
            }
            Self::TargetNotRecentlyAcked {
                target,
                qualification,
            } => {
                write!(
                    formatter,
                    "target {target} is not recently acknowledged: {qualification:?}"
                )
            }
            Self::TargetProgressUnavailable {
                target,
                qualification,
            } => {
                write!(
                    formatter,
                    "target {target} progress is unavailable: {qualification:?}"
                )
            }
            Self::TargetLagging {
                target,
                matched,
                required,
            } => {
                write!(
                    formatter,
                    "target {target} is lagging: matched {matched:?}, required {required:?}"
                )
            }
        }
    }
}

impl std::error::Error for GroupControlPrecheckError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupControlPrecheckRejection {
    Sample(GroupControlSampleError),
    Preconditions(GroupControlPrecheckError),
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupControlRequestResult {
    PrecheckRejected {
        echo: GroupControlRequestEcho,
        reason: GroupControlPrecheckRejection,
    },
    NotSubmitted {
        echo: GroupControlRequestEcho,
        reason: ControlSubmissionError,
    },
    TriggerQueued {
        echo: GroupControlRequestEcho,
    },
    OutcomeUnknown {
        echo: GroupControlRequestEcho,
        reason: ControlSubmissionError,
    },
}

impl GroupControlRequestResult {
    pub fn outcome_unknown(echo: GroupControlRequestEcho, reason: ControlSubmissionError) -> Self {
        Self::OutcomeUnknown { echo, reason }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupControlLayoutObservation {
    TargetObserved {
        echo: GroupControlRequestEcho,
        observed_leader: NodeId,
        observed_vote: VoteObservation,
        sample_age: Duration,
    },
    DifferentLeaderObserved {
        echo: GroupControlRequestEcho,
        observed_leader: NodeId,
        observed_vote: VoteObservation,
        sample_age: Duration,
    },
    SourceObserved {
        echo: GroupControlRequestEcho,
        observed_leader: NodeId,
        observed_vote: VoteObservation,
        sample_age: Duration,
    },
    Unavailable {
        echo: GroupControlRequestEcho,
        reason: GroupControlSampleError,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupControlSampleError {
    Busy {
        group_id: GroupId,
    },
    Deadline {
        group_id: GroupId,
        stage: ControlStage,
    },
    WrongNode {
        group_id: GroupId,
        expected: NodeId,
        actual: NodeId,
    },
    InvalidOptions {
        group_id: GroupId,
    },
    UnknownGroup {
        group_id: GroupId,
    },
    Closed {
        group_id: GroupId,
    },
    NotLeader {
        group_id: GroupId,
        local_node_id: NodeId,
        leader_hint: Option<NodeId>,
    },
    ReadIndexFailed {
        group_id: GroupId,
        source: ReadIndexFailure,
    },
    InconsistentIdentity {
        group_id: GroupId,
        evidence: Box<ControlIdentityMismatch>,
    },
    UnexpectedMembership {
        group_id: GroupId,
        expected_voters: BTreeSet<NodeId>,
        effective_membership: Box<MembershipObservation>,
        committed_membership: Box<MembershipObservation>,
    },
    SampleTooOld {
        group_id: GroupId,
        elapsed: Duration,
        max: Duration,
    },
}

impl fmt::Display for GroupControlSampleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy { .. } => formatter.write_str("control slot busy"),
            Self::Deadline { .. } => formatter.write_str("control deadline expired"),
            Self::WrongNode { .. } => {
                formatter.write_str("control source does not match local node")
            }
            Self::InvalidOptions { .. } => formatter.write_str("invalid control freshness options"),
            Self::UnknownGroup { group_id } => {
                write!(formatter, "unknown group {group_id}")
            }
            Self::Closed { group_id } => {
                write!(formatter, "group {group_id} control sample is closed")
            }
            Self::NotLeader {
                group_id,
                local_node_id,
                leader_hint,
            } => {
                write!(
                    formatter,
                    "group {group_id} local node {local_node_id} is not leader; hint={leader_hint:?}"
                )
            }
            Self::ReadIndexFailed { group_id, source } => {
                write!(formatter, "group {group_id} ReadIndex failed: {source}")
            }
            Self::InconsistentIdentity { group_id, .. } => {
                write!(
                    formatter,
                    "group {group_id} control sample identity changed"
                )
            }
            Self::UnexpectedMembership {
                group_id,
                expected_voters,
                ..
            } => {
                write!(
                    formatter,
                    "group {group_id} membership is not the expected fixed voter set {expected_voters:?}"
                )
            }
            Self::SampleTooOld {
                group_id,
                elapsed,
                max,
            } => {
                write!(
                    formatter,
                    "group {group_id} control sample is too old: {elapsed:?} >= {max:?}"
                )
            }
        }
    }
}

impl std::error::Error for GroupControlSampleError {}

/// Transient caller-supplied correlation; never a persisted operation or replay key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlInvocationId(pub [u8; 16]);
impl ControlInvocationId {
    /// Generates a process-local diagnostic identity for legacy facade callers.
    pub fn fresh() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let mut bytes = [0; 16];
        let boot = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        bytes[..8].copy_from_slice(&boot.to_be_bytes());
        bytes[8..].copy_from_slice(
            &NEXT
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .to_be_bytes(),
        );
        Self(bytes)
    }
}
impl fmt::Display for ControlInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
/// A single inherited absolute deadline and freshness policy for an invocation.
#[derive(Debug, Clone, Copy)]
pub struct ControlContext {
    pub invocation_id: ControlInvocationId,
    pub deadline: tokio::time::Instant,
    pub max_sample_age: Duration,
    pub max_target_ack_age: Duration,
}
impl ControlContext {
    pub fn new(invocation_id: ControlInvocationId, deadline: tokio::time::Instant) -> Self {
        Self {
            invocation_id,
            deadline,
            max_sample_age: Duration::from_secs(1),
            max_target_ack_age: Duration::from_secs(1),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlStage {
    Admission,
    Sampling,
    Precheck,
    Trigger,
    Layout,
}
/// Only source-known reasons are classified. No arbitrary native error strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSubmissionError {
    Stopped,
    Backend(NativeFailure),
    Deadline,
    Cancelled,
    Closed,
    Unknown,
}
/// Full source evidence for an inconsistent sample, without string parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlIdentityMismatch {
    Group {
        expected: GroupId,
        actual: GroupId,
    },
    State {
        expected: GroupServerState,
        observed: GroupServerState,
    },
    Membership {
        kind: MembershipChangeKind,
        expected: MembershipObservation,
        observed: MembershipObservation,
    },
    Vote {
        expected_node: NodeId,
        observed: VoteObservation,
    },
}
/// Fresh sample is retained even when a precondition rejects the attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlTransferOutcome {
    pub invocation_id: ControlInvocationId,
    pub local_node_id: NodeId,
    pub result: GroupControlRequestResult,
    pub sample: Option<GroupControlSample>,
}
