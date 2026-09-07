//! Best-effort local group-control sampling.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::time::Duration;
use std::time::Instant;

use multiraft_core::GroupId;
use multiraft_core::NodeId;
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::async_runtime::WatchReceiver as _;
use openraft::vote::RaftLeaderId;
use openraft::Instant as _;
use openraft::ReadPolicy;

use crate::group_observation::normalize_log_id;
use crate::group_observation::normalize_membership;
use crate::group_observation::normalize_server_state;
use crate::GroupServerState;
use crate::MembershipObservation;
use crate::ObservedLogId;
use crate::VoteObservation;

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

impl GroupControlSample {
    pub fn observed_preconditions_for(&self, target: NodeId) -> GroupControlPreconditions {
        GroupControlPreconditions {
            group_id: self.group_id,
            expected_source: self.leader_id,
            observed_vote: self.flushed_vote,
            effective_membership: self.effective_membership.clone(),
            committed_membership: self.committed_membership.clone(),
            target,
        }
    }

    pub fn check_transfer_preconditions(
        &self,
        preconditions: &GroupControlPreconditions,
    ) -> Result<(), GroupControlPrecheckError> {
        if preconditions.group_id != self.group_id {
            return Err(GroupControlPrecheckError::GroupChanged {
                expected: preconditions.group_id,
                actual: self.group_id,
            });
        }
        if preconditions.expected_source != self.leader_id {
            return Err(GroupControlPrecheckError::SourceChanged {
                expected: preconditions.expected_source,
                actual: self.leader_id,
            });
        }
        if preconditions.observed_vote != self.flushed_vote {
            return Err(GroupControlPrecheckError::VoteChanged {
                expected: preconditions.observed_vote,
                actual: self.flushed_vote,
            });
        }
        if preconditions.effective_membership != self.effective_membership {
            return Err(GroupControlPrecheckError::MembershipChanged {
                kind: MembershipChangeKind::Effective,
                expected: preconditions.effective_membership.clone(),
                actual: self.effective_membership.clone(),
            });
        }
        if preconditions.committed_membership != self.committed_membership {
            return Err(GroupControlPrecheckError::MembershipChanged {
                kind: MembershipChangeKind::Committed,
                expected: preconditions.committed_membership.clone(),
                actual: self.committed_membership.clone(),
            });
        }

        match self.target_qualifications.get(&preconditions.target) {
            Some(TargetQualification::Qualified { .. }) => Ok(()),
            Some(TargetQualification::Source) => Err(GroupControlPrecheckError::TargetIsSource {
                target: preconditions.target,
            }),
            Some(
                qualification @ (TargetQualification::MissingAck
                | TargetQualification::AckTooOld { .. }),
            ) => Err(GroupControlPrecheckError::TargetNotRecentlyAcked {
                target: preconditions.target,
                qualification: *qualification,
            }),
            Some(
                qualification @ (TargetQualification::MissingLocalCommitted
                | TargetQualification::MissingMatched),
            ) => Err(GroupControlPrecheckError::TargetProgressUnavailable {
                target: preconditions.target,
                qualification: *qualification,
            }),
            Some(TargetQualification::Lagging { matched, required }) => {
                Err(GroupControlPrecheckError::TargetLagging {
                    target: preconditions.target,
                    matched: *matched,
                    required: *required,
                })
            }
            Some(TargetQualification::NotVoter) | None => {
                Err(GroupControlPrecheckError::TargetNotVoter {
                    target: preconditions.target,
                })
            }
        }
    }
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
        expected: MembershipObservation,
        actual: MembershipObservation,
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
        reason: String,
    },
    TriggerQueued {
        echo: GroupControlRequestEcho,
    },
    OutcomeUnknown {
        echo: GroupControlRequestEcho,
        reason: String,
    },
}

impl GroupControlRequestResult {
    pub fn outcome_unknown(echo: GroupControlRequestEcho, reason: String) -> Self {
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
        reason: String,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupControlSampleError {
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
        reason: String,
    },
    InconsistentIdentity {
        group_id: GroupId,
        reason: String,
    },
    UnexpectedMembership {
        group_id: GroupId,
        expected_voters: BTreeSet<NodeId>,
        effective_membership: MembershipObservation,
        committed_membership: MembershipObservation,
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
            Self::ReadIndexFailed { group_id, reason } => {
                write!(formatter, "group {group_id} ReadIndex failed: {reason}")
            }
            Self::InconsistentIdentity { group_id, reason } => {
                write!(
                    formatter,
                    "group {group_id} control sample identity changed: {reason}"
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct StatePoint {
    server_state: GroupServerState,
    effective_membership: MembershipObservation,
    committed_membership: MembershipObservation,
    local_committed: Option<ObservedLogId>,
}

pub(crate) async fn read_group_control_sample<S: StateMachine>(
    raft: &Raft<S>,
    group_id: GroupId,
    local_node_id: NodeId,
    expected_voters: &[NodeId],
    max_sample_age: Duration,
    max_target_ack_age: Duration,
) -> Result<GroupControlSample, GroupControlSampleError> {
    let started = Instant::now();
    let state0 = read_state_point(raft, group_id).await?;
    let read_log_id = match raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
        Ok(log_id) => log_id.as_ref().map(normalize_log_id),
        Err(error) => {
            if let Some(forward) = error.forward_to_leader() {
                return Err(GroupControlSampleError::NotLeader {
                    group_id,
                    local_node_id,
                    leader_hint: forward.leader_id,
                });
            }
            return Err(GroupControlSampleError::ReadIndexFailed {
                group_id,
                reason: error.to_string(),
            });
        }
    };
    let metrics = raft.metrics().borrow_watched().clone();
    if metrics.running_state.is_err() {
        return Err(GroupControlSampleError::Closed { group_id });
    }
    let state1 = read_state_point(raft, group_id).await?;

    ensure_state_identity_matches(group_id, "point reads", &state0, &state1)?;

    let metrics_state = normalize_server_state(group_id, metrics.state)
        .map_err(|_| GroupControlSampleError::Closed { group_id })?;
    let metrics_effective = normalize_membership(metrics.membership_config.as_ref());
    let metrics_committed = normalize_membership(metrics.committed_membership_config.as_ref());
    ensure_metrics_identity_matches(
        group_id,
        &state1,
        metrics_state,
        &metrics_effective,
        &metrics_committed,
    )?;

    if metrics_state != GroupServerState::Leader || metrics.current_leader != Some(local_node_id) {
        return Err(GroupControlSampleError::NotLeader {
            group_id,
            local_node_id,
            leader_hint: metrics.current_leader,
        });
    }

    let vote = VoteObservation {
        term: metrics.vote.leader_id().term(),
        node_id: *metrics.vote.leader_id().node_id(),
        committed: metrics.vote.committed,
    };
    if vote.node_id != local_node_id || !vote.committed {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            reason: format!(
                "flushed vote {:?} does not match local leader {}",
                vote, local_node_id
            ),
        });
    }

    let expected_voters = expected_voters.iter().copied().collect::<BTreeSet<_>>();
    ensure_expected_membership(
        group_id,
        &expected_voters,
        &state1.effective_membership,
        &state1.committed_membership,
    )?;

    let target_qualifications = classify_target_qualifications(
        &metrics,
        &expected_voters,
        local_node_id,
        state1.local_committed,
        max_target_ack_age,
    );

    let elapsed = started.elapsed();
    if max_sample_age.is_zero() || elapsed >= max_sample_age {
        return Err(GroupControlSampleError::SampleTooOld {
            group_id,
            elapsed,
            max: max_sample_age,
        });
    }

    Ok(GroupControlSample {
        group_id,
        local_node_id,
        leader_id: local_node_id,
        server_state: metrics_state,
        flushed_vote: vote,
        effective_membership: state1.effective_membership,
        committed_membership: state1.committed_membership,
        read_log_id,
        local_committed: state1.local_committed,
        target_qualifications,
        sample_age: elapsed,
    })
}

pub(crate) async fn submit_group_control_transfer<S: StateMachine>(
    raft: &Raft<S>,
    group_id: GroupId,
    local_node_id: NodeId,
    preconditions: &GroupControlPreconditions,
    expected_voters: &[NodeId],
    max_sample_age: Duration,
    max_target_ack_age: Duration,
) -> GroupControlRequestResult {
    let sample_result = read_group_control_sample(
        raft,
        group_id,
        local_node_id,
        expected_voters,
        max_sample_age,
        max_target_ack_age,
    )
    .await;
    submit_transfer_from_sample(preconditions, sample_result, |target| async move {
        raft.trigger()
            .transfer_leader(target)
            .await
            .map_err(|error| error.to_string())
    })
    .await
}

pub(crate) async fn submit_transfer_from_sample<F, Fut>(
    preconditions: &GroupControlPreconditions,
    sample_result: Result<GroupControlSample, GroupControlSampleError>,
    trigger: F,
) -> GroupControlRequestResult
where
    F: FnOnce(NodeId) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let echo = preconditions.echo();
    let sample = match sample_result {
        Ok(sample) => sample,
        Err(error) => {
            return GroupControlRequestResult::PrecheckRejected {
                echo,
                reason: GroupControlPrecheckRejection::Sample(error),
            };
        }
    };

    if let Err(error) = sample.check_transfer_preconditions(preconditions) {
        return GroupControlRequestResult::PrecheckRejected {
            echo,
            reason: GroupControlPrecheckRejection::Preconditions(error),
        };
    }

    match trigger(preconditions.target).await {
        Ok(()) => GroupControlRequestResult::TriggerQueued { echo },
        Err(reason) => GroupControlRequestResult::NotSubmitted { echo, reason },
    }
}

pub fn classify_group_control_layout(
    echo: GroupControlRequestEcho,
    sample_result: Result<&GroupControlSample, &GroupControlSampleError>,
) -> GroupControlLayoutObservation {
    let sample = match sample_result {
        Ok(sample) => sample,
        Err(error) => {
            return GroupControlLayoutObservation::Unavailable {
                echo,
                reason: error.to_string(),
            };
        }
    };
    if sample.leader_id == echo.target {
        return GroupControlLayoutObservation::TargetObserved {
            echo,
            observed_leader: sample.leader_id,
            observed_vote: sample.flushed_vote,
            sample_age: sample.sample_age,
        };
    }
    if sample.leader_id == echo.source {
        return GroupControlLayoutObservation::SourceObserved {
            echo,
            observed_leader: sample.leader_id,
            observed_vote: sample.flushed_vote,
            sample_age: sample.sample_age,
        };
    }
    GroupControlLayoutObservation::DifferentLeaderObserved {
        echo,
        observed_leader: sample.leader_id,
        observed_vote: sample.flushed_vote,
        sample_age: sample.sample_age,
    }
}

async fn read_state_point<S: StateMachine>(
    raft: &Raft<S>,
    group_id: GroupId,
) -> Result<StatePoint, GroupControlSampleError> {
    raft.with_raft_state(move |state| {
        let server_state = normalize_server_state(group_id, state.server_state)
            .map_err(|_| GroupControlSampleError::Closed { group_id })?;
        Ok(StatePoint {
            server_state,
            effective_membership: normalize_membership(state.membership_state.effective().as_ref()),
            committed_membership: normalize_membership(state.membership_state.committed().as_ref()),
            local_committed: state.local_committed().map(normalize_log_id),
        })
    })
    .await
    .map_err(|_| GroupControlSampleError::Closed { group_id })?
}

fn ensure_state_identity_matches(
    group_id: GroupId,
    label: &str,
    first: &StatePoint,
    second: &StatePoint,
) -> Result<(), GroupControlSampleError> {
    if first.server_state != second.server_state {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            reason: format!(
                "{label} server state differs: {:?} != {:?}",
                first.server_state, second.server_state
            ),
        });
    }
    if first.effective_membership != second.effective_membership {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            reason: format!("{label} effective membership differs"),
        });
    }
    if first.committed_membership != second.committed_membership {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            reason: format!("{label} committed membership differs"),
        });
    }
    Ok(())
}

fn ensure_metrics_identity_matches(
    group_id: GroupId,
    state: &StatePoint,
    metrics_state: GroupServerState,
    metrics_effective: &MembershipObservation,
    metrics_committed: &MembershipObservation,
) -> Result<(), GroupControlSampleError> {
    if state.server_state != metrics_state {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            reason: format!(
                "metrics server state differs: {:?} != {:?}",
                state.server_state, metrics_state
            ),
        });
    }
    if &state.effective_membership != metrics_effective {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            reason: "metrics effective membership differs".to_string(),
        });
    }
    if &state.committed_membership != metrics_committed {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            reason: "metrics committed membership differs".to_string(),
        });
    }
    Ok(())
}

fn ensure_expected_membership(
    group_id: GroupId,
    expected_voters: &BTreeSet<NodeId>,
    effective: &MembershipObservation,
    committed: &MembershipObservation,
) -> Result<(), GroupControlSampleError> {
    if is_expected_fixed_voter_membership(effective, expected_voters)
        && is_expected_fixed_voter_membership(committed, expected_voters)
    {
        return Ok(());
    }

    Err(GroupControlSampleError::UnexpectedMembership {
        group_id,
        expected_voters: expected_voters.clone(),
        effective_membership: effective.clone(),
        committed_membership: committed.clone(),
    })
}

fn is_expected_fixed_voter_membership(
    membership: &MembershipObservation,
    expected_voters: &BTreeSet<NodeId>,
) -> bool {
    membership.voter_configs.len() == 1
        && membership.voter_configs.first() == Some(expected_voters)
        && membership.learner_ids.is_empty()
}

fn classify_target_qualifications(
    metrics: &openraft::RaftMetrics<multiraft_core::TypeConfig>,
    expected_voters: &BTreeSet<NodeId>,
    local_node_id: NodeId,
    local_committed: Option<ObservedLogId>,
    max_target_ack_age: Duration,
) -> BTreeMap<NodeId, TargetQualification> {
    expected_voters
        .iter()
        .map(|&target| {
            let qualification = if target == local_node_id {
                TargetQualification::Source
            } else {
                classify_one_target(metrics, target, local_committed, max_target_ack_age)
            };
            (target, qualification)
        })
        .collect()
}

fn classify_one_target(
    metrics: &openraft::RaftMetrics<multiraft_core::TypeConfig>,
    target: NodeId,
    local_committed: Option<ObservedLogId>,
    max_target_ack_age: Duration,
) -> TargetQualification {
    let Some(heartbeat) = metrics.heartbeat.as_ref() else {
        return TargetQualification::MissingAck;
    };
    let Some(Some(ack)) = heartbeat.get(&target) else {
        return TargetQualification::MissingAck;
    };
    let ack_age = ack.into_inner().elapsed();
    if max_target_ack_age.is_zero() || ack_age >= max_target_ack_age {
        return TargetQualification::AckTooOld {
            age: ack_age,
            max: max_target_ack_age,
        };
    }

    let Some(required) = local_committed else {
        return TargetQualification::MissingLocalCommitted;
    };
    let Some(replication) = metrics.replication.as_ref() else {
        return TargetQualification::MissingMatched;
    };
    let Some(Some(matched)) = replication.get(&target) else {
        return TargetQualification::MissingMatched;
    };
    let matched = normalize_log_id(matched);
    if covers_log_id(matched, required) {
        TargetQualification::Qualified { ack_age, matched }
    } else {
        TargetQualification::Lagging { matched, required }
    }
}

fn covers_log_id(matched: ObservedLogId, required: ObservedLogId) -> bool {
    matched.term == required.term
        && matched.node_id == required.node_id
        && matched.index >= required.index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    fn log_id(term: u64, node_id: u64, index: u64) -> ObservedLogId {
        ObservedLogId {
            term,
            node_id,
            index,
        }
    }

    fn fixed_membership(voters: &[u64]) -> MembershipObservation {
        MembershipObservation {
            log_id: Some(log_id(1, 1, 0)),
            voter_configs: vec![set(voters)],
            learner_ids: BTreeSet::new(),
        }
    }

    fn synthetic_sample(target_2: TargetQualification) -> GroupControlSample {
        let membership = fixed_membership(&[1, 2, 3]);
        GroupControlSample {
            group_id: 10,
            local_node_id: 1,
            leader_id: 1,
            server_state: GroupServerState::Leader,
            flushed_vote: VoteObservation {
                term: 3,
                node_id: 1,
                committed: true,
            },
            effective_membership: membership.clone(),
            committed_membership: membership,
            read_log_id: Some(log_id(3, 1, 7)),
            local_committed: Some(log_id(3, 1, 7)),
            target_qualifications: BTreeMap::from([
                (1, TargetQualification::Source),
                (2, target_2),
                (
                    3,
                    TargetQualification::Qualified {
                        ack_age: Duration::from_millis(5),
                        matched: log_id(3, 1, 7),
                    },
                ),
            ]),
            sample_age: Duration::from_millis(1),
        }
    }

    #[test]
    fn precheck_rejects_changed_observed_identity_before_trigger() {
        let sample = synthetic_sample(TargetQualification::Qualified {
            ack_age: Duration::from_millis(5),
            matched: log_id(3, 1, 7),
        });
        let mut preconditions = sample.observed_preconditions_for(2);
        preconditions.expected_source = 3;

        let err = sample
            .check_transfer_preconditions(&preconditions)
            .expect_err("changed source must reject before trigger");

        assert!(matches!(
            err,
            GroupControlPrecheckError::SourceChanged { .. }
        ));

        let mut preconditions = sample.observed_preconditions_for(2);
        preconditions.observed_vote.term -= 1;

        let err = sample
            .check_transfer_preconditions(&preconditions)
            .expect_err("changed vote must reject before trigger");

        assert!(matches!(err, GroupControlPrecheckError::VoteChanged { .. }));

        let mut preconditions = sample.observed_preconditions_for(2);
        preconditions.effective_membership = fixed_membership(&[1, 2]);
        let err = sample
            .check_transfer_preconditions(&preconditions)
            .expect_err("changed membership must reject before trigger");
        assert!(matches!(
            err,
            GroupControlPrecheckError::MembershipChanged { .. }
        ));
    }

    #[test]
    fn precheck_rejects_missing_ack_before_trigger() {
        let sample = synthetic_sample(TargetQualification::MissingAck);
        let preconditions = sample.observed_preconditions_for(2);

        let err = sample
            .check_transfer_preconditions(&preconditions)
            .expect_err("missing ack must reject before trigger");

        assert!(matches!(
            err,
            GroupControlPrecheckError::TargetNotRecentlyAcked {
                target: 2,
                qualification: TargetQualification::MissingAck,
            }
        ));
    }

    #[test]
    fn precheck_uses_complete_log_identity_not_bare_index() {
        let sample = synthetic_sample(TargetQualification::Lagging {
            matched: log_id(2, 1, 99),
            required: log_id(3, 1, 7),
        });
        let preconditions = sample.observed_preconditions_for(2);

        let err = sample
            .check_transfer_preconditions(&preconditions)
            .expect_err("higher bare index with older term is still lagging");

        assert!(matches!(
            err,
            GroupControlPrecheckError::TargetLagging {
                target: 2,
                matched: ObservedLogId {
                    term: 2,
                    index: 99,
                    ..
                },
                required: ObservedLogId {
                    term: 3,
                    index: 7,
                    ..
                },
            }
        ));
    }

    #[test]
    fn precheck_rejects_non_voter_target_before_trigger() {
        let sample = synthetic_sample(TargetQualification::Qualified {
            ack_age: Duration::from_millis(5),
            matched: log_id(3, 1, 7),
        });
        let preconditions = GroupControlPreconditions {
            group_id: sample.group_id,
            expected_source: sample.leader_id,
            observed_vote: sample.flushed_vote,
            effective_membership: sample.effective_membership.clone(),
            committed_membership: sample.committed_membership.clone(),
            target: 9,
        };

        let err = sample
            .check_transfer_preconditions(&preconditions)
            .expect_err("non-voter target must reject before trigger");

        assert!(matches!(
            err,
            GroupControlPrecheckError::TargetNotVoter { target: 9, .. }
        ));
    }

    #[tokio::test]
    async fn submit_precheck_rejection_makes_zero_trigger_calls() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        use std::sync::Arc;

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_trigger = calls.clone();
        let sample = synthetic_sample(TargetQualification::MissingAck);
        let preconditions = sample.observed_preconditions_for(2);

        let result = submit_transfer_from_sample(&preconditions, Ok(sample), move |_| async move {
            calls_for_trigger.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

        assert!(matches!(
            result,
            GroupControlRequestResult::PrecheckRejected { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn submit_reports_not_submitted_only_for_definitive_send_failure() {
        let sample = synthetic_sample(TargetQualification::Qualified {
            ack_age: Duration::from_millis(5),
            matched: log_id(3, 1, 7),
        });
        let preconditions = sample.observed_preconditions_for(2);

        let result = submit_transfer_from_sample(&preconditions, Ok(sample), |_| async {
            Err("raft core stopped".to_string())
        })
        .await;

        assert!(matches!(
            result,
            GroupControlRequestResult::NotSubmitted {
                reason,
                ..
            } if reason == "raft core stopped"
        ));
    }

    #[test]
    fn layout_observation_is_independent_from_request_outcome() {
        let queued_source = synthetic_sample(TargetQualification::Qualified {
            ack_age: Duration::from_millis(5),
            matched: log_id(3, 1, 7),
        });
        let echo = queued_source.observed_preconditions_for(2).echo();
        let outcome =
            GroupControlRequestResult::outcome_unknown(echo, "caller canceled after send".into());

        let source_observed = classify_group_control_layout(echo, Ok(&queued_source));
        assert!(matches!(
            source_observed,
            GroupControlLayoutObservation::SourceObserved { .. }
        ));

        let mut target_sample = queued_source.clone();
        target_sample.local_node_id = 2;
        target_sample.leader_id = 2;
        let target_observed = classify_group_control_layout(echo, Ok(&target_sample));
        assert!(matches!(
            target_observed,
            GroupControlLayoutObservation::TargetObserved { .. }
        ));
        assert!(matches!(
            outcome,
            GroupControlRequestResult::OutcomeUnknown { .. }
        ));

        let mut other_sample = queued_source.clone();
        other_sample.local_node_id = 3;
        other_sample.leader_id = 3;
        let different_observed = classify_group_control_layout(echo, Ok(&other_sample));
        assert!(matches!(
            different_observed,
            GroupControlLayoutObservation::DifferentLeaderObserved { .. }
        ));

        let unavailable = classify_group_control_layout(
            echo,
            Err(&GroupControlSampleError::UnknownGroup {
                group_id: echo.group_id,
            }),
        );
        assert!(matches!(
            unavailable,
            GroupControlLayoutObservation::Unavailable { .. }
        ));
    }
}
