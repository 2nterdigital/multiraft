//! Point-read and native metrics consistency sampling.
use super::*;
use crate::group_observation::{normalize_log_id, normalize_membership, normalize_server_state};
use multiraft_core::TypeConfig;
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use openraft::async_runtime::WatchReceiver as _;
use openraft::vote::RaftLeaderId;
use openraft::{Instant as _, ReadPolicy};
use std::time::Instant;
#[derive(Debug, Clone, PartialEq, Eq)]
struct StatePoint {
    server_state: GroupServerState,
    effective_membership: MembershipObservation,
    committed_membership: MembershipObservation,
    local_committed: Option<ObservedLogId>,
}

/// Reads one best-effort local leader/control sample without requesting a transfer.
///
/// The caller supplies the expected fixed voter set and both age budgets. The
/// sample remains diagnostic input; it does not authorize a transfer by itself.
/// The caller must bind `raft` to `group_id` using its owned Group registration;
/// the Group label is not inferred from the handle. `local_node_id` must identify
/// that local voter. Sampling uses ReadIndex and the existing native checks.
/// `sample_age` measures collection duration, not the age of a later stored copy.
pub async fn read_group_control_sample<S: StateMachine>(
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
                source: match error {
                    openraft::error::RaftError::Fatal(error) => fatal_failure(error),
                    openraft::error::RaftError::APIError(
                        openraft::error::LinearizableReadError::QuorumNotEnough(error),
                    ) => ReadIndexFailure::QuorumUnavailable {
                        responders: error.got,
                    },
                    openraft::error::RaftError::APIError(
                        openraft::error::LinearizableReadError::ForwardToLeader(_),
                    ) => unreachable!("forward handled above"),
                },
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
            evidence: Box::new(ControlIdentityMismatch::Vote {
                expected_node: local_node_id,
                observed: vote,
            }),
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
    .map_err(|error| GroupControlSampleError::ReadIndexFailed {
        group_id,
        source: fatal_failure(error),
    })?
}

fn ensure_state_identity_matches(
    group_id: GroupId,
    _label: &str,
    first: &StatePoint,
    second: &StatePoint,
) -> Result<(), GroupControlSampleError> {
    if first.server_state != second.server_state {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            evidence: Box::new(ControlIdentityMismatch::State {
                expected: first.server_state,
                observed: second.server_state,
            }),
        });
    }
    if first.effective_membership != second.effective_membership {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            evidence: Box::new(ControlIdentityMismatch::Membership {
                kind: MembershipChangeKind::Effective,
                expected: first.effective_membership.clone(),
                observed: second.effective_membership.clone(),
            }),
        });
    }
    if first.committed_membership != second.committed_membership {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            evidence: Box::new(ControlIdentityMismatch::Membership {
                kind: MembershipChangeKind::Committed,
                expected: first.committed_membership.clone(),
                observed: second.committed_membership.clone(),
            }),
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
            evidence: Box::new(ControlIdentityMismatch::State {
                expected: state.server_state,
                observed: metrics_state,
            }),
        });
    }
    if &state.effective_membership != metrics_effective {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            evidence: Box::new(ControlIdentityMismatch::Membership {
                kind: MembershipChangeKind::Effective,
                expected: state.effective_membership.clone(),
                observed: metrics_effective.clone(),
            }),
        });
    }
    if &state.committed_membership != metrics_committed {
        return Err(GroupControlSampleError::InconsistentIdentity {
            group_id,
            evidence: Box::new(ControlIdentityMismatch::Membership {
                kind: MembershipChangeKind::Committed,
                expected: state.committed_membership.clone(),
                observed: metrics_committed.clone(),
            }),
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
        effective_membership: Box::new(effective.clone()),
        committed_membership: Box::new(committed.clone()),
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

fn fatal_failure(error: openraft::error::Fatal<TypeConfig>) -> ReadIndexFailure {
    match error {
        openraft::error::Fatal::Stopped => ReadIndexFailure::Closed,
        openraft::error::Fatal::Panicked => ReadIndexFailure::Backend(NativeFailure::Panicked),
        openraft::error::Fatal::StorageError(_) => {
            ReadIndexFailure::Backend(NativeFailure::Storage)
        }
    }
}
