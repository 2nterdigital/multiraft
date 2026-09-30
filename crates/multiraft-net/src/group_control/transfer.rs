//! One native trigger and independent layout classifier, over the existing precheck.
use super::*;
use logging::{sample_reason, ControlGuard};
use multiraft_fsm::StateMachine;
use multiraft_store::Raft;
use std::future::Future;

pub(crate) async fn submit_group_control_transfer<S: StateMachine>(
    raft: &Raft<S>,
    local_node_id: NodeId,
    preconditions: &GroupControlPreconditions,
    expected_voters: &[NodeId],
    guard: &mut ControlGuard,
    mut abort: Option<tokio::sync::watch::Receiver<bool>>,
) -> ControlTransferOutcome {
    guard.stage(ControlStage::Sampling);
    let context = guard.context;
    let sampling = async {
        tokio::time::timeout_at(
            context.deadline,
            read_group_control_sample(
                raft,
                preconditions.group_id,
                local_node_id,
                expected_voters,
                context.max_sample_age,
                context.max_target_ack_age,
            ),
        )
        .await
        .unwrap_or(Err(GroupControlSampleError::Deadline {
            group_id: preconditions.group_id,
            stage: ControlStage::Sampling,
        }))
    };
    let sample_result = tokio::select! { biased;
        _ = interrupted(&mut abort) => Err(GroupControlSampleError::Closed { group_id: preconditions.group_id }),
        sample = sampling => sample,
    };
    submit_transfer_from_sample_at(
        preconditions,
        sample_result,
        guard,
        abort,
        |target| async move {
            raft.trigger()
                .transfer_leader(target)
                .await
                .map_err(|error| match error {
                    openraft::error::Fatal::Stopped => ControlSubmissionError::Stopped,
                    openraft::error::Fatal::Panicked => {
                        ControlSubmissionError::Backend(NativeFailure::Panicked)
                    }
                    openraft::error::Fatal::StorageError(_) => {
                        ControlSubmissionError::Backend(NativeFailure::Storage)
                    }
                })
        },
    )
    .await
}

#[cfg(test)]
pub(super) async fn submit_transfer_from_sample<F, Fut>(
    preconditions: &GroupControlPreconditions,
    sample: Result<GroupControlSample, GroupControlSampleError>,
    trigger: F,
) -> GroupControlRequestResult
where
    F: FnOnce(NodeId) -> Fut,
    Fut: Future<Output = Result<(), ControlSubmissionError>>,
{
    let context = ControlContext::new(
        ControlInvocationId::fresh(),
        tokio::time::Instant::now() + Duration::from_secs(5),
    );
    let mut guard = ControlGuard::new(context, preconditions.echo(), preconditions.expected_source);
    submit_transfer_from_sample_at(preconditions, sample, &mut guard, None, trigger)
        .await
        .result
}

pub(super) async fn submit_transfer_from_sample_at<F, Fut>(
    preconditions: &GroupControlPreconditions,
    sample_result: Result<GroupControlSample, GroupControlSampleError>,
    guard: &mut ControlGuard,
    mut abort: Option<tokio::sync::watch::Receiver<bool>>,
    trigger: F,
) -> ControlTransferOutcome
where
    F: FnOnce(NodeId) -> Fut,
    Fut: Future<Output = Result<(), ControlSubmissionError>>,
{
    let echo = preconditions.echo();
    let sample = match sample_result {
        Ok(sample) => sample,
        Err(error) => {
            guard.finish("precheck_rejected", sample_reason(&error));
            return ControlTransferOutcome {
                invocation_id: guard.context.invocation_id,
                local_node_id: preconditions.expected_source,
                sample: None,
                result: GroupControlRequestResult::PrecheckRejected {
                    echo,
                    reason: GroupControlPrecheckRejection::Sample(error),
                },
            };
        }
    };
    guard.sample(&sample);
    guard.stage(ControlStage::Precheck);
    if let Err(error) = sample.check_transfer_preconditions(preconditions) {
        guard.rejection(&error);
        guard.finish("precheck_rejected", logging::precheck_reason(&error));
        return ControlTransferOutcome {
            invocation_id: guard.context.invocation_id,
            local_node_id: sample.local_node_id,
            result: GroupControlRequestResult::PrecheckRejected {
                echo,
                reason: GroupControlPrecheckRejection::Preconditions(error),
            },
            sample: Some(sample),
        };
    }
    if abort.as_ref().is_some_and(|receiver| *receiver.borrow()) {
        guard.finish("precheck_rejected", "closed");
        return ControlTransferOutcome {
            invocation_id: guard.context.invocation_id,
            local_node_id: sample.local_node_id,
            result: GroupControlRequestResult::PrecheckRejected {
                echo,
                reason: GroupControlPrecheckRejection::Sample(GroupControlSampleError::Closed {
                    group_id: echo.group_id,
                }),
            },
            sample: Some(sample),
        };
    }
    if tokio::time::Instant::now() >= guard.context.deadline {
        guard.finish("precheck_rejected", "deadline");
        return ControlTransferOutcome {
            invocation_id: guard.context.invocation_id,
            local_node_id: sample.local_node_id,
            result: GroupControlRequestResult::PrecheckRejected {
                echo,
                reason: GroupControlPrecheckRejection::Sample(GroupControlSampleError::Deadline {
                    group_id: echo.group_id,
                    stage: ControlStage::Precheck,
                }),
            },
            sample: Some(sample),
        };
    }
    // Synchronous recheck -> exactly one trigger future; no queue or async work in between.
    guard.stage(ControlStage::Trigger);
    let attempt = tokio::select! { biased;
        _ = interrupted(&mut abort) => Err(ControlSubmissionError::Closed),
        result = tokio::time::timeout_at(guard.context.deadline, trigger(preconditions.target)) => result.map_err(|_| ControlSubmissionError::Deadline),
    };
    let result = match attempt {
        Ok(Ok(())) => {
            guard.finish("trigger_queued", "none");
            GroupControlRequestResult::TriggerQueued { echo }
        }
        Ok(Err(
            reason @ (ControlSubmissionError::Stopped | ControlSubmissionError::Backend(_)),
        )) => {
            guard.finish("not_submitted", logging::submission_reason(reason));
            GroupControlRequestResult::NotSubmitted { echo, reason }
        }
        Ok(Err(reason)) => {
            guard.finish("outcome_unknown", logging::submission_reason(reason));
            GroupControlRequestResult::OutcomeUnknown { echo, reason }
        }
        Err(reason) => {
            guard.finish(
                "outcome_unknown",
                if reason == ControlSubmissionError::Deadline {
                    "deadline"
                } else {
                    "closed"
                },
            );
            GroupControlRequestResult::OutcomeUnknown { echo, reason }
        }
    };
    ControlTransferOutcome {
        invocation_id: guard.context.invocation_id,
        local_node_id: sample.local_node_id,
        result,
        sample: Some(sample),
    }
}

/// Classifies a fresh sample relative to an earlier control request echo.
///
/// This is an observation only. It does not establish that this request caused
/// the observed leader layout. The caller must supply a sample for the echo's
/// Group; classification does not bind a handle or recheck transfer eligibility.
pub fn classify_group_control_layout(
    echo: GroupControlRequestEcho,
    sample_result: Result<&GroupControlSample, &GroupControlSampleError>,
) -> GroupControlLayoutObservation {
    let sample = match sample_result {
        Ok(sample) => sample,
        Err(error) => {
            return GroupControlLayoutObservation::Unavailable {
                echo,
                reason: error.clone(),
            };
        }
    };
    if sample.group_id != echo.group_id {
        return GroupControlLayoutObservation::Unavailable {
            echo,
            reason: GroupControlSampleError::InconsistentIdentity {
                group_id: echo.group_id,
                evidence: Box::new(ControlIdentityMismatch::Group {
                    expected: echo.group_id,
                    actual: sample.group_id,
                }),
            },
        };
    }
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

async fn interrupted(abort: &mut Option<tokio::sync::watch::Receiver<bool>>) {
    match abort {
        Some(receiver) => {
            if !*receiver.borrow_and_update() {
                let _ = receiver.changed().await;
            }
        }
        None => std::future::pending::<()>().await,
    }
}
