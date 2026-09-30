//! Qualification and one-shot transfer contracts.

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
        Err(ControlSubmissionError::Stopped)
    })
    .await;

    assert!(matches!(
        result,
        GroupControlRequestResult::NotSubmitted {
            reason,
            ..
        } if reason == ControlSubmissionError::Stopped
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
        GroupControlRequestResult::outcome_unknown(echo, ControlSubmissionError::Cancelled);

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

#[tokio::test(start_paused = true)]
async fn expired_precheck_is_zero_submission_and_trigger_timeout_is_unknown_once() {
    use super::logging::ControlGuard;
    use super::transfer::submit_transfer_from_sample_at;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let sample = synthetic_sample(TargetQualification::Qualified {
        ack_age: Duration::from_millis(5),
        matched: log_id(3, 1, 7),
    });
    let preconditions = sample.observed_preconditions_for(2);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut guard = ControlGuard::new(
        ControlContext::new(ControlInvocationId([1; 16]), tokio::time::Instant::now()),
        preconditions.echo(),
        1,
    );
    let count = calls.clone();
    let rejected = submit_transfer_from_sample_at(
        &preconditions,
        Ok(sample.clone()),
        &mut guard,
        None,
        move |_| async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    assert!(matches!(
        rejected.result,
        GroupControlRequestResult::PrecheckRejected {
            reason: GroupControlPrecheckRejection::Sample(GroupControlSampleError::Deadline {
                stage: ControlStage::Precheck,
                ..
            }),
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut guard = ControlGuard::new(
        ControlContext::new(
            ControlInvocationId([2; 16]),
            tokio::time::Instant::now() + Duration::from_millis(10),
        ),
        preconditions.echo(),
        1,
    );
    let count = calls.clone();
    let unknown = submit_transfer_from_sample_at(
        &preconditions,
        Ok(sample),
        &mut guard,
        None,
        move |_| async move {
            count.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<Result<(), ControlSubmissionError>>().await
        },
    )
    .await;
    assert!(matches!(
        unknown.result,
        GroupControlRequestResult::OutcomeUnknown {
            reason: ControlSubmissionError::Deadline,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn owner_interruption_before_submission_is_rejected_and_after_start_is_unknown_once() {
    use super::logging::ControlGuard;
    use super::transfer::submit_transfer_from_sample_at;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let sample = synthetic_sample(TargetQualification::Qualified {
        ack_age: Duration::from_millis(5),
        matched: log_id(3, 1, 7),
    });
    let preconditions = sample.observed_preconditions_for(2);
    let (stop, receiver) = tokio::sync::watch::channel(true);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut guard = ControlGuard::new(
        ControlContext::new(
            ControlInvocationId([3; 16]),
            tokio::time::Instant::now() + Duration::from_secs(1),
        ),
        preconditions.echo(),
        1,
    );
    let count = calls.clone();
    let refused = submit_transfer_from_sample_at(
        &preconditions,
        Ok(sample.clone()),
        &mut guard,
        Some(receiver),
        move |_| async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    assert!(matches!(
        refused.result,
        GroupControlRequestResult::PrecheckRejected {
            reason: GroupControlPrecheckRejection::Sample(GroupControlSampleError::Closed { .. }),
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    stop.send_replace(false);
    let mut guard = ControlGuard::new(
        ControlContext::new(
            ControlInvocationId([4; 16]),
            tokio::time::Instant::now() + Duration::from_secs(1),
        ),
        preconditions.echo(),
        1,
    );
    let count = calls.clone();
    let unknown = submit_transfer_from_sample_at(
        &preconditions,
        Ok(sample),
        &mut guard,
        Some(stop.subscribe()),
        move |_| async move {
            count.fetch_add(1, Ordering::SeqCst);
            stop.send_replace(true);
            std::future::pending::<Result<(), ControlSubmissionError>>().await
        },
    )
    .await;
    assert!(matches!(
        unknown.result,
        GroupControlRequestResult::OutcomeUnknown {
            reason: ControlSubmissionError::Closed,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_waiter_logs_zero_before_trigger_and_unknown_after_one_trigger() {
    use super::{logging::ControlGuard, transfer::submit_transfer_from_sample_at};
    use futures::FutureExt;
    use std::{
        io,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };
    #[derive(Clone, Default)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Writer {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }
    let writer = Writer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer.clone())
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .finish();
    let calls = Arc::new(AtomicUsize::new(0));
    let sample = synthetic_sample(TargetQualification::Qualified {
        ack_age: Duration::from_millis(5),
        matched: log_id(3, 1, 7),
    });
    let expected = sample.observed_preconditions_for(2);
    // This binary has one log-capture test. A retained global dispatch avoids
    // parallel no-subscriber callsite-cache races while the tested waiter drops.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    {
        assert!(async {
            let mut guard = ControlGuard::new(
                ControlContext::new(
                    ControlInvocationId([8; 16]),
                    tokio::time::Instant::now() + Duration::from_secs(1),
                ),
                expected.echo(),
                1,
            );
            guard.stage(ControlStage::Sampling);
            std::future::pending::<()>().await;
        }
        .now_or_never()
        .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(async {
            let mut guard = ControlGuard::new(
                ControlContext::new(
                    ControlInvocationId([9; 16]),
                    tokio::time::Instant::now() + Duration::from_secs(1),
                ),
                expected.echo(),
                1,
            );
            submit_transfer_from_sample_at(&expected, Ok(sample), &mut guard, None, |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<(), ControlSubmissionError>>().await
            })
            .await
        }
        .now_or_never()
        .is_none());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let logs = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("stage=Sampling result=\"not_submitted\" reason_code=\"cancelled\""),
        "{logs}"
    );
    assert!(
        logs.contains("stage=Trigger result=\"outcome_unknown\" reason_code=\"cancelled\""),
        "{logs}"
    );
    assert!(
        !logs.contains("sampled target qualification"),
        "detail disabled at INFO"
    );
}

#[test]
fn layout_rejects_a_sample_for_another_group_without_claiming_causality() {
    let sample = synthetic_sample(TargetQualification::Source);
    let mut echo = sample.observed_preconditions_for(2).echo();
    echo.group_id += 1;
    match classify_group_control_layout(echo, Ok(&sample)) {
        GroupControlLayoutObservation::Unavailable {
            reason: GroupControlSampleError::InconsistentIdentity { evidence, .. },
            ..
        } => assert_eq!(
            *evidence,
            ControlIdentityMismatch::Group {
                expected: echo.group_id,
                actual: sample.group_id
            }
        ),
        other => panic!("unexpected cross-group layout {other:?}"),
    }
}
