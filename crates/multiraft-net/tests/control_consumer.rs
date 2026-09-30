//! Independent opaque FSM consumer: weak normative APIs and actual RF3 gRPC.
use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    ControlContext, ControlInvocationId, GroupConfig, GroupControlLayoutObservation,
    GroupControlPrecheckError, GroupControlPrecheckRejection, GroupControlRequestResult,
    GroupControlSampleError, NodeOwner, RuntimeConfig, TargetQualification,
};
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{sleep, timeout, Instant};
use tracing_subscriber::prelude::*;
#[derive(Default)]
struct Counter(u64);
impl StateMachine for Counter {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, bytes: &[u8]) -> Result<ApplyOut, io::Error> {
        self.0 += u64::from_be_bytes(bytes.try_into().map_err(|_| io::Error::other("command"))?);
        Ok(ApplyOut {
            effects: self.0.to_be_bytes().to_vec(),
        })
    }
    fn snapshot(&self, _: GroupId) -> Result<Vec<u8>, io::Error> {
        Ok(self.0.to_be_bytes().to_vec())
    }
    fn restore(&mut self, _: GroupId, bytes: &[u8]) -> Result<(), io::Error> {
        self.0 = u64::from_be_bytes(bytes.try_into().map_err(|_| io::Error::other("snapshot"))?);
        Ok(())
    }
}
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
impl io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
fn context(id: u8) -> ControlContext {
    ControlContext::new(
        ControlInvocationId([id; 16]),
        Instant::now() + Duration::from_secs(3),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normative_transfer_retains_evidence_and_source_logs_without_business_payloads() {
    let logs = Logs::default();
    let filter = tracing_subscriber::filter::Targets::new()
        .with_target("multiraft::control", tracing::Level::DEBUG);
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .with_filter(filter),
    );
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let listeners: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: Vec<_> = listeners
        .iter()
        .enumerate()
        .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap()))
        .collect();
    drop(listeners);
    let owners = futures::future::join_all((1..=3).map(|id| {
        let mut config = ClusterConfig::for_test(id, &[1, 2, 3]);
        config.peers = peers.clone();
        async move {
            NodeOwner::start(
                RuntimeConfig::new(
                    config,
                    vec![GroupConfig {
                        group_id: 7,
                        voters: vec![1, 2, 3],
                    }],
                ),
                |_| Ok(Counter::default()),
                Instant::now() + Duration::from_secs(8),
            )
            .await
            .unwrap()
        }
    }))
    .await;
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let voters = [1, 2, 3];
    let (source_index, sample) = timeout(Duration::from_secs(5), async {
        loop {
            for (i, h) in handles.iter().enumerate() {
                if let Ok(sample) = h.read_group_control_sample(7, &voters, context(1)).await {
                    if sample
                        .target_qualifications
                        .values()
                        .any(|q| matches!(q, TargetQualification::Qualified { .. }))
                    {
                        return (i, sample);
                    }
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let target = *sample
        .target_qualifications
        .iter()
        .find(|(_, q)| matches!(q, TargetQualification::Qualified { .. }))
        .unwrap()
        .0;
    let preconditions = sample.observed_preconditions_for(target);
    let mut stale = preconditions.clone();
    stale.observed_vote.term += 100;
    let rejected = handles[source_index]
        .try_transfer_group_leader(&stale, &voters, context(2))
        .await;
    assert_eq!(rejected.invocation_id, ControlInvocationId([2; 16]));
    assert!(rejected.sample.is_some());
    assert!(
        matches!(rejected.result,GroupControlRequestResult::PrecheckRejected{reason:GroupControlPrecheckRejection::Preconditions(GroupControlPrecheckError::VoteChanged{expected,actual}),..} if expected==stale.observed_vote && actual==sample.flushed_vote)
    );
    let mut stale_members = preconditions.clone();
    stale_members
        .effective_membership
        .log_id
        .as_mut()
        .unwrap()
        .index += 100;
    let membership = handles[source_index]
        .try_transfer_group_leader(&stale_members, &voters, context(3))
        .await;
    assert!(
        matches!(membership.result,GroupControlRequestResult::PrecheckRejected{reason:GroupControlPrecheckRejection::Preconditions(GroupControlPrecheckError::MembershipChanged{expected,..}),..} if *expected==stale_members.effective_membership)
    );
    let wrong = handles[(source_index + 1) % 3]
        .try_transfer_group_leader(&preconditions, &voters, context(4))
        .await;
    assert!(matches!(
        wrong.result,
        GroupControlRequestResult::PrecheckRejected {
            reason: GroupControlPrecheckRejection::Sample(
                GroupControlSampleError::WrongNode { .. }
            ),
            ..
        }
    ));
    let mut expired = context(5);
    expired.deadline = Instant::now();
    let expiry = handles[source_index]
        .try_transfer_group_leader(&preconditions, &voters, expired)
        .await;
    assert!(matches!(
        expiry.result,
        GroupControlRequestResult::PrecheckRejected {
            reason: GroupControlPrecheckRejection::Sample(GroupControlSampleError::Deadline { .. }),
            ..
        }
    ));
    let outcome = handles[source_index]
        .try_transfer_group_leader(&preconditions, &voters, context(6))
        .await;
    assert!(matches!(
        outcome.result,
        GroupControlRequestResult::TriggerQueued { .. }
    ));
    assert!(matches!(
        outcome
            .sample
            .as_ref()
            .unwrap()
            .target_qualifications
            .get(&target),
        Some(TargetQualification::Qualified { .. })
    ));
    let target_handle = &handles[(target - 1) as usize];
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(GroupControlLayoutObservation::TargetObserved {
                observed_leader, ..
            }) = Ok::<_, ()>(
                target_handle
                    .observe_group_control_layout(preconditions.echo(), &voters, context(6))
                    .await,
            ) {
                assert_eq!(observed_leader, target);
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    target_handle
        .propose(7, 9u64.to_be_bytes().to_vec(), context(7).deadline)
        .await
        .unwrap();
    assert_eq!(
        target_handle
            .read_linearizable(7, context(8).deadline, |fsm| fsm.0)
            .await
            .unwrap(),
        9
    );
    for owner in owners {
        owner
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    }
    let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(
        text.lines()
            .any(|line| line.trim_start().starts_with("INFO")
                && line.contains("leader_vote_changed")
                && line.contains("cause_unknown")),
        "actual changed native observation stays INFO with unknown causality"
    );
    for field in [
        "invocation_id=",
        "group_id=7",
        "local_node_id=",
        "source_node_id=",
        "target_node_id=",
        "stage=",
        "result=",
        "reason_code=",
        "vote_term=",
        "target_ack_age_ms=",
        "matched_term=",
        "expected_vote_term=",
    ] {
        assert!(text.contains(field), "missing {field}: {text}")
    }
    for result in [
        "precheck_rejected",
        "trigger_queued",
        "target_observed",
        "leader_vote_changed",
    ] {
        assert!(text.contains(result), "missing {result}: {text}")
    }
    assert!(
        !text.contains("payload=") && !text.contains("credential=") && !text.contains("command=")
    );
    assert!(
        matches!(
            outcome.result,
            GroupControlRequestResult::TriggerQueued { .. }
        ),
        "independent layout does not rewrite submission"
    );
}
