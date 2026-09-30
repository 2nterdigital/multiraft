//! External consumer of owned byte writes; no native Raft or injected native errors.
use multiraft_core::{ClusterConfig, MultiRaftError, NativeFailure, ProposalFailure};
use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeError};
use std::{error::Error, io, net::TcpListener, time::Duration};
use tokio::time::Instant;

struct RejectingFsm;
impl StateMachine for RejectingFsm {
    type Error = io::Error;
    fn apply(&mut self, _: u64, _: u64, _: &[u8]) -> Result<ApplyOut, io::Error> {
        Err(io::Error::other("SECRET_COMMAND application failure"))
    }
    fn snapshot(&self, _: u64) -> Result<Vec<u8>, io::Error> {
        Ok(vec![])
    }
    fn restore(&mut self, _: u64, _: &[u8]) -> Result<(), io::Error> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_apply_failure_keeps_source_and_unknown_without_payload_display() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let cluster = ClusterConfig::new(1, vec![(1, addr)]);
    let config = RuntimeConfig::new(
        cluster,
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    );
    let owner = NodeOwner::start(
        config,
        |_| Ok(RejectingFsm),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match handle.read_linearizable(7, deadline, |_| ()).await {
            Ok(()) => break,
            Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                tokio::time::sleep(Duration::from_millis(5)).await
            }
            other => panic!("leader readiness: {other:?}"),
        }
    }
    let error = handle
        .propose(7, b"SECRET_COMMAND".to_vec(), deadline)
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("SECRET_COMMAND"));
    assert!(!format!("{error:?}").contains("SECRET_COMMAND"));
    let RuntimeError::Source(MultiRaftError::Proposal(source)) = error else {
        panic!("typed proposal failure expected")
    };
    assert_eq!(
        source.failure,
        ProposalFailure::Backend(NativeFailure::Storage)
    );
    assert!(source.outcome_unknown());
    assert!(
        source.source().is_some(),
        "opaque original error chain retained"
    );
    owner
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let _port = TcpListener::bind(addr).expect("failed native owner listener released");
}
