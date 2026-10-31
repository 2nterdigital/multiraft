//! Portable native target contract exercised against the actual RocksDB/native FSM boundary.
#[allow(dead_code)]
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[allow(dead_code)]
#[path = "controlled_protocol_support/cluster.rs"]
mod cluster;
#[allow(dead_code)]
#[path = "controlled_protocol_support/session.rs"]
mod session;
use authority::{StartupAuthority, StartupAuthorityResult};
use cluster::{deadline, GROUP};
#[allow(dead_code)]
#[path = "controlled_protocol_support/process.rs"]
mod process;
use multiraft_net::{controlled_startup as authority, portable::*};
use session::Session;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

struct LostFinish {
    endpoint: NativeTargetEndpoint<cluster::DiskImage>,
    lost: AtomicBool,
}
impl NativeTargetTransport for LostFinish {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        self.endpoint.peer()
    }
    fn exchange(
        &self,
        bytes: Vec<u8>,
    ) -> authority::StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        Box::pin(async move {
            let request: NativeTargetRequest = serde_json::from_slice(&bytes).unwrap();
            let reply = self.endpoint.exchange(bytes).await?;
            if matches!(request.command, NativeTargetCommand::FinishSeed)
                && !self.lost.swap(true, Ordering::SeqCst)
            {
                return Err(NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: "lost durable finish reply".to_owned(),
                });
            }
            Ok(reply)
        })
    }
}

fn admitted<T: std::fmt::Debug>(outcome: StartupAuthorityResult<T>) -> T {
    match outcome {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("expected durable admission: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encoded_scope_rejection_cannot_construct_target_and_real_port_completes_replacement() {
    let root = cluster::scratch("portable-native-");
    let mut session = Session::fresh(root.as_path()).await;
    let (leader, spec, target) = session.reserve(false).await;
    let endpoint = NativeTargetEndpoint::new(target.clone(), spec.clone()).unwrap();
    let scope = NativeTargetScope {
        version: NATIVE_TARGET_VERSION,
        target: endpoint.identity().clone(),
        specification: spec.clone(),
        correlation: [7; 16],
    };
    let mut wrong = scope.clone();
    wrong.target.generation += 1;
    let request = NativeTargetRequest {
        scope: wrong,
        command: NativeTargetCommand::Observe {
            configuration: None,
            barrier: None,
        },
    };
    let response = endpoint
        .dispatch(&serde_json::to_vec(&request).unwrap())
        .await
        .unwrap();
    let response: NativeTargetReply = serde_json::from_slice(&response).unwrap();
    assert_eq!(response.outcome, NativeTargetOutcome::Rejected);
    assert!(
        session.nodes[3].with_fsm(GROUP, |_| ()).await.is_none(),
        "untrusted wrong generation must not construct target"
    );
    let mut bad_version = scope.clone();
    bad_version.version += 1;
    let mut bad_domain = scope.clone();
    bad_domain.target.domain = Some([99; 32]);
    let mut bad_config = scope.clone();
    bad_config.specification.target_voters = [1, 2, 99].into();
    for bad in [bad_version, bad_domain, bad_config] {
        let request = NativeTargetRequest {
            scope: bad,
            command: NativeTargetCommand::Observe {
                configuration: None,
                barrier: None,
            },
        };
        let reply: NativeTargetReply = serde_json::from_slice(
            &endpoint
                .dispatch(&serde_json::to_vec(&request).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(reply.outcome, NativeTargetOutcome::Rejected);
    }
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        spec.clone(),
        Arc::new(LostFinish {
            endpoint: endpoint.clone(),
            lost: AtomicBool::new(false),
        }),
    )
    .unwrap();
    let executor = session
        .group(leader)
        .begin_with_port(spec.clone(), 1, &port, deadline())
        .await
        .unwrap();
    let seed = session
        .group(leader)
        .prepare_seed(&executor, deadline())
        .await
        .unwrap();
    let admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .unwrap(),
    );
    let export = seed.export(GROUP).await.unwrap();
    let mut wrong_vote = export.header().clone();
    drop(export);
    wrong_vote.vote = multiraft_core::typ::Vote::new_committed(999_999, 1);
    let request = NativeTargetRequest {
        scope: scope.clone(),
        command: NativeTargetCommand::StartSeed {
            header: Box::new(wrong_vote),
            admission: admission.clone(),
        },
    };
    let reply: NativeTargetReply = serde_json::from_slice(
        &endpoint
            .dispatch(&serde_json::to_vec(&request).unwrap())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        reply.outcome,
        NativeTargetOutcome::Rejected,
        "altered vote cannot create native target state"
    );
    assert!(session.nodes[3].with_fsm(GROUP, |_| ()).await.is_none());
    let result = port
        .install_seed(seed.clone(), admission.clone(), deadline())
        .await;
    assert!(
        matches!(
            result,
            Err(multiraft_net::ControlledError::TargetOutcome {
                outcome: NativeTargetOutcome::Unknown,
                ..
            })
        ),
        "lost native durable reply remains typed uncertainty: {result:?}"
    );
    session
        .group(leader)
        .claim(spec.operation, 1, deadline())
        .await
        .unwrap();
    cluster::compact(&session.nodes[leader]).await;
    assert!(
        seed.export(GROUP).await.is_err(),
        "original checkpoint must be expired"
    );
    port.install_seed(seed, admission, deadline())
        .await
        .unwrap();
    assert_eq!(
        port.status().await.unwrap().stage,
        NativeTargetStage::Opened
    );
    session
        .group(leader)
        .add_learner_with_port(&executor, &port, deadline())
        .await
        .unwrap();
    session
        .group(leader)
        .enter_joint_with_port(&executor, &port, deadline())
        .await
        .unwrap();
    session
        .group(leader)
        .finalize_uniform(&executor, deadline())
        .await
        .unwrap();
    let terminal = session
        .group(leader)
        .complete_with_port(&executor, &port, deadline())
        .await
        .unwrap();
    assert_eq!(terminal.proof().target_open.target_node, 4);
    for node in &session.nodes {
        node.shutdown().await.unwrap();
    }
}

struct PausedFinish {
    endpoint: NativeTargetEndpoint<cluster::DiskImage>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl NativeTargetTransport for PausedFinish {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        self.endpoint.peer()
    }
    fn exchange(
        &self,
        bytes: Vec<u8>,
    ) -> authority::StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        Box::pin(async move {
            let request: NativeTargetRequest = serde_json::from_slice(&bytes).unwrap();
            let reply = self.endpoint.exchange(bytes).await?;
            if matches!(request.command, NativeTargetCommand::FinishSeed) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(reply)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoning_install_waiter_does_not_cancel_source_owned_transfer_or_target_open() {
    let root = cluster::scratch("portable-waiter-");
    let mut session = Session::fresh(root.as_path()).await;
    let (leader, spec, target) = session.reserve(false).await;
    let endpoint = NativeTargetEndpoint::new(target, spec.clone()).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        spec.clone(),
        Arc::new(PausedFinish {
            endpoint,
            entered: entered.clone(),
            release: release.clone(),
        }),
    )
    .unwrap();
    let executor = session
        .group(leader)
        .begin_with_port(spec, 1, &port, deadline())
        .await
        .unwrap();
    let seed = session
        .group(leader)
        .prepare_seed(&executor, deadline())
        .await
        .unwrap();
    let admission = admitted(
        session
            .authority
            .admit(session.request(3, 1))
            .await
            .unwrap(),
    );
    let waiting_port = port.clone();
    let waiter =
        tokio::spawn(async move { waiting_port.install_seed(seed, admission, deadline()).await });
    tokio::time::timeout(std::time::Duration::from_secs(20), entered.notified())
        .await
        .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    release.notify_one();
    assert_eq!(
        port.status().await.unwrap().stage,
        NativeTargetStage::Opened
    );
    assert_eq!(
        session.creations[3].load(Ordering::SeqCst),
        1,
        "native target constructed exactly once"
    );
    for node in &session.nodes {
        node.shutdown().await.unwrap();
    }
}
