//! Interrupted portable streams resume the same native operation without overlapping target writers.
#[allow(dead_code)]
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[allow(dead_code)]
#[path = "controlled_protocol_support/cluster.rs"]
mod cluster;
#[allow(dead_code)]
#[path = "controlled_protocol_support/process.rs"]
mod process;
#[allow(dead_code)]
#[path = "controlled_protocol_support/session.rs"]
mod session;
use authority::{StartupAuthority, StartupAuthorityResult};
use cluster::{deadline, GROUP};
use multiraft_net::{controlled_startup as authority, portable::*, ControlledError};
use session::Session;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

fn admitted<T: std::fmt::Debug>(outcome: StartupAuthorityResult<T>) -> T {
    match outcome {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("expected durable admission: {other:?}"),
    }
}
#[derive(Clone, Copy)]
enum Fault {
    Start,
    Partial,
    Eof,
}
struct Interrupted {
    endpoint: NativeTargetEndpoint<cluster::DiskImage>,
    fault: Fault,
    fired: AtomicBool,
    old: Arc<Mutex<Option<NativeTargetScope>>>,
}
impl NativeTargetTransport for Interrupted {
    fn peer(&self) -> (u64, Option<[u8; 32]>) {
        self.endpoint.peer()
    }
    fn exchange(
        &self,
        bytes: Vec<u8>,
    ) -> authority::StartupAuthorityFuture<'_, Result<Vec<u8>, NativeTargetTransportError>> {
        Box::pin(async move {
            let request: NativeTargetRequest = serde_json::from_slice(&bytes).unwrap();
            let chosen = match (&request.command, self.fault) {
                (NativeTargetCommand::StartSeed { .. }, Fault::Start) => true,
                (NativeTargetCommand::SeedChunk { sequence: 1, .. }, Fault::Partial) => true,
                (NativeTargetCommand::SeedChunk { chunk: None, .. }, Fault::Eof) => true,
                _ => false,
            };
            let reply = self.endpoint.exchange(bytes).await?;
            if chosen && !self.fired.swap(true, Ordering::SeqCst) {
                *self.old.lock().unwrap() = Some(request.scope);
                return Err(NativeTargetTransportError {
                    outcome: NativeTargetOutcome::Unknown,
                    message: "accepted native request reply lost".to_owned(),
                });
            }
            Ok(reply)
        })
    }
}
async fn interrupted(fault: Fault) {
    let root = cluster::scratch("portable-stream-retry-");
    let mut session = Session::fresh(root.as_path()).await;
    let (leader, spec, target) = session.reserve(false).await;
    let endpoint = NativeTargetEndpoint::new(target.clone(), spec.clone()).unwrap();
    let old = Arc::new(Mutex::new(None));
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        spec.clone(),
        Arc::new(Interrupted {
            endpoint: endpoint.clone(),
            fault,
            fired: AtomicBool::new(false),
            old: old.clone(),
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
    let gate = root
        .join("node-4/seed-install-gate")
        .join(GROUP.to_string());
    if matches!(fault, Fault::Eof) {
        std::fs::create_dir_all(&gate).unwrap();
        std::fs::write(gate.join("armed"), b"hold actual checkpoint callback").unwrap();
    }
    let result = port
        .install_seed(seed.clone(), admission.clone(), deadline())
        .await;
    assert!(
        matches!(
            result,
            Err(ControlledError::TargetOutcome {
                outcome: NativeTargetOutcome::Unknown,
                ..
            })
        ),
        "accepted request loss preserves uncertainty: {result:?}"
    );
    let retry_port = port.clone();
    let retry_seed = seed.clone();
    if matches!(fault, Fault::Eof) {
        tokio::time::timeout(Duration::from_secs(20), async {
            while !gate.join("installed").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let retry = tokio::spawn(async move {
            retry_port
                .install_seed(retry_seed, admission, deadline())
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !retry.is_finished(),
            "supersede must await actual native install, not a departed RPC waiter"
        );
        assert_eq!(session.creations[3].load(Ordering::SeqCst), 1);
        assert_eq!(
            target.receipt().unwrap().stage,
            multiraft_store::ControlledGroupStage::Opening
        );
        session
            .group(leader)
            .claim(spec.operation, 1, deadline())
            .await
            .unwrap();
        cluster::compact(&session.nodes[leader]).await;
        assert!(
            seed.export(GROUP).await.is_err(),
            "old source checkpoint expired while target install remains owned"
        );
        std::fs::write(gate.join("release"), b"complete actual target installation").unwrap();
        retry.await.unwrap().unwrap();
    } else {
        retry_port
            .install_seed(retry_seed, admission, deadline())
            .await
            .unwrap();
    }
    assert_eq!(
        port.status().await.unwrap().stage,
        NativeTargetStage::Opened
    );
    assert_eq!(
        session.creations[3].load(Ordering::SeqCst),
        1,
        "retry reuses the one quarantined native core"
    );
    let late = NativeTargetRequest {
        scope: old.lock().unwrap().clone().unwrap(),
        command: NativeTargetCommand::SeedChunk {
            sequence: 0,
            chunk: None,
        },
    };
    let reply: NativeTargetReply = serde_json::from_slice(
        &endpoint
            .dispatch(&serde_json::to_vec(&late).unwrap())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        reply.outcome,
        NativeTargetOutcome::Rejected,
        "late frames from the superseded correlation cannot enter the replacement stream"
    );
    session.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_start_reply_lost_before_any_chunks_retries_same_operation() {
    interrupted(Fault::Start).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partially_received_checkpoint_restarts_after_actual_old_writer_completion() {
    interrupted(Fault::Partial).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eof_supersede_waits_for_actual_install_then_replays_with_expired_source() {
    interrupted(Fault::Eof).await;
}

async fn wire(
    endpoint: &NativeTargetEndpoint<cluster::DiskImage>,
    scope: &NativeTargetScope,
    command: NativeTargetCommand,
) -> NativeTargetReply {
    serde_json::from_slice(
        &endpoint
            .dispatch(
                &serde_json::to_vec(&NativeTargetRequest {
                    scope: scope.clone(),
                    command,
                })
                .unwrap(),
            )
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replaying_identical_spooled_transfer_ids_uses_new_staging_and_fences_old_frames() {
    let root = cluster::scratch("portable-identical-transfer-retry-");
    let mut session = Session::fresh(root.as_path()).await;
    let (leader, spec, target) = session.reserve(false).await;
    let endpoint = NativeTargetEndpoint::new(target, spec.clone()).unwrap();
    let port = SourceTargetPort::new(
        endpoint.identity().clone(),
        spec.clone(),
        Arc::new(endpoint.clone()),
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
    let mut export = seed.export(GROUP).await.unwrap();
    let header = export.header().clone();
    let spool = root.join("retry-spool");
    std::fs::create_dir_all(&spool).unwrap();
    let mut count = 0_u64;
    loop {
        let chunk = export.next_chunk().await.unwrap();
        std::fs::write(
            spool.join(count.to_string()),
            serde_json::to_vec(&chunk).unwrap(),
        )
        .unwrap();
        count += 1;
        if chunk.is_none() {
            break;
        }
    }
    assert!(
        count > 2,
        "real checkpoint has Begin, file bytes, and explicit EOF"
    );
    let old = NativeTargetScope {
        version: NATIVE_TARGET_VERSION,
        target: endpoint.identity().clone(),
        specification: spec,
        correlation: [31; 16],
    };
    let start = NativeTargetCommand::StartSeed {
        header: Box::new(header.clone()),
        admission: admission.clone(),
    };
    assert_eq!(
        wire(&endpoint, &old, start.clone()).await.outcome,
        NativeTargetOutcome::InProgress
    );
    for sequence in 0..2 {
        let chunk =
            serde_json::from_slice(&std::fs::read(spool.join(sequence.to_string())).unwrap())
                .unwrap();
        assert_eq!(
            wire(
                &endpoint,
                &old,
                NativeTargetCommand::SeedChunk { sequence, chunk }
            )
            .await
            .outcome,
            NativeTargetOutcome::InProgress
        );
    }
    let mut weaker = old.clone();
    weaker.correlation = [32; 16];
    let mut weak_header = header.clone();
    weak_header.record.executor_epoch = 0;
    assert_eq!(
        wire(
            &endpoint,
            &weaker,
            NativeTargetCommand::StartSeed {
                header: Box::new(weak_header),
                admission: admission.clone()
            }
        )
        .await
        .outcome,
        NativeTargetOutcome::Rejected
    );
    // A structurally valid but false executor claim must fail actual body inspection;
    // its rejected epoch cannot become a permanent native authority fence.
    let mut poisoned = old.clone();
    poisoned.correlation = [34; 16];
    let mut false_header = header.clone();
    false_header.record.executor_epoch = 999;
    assert_eq!(
        wire(
            &endpoint,
            &poisoned,
            NativeTargetCommand::StartSeed {
                header: Box::new(false_header),
                admission: admission.clone()
            }
        )
        .await
        .outcome,
        NativeTargetOutcome::InProgress
    );
    for sequence in 0..count {
        let chunk =
            serde_json::from_slice(&std::fs::read(spool.join(sequence.to_string())).unwrap())
                .unwrap();
        assert_eq!(
            wire(
                &endpoint,
                &poisoned,
                NativeTargetCommand::SeedChunk { sequence, chunk }
            )
            .await
            .outcome,
            NativeTargetOutcome::InProgress
        );
    }
    assert_eq!(
        wire(&endpoint, &poisoned, NativeTargetCommand::FinishSeed)
            .await
            .outcome,
        NativeTargetOutcome::Rejected
    );
    assert!(
        session.nodes[3].with_fsm(GROUP, |_| ()).await.is_none(),
        "false header body mismatch cannot publish native target"
    );
    let mut new = old.clone();
    new.correlation = [33; 16];
    assert_eq!(
        wire(&endpoint, &new, start).await.outcome,
        NativeTargetOutcome::InProgress
    );
    assert_eq!(
        wire(
            &endpoint,
            &old,
            NativeTargetCommand::SeedChunk {
                sequence: 2,
                chunk: None
            }
        )
        .await
        .outcome,
        NativeTargetOutcome::Rejected
    );
    for sequence in 0..count {
        // Re-read exactly the same serialized Begin/data, including the SAME transfer id.
        let chunk =
            serde_json::from_slice(&std::fs::read(spool.join(sequence.to_string())).unwrap())
                .unwrap();
        assert_eq!(
            wire(
                &endpoint,
                &new,
                NativeTargetCommand::SeedChunk { sequence, chunk }
            )
            .await
            .outcome,
            NativeTargetOutcome::InProgress
        );
    }
    assert_eq!(
        wire(&endpoint, &new, NativeTargetCommand::FinishSeed)
            .await
            .outcome,
        NativeTargetOutcome::Durable
    );
    assert_eq!(session.creations[3].load(Ordering::SeqCst), 1);
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
    session
        .group(leader)
        .complete_with_port(&executor, &port, deadline())
        .await
        .unwrap();
    session.stop().await;
}
