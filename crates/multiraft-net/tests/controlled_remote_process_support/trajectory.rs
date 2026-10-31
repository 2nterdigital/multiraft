//! Finite proof: source checkpoint expiry precedes remote byte transfer and native completion.
use super::{
    authority::{StartupAuthority, StartupRequest, StartupTargetGeneration},
    pipe::{self, Pipe, Setup},
    source::{self, deadline, Sources, DOMAIN, GROUP},
};
use multiraft_core::{OperationId, ReplacementSpec};
use multiraft_net::portable::{
    NativeTargetCommand, NativeTargetOutcome, NativeTargetReply, NativeTargetRequest,
    NativeTargetScope, NativeTargetStage, NativeTargetTransport, NativeTargetTransportError,
    SourceTargetPort, NATIVE_TARGET_VERSION,
};
use multiraft_store::disk_snapshot::{DiskChunk, DiskManifest};
use std::{fs::File, io::Write, path::Path, time::Duration};

async fn exchange(
    pipe: &Pipe,
    scope: &NativeTargetScope,
    command: NativeTargetCommand,
) -> Result<NativeTargetReply, NativeTargetTransportError> {
    let expected_command = serde_json::to_value(&command).unwrap();
    let bytes = pipe
        .exchange(
            serde_json::to_vec(&NativeTargetRequest {
                scope: scope.clone(),
                command,
            })
            .unwrap(),
        )
        .await?;
    let reply: NativeTargetReply = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        &reply.scope, scope,
        "pipe reply correlation and immutable native scope"
    );
    assert_eq!(
        serde_json::to_value(&reply.command).unwrap(),
        expected_command,
        "pipe reply identifies exact command and chunk sequence"
    );
    Ok(reply)
}
pub async fn run() {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/controlled-remote-process");
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("isolated-")
        .tempdir_in(parent)
        .unwrap();
    let configs = source::configurations(root.path());
    let sources = Sources::fresh(root.path(), &configs).await;
    assert_eq!(
        sources.nodes.len(),
        3,
        "only three native owners live in parent"
    );
    let leader = sources.leader().await;
    let group = &sources.groups[leader];
    let node = &sources.nodes[leader];
    let prefix = br#"{"control":{"Begin":900},"opaque":[0,255]}"#.to_vec();
    node.propose(GROUP, prefix.clone()).await.unwrap();
    let removed = if leader == 0 { 3 } else { 1 };
    let spec = ReplacementSpec {
        operation: OperationId {
            sequence: 1,
            nonce: [83; 16],
        },
        expected_start: group.membership(deadline()).await.unwrap(),
        target_voters: [1, 2, 3]
            .into_iter()
            .filter(|id| *id != removed)
            .chain([4])
            .collect(),
        target_generation: 40,
    };
    let (pipe, identity) = Pipe::spawn(Setup {
        root: configs[3].data_dir.clone(),
        peers: configs[3].peers.clone(),
        catalog: root.path().join("catalog.json"),
        specification: spec.clone(),
    });
    assert_eq!(
        (
            identity.node,
            identity.group,
            identity.generation,
            identity.domain
        ),
        (4, 7, 40, Some(DOMAIN))
    );
    sources
        .authority
        .consume_target(StartupTargetGeneration {
            node_id: 4,
            generation: 40,
            consumption_id: identity.consumption.clone(),
        })
        .unwrap();
    let port = SourceTargetPort::new(identity.clone(), spec.clone(), pipe.clone()).unwrap();
    let executor = group
        .begin_with_port(spec.clone(), 1, &port, deadline())
        .await
        .unwrap();
    let seed = group.prepare_seed(&executor, deadline()).await.unwrap();
    let mut export = seed.export(GROUP).await.unwrap();
    let header = export.header().clone();
    let original = DiskManifest::decode(&header.manifest, GROUP)
        .unwrap()
        .source;
    assert!(
        original.exists(),
        "actual original source checkpoint exists"
    );
    let spool_path = root.path().join("source/seed.frames");
    let mut spool = File::create(&spool_path).unwrap();
    let mut chunks = 0;
    loop {
        let chunk = export.next_chunk().await.unwrap();
        let done = chunk.is_none();
        pipe::write_frame(&mut spool, &serde_json::to_vec(&chunk).unwrap()).unwrap();
        chunks += 1;
        if done {
            break;
        }
    }
    spool.flush().unwrap();
    spool.sync_all().unwrap();
    drop(spool);
    drop(export);
    // A control-only tail makes a different checkpoint, allowing native GC to expire the first.
    let executor = group.claim(spec.operation, 2, deadline()).await.unwrap();
    source::compact(node).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while original.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("original checkpoint actually expired after export lease release");
    match seed.export(GROUP).await {
        Err(_) => {}
        Ok(mut expired) => assert!(
            expired.next_chunk().await.is_err(),
            "same descriptor cannot re-export expired source storage"
        ),
    }
    assert!(
        !original.exists(),
        "target installation starts only after source checkpoint removal"
    );
    let admission = source::admitted(
        sources
            .authority
            .admit(StartupRequest {
                group_id: 7,
                node_id: 4,
                generation: 40,
                request_id: [44; 16],
            })
            .await
            .unwrap(),
    );
    let scope = NativeTargetScope {
        version: NATIVE_TARGET_VERSION,
        target: identity.clone(),
        specification: spec.clone(),
        correlation: [59; 16],
    };
    // Independent known literals reject old generation, domain, version and full configuration.
    for mutation in 0..5 {
        let mut wrong = scope.clone();
        match mutation {
            0 => wrong.target.generation = 39,
            1 => wrong.target.domain = Some([72; 32]),
            2 => wrong.version = 0,
            3 => wrong.specification.target_voters = [1, 2, 3].into(),
            _ => {
                wrong.specification.expected_start = multiraft_core::typ::StoredMembership::new(
                    *spec.expected_start.log_id(),
                    openraft::Membership::new(
                        vec![[1, 2, 3].into()],
                        [1, 2, 3]
                            .into_iter()
                            .map(|id| {
                                (
                                    id,
                                    openraft::BasicNode {
                                        addr: "different-config-identity".into(),
                                    },
                                )
                            })
                            .collect::<std::collections::BTreeMap<_, _>>(),
                    )
                    .unwrap(),
                )
            }
        }
        let reply = exchange(
            &pipe,
            &wrong,
            NativeTargetCommand::Observe {
                configuration: None,
                barrier: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            reply.outcome,
            NativeTargetOutcome::Rejected,
            "bad scope case {mutation}"
        );
    }
    let bad_evidence = exchange(
        &pipe,
        &scope,
        NativeTargetCommand::Observe {
            configuration: Some(spec.expected_start.clone()),
            barrier: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        bad_evidence.outcome,
        NativeTargetOutcome::Rejected,
        "evidence must name the exact configuration log barrier"
    );
    let start = exchange(
        &pipe,
        &scope,
        NativeTargetCommand::StartSeed {
            header: Box::new(header),
            admission,
        },
    )
    .await
    .unwrap();
    assert_eq!(start.outcome, NativeTargetOutcome::InProgress);
    let mut spool = File::open(&spool_path).unwrap();
    for sequence in 0..chunks {
        let chunk: Option<DiskChunk> =
            serde_json::from_slice(&pipe::read_frame(&mut spool).unwrap()).unwrap();
        assert_eq!(
            exchange(
                &pipe,
                &scope,
                NativeTargetCommand::SeedChunk { sequence, chunk }
            )
            .await
            .unwrap()
            .outcome,
            NativeTargetOutcome::InProgress
        );
    }
    let lost = exchange(&pipe, &scope, NativeTargetCommand::FinishSeed)
        .await
        .unwrap_err();
    assert_eq!(lost.outcome, NativeTargetOutcome::Unknown);
    let status = port.status().await.unwrap();
    assert_eq!(status.stage, NativeTargetStage::Opened);
    assert_eq!(port.identity(), &identity);
    assert_eq!(status.record.as_ref().unwrap().specification, spec);
    assert_eq!(
        pipe.diagnostics().await,
        (vec![prefix.clone()], 1),
        "actual target disk rows and exactly one factory construction after lost reply"
    );
    pipe.lose_next_observe();
    let unknown = group
        .add_learner_with_port(&executor, &port, deadline())
        .await
        .unwrap_err();
    assert!(
        matches!(
            unknown,
            multiraft_net::ControlledError::TargetOutcome {
                outcome: NativeTargetOutcome::Unknown,
                ..
            }
        ),
        "source actor conservatively classifies lost target observation: {unknown:?}"
    );
    assert_eq!(
        port.status().await.unwrap().stage,
        NativeTargetStage::Opened
    );
    group
        .add_learner_with_port(&executor, &port, deadline())
        .await
        .unwrap();
    let tail = br#"{"control":{"Complete":999},"opaque":"tail"}"#.to_vec();
    node.propose(GROUP, tail.clone()).await.unwrap();
    group
        .enter_joint_with_port(&executor, &port, deadline())
        .await
        .unwrap();
    assert_eq!(
        group
            .membership(deadline())
            .await
            .unwrap()
            .membership()
            .get_joint_config()
            .len(),
        2
    );
    group.finalize_uniform(&executor, deadline()).await.unwrap();
    let membership = group.membership(deadline()).await.unwrap();
    assert_eq!(
        membership.membership().get_joint_config(),
        &vec![spec.target_voters.clone()]
    );
    assert_eq!(membership.membership().learner_ids().count(), 0);
    sources.authority.lose_next_handoff_reply();
    let terminal = group
        .complete_with_port(&executor, &port, deadline())
        .await
        .unwrap();
    assert_eq!(terminal.proof().configuration, membership);
    assert_eq!(terminal.proof().target_open.target_node, 4);
    assert_eq!(terminal.proof().target_open.generation, 40);
    assert!(terminal.proof().target_open.applied.index >= membership.log_id().unwrap().index);
    assert!(terminal.proof().barrier.index > membership.log_id().unwrap().index);
    let catalog = sources.authority.read(GROUP).await.unwrap();
    assert_eq!(catalog.completed.len(), 1);
    assert_eq!(
        catalog
            .authorized
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        spec.target_voters
    );
    let after = b"actual application write after native completion".to_vec();
    node.propose(GROUP, after.clone()).await.unwrap();
    let barrier = node
        .read_linearizable_with_applied_at(GROUP, deadline(), |_| ())
        .await
        .unwrap()
        .applied;
    let expected = vec![prefix, tail, after];
    for peer in &sources.nodes {
        if spec.target_voters.contains(&peer.node_id()) {
            source::wait_payloads(peer, &expected).await;
            let view = peer
                .read_applied_with_watermark(GROUP, |fsm| fsm.payloads().unwrap())
                .await
                .unwrap();
            assert_eq!(view.value, expected);
            let applied = view.applied.unwrap();
            assert!(
                applied.index > barrier.index || applied == barrier,
                "retained source covers quorum-confirmed read barrier"
            );
        }
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (rows, creations) = pipe.diagnostics().await;
            assert_eq!(creations, 1);
            if rows == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("new write applies to independent target disk");
    let status = port.status().await.unwrap();
    assert_eq!(status.configuration, Some(membership));
    assert!(status.applied.unwrap().index >= terminal.proof().barrier.index);
    assert!(status.record.unwrap().terminal.is_some());
    pipe.read_barrier(barrier, expected).await;
    pipe.stop().await;
    drop(port);
    drop(pipe);
    sources.stop().await;
}
