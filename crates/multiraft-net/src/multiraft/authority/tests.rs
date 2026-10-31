//! Real native membership identities and durable control facts survive snapshot serialization.
use super::*;
#[path = "../../../tests/local_authority_support/disk.rs"]
mod disk;
use disk::Disk;
use multiraft_core::{
    ControlReply, NativeControl, OperationId, ReplacementSpec, TargetOpenReceipt,
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn scratch() -> tempfile::TempDir {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/local-authority");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}
async fn cluster(root: &std::path::Path) -> Vec<MultiRaft<Disk>> {
    let fabric = SharedFabric::new();
    let mut nodes = Vec::new();
    for id in 1..=4 {
        let mut config = ClusterConfig::for_test(id, &[1, 2, 3, 4]);
        config.data_dir = root.join(format!("node-{id}"));
        config.log_storage_backend = LogStorageBackend::RocksDb;
        config.snapshot_mode = multiraft_core::SnapshotMode::NativeDurable;
        let path = config.data_dir.clone();
        nodes.push(
            fabric
                .start_node_with_factory(config, move |context: FsmFactoryContext| {
                    Ok(Disk::open(&path, context.group_id())?)
                })
                .await
                .unwrap(),
        );
    }
    for node in &nodes[..3] {
        node.create_group(7, &[1, 2, 3]).await.unwrap();
    }
    nodes[3].spawn_local_group(7).await.unwrap();
    nodes
}
async fn snapshot(node: &MultiRaft<Disk>) -> LocalAuthoritativeMembership {
    let until = deadline();
    loop {
        match node.local_authoritative_membership(7, until).await {
            Ok(snapshot) => return snapshot,
            Err(LocalAuthorityError::Unavailable | LocalAuthorityError::NotLeader { .. })
                if Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            other => panic!("native authority must become available: {other:?}"),
        }
    }
}
async fn control(raft: &Raft<Disk>, command: NativeControl) -> NativeLedger {
    let (responder, receiver) = openraft::impls::ProgressResponder::complete_only();
    raft.write(Request::native_control(command))
        .responder(responder)
        .await
        .unwrap();
    let response = receiver.await.unwrap().unwrap();
    match serde_json::from_slice::<ControlReply>(&response.data.effects).unwrap() {
        ControlReply::Applied(ledger) => *ledger,
        rejection => panic!("real native control must apply: {rejection:?}"),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_configuration_identity_metadata_and_terminal_ledger_are_retained() {
    let root = scratch();
    let nodes = cluster(root.path()).await;
    let until = deadline();
    let leader = loop {
        if let Some(index) = nodes[..3].iter().position(|node| node.is_leader(7)) {
            break index;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let node = &nodes[leader];
    let initial = snapshot(node).await;
    let raft = node.raft(7).unwrap();
    let metadata: BTreeMap<_, _> = (1..=3)
        .map(|id| {
            (
                id,
                BasicNode {
                    addr: format!("peer-{id}.authority.invalid:22101"),
                },
            )
        })
        .collect();
    raft.change_membership(ChangeMembers::SetNodes(metadata.clone()), false)
        .await
        .unwrap();
    let updated = snapshot(node).await;
    assert_eq!(
        initial
            .membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        updated
            .membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>()
    );
    assert_ne!(
        initial.membership.log_id(),
        updated.membership.log_id(),
        "same voters do not imply same configuration identity"
    );
    assert_eq!(
        updated
            .membership
            .membership()
            .nodes()
            .map(|(id, node)| (*id, node.clone()))
            .collect::<BTreeMap<_, _>>(),
        metadata
    );
    assert_eq!(
        serde_json::from_slice::<LocalAuthoritativeMembership>(
            &serde_json::to_vec(&updated).unwrap()
        )
        .unwrap(),
        updated
    );
    let removed = (1..=3).find(|id| *id != node.node_id()).unwrap();
    let targets = (1..=4).filter(|id| *id != removed).collect();
    let operation = OperationId {
        sequence: 37,
        nonce: [11; 16],
    };
    let spec = ReplacementSpec {
        operation,
        expected_start: updated.membership.clone(),
        target_voters: targets,
        target_generation: 40,
    };
    let begun = control(
        &raft,
        NativeControl::Begin {
            specification: spec.clone(),
            executor_epoch: 1,
        },
    )
    .await;
    assert_eq!(snapshot(node).await.ledger, begun);
    raft.add_learner(
        4,
        BasicNode {
            addr: "target.authority.invalid:22101".into(),
        },
        true,
    )
    .await
    .unwrap();
    raft.change_membership(ChangeMembers::ReplaceAllVoters(spec.target_voters), false)
        .await
        .unwrap();
    let uniform = snapshot(node).await;
    let mut target_sm = nodes[3]
        .groups
        .lock()
        .unwrap()
        .get(&7)
        .unwrap()
        .state_machine
        .clone();
    let until = deadline();
    let target_applied = loop {
        let applied = target_sm.applied_state().await.unwrap();
        if applied.1 == uniform.membership {
            break applied.0.unwrap();
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let terminal = control(
        &raft,
        NativeControl::Complete {
            operation,
            executor_epoch: 1,
            target_open: TargetOpenReceipt {
                operation,
                target_node: 4,
                generation: 40,
                applied: target_applied,
                receipt: vec![17],
            },
        },
    )
    .await;
    let terminal_snapshot = snapshot(node).await;
    assert_eq!(terminal_snapshot.ledger, terminal);
    assert_eq!(terminal_snapshot.ledger.high_water, 37);
    assert_eq!(
        terminal_snapshot
            .ledger
            .current
            .as_ref()
            .unwrap()
            .specification
            .operation,
        operation
    );
    assert!(terminal_snapshot
        .ledger
        .current
        .as_ref()
        .unwrap()
        .terminal
        .is_some());
    assert_eq!(
        serde_json::from_slice::<LocalAuthoritativeMembership>(
            &serde_json::to_vec(&terminal_snapshot).unwrap()
        )
        .unwrap(),
        terminal_snapshot
    );
    drop(target_sm);
    drop(raft);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_configuration_and_begin_never_publish_old_membership_with_new_ledger() {
    let root = scratch();
    let nodes = cluster(root.path()).await;
    let until = deadline();
    let leader = loop {
        if let Some(index) = nodes[..3].iter().position(|node| node.is_leader(7)) {
            break index;
        }
        assert!(Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let node = &nodes[leader];
    let initial = snapshot(node).await;
    let raft = node.raft(7).unwrap();
    let mutate = async {
        let metadata: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                (
                    id,
                    BasicNode {
                        addr: format!("new-peer-{id}.authority.invalid:22101"),
                    },
                )
            })
            .collect();
        raft.change_membership(ChangeMembers::SetNodes(metadata), false)
            .await
            .unwrap();
        let changed = snapshot(node).await;
        assert_ne!(changed.membership.log_id(), initial.membership.log_id());
        let removed = (1..=3).find(|id| *id != node.node_id()).unwrap();
        control(
            &raft,
            NativeControl::Begin {
                specification: ReplacementSpec {
                    operation: OperationId {
                        sequence: 41,
                        nonce: [19; 16],
                    },
                    expected_start: changed.membership,
                    target_voters: (1..=4).filter(|id| *id != removed).collect(),
                    target_generation: 40,
                },
                executor_epoch: 1,
            },
        )
        .await
    };
    let observe = async {
        for _ in 0..128 {
            match node.local_authoritative_membership(7, until).await {
                Ok(view) if view.ledger.high_water == 41 => {
                    assert_eq!(
                        view.membership,
                        view.ledger
                            .current
                            .as_ref()
                            .unwrap()
                            .specification
                            .expected_start,
                        "a new Begin cannot be paired with the older membership"
                    );
                }
                Ok(view) => assert_eq!(view.ledger, NativeLedger::default()),
                Err(LocalAuthorityError::Unavailable) => {}
                other => panic!("stable leader concurrent snapshot outcome: {other:?}"),
            }
            tokio::task::yield_now().await;
        }
    };
    let (begun, ()) = tokio::join!(mutate, observe);
    let final_view = snapshot(node).await;
    assert_eq!(final_view.ledger, begun);
    assert_eq!(
        final_view.membership,
        begun.current.unwrap().specification.expected_start
    );
    drop(raft);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
