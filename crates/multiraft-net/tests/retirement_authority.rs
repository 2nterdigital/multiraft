//! Cold excluded sources resolve retirement locally from retained, space-bound completion authority.
#[allow(dead_code)]
#[path = "startup_authority_support/catalog.rs"]
mod catalog;
#[path = "retirement_authority_support/catalog_view.rs"]
mod catalog_view;
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
use multiraft_net::{controlled_startup as authority, ControlledError};
use session::Session;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

fn admitted<T: std::fmt::Debug>(outcome: StartupAuthorityResult<T>) -> T {
    match outcome {
        StartupAuthorityResult::Applied(value) => value,
        other => panic!("expected durable admission: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_source_resolves_fresh_retirement_without_core_quorum_or_catalog_writes() {
    let root = cluster::scratch("authority-only-retirement-");
    let (operation, source, source_id, receipt, catalog_digest) = {
        let mut session = Session::fresh(root.as_path()).await;
        let (leader, spec, executor) = session.prepare(false).await;
        session
            .group(leader)
            .enter_joint(&executor, session.group(3), deadline())
            .await
            .unwrap();
        session
            .group(leader)
            .finalize_uniform(&executor, deadline())
            .await
            .unwrap();
        let terminal = session
            .group(leader)
            .complete(&executor, session.group(3), deadline())
            .await
            .unwrap();
        let source_id = spec
            .expected_start
            .membership()
            .voter_ids()
            .find(|id| !spec.target_voters.contains(id))
            .unwrap();
        let source = (source_id - 1) as usize;
        let receipt = session.group(source).receipt().unwrap();
        let operation = spec.operation;
        let catalog_digest = Sha256::digest(std::fs::read(root.join("catalog.json")).unwrap());
        assert!(receipt.terminal_proof.is_none());
        let metadata = (operation, source, source_id, receipt, catalog_digest);
        drop(terminal);
        session.stop().await;
        metadata
    };
    let backend = Arc::new(catalog::FileAuthority::open(&root.join("catalog.json")));
    // Every previous native runtime is gone. Only this source listener/log owner restarts;
    // no group is opened and there is no current quorum available to answer ReadIndex.
    let config = cluster::configurations(&root).remove(source);
    let creations = Arc::new(AtomicUsize::new(0));
    let mut wrong_config = config.clone();
    wrong_config.raft_transport_fingerprint = Some([31; 32]);
    let wrong_node = cluster::start(wrong_config, creations.clone()).await;
    let wrong_handle = wrong_node
        .inspect_controlled_group(
            GROUP,
            receipt.generation,
            receipt.identity.clone(),
            backend.clone(),
        )
        .await
        .unwrap();
    assert!(
        wrong_handle
            .retirement_from_authority(operation, deadline())
            .await
            .is_err(),
        "local storage-owner data space must match before sealing permission"
    );
    wrong_node.shutdown().await.unwrap();
    drop(wrong_node);
    let node = cluster::start(config.clone(), creations.clone()).await;
    let authority = Arc::new(catalog_view::CatalogView {
        backend: backend.clone(),
        mode: AtomicU8::new(0),
        writes: AtomicUsize::new(0),
        source: source_id,
        generation: receipt.generation,
    });
    let handle = node
        .inspect_controlled_group(
            GROUP,
            receipt.generation,
            receipt.identity.clone(),
            authority.clone(),
        )
        .await
        .unwrap();
    assert!(node.observe_group(GROUP).is_err());
    assert_eq!(creations.load(Ordering::SeqCst), 0);
    let permit = handle
        .retirement_from_authority(operation, deadline())
        .await
        .unwrap();
    assert_eq!(
        handle.receipt().unwrap().stage,
        multiraft_store::ControlledGroupStage::Opened,
        "permission is not a stop acknowledgement"
    );
    let live = multiraft_store::DiskFsmDb::group_path(&config.data_dir, GROUP);
    assert!(live.exists());
    for mode in [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14] {
        authority.mode.store(mode, Ordering::SeqCst);
        assert!(
            handle
                .retirement_from_authority(operation, deadline())
                .await
                .is_err(),
            "corrupt/unavailable authority mode {mode} cannot mint a permit"
        );
        assert!(
            handle.retire(&permit, deadline()).await.is_err(),
            "retire must recheck permit against changed authority mode {mode}"
        );
        assert_eq!(
            handle.receipt().unwrap().stage,
            multiraft_store::ControlledGroupStage::Opened
        );
        assert!(live.exists());
    }
    authority.mode.store(9, Ordering::SeqCst);
    assert!(matches!(
        handle
            .retirement_from_authority(
                operation,
                tokio::time::Instant::now() + std::time::Duration::from_millis(20)
            )
            .await,
        Err(ControlledError::Deadline {
            outcome_unknown: false
        })
    ));
    authority.mode.store(0, Ordering::SeqCst);
    let mut missing = operation;
    missing.sequence += 1;
    assert!(handle
        .retirement_from_authority(missing, deadline())
        .await
        .is_err());
    assert!(node
        .inspect_controlled_group(
            GROUP,
            receipt.generation + 1,
            receipt.identity.clone(),
            authority.clone()
        )
        .await
        .is_err());
    handle.retire(&permit, deadline()).await.unwrap();
    assert!(live.exists(), "retire preserves the actual source database");
    assert_eq!(
        handle.receipt().unwrap().stage,
        multiraft_store::ControlledGroupStage::Retired
    );
    authority.mode.store(15, Ordering::SeqCst);
    assert!(handle.retirement_from_authority(operation, deadline()).await.is_err(), "retired local proof must agree with retained authority even when a changed proof is internally consistent");
    authority.mode.store(1, Ordering::SeqCst);
    assert!(
        handle.cleanup(&permit, deadline()).await.is_err(),
        "cleanup independently rechecks retained completed evidence"
    );
    assert!(live.exists());
    authority.mode.store(0, Ordering::SeqCst);
    handle.cleanup(&permit, deadline()).await.unwrap();
    assert!(
        !live.exists(),
        "only explicit cleanup removes source payload"
    );
    handle
        .cleanup(
            &handle
                .retirement_from_authority(operation, deadline())
                .await
                .unwrap(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        creations.load(Ordering::SeqCst),
        0,
        "query and cold retirement never construct an FSM"
    );
    assert_eq!(
        authority.writes.load(Ordering::SeqCst),
        0,
        "authority access is strictly read-only"
    );
    assert_eq!(
        Sha256::digest(std::fs::read(root.join("catalog.json")).unwrap()),
        catalog_digest,
        "native permission query/retirement does not mutate the completed catalog"
    );
    let membership = backend.read(GROUP).await.unwrap().membership;
    let next = multiraft_core::ReplacementSpec {
        operation: multiraft_core::OperationId {
            sequence: 2,
            nonce: [7; 16],
        },
        expected_start: membership.clone(),
        target_voters: membership
            .membership()
            .voter_ids()
            .filter(|id| *id != 4)
            .chain([source_id])
            .collect(),
        target_generation: receipt.generation + 1,
    };
    let new = node
        .reserve_controlled_target(GROUP, next, vec![77], backend, deadline())
        .await
        .unwrap();
    assert_eq!(new.generation(), receipt.generation + 1);
    assert!(
        handle
            .retirement_from_authority(operation, deadline())
            .await
            .is_err(),
        "old handle cannot mint permission after rejoin"
    );
    assert!(
        handle
            .retire_and_cleanup(&permit, deadline())
            .await
            .is_err(),
        "old token cannot mutate a later generation"
    );
    assert_eq!(
        new.receipt().unwrap().stage,
        multiraft_store::ControlledGroupStage::Reserved
    );
    node.shutdown().await.unwrap();
}
