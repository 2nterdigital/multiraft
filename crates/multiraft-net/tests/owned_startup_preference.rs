//! Public gRPC consumer: preference never proves leader causality or quorum.
mod startup_support;
use multiraft_core::{MultiRaftError, StartupProvenance};
use multiraft_net::{
    InitializeDisposition, RuntimeError, StartupCleanup, StartupPhase, StartupRejection,
};
use startup_support::*;
use std::time::Duration;
use tokio::time::{timeout, Instant};
const BUDGET: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_groups_register_before_initialization_and_publish_after_all_validators() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let mut factory = Factory::new(root.path());
    factory.validate = Some((8, gate.clone()));
    let calls = factory.calls.clone();
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let creating = h.clone();
    let task = tokio::spawn(async move {
        creating
            .start_groups_with_preference(batch(&[7, 8], &[1], Some(1), BUDGET))
            .await
    });
    timeout(BUDGET, gate.entered.notified()).await.unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![7, 8]);
    for group in [7, 8] {
        assert!(matches!(
            h.local_group_status(group, deadline()).await,
            Err(RuntimeError::Source(MultiRaftError::UnknownGroup(_)))
        ));
    }
    // Validators do not consume or reset native startup deadlines.
    tokio::time::sleep(Duration::from_millis(120)).await;
    gate.release();
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.input_digest, Some([7; 32]));
    assert!(!report.configuration_verified);
    for g in &report.groups {
        assert_eq!(g.phase, StartupPhase::Ready);
        assert_eq!(g.provenance, Some(StartupProvenance::Pristine));
        assert_eq!(g.initialization, InitializeDisposition::InitOk);
        assert_eq!(g.deadline.unwrap() - g.registered_at.unwrap(), BUDGET);
    }
    assert!(report.groups[1].registered_at.unwrap() >= report.groups[0].registered_at.unwrap());
    leader(std::slice::from_ref(&h), 7)
        .await
        .propose(7, vec![4, 8], deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_preference_retains_normal_native_initialization_in_batch() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let report = owner
        .handle()
        .start_groups_with_preference(batch(&[7], &[1], None, BUDGET))
        .await
        .unwrap();
    assert_eq!(
        report.groups[0].initialization,
        InitializeDisposition::InitOk
    );
    assert!(!report.groups[0].fallback);
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn absent_preferred_node_two_grpc_voters_fallback_and_commit_without_controller() {
    let r2 = tempfile::tempdir().unwrap();
    let r3 = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let o2 = start(r2.path(), 2, &peers, Factory::new(r2.path()), true).await;
    let o3 = start(r3.path(), 3, &peers, Factory::new(r3.path()), true).await;
    let h2 = o2.handle();
    let h3 = o3.handle();
    let begun = Instant::now();
    let (a, b) = tokio::join!(
        h2.start_groups_with_preference(batch(&[7, 8], &[1, 2, 3], Some(1), BUDGET)),
        h3.start_groups_with_preference(batch(&[7, 8], &[1, 2, 3], Some(1), BUDGET))
    );
    let reports = [a.unwrap(), b.unwrap()];
    assert!(begun.elapsed() >= GRACE);
    for report in &reports {
        assert!(report
            .groups
            .iter()
            .all(|g| g.initialization != InitializeDisposition::Unknown));
    }
    assert!(reports
        .iter()
        .flat_map(|r| &r.groups)
        .any(|g| g.fallback && g.initialization == InitializeDisposition::InitOk));
    let handles = [h2, h3];
    let authoritative = leader(&handles, 7).await;
    assert_eq!(
        authoritative
            .propose(7, vec![12], deadline())
            .await
            .unwrap()
            .effects,
        vec![12]
    );
    assert_eq!(
        authoritative
            .read_linearizable(7, deadline(), |fsm| fsm.value.clone())
            .await
            .unwrap(),
        vec![12]
    );
    o2.shutdown(deadline()).await.unwrap();
    o3.shutdown(deadline()).await.unwrap();
    reusable(r2.path(), &peers[1..2]);
    reusable(r3.path(), &peers[2..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persisted_restart_keeps_original_data_and_never_reclaims_initialization_preference() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let h = owner.handle();
    h.start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
        .await
        .unwrap();
    leader(std::slice::from_ref(&h), 7)
        .await
        .propose(7, vec![17, 19], deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let h = owner.handle();
    let report = h
        .start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
        .await
        .unwrap();
    let g = &report.groups[0];
    assert_eq!(g.provenance, Some(StartupProvenance::Persisted));
    assert_eq!(g.initialization, InitializeDisposition::NotDispatched);
    assert!(!g.fallback);
    assert_eq!(
        leader(std::slice::from_ref(&h), 7)
            .await
            .read_linearizable(7, deadline(), |fsm| fsm.value.clone())
            .await
            .unwrap(),
        vec![17, 19]
    );
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_and_corrupt_native_namespaces_fail_closed_before_factory() {
    for (name, bytes) in [
        ("unknown-native", b"unknown".as_slice()),
        ("log.bin", b"broken".as_slice()),
        ("hard_state.json", b"not-json".as_slice()),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("group-7")).unwrap();
        std::fs::write(root.path().join("group-7").join(name), bytes).unwrap();
        let peers = vec![(1, addr())];
        let factory = Factory::new(root.path());
        let calls = factory.calls.clone();
        let owner = start(root.path(), 1, &peers, factory, true).await;
        let error = owner
            .handle()
            .start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
            .await
            .unwrap_err();
        assert_eq!(error.phase, StartupPhase::Construct);
        assert_eq!(error.cleanup, StartupCleanup::Released);
        assert_eq!(
            error.report.groups[0].initialization,
            InitializeDisposition::NotDispatched
        );
        assert!(calls.lock().unwrap().is_empty());
        reusable(root.path(), &peers);
        owner.shutdown(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_batch_is_rejected_without_any_native_or_factory_effect() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let factory = Factory::new(root.path());
    let calls = factory.calls.clone();
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let mut invalid = vec![
        batch(&[7, 7], &[1], Some(1), BUDGET),
        batch(&[7], &[1], Some(2), BUDGET),
        batch(&[7], &[2], Some(2), BUDGET),
        batch(&[7], &[1, 1], None, BUDGET),
    ];
    let mut grace = batch(&[7], &[1], None, BUDGET);
    grace.grace = Duration::from_millis(99);
    invalid.push(grace);
    for input in invalid {
        let error = h.start_groups_with_preference(input).await.unwrap_err();
        assert_eq!(error.rejection, Some(StartupRejection::InvalidInput));
        assert_eq!(error.cleanup, StartupCleanup::NotRequired);
        assert!(!error.outcome_unknown);
    }
    assert!(calls.lock().unwrap().is_empty());
    h.start_groups_with_preference(batch(&[7], &[1], None, BUDGET))
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonempty_owner_rejection_preserves_existing_group_and_listener() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let h = owner.handle();
    h.create_group_with_recovery_timeout(
        multiraft_net::GroupConfig {
            group_id: 7,
            voters: vec![1],
        },
        BUDGET,
    )
    .await
    .unwrap();
    let e = h
        .start_groups_with_preference(batch(&[8], &[1], None, BUDGET))
        .await
        .unwrap_err();
    assert_eq!(e.rejection, Some(StartupRejection::OwnerNotEmpty));
    assert_eq!(e.cleanup, StartupCleanup::NotRequired);
    assert!(std::net::TcpListener::bind(peers[0].1).is_err());
    assert!(!root.path().join("group-8").exists());
    leader(std::slice::from_ref(&h), 7)
        .await
        .propose(7, vec![42], deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_constructor_failure_and_panic_reclaim_every_real_resource() {
    for panic in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let peers = vec![(1, addr())];
        let mut factory = Factory::new(root.path());
        if panic {
            factory.panic = Some(8);
        } else {
            factory.reject = Some(8);
        }
        let owner = start(root.path(), 1, &peers, factory, true).await;
        let e = owner
            .handle()
            .start_groups_with_preference(batch(&[7, 8], &[1], None, BUDGET))
            .await
            .unwrap_err();
        assert_eq!(e.group_id, Some(8));
        assert_eq!(e.phase, StartupPhase::Construct);
        assert_eq!(
            e.report.groups[0].initialization,
            InitializeDisposition::NotDispatched
        );
        assert!(e.report.groups[0].registered_at.is_some());
        assert_eq!(e.cleanup, StartupCleanup::Released);
        reusable(root.path(), &peers);
        owner.shutdown(deadline()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_local_startup_is_not_quorum_readiness_and_memory_budget_stays_bounded() {
    for durable in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let peers = vec![(1, addr()), (2, addr()), (3, addr())];
        let owner = start(root.path(), 2, &peers, Factory::new(root.path()), durable).await;
        let h = owner.handle();
        let result = h
            .start_groups_with_preference(batch(
                &[7],
                &[1, 2, 3],
                Some(1),
                Duration::from_millis(1200),
            ))
            .await;
        if durable {
            result.unwrap();
            assert!(matches!(
                h.propose(7, vec![99], Instant::now() + Duration::from_millis(100))
                    .await,
                Err(RuntimeError::Source(MultiRaftError::NotLeader { .. }))
                    | Err(RuntimeError::Deadline { .. })
            ));
        } else {
            let e = result.unwrap_err();
            assert_eq!(e.phase, StartupPhase::NativeWait);
            assert_eq!(e.cleanup, StartupCleanup::Released);
        }
        owner.shutdown(deadline()).await.unwrap();
        reusable(root.path(), &peers[1..2]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activated_native_snapshot_restart_restores_original_public_data() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let owner = start_native(root.path(), 1, &peers, Factory::new(root.path())).await;
    let h = owner.handle();
    h.start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
        .await
        .unwrap();
    leader(std::slice::from_ref(&h), 7)
        .await
        .propose(7, vec![3, 5, 8], deadline())
        .await
        .unwrap();
    h.request_compaction(7, deadline()).await.unwrap();
    timeout(BUDGET, async {
        loop {
            let status = h.local_storage_status(7, deadline()).await.unwrap();
            if status.durable_snapshot.is_some() && status.purged.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    let owner = start_native(root.path(), 1, &peers, Factory::new(root.path())).await;
    let h = owner.handle();
    let report = h
        .start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
        .await
        .unwrap();
    assert_eq!(
        report.groups[0].provenance,
        Some(StartupProvenance::Persisted)
    );
    assert_eq!(
        report.groups[0].initialization,
        InitializeDisposition::NotDispatched
    );
    assert_eq!(
        h.try_read_applied(7, deadline(), |fsm| Ok::<_, std::io::Error>(
            fsm.value.clone()
        ))
        .await
        .unwrap(),
        vec![3, 5, 8]
    );
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_independent_grpc_nodes_initialization_race_keeps_raw_dispositions() {
    let roots = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let mut owners = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        owners.push(
            start(
                root.path(),
                (index + 1) as u64,
                &peers,
                Factory::new(root.path()),
                true,
            )
            .await,
        );
    }
    let handles: Vec<_> = owners.iter().map(|o| o.handle()).collect();
    let (a, b, c) = tokio::join!(
        handles[0].start_groups_with_preference(batch(&[7, 8], &[1, 2, 3], Some(1), BUDGET)),
        handles[1].start_groups_with_preference(batch(&[7, 8], &[1, 2, 3], Some(2), BUDGET)),
        handles[2].start_groups_with_preference(batch(&[7, 8], &[1, 2, 3], Some(3), BUDGET))
    );
    let reports = [a.unwrap(), b.unwrap(), c.unwrap()];
    assert!(reports.iter().all(|r| !r.configuration_verified));
    assert!(reports.iter().flat_map(|r| &r.groups).all(|g| matches!(
        g.initialization,
        InitializeDisposition::InitOk
            | InitializeDisposition::NotAllowed
            | InitializeDisposition::NotDispatched
    )));
    assert!(reports
        .iter()
        .flat_map(|r| &r.groups)
        .any(|g| g.initialization == InitializeDisposition::InitOk));
    let leader = leader(&handles, 8).await;
    assert_eq!(
        leader
            .propose(8, vec![21], deadline())
            .await
            .unwrap()
            .effects,
        vec![21]
    );
    for owner in owners {
        owner.shutdown(deadline()).await.unwrap();
    }
    for (index, root) in roots.iter().enumerate() {
        reusable(root.path(), &peers[index..index + 1]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn preferred_arrival_before_or_after_grace_uses_native_election_and_original_budget() {
    for late_after_grace in [false, true] {
        let roots = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let peers = vec![(1, addr()), (2, addr()), (3, addr())];
        let o2 = start(
            roots[1].path(),
            2,
            &peers,
            Factory::new(roots[1].path()),
            true,
        )
        .await;
        let o3 = start(
            roots[2].path(),
            3,
            &peers,
            Factory::new(roots[2].path()),
            true,
        )
        .await;
        let h2 = o2.handle();
        let h3 = o3.handle();
        let c2 = h2.clone();
        let c3 = h3.clone();
        let grace = Duration::from_millis(500);
        let t2 = tokio::spawn(async move {
            let mut input = batch(&[7], &[1, 2, 3], Some(1), BUDGET);
            input.grace = grace;
            c2.start_groups_with_preference(input).await
        });
        let t3 = tokio::spawn(async move {
            let mut input = batch(&[7], &[1, 2, 3], Some(1), BUDGET);
            input.grace = grace;
            c3.start_groups_with_preference(input).await
        });
        let mut previous = Vec::new();
        let mut t2 = Some(t2);
        let mut t3 = Some(t3);
        if late_after_grace {
            previous.push(t2.take().unwrap().await.unwrap().unwrap());
            previous.push(t3.take().unwrap().await.unwrap().unwrap());
            assert!(previous.iter().any(|r| r.groups[0].fallback));
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let o1 = start(
            roots[0].path(),
            1,
            &peers,
            Factory::new(roots[0].path()),
            true,
        )
        .await;
        let h1 = o1.handle();
        let preferred = h1
            .start_groups_with_preference(batch(&[7], &[1, 2, 3], Some(1), BUDGET))
            .await
            .unwrap();
        assert_eq!(
            preferred.groups[0].deadline.unwrap() - preferred.groups[0].registered_at.unwrap(),
            BUDGET
        );
        if !late_after_grace {
            previous.push(t2.take().unwrap().await.unwrap().unwrap());
            previous.push(t3.take().unwrap().await.unwrap().unwrap());
            assert!(previous.iter().all(|r| !r.groups[0].fallback));
        }
        for r in previous {
            assert_eq!(
                r.groups[0].deadline.unwrap() - r.groups[0].registered_at.unwrap(),
                BUDGET
            );
        }
        let handles = [h1, h2, h3];
        let authoritative = leader(&handles, 7).await;
        authoritative
            .propose(7, vec![31], deadline())
            .await
            .unwrap();
        assert_eq!(
            authoritative
                .read_linearizable(7, deadline(), |fsm| fsm.value.clone())
                .await
                .unwrap(),
            vec![31]
        );
        o1.shutdown(deadline()).await.unwrap();
        o2.shutdown(deadline()).await.unwrap();
        o3.shutdown(deadline()).await.unwrap();
        for (i, root) in roots.iter().enumerate() {
            reusable(root.path(), &peers[i..i + 1]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vote_only_history_is_recovery_even_when_native_basis_is_none() {
    use openraft::storage::RaftLogStorage;
    let root = tempfile::tempdir().unwrap();
    let mut log = multiraft_store::FileLogStoreOf::open(root.path().join("group-7")).unwrap();
    log.save_vote(&multiraft_core::typ::Vote::new(4, 1))
        .await
        .unwrap();
    drop(log);
    let peers = vec![(1, addr())];
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let h = owner.handle();
    let report = h
        .start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
        .await
        .unwrap();
    assert_eq!(
        report.groups[0].provenance,
        Some(StartupProvenance::Persisted)
    );
    assert_eq!(
        report.groups[0].initialization,
        InitializeDisposition::NotDispatched
    );
    assert!(matches!(
        h.propose(7, vec![1], deadline()).await,
        Err(RuntimeError::Source(MultiRaftError::NotLeader { .. }))
    ));
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_proves_valid_inactive_snapshot_inert_before_allowing_first_initialization() {
    let root = tempfile::tempdir().unwrap();
    let catalog = multiraft_store::SnapshotCatalog::new(root.path().join("snapshots"), 1);
    let meta = openraft::alias::SnapshotMetaOf::<multiraft_core::TypeConfig> {
        last_log_id: None,
        last_membership: Default::default(),
        snapshot_id: "inert".to_owned(),
    };
    let staged = catalog
        .stage_native(7, &meta, b"must-not-restore", 1024)
        .unwrap();
    drop(staged);
    let peers = vec![(1, addr())];
    let owner = start_native(root.path(), 1, &peers, Factory::new(root.path())).await;
    let h = owner.handle();
    let report = h
        .start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
        .await
        .unwrap();
    assert_eq!(
        report.groups[0].provenance,
        Some(StartupProvenance::Pristine)
    );
    assert_eq!(
        report.groups[0].initialization,
        InitializeDisposition::InitOk
    );
    assert!(h
        .try_read_applied(7, deadline(), |fsm| Ok::<_, std::io::Error>(
            fsm.value.clone()
        ))
        .await
        .unwrap()
        .is_empty());
    assert!(catalog.load_native(7, 1024).unwrap().is_none());
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn committed_leader_failure_and_original_disk_return_use_native_recovery_without_reinitialization(
) {
    let roots = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let mut owners = Vec::new();
    for (i, root) in roots.iter().enumerate() {
        owners.push(Some(
            start(
                root.path(),
                (i + 1) as u64,
                &peers,
                Factory::new(root.path()),
                true,
            )
            .await,
        ));
    }
    let handles: Vec<_> = owners
        .iter()
        .map(|o| o.as_ref().unwrap().handle())
        .collect();
    let (a, b, c) = tokio::join!(
        handles[0].start_groups_with_preference(batch(&[7], &[1, 2, 3], Some(1), BUDGET)),
        handles[1].start_groups_with_preference(batch(&[7], &[1, 2, 3], Some(1), BUDGET)),
        handles[2].start_groups_with_preference(batch(&[7], &[1, 2, 3], Some(1), BUDGET))
    );
    a.unwrap();
    b.unwrap();
    c.unwrap();
    let original = leader(&handles, 7).await;
    original.propose(7, vec![33], deadline()).await.unwrap();
    let victim = (original.node_id() - 1) as usize;
    owners[victim]
        .take()
        .unwrap()
        .shutdown(deadline())
        .await
        .unwrap();
    let survivors: Vec<_> = handles
        .iter()
        .filter(|h| h.node_id() != original.node_id())
        .cloned()
        .collect();
    let current = leader(&survivors, 7).await;
    assert_eq!(
        current
            .read_linearizable(7, deadline(), |fsm| fsm.value.clone())
            .await
            .unwrap(),
        vec![33]
    );
    current.propose(7, vec![44], deadline()).await.unwrap();
    let returned = start(
        roots[victim].path(),
        (victim + 1) as u64,
        &peers,
        Factory::new(roots[victim].path()),
        true,
    )
    .await;
    let handle = returned.handle();
    let report = handle
        .start_groups_with_preference(batch(&[7], &[1, 2, 3], Some((victim + 1) as u64), BUDGET))
        .await
        .unwrap();
    assert_eq!(
        report.groups[0].provenance,
        Some(StartupProvenance::Persisted)
    );
    assert_eq!(
        report.groups[0].initialization,
        InitializeDisposition::NotDispatched
    );
    assert!(!report.groups[0].fallback);
    timeout(BUDGET, async {
        loop {
            if handle
                .try_read_applied(7, deadline(), |fsm| {
                    Ok::<_, std::io::Error>(fsm.value.clone())
                })
                .await
                .unwrap()
                == vec![44]
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    returned.shutdown(deadline()).await.unwrap();
    for owner in owners.into_iter().flatten() {
        owner.shutdown(deadline()).await.unwrap();
    }
    for (i, root) in roots.iter().enumerate() {
        reusable(root.path(), &peers[i..i + 1]);
    }
}
