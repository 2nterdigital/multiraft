//! Cancellation/serialization and the registration deadline through public APIs.
mod startup_support;
use multiraft_core::MultiRaftError;
use multiraft_net::{
    InitializeDisposition, RuntimeError, StartupCleanup, StartupPhase, StartupRejection,
};
use startup_support::*;
use std::time::Duration;
use tokio::time::{timeout, Instant};
const BUDGET: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_later_constructor_charges_earlier_registration_and_never_dispatches_initialize() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let mut factory = Factory::new(root.path());
    factory.construct = Some((8, gate.clone()));
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let worker = h.clone();
    let budget = Duration::from_millis(150);
    let mut task = tokio::spawn(async move {
        worker
            .start_groups_with_preference(batch(&[7, 8], &[1], None, budget))
            .await
    });
    timeout(BUDGET, gate.entered.notified()).await.unwrap();
    tokio::time::sleep(budget * 2).await;
    assert!(timeout(Duration::from_millis(20), &mut task).await.is_err());
    assert!(root.path().join("consumer-7.lease").exists());
    assert!(root.path().join("consumer-8.lease").exists());
    gate.release();
    let e = task.await.unwrap().unwrap_err();
    assert_eq!(e.group_id, Some(7));
    assert_eq!(e.phase, StartupPhase::Registered);
    assert_eq!(e.cleanup, StartupCleanup::Released);
    for g in e.report.groups {
        assert_eq!(g.initialization, InitializeDisposition::NotDispatched);
        assert_eq!(g.deadline.unwrap() - g.registered_at.unwrap(), budget);
    }
    reusable(root.path(), &peers);
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_cannot_race_a_legacy_startup_admitted_before_its_native_registration() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let mut factory = Factory::new(root.path());
    factory.construct = Some((7, gate.clone()));
    let calls = factory.calls.clone();
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let worker = h.clone();
    let legacy = tokio::spawn(async move {
        worker
            .create_group_with_recovery_timeout(
                multiraft_net::GroupConfig {
                    group_id: 7,
                    voters: vec![1],
                },
                BUDGET,
            )
            .await
    });
    timeout(BUDGET, gate.entered.notified()).await.unwrap();
    let e = h
        .start_groups_with_preference(batch(&[8], &[1], None, BUDGET))
        .await
        .unwrap_err();
    assert_eq!(e.rejection, Some(StartupRejection::Busy));
    assert_eq!(e.cleanup, StartupCleanup::NotRequired);
    assert_eq!(*calls.lock().unwrap(), vec![7]);
    gate.release();
    legacy.await.unwrap().unwrap();
    leader(std::slice::from_ref(&h), 7)
        .await
        .propose(7, vec![5], deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_batch_waiter_retains_reservation_and_factory_until_completion() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let mut factory = Factory::new(root.path());
    factory.construct = Some((7, gate.clone()));
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let worker = h.clone();
    let task = tokio::spawn(async move {
        worker
            .start_groups_with_preference(batch(&[7], &[1], None, BUDGET))
            .await
    });
    timeout(BUDGET, gate.entered.notified()).await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let e = h
        .start_groups_with_preference(batch(&[8], &[1], None, BUDGET))
        .await
        .unwrap_err();
    assert_eq!(e.rejection, Some(StartupRejection::Busy));
    assert!(root.path().join("consumer-7.lease").exists());
    gate.release();
    timeout(BUDGET, async {
        loop {
            if h.local_group_status(7, deadline()).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let e = h
        .start_groups_with_preference(batch(&[8], &[1], None, BUDGET))
        .await
        .unwrap_err();
    assert_eq!(e.rejection, Some(StartupRejection::OwnerNotEmpty));
    leader(std::slice::from_ref(&h), 7)
        .await
        .propose(7, vec![6], deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_during_grace_fences_dispatch_and_releases_listener_and_fsm() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let factory = Factory::new(root.path());
    let calls = factory.calls.clone();
    let owner = start(root.path(), 2, &peers, factory, true).await;
    let h = owner.handle();
    let worker = h.clone();
    let mut input = batch(&[7], &[1, 2, 3], Some(1), BUDGET);
    input.grace = Duration::from_millis(1000);
    let task = tokio::spawn(async move { worker.start_groups_with_preference(input).await });
    timeout(BUDGET, async {
        while calls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    owner.shutdown(deadline()).await.unwrap();
    let e = task.await.unwrap().unwrap_err();
    assert_eq!(
        e.report.groups[0].initialization,
        InitializeDisposition::NotDispatched
    );
    assert_eq!(e.cleanup, StartupCleanup::Released);
    assert!(matches!(
        h.local_group_status(7, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    reusable(root.path(), &peers[1..2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validator_outside_budget_still_holds_every_executable_capability_until_completion() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let mut factory = Factory::new(root.path());
    factory.validate = Some((8, gate.clone()));
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let worker = h.clone();
    let budget = Duration::from_millis(500);
    let task = tokio::spawn(async move {
        worker
            .start_groups_with_preference(batch(&[7, 8], &[1], None, budget))
            .await
    });
    timeout(BUDGET, gate.entered.notified()).await.unwrap();
    tokio::time::sleep(budget * 2).await;
    assert!(matches!(
        h.propose(7, vec![1], deadline()).await,
        Err(RuntimeError::Source(MultiRaftError::UnknownGroup(7)))
    ));
    gate.release();
    let report = task.await.unwrap().unwrap();
    assert!(report
        .groups
        .iter()
        .all(|g| Instant::now() > g.deadline.unwrap()));
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persisted_earlier_group_also_expires_during_a_later_slow_constructor() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let owner = start(root.path(), 1, &peers, Factory::new(root.path()), true).await;
    let h = owner.handle();
    h.start_groups_with_preference(batch(&[7], &[1], Some(1), BUDGET))
        .await
        .unwrap();
    leader(std::slice::from_ref(&h), 7)
        .await
        .propose(7, vec![77], deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    let gate = Gate::default();
    let _release = Release(vec![gate.clone()]);
    let mut factory = Factory::new(root.path());
    factory.construct = Some((8, gate.clone()));
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let task = tokio::spawn(async move {
        h.start_groups_with_preference(batch(&[7, 8], &[1], Some(1), Duration::from_millis(150)))
            .await
    });
    timeout(BUDGET, gate.entered.notified()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.release();
    let failure = task.await.unwrap().unwrap_err();
    assert_eq!(failure.group_id, Some(7));
    assert_eq!(failure.phase, StartupPhase::Registered);
    assert_eq!(
        failure.report.groups[0].provenance,
        Some(multiraft_core::StartupProvenance::Persisted)
    );
    assert_eq!(
        failure.report.groups[0].initialization,
        InitializeDisposition::NotDispatched
    );
    assert_eq!(failure.cleanup, StartupCleanup::Released);
    reusable(root.path(), &peers);
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn later_validator_rejection_preserves_original_chain_and_rolls_back_whole_batch() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr())];
    let mut factory = Factory::new(root.path());
    factory.reject_validation = Some(8);
    let owner = start(root.path(), 1, &peers, factory, true).await;
    let h = owner.handle();
    let failure = h
        .start_groups_with_preference(batch(&[7, 8], &[1], None, BUDGET))
        .await
        .unwrap_err();
    assert_eq!(failure.group_id, Some(8));
    assert_eq!(failure.phase, StartupPhase::Validate);
    assert!(failure
        .report
        .groups
        .iter()
        .all(|g| g.phase != StartupPhase::Ready));
    let mut error: Option<&(dyn std::error::Error + 'static)> = Some(&failure);
    let mut original = false;
    while let Some(source) = error {
        original |= source.to_string().contains("original validator failure");
        error = source.source();
    }
    assert!(original);
    assert_eq!(failure.cleanup, StartupCleanup::Released);
    assert!(matches!(
        h.local_group_status(7, deadline()).await,
        Err(RuntimeError::Closed)
    ));
    reusable(root.path(), &peers);
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_native_waiter_keeps_original_deadline_then_owner_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    let peers = vec![(1, addr()), (2, addr()), (3, addr())];
    let factory = Factory::new(root.path());
    let calls = factory.calls.clone();
    let owner = start(root.path(), 2, &peers, factory, false).await;
    let h = owner.handle();
    let worker = h.clone();
    let mut input = batch(&[7], &[1, 2, 3], Some(2), Duration::from_millis(1000));
    input.grace = Duration::from_millis(100);
    let task = tokio::spawn(async move { worker.start_groups_with_preference(input).await });
    timeout(BUDGET, async {
        while calls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let error = h
        .start_groups_with_preference(batch(&[8], &[1, 2, 3], Some(2), BUDGET))
        .await
        .unwrap_err();
    assert_eq!(error.rejection, Some(StartupRejection::Busy));
    timeout(BUDGET, async {
        loop {
            if matches!(
                h.local_group_status(7, deadline()).await,
                Err(RuntimeError::Closed)
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    reusable(root.path(), &peers[1..2]);
}
