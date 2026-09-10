//! Catch volatile-only checkpoint success, non-frozen builders and refusal misclassification.
use futures::stream;
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{Request, TypeConfig};
use multiraft_fsm::CounterFsm;
use multiraft_store::{NativeSmOptions, SnapshotCatalog, StateMachineStore};
use openraft::alias::LeaderIdOf;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;
use openraft::{EntryPayload, RaftSnapshotBuilder};
use std::sync::Arc;

fn store(root: &std::path::Path, cap: usize) -> StateMachineStore<CounterFsm> {
    StateMachineStore::with_native_options(
        7,
        CounterFsm::new(),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root, 1)),
            max_snapshot_bytes: cap,
            build_budget: Arc::new(tokio::sync::Semaphore::new(1)),
        },
    )
    .unwrap()
}
async fn apply(sm: &mut StateMachineStore<CounterFsm>, index: u64, delta: i64) {
    if sm.applied_state().await.unwrap().0.is_none() {
        let origin = Entry {
            log_id: LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), 0),
            payload: EntryPayload::Blank,
        };
        sm.apply(stream::iter([Ok((origin, None))])).await.unwrap();
    }
    let entry = Entry {
        log_id: LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), index),
        payload: EntryPayload::Normal(Request::new(CounterFsm::encode_add(delta, index))),
    };
    sm.apply(stream::iter([Ok((entry, None))])).await.unwrap();
}

#[tokio::test]
async fn builder_owns_consistent_cut_and_reopen_serves_durable_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = store(dir.path(), 1024);
    apply(&mut sm, 1, 5).await;
    let mut builder = sm.try_create_snapshot_builder(false).await.unwrap();
    apply(&mut sm, 2, 10).await;
    let snapshot = builder.build_snapshot().await.unwrap();
    assert_eq!(snapshot.meta.last_log_id.unwrap().index, 1);
    let mut reopened = store(dir.path(), 1024);
    assert_eq!(reopened.applied_state().await.unwrap().0, None);
    let recovered = reopened.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(recovered.meta, snapshot.meta);
    reopened
        .install_snapshot(&recovered.meta, recovered.snapshot)
        .await
        .unwrap();
    assert_eq!(reopened.with_fsm(|fsm| fsm.value(7)).await, 5);
    assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 1);
}

#[tokio::test]
async fn normal_refusal_defers_but_forced_builder_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = store(dir.path(), 1);
    apply(&mut sm, 1, 5).await;
    assert!(sm.try_create_snapshot_builder(false).await.is_none());
    let mut forced = sm
        .try_create_snapshot_builder(true)
        .await
        .expect("force must return a builder");
    assert!(forced.build_snapshot().await.is_err());
    assert!(sm.get_current_snapshot().await.unwrap().is_none());
}

#[tokio::test]
async fn captured_builder_holds_node_admission_until_native_work_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let budget = Arc::new(tokio::sync::Semaphore::new(1));
    let mut a = StateMachineStore::with_native_options(
        7,
        CounterFsm::new(),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(dir.path(), 1)),
            max_snapshot_bytes: 1024,
            build_budget: budget.clone(),
        },
    )
    .unwrap();
    let mut b = StateMachineStore::with_native_options(
        8,
        CounterFsm::new(),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(dir.path(), 1)),
            max_snapshot_bytes: 1024,
            build_budget: budget,
        },
    )
    .unwrap();
    let builder = a.try_create_snapshot_builder(false).await.unwrap();
    assert!(b.try_create_snapshot_builder(false).await.is_none());
    drop(builder);
    assert!(b.try_create_snapshot_builder(false).await.is_some());
}

#[tokio::test]
async fn received_snapshot_is_durable_before_install_returns() {
    let source_root = tempfile::tempdir().unwrap();
    let receiver_root = tempfile::tempdir().unwrap();
    let mut source = store(source_root.path(), 1024);
    apply(&mut source, 1, 9).await;
    let snapshot = source
        .try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap();
    let mut receiver = store(receiver_root.path(), 1024);
    receiver
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    drop(receiver);
    let mut reopened = store(receiver_root.path(), 1024);
    let local = reopened.get_current_snapshot().await.unwrap().unwrap();
    reopened
        .install_snapshot(&local.meta, local.snapshot)
        .await
        .unwrap();
    assert_eq!(reopened.with_fsm(|fsm| fsm.value(7)).await, 9);
}

#[tokio::test]
async fn old_captured_builder_cannot_overwrite_newer_received_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let source_root = tempfile::tempdir().unwrap();
    let mut receiver = store(root.path(), 1024);
    apply(&mut receiver, 1, 5).await;
    let mut old = receiver.try_create_snapshot_builder(false).await.unwrap();
    let mut source = store(source_root.path(), 1024);
    apply(&mut source, 1, 5).await;
    apply(&mut source, 2, 7).await;
    let newer = source
        .try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap();
    receiver
        .install_snapshot(&newer.meta, newer.snapshot)
        .await
        .unwrap();
    let completed = old.build_snapshot().await.unwrap();
    assert_eq!(completed.meta.last_log_id.unwrap().index, 2);
    let current = receiver.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta.last_log_id.unwrap().index, 2);
    assert_eq!(receiver.with_fsm(|fsm| fsm.value(7)).await, 12);
}

#[tokio::test]
async fn forced_builder_can_serve_sufficient_checkpoint_while_build_budget_is_busy() {
    let root = tempfile::tempdir().unwrap();
    let budget = Arc::new(tokio::sync::Semaphore::new(1));
    let mut sm = StateMachineStore::with_native_options(
        7,
        CounterFsm::new(),
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root.path(), 1)),
            max_snapshot_bytes: 1024,
            build_budget: budget.clone(),
        },
    )
    .unwrap();
    apply(&mut sm, 1, 9).await;
    let first = sm
        .try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap();
    let _busy = budget.acquire_owned().await.unwrap();
    assert!(sm.try_create_snapshot_builder(false).await.is_none());
    let served = sm
        .try_create_snapshot_builder(true)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(served.meta, first.meta);
    assert_eq!(served.snapshot.into_inner(), first.snapshot.into_inner());
}

#[tokio::test]
async fn native_apply_rejects_a_missing_required_middle_entry() {
    let root = tempfile::tempdir().unwrap();
    let mut sm = store(root.path(), 1024);
    apply(&mut sm, 1, 5).await;
    let skipped = Entry {
        log_id: LogId::new(LeaderIdOf::<TypeConfig>::new_committed(1, 1), 3),
        payload: EntryPayload::Normal(Request::new(CounterFsm::encode_add(7, 3))),
    };
    assert!(sm.apply(stream::iter([Ok((skipped, None))])).await.is_err());
    assert_eq!(sm.with_fsm(|fsm| fsm.value(7)).await, 5);
    assert_eq!(sm.applied_state().await.unwrap().0.unwrap().index, 1);
}

#[tokio::test]
async fn reopened_builder_uses_distinct_snapshot_identity_for_a_new_native_cut() {
    let root = tempfile::tempdir().unwrap();
    let mut first = store(root.path(), 1024);
    apply(&mut first, 1, 5).await;
    let old = first
        .try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap();
    let mut reopened = store(root.path(), 1024);
    let saved = reopened.get_current_snapshot().await.unwrap().unwrap();
    reopened
        .install_snapshot(&saved.meta, saved.snapshot)
        .await
        .unwrap();
    apply(&mut reopened, 2, 7).await;
    let new = reopened
        .try_create_snapshot_builder(false)
        .await
        .unwrap()
        .build_snapshot()
        .await
        .unwrap();
    assert_ne!(old.meta.snapshot_id, new.meta.snapshot_id);
    assert_eq!(new.meta.last_log_id.unwrap().index, 2);
}
