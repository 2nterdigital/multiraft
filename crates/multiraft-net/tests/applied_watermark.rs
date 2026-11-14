//! Local values and watermarks must describe one application-lock view.
use futures::{stream, StreamExt};
use multiraft_core::typ::{Entry, LogId};
use multiraft_core::{ClusterConfig, MultiRaftError, Request, TypeConfig};
use multiraft_fsm::{ApplyOut, GroupId, StateMachine};
use multiraft_net::{
    GroupConfig, NodeOwner, ObservedLogId, RuntimeConfig, RuntimeError, RuntimeTransport,
    SharedFabric, TryReadError,
};
use multiraft_store::StateMachineStore;
use openraft::alias::LeaderIdOf;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;
use openraft::EntryPayload;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Default)]
struct IndexFsm {
    index: u64,
    validate: bool,
}
impl StateMachine for IndexFsm {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, index: u64, _: &[u8]) -> io::Result<ApplyOut> {
        self.index = index;
        Ok(ApplyOut::default())
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        Ok(self.index.to_le_bytes().to_vec())
    }
    fn restore(&mut self, _: GroupId, bytes: &[u8]) -> io::Result<()> {
        self.index = u64::from_le_bytes(bytes.try_into().unwrap());
        Ok(())
    }
    fn requires_recovery_validation(&self) -> bool {
        self.validate
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn entry(index: u64) -> Entry {
    Entry {
        log_id: LogId::new(LeaderIdOf::<TypeConfig>::new_committed(3, 1), index),
        payload: EntryPayload::Normal(Request::new(vec![1])),
    }
}

#[tokio::test]
async fn never_applied_is_none_and_recovery_gate_prevents_query_execution() {
    let store = StateMachineStore::new(
        7,
        IndexFsm {
            validate: true,
            ..IndexFsm::default()
        },
    );
    let (value, applied) = store
        .try_with_fsm_and_applied(|fsm| fsm.index)
        .await
        .unwrap();
    assert_eq!((value, applied), (0, None));
    store.begin_recovery_validation();
    let called = AtomicBool::new(false);
    let error = store
        .try_with_fsm_and_applied(|_| called.store(true, Ordering::SeqCst))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert!(!called.load(Ordering::SeqCst));
    store.validate_recovery(|_| Ok(())).await.unwrap();
    assert_eq!(
        store
            .try_with_fsm_and_applied(|fsm| fsm.index)
            .await
            .unwrap(),
        (0, None)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_apply_holds_one_view_lock_through_value_and_watermark() {
    let store = StateMachineStore::new(7, IndexFsm::default());
    let mut applying = store.clone();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let apply_entered = entered.clone();
    let apply_release = release.clone();
    let writer = tokio::spawn(async move {
        // apply() holds the application lock while waiting for its entry stream.
        let entries = stream::once(async move {
            apply_entered.notify_one();
            apply_release.notified().await;
            Ok((entry(11), None))
        })
        .boxed();
        applying.apply(entries).await.unwrap();
    });
    entered.notified().await;
    let querying = store.clone();
    let reader = tokio::spawn(async move {
        querying
            .try_with_fsm_and_applied(|fsm| fsm.index)
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    assert!(!reader.is_finished());
    release.notify_one();
    writer.await.unwrap();
    let (value, applied) = reader.await.unwrap();
    assert_eq!(value, 11);
    assert_eq!(applied, Some(entry(11).log_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_queries_pair_concurrent_applies_and_preserve_runtime_and_query_errors() {
    let mut config = RuntimeConfig::new(
        ClusterConfig::for_test(1, &[1]),
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    );
    config.transport = RuntimeTransport::InProcess(SharedFabric::new());
    let owner = NodeOwner::start(config, |_| Ok(IndexFsm::default()), deadline())
        .await
        .unwrap();
    let handle = owner.handle();
    tokio::time::timeout_at(deadline(), async {
        loop {
            match handle.read_linearizable(7, deadline(), |_| ()).await {
                Ok(()) => break,
                Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                    tokio::task::yield_now().await
                }
                error => panic!("unexpected readiness: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    let first = handle.propose(7, vec![1], deadline()).await.unwrap();
    let initial = handle
        .try_read_applied_with_watermark(7, deadline(), |fsm| Ok::<_, io::Error>(fsm.index))
        .await
        .unwrap();
    assert_eq!(initial.value, first.index);
    assert_eq!(
        initial.applied,
        Some(ObservedLogId::new(first.term, 1, first.index))
    );
    let writing = handle.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let writer_finished = finished.clone();
    let writer = tokio::spawn(async move {
        for _ in 0..128 {
            writing.propose(7, vec![1], deadline()).await.unwrap();
            tokio::task::yield_now().await;
        }
        writer_finished.store(true, Ordering::SeqCst);
    });
    let mut samples = 0;
    tokio::time::timeout_at(deadline(), async {
        while !finished.load(Ordering::SeqCst) {
            let view = handle
                .try_read_applied_with_watermark(7, deadline(), |fsm| Ok::<_, io::Error>(fsm.index))
                .await
                .unwrap();
            assert_eq!(view.value, view.applied.unwrap().index);
            samples += 1;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    writer.await.unwrap();
    assert!(samples > 0);
    assert!(matches!(
        handle
            .try_read_applied_with_watermark(8, deadline(), |_| Ok::<_, io::Error>(()))
            .await,
        Err(TryReadError::Runtime(RuntimeError::Source(
            MultiRaftError::UnknownGroup(8)
        )))
    ));
    assert!(matches!(
        handle
            .try_read_applied_with_watermark(7, Instant::now(), |_| Ok::<_, io::Error>(()))
            .await,
        Err(TryReadError::Runtime(RuntimeError::Deadline {
            outcome_unknown: false,
            ..
        }))
    ));
    assert!(matches!(
        handle
            .try_read_applied_with_watermark(7, deadline(), |_| Err::<(), _>("query refused"))
            .await,
        Err(TryReadError::Application("query refused"))
    ));
    owner.shutdown(deadline()).await.unwrap();
    assert!(matches!(
        handle
            .try_read_applied_with_watermark(7, deadline(), |_| Ok::<_, io::Error>(()))
            .await,
        Err(TryReadError::Runtime(RuntimeError::Closed))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_stale_read_preserves_flag_and_result_shape_with_atomic_view() {
    for enabled in [false, true] {
        let mut config = ClusterConfig::for_test(1, &[1]);
        config.enable_stale_queries = enabled;
        let node = SharedFabric::new()
            .start_node_with_factory(config, |_| Ok(IndexFsm::default()))
            .await
            .unwrap();
        node.create_group(7, &[1]).await.unwrap();
        tokio::time::timeout_at(deadline(), async {
            loop {
                match node.read_linearizable(7, |_| ()).await {
                    Ok(()) => break,
                    Err(MultiRaftError::NotLeader { .. }) => tokio::task::yield_now().await,
                    error => panic!("unexpected readiness: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        let applied = node.propose_with_effects(7, vec![1]).await.unwrap();
        let read = node.read_stale(7, |fsm| fsm.index).await;
        if enabled {
            let read = read.unwrap();
            assert_eq!(read.value, applied.index);
            assert_eq!(
                (read.applied_index, read.applied_term),
                (applied.index, applied.term)
            );
        } else {
            assert!(matches!(read, Err(MultiRaftError::StaleQueriesDisabled)));
        }
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_reads_pair_value_with_applied_id_under_one_lock() {
    let mut config = RuntimeConfig::new(
        ClusterConfig::for_test(1, &[1]),
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    );
    config.transport = RuntimeTransport::InProcess(SharedFabric::new());
    let owner = NodeOwner::start(config, |_| Ok(IndexFsm::default()), deadline())
        .await
        .unwrap();
    let handle = owner.handle();
    tokio::time::timeout_at(deadline(), async {
        loop {
            match handle
                .read_linearizable_with_applied(7, deadline(), |_| ())
                .await
            {
                Ok(_) => break,
                Err(RuntimeError::Source(MultiRaftError::NotLeader { .. })) => {
                    tokio::task::yield_now().await
                }
                error => panic!("unexpected readiness: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    let first = handle.propose(7, vec![1], deadline()).await.unwrap();
    let initial = handle
        .read_linearizable_with_applied(7, deadline(), |fsm| fsm.index)
        .await
        .unwrap();
    assert_eq!(initial.value, first.index);
    assert_eq!(
        initial.applied,
        ObservedLogId::new(first.term, 1, first.index)
    );
    let direct = owner
        .handle()
        .read_linearizable(7, deadline(), |fsm| fsm.index)
        .await
        .unwrap();
    assert_eq!(direct, first.index);
    let writing = handle.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let writer_finished = finished.clone();
    let writer = tokio::spawn(async move {
        for _ in 0..128 {
            writing.propose(7, vec![1], deadline()).await.unwrap();
            tokio::task::yield_now().await;
        }
        writer_finished.store(true, Ordering::SeqCst);
    });
    let mut samples = 0;
    let mut last = first.index;
    tokio::time::timeout_at(deadline(), async {
        while !finished.load(Ordering::SeqCst) {
            let view = handle
                .read_linearizable_with_applied(7, deadline(), |fsm| fsm.index)
                .await
                .unwrap();
            // The value and the id come from one FSM view, and reads never regress.
            assert_eq!(view.value, view.applied.index);
            assert!(view.applied.index >= last);
            last = view.applied.index;
            samples += 1;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    writer.await.unwrap();
    assert!(samples > 0);
    let after = handle
        .read_linearizable_with_applied(7, deadline(), |fsm| fsm.index)
        .await
        .unwrap();
    assert_eq!(after.applied.index, first.index + 128);
    assert!(matches!(
        handle
            .read_linearizable_with_applied(8, deadline(), |_| ())
            .await,
        Err(RuntimeError::Source(MultiRaftError::UnknownGroup(8)))
    ));
    owner.shutdown(deadline()).await.unwrap();
    assert!(matches!(
        handle
            .read_linearizable_with_applied(7, deadline(), |_| ())
            .await,
        Err(RuntimeError::Closed)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiraft_linearizable_read_with_applied_matches_proposal() {
    let node = SharedFabric::new()
        .start_node_with_factory(
            ClusterConfig::for_test(1, &[1]),
            |_| Ok(IndexFsm::default()),
        )
        .await
        .unwrap();
    node.create_group(7, &[1]).await.unwrap();
    tokio::time::timeout_at(deadline(), async {
        loop {
            match node.read_linearizable(7, |_| ()).await {
                Ok(()) => break,
                Err(MultiRaftError::NotLeader { .. }) => tokio::task::yield_now().await,
                error => panic!("unexpected readiness: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    let applied = node.propose_with_effects(7, vec![1]).await.unwrap();
    let view = node
        .read_linearizable_with_applied_at(7, deadline(), |fsm| fsm.index)
        .await
        .unwrap();
    assert_eq!(view.value, applied.index);
    assert_eq!(
        view.applied,
        ObservedLogId::new(applied.term, 1, applied.index)
    );
    node.shutdown().await.unwrap();
}
