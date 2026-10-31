//! Strict owned recovery rejects absent native authority before application construction.
use multiraft_core::{ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeTransport, SharedFabric};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn config(root: &Path) -> RuntimeConfig {
    let mut cluster = ClusterConfig::for_test(1, &[1]);
    cluster.data_dir = root.to_owned();
    cluster.log_storage_backend = LogStorageBackend::RocksDb;
    cluster.snapshot_mode = SnapshotMode::NativeDurable;
    let mut config = RuntimeConfig::new(cluster, vec![]);
    config.transport = RuntimeTransport::InProcess(SharedFabric::new());
    config
}
fn group(id: u64) -> GroupConfig {
    GroupConfig {
        group_id: id,
        voters: vec![1],
    }
}
fn scratch() -> tempfile::TempDir {
    tempfile::tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp")).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn missing_group_in_initialized_node_never_constructs_an_application() {
    let root = scratch();
    let owner = NodeOwner::start(
        config(root.path()),
        |_| Ok(CounterFsm::default()),
        deadline(),
    )
    .await
    .unwrap();
    owner
        .handle()
        .create_group(group(7), deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let owner = NodeOwner::start(
        config(root.path()),
        move |_| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CounterFsm::default())
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    assert!(handle
        .recover_existing_group(group(8), deadline())
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(handle.local_group_status(8, deadline()).await.is_err());
    drop(owner);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pristine_namespace_and_vote_only_history_reject_before_factory_and_preserve_vote() {
    use multiraft_core::{typ, TypeConfig};
    use multiraft_store::RocksLogDb;
    use openraft::{storage::RaftLogStorage, RaftLogReader};
    for vote_only in [false, true] {
        let root = scratch();
        {
            let db = RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb")).unwrap();
            let mut store = db.group(7).unwrap();
            if vote_only {
                store.save_vote(&typ::Vote::new(4, 1)).await.unwrap();
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = calls.clone();
        let owner = NodeOwner::start(
            config(root.path()),
            move |_| {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok(CounterFsm::default())
            },
            deadline(),
        )
        .await
        .unwrap();
        assert!(owner
            .handle()
            .recover_existing_group(group(7), deadline())
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let db = RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb")).unwrap();
        assert_eq!(
            db.group(7).unwrap().read_vote().await.unwrap(),
            vote_only.then(|| typ::Vote::new(4, 1))
        );
        drop(owner);
    }
}

async fn seed(root: &Path) {
    let fabric = SharedFabric::new();
    let node = fabric.start_node(config(root).cluster).await.unwrap();
    node.create_group(7, &[1]).await.unwrap();
    multiraft_net::wait_for_leader(std::slice::from_ref(&node), 7, Duration::from_secs(5))
        .await
        .unwrap();
    node.propose(7, CounterFsm::encode_add(15, 1))
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cold_recovery_replays_durable_log_and_ignores_declaration_voters() {
    let root = scratch();
    seed(root.path()).await;
    let owner = NodeOwner::start(
        config(root.path()),
        |_| Ok(CounterFsm::default()),
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    handle
        .recover_existing_group(
            GroupConfig {
                group_id: 7,
                voters: vec![99],
            },
            deadline(),
        )
        .await
        .unwrap();
    let value = handle
        .try_read_applied(7, deadline(), |fsm| {
            Ok::<_, std::convert::Infallible>(fsm.value(7))
        })
        .await
        .unwrap();
    assert_eq!(value, 15);
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn purged_log_without_local_fsm_or_snapshot_fails_before_group_publication() {
    use multiraft_core::TypeConfig;
    use multiraft_store::RocksLogDb;
    use openraft::storage::RaftLogStorage;
    let root = scratch();
    seed(root.path()).await;
    {
        let db = RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb")).unwrap();
        let mut log = db.group(7).unwrap();
        let committed = log.read_committed().await.unwrap().unwrap();
        log.purge(committed).await.unwrap();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let owner = NodeOwner::start(
        config(root.path()),
        move |_| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CounterFsm::default())
        },
        deadline(),
    )
    .await
    .unwrap();
    let handle = owner.handle();
    assert!(handle
        .recover_existing_group(group(7), deadline())
        .await
        .is_err());
    // The generic factory must open local disk state before its basis can be read.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(handle.local_group_status(7, deadline()).await.is_err());
    drop(owner);
    // Failure never repairs or clears source native history.
    let db = RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb")).unwrap();
    assert!(db
        .group(7)
        .unwrap()
        .get_log_state()
        .await
        .unwrap()
        .last_purged_log_id
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn already_ready_legacy_group_cannot_bypass_strict_recovery_validation() {
    let mut inputs = config(Path::new(""));
    inputs.cluster.snapshot_mode = SnapshotMode::Disabled;
    inputs.groups = vec![group(7)];
    let owner = NodeOwner::start(inputs, |_| Ok(CounterFsm::default()), deadline())
        .await
        .unwrap();
    assert!(owner
        .handle()
        .recover_existing_group(group(7), deadline())
        .await
        .is_err());
    drop(owner);
}

struct ValidateCounter {
    calls: Arc<AtomicUsize>,
    reject: bool,
}
impl multiraft_net::StateMachineFactory<CounterFsm> for ValidateCounter {
    fn create(&self, _: multiraft_net::FsmFactoryContext) -> anyhow::Result<CounterFsm> {
        Ok(CounterFsm::default())
    }
    fn validate_recovered(
        &self,
        context: multiraft_net::FsmFactoryContext,
        fsm: &CounterFsm,
    ) -> anyhow::Result<()> {
        assert_eq!(
            fsm.value(context.group_id()),
            20,
            "snapshot and suffix recovered before validator"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(!self.reject, "application rejected recovery");
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_snapshot_and_suffix_recover_before_app_validator_and_rejection_drains_owner() {
    let root = scratch();
    let fabric = SharedFabric::new();
    let mut cluster = config(root.path()).cluster;
    cluster.retain_log_entries = 0;
    let node = fabric.start_node(cluster).await.unwrap();
    node.create_group(7, &[1]).await.unwrap();
    multiraft_net::wait_for_leader(std::slice::from_ref(&node), 7, Duration::from_secs(5))
        .await
        .unwrap();
    node.propose(7, CounterFsm::encode_add(15, 1))
        .await
        .unwrap();
    node.request_compaction(7).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if node.local_storage_status(7).await.unwrap().progress
                == multiraft_net::CompactionProgress::CompletedObserved
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    node.propose(7, CounterFsm::encode_add(5, 2)).await.unwrap();
    node.shutdown().await.unwrap();
    drop(node);
    drop(fabric);
    for reject in [true, false] {
        let calls = Arc::new(AtomicUsize::new(0));
        let owner = NodeOwner::start(
            config(root.path()),
            ValidateCounter {
                calls: calls.clone(),
                reject,
            },
            deadline(),
        )
        .await
        .unwrap();
        let handle = owner.handle();
        let result = handle.recover_existing_group(group(7), deadline()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if reject {
            assert!(result.is_err());
            assert!(handle.local_group_status(7, deadline()).await.is_err());
            // Error completion already drained the actual DB/FSM resources.
            let db = multiraft_store::RocksLogDb::<multiraft_core::TypeConfig>::open(
                root.path().join("raft-rocksdb"),
            )
            .unwrap();
            drop(db);
        } else {
            result.unwrap();
            assert_eq!(
                handle
                    .try_read_applied(7, deadline(), |fsm| Ok::<_, std::convert::Infallible>(
                        fsm.value(7)
                    ))
                    .await
                    .unwrap(),
                20
            );
            owner.shutdown(deadline()).await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn persisted_vote_older_than_committed_history_rejects_before_factory() {
    use multiraft_core::{typ, TypeConfig};
    use openraft::storage::RaftLogStorage;
    let root = scratch();
    seed(root.path()).await;
    {
        let db = multiraft_store::RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb"))
            .unwrap();
        db.group(7)
            .unwrap()
            .save_vote(&typ::Vote::new(0, 1))
            .await
            .unwrap();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let owner = NodeOwner::start(
        config(root.path()),
        move |_| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CounterFsm::default())
        },
        deadline(),
    )
    .await
    .unwrap();
    let result = owner
        .handle()
        .recover_existing_group(group(7), deadline())
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(result.is_err());
    drop(owner);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rf3_existing_group_recovers_from_local_native_history_without_live_peers() {
    let roots = [scratch(), scratch(), scratch()];
    let fabric = SharedFabric::new();
    let mut nodes = Vec::new();
    for (offset, root) in roots.iter().enumerate() {
        let mut cluster = config(root.path()).cluster;
        cluster.node_id = offset as u64 + 1;
        cluster.peers = ClusterConfig::for_test(cluster.node_id, &[1, 2, 3]).peers;
        nodes.push(fabric.start_node(cluster).await.unwrap());
    }
    for node in &nodes {
        node.create_group(7, &[1, 2, 3]).await.unwrap();
    }
    let leader = multiraft_net::wait_for_leader(&nodes, 7, Duration::from_secs(5))
        .await
        .unwrap();
    nodes
        .iter()
        .find(|node| node.node_id() == leader)
        .unwrap()
        .propose(7, CounterFsm::encode_add(15, 1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if nodes[0].with_fsm(7, |fsm| fsm.value(7)).await == Some(15) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut isolated = config(roots[0].path());
    isolated.cluster.peers = ClusterConfig::for_test(1, &[1, 2, 3]).peers;
    for node in &nodes {
        node.shutdown().await.unwrap();
    }
    drop(nodes);
    drop(fabric);
    let owner = NodeOwner::start(isolated, |_| Ok(CounterFsm::default()), deadline())
        .await
        .unwrap();
    let handle = owner.handle();
    handle
        .recover_existing_group(group(7), deadline())
        .await
        .unwrap();
    assert_eq!(
        handle
            .try_read_applied(7, deadline(), |fsm| Ok::<_, std::convert::Infallible>(
                fsm.value(7)
            ))
            .await
            .unwrap(),
        15
    );
    owner.shutdown(deadline()).await.unwrap();
}
