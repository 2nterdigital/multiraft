//! Public startup refuses backend changes or lost RocksDB consensus history.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, LogStorageBackend, SnapshotMode};
use multiraft_fsm::CounterFsm;
use multiraft_net::{GroupConfig, NodeOwner, RuntimeConfig};
use std::{net::TcpListener, path::Path, time::Duration};
use tokio::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(8)
}
fn config(root: &Path, backend: LogStorageBackend) -> RuntimeConfig {
    let address = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut cluster = ClusterConfig::new(1, vec![(1, address)]);
    cluster.data_dir = root.to_path_buf();
    cluster.log_storage_backend = backend;
    cluster.file_log_sync_level = FileLogSyncLevel::Data;
    cluster.snapshot_mode = SnapshotMode::NativeDurable;
    cluster.retain_log_entries = 0;
    RuntimeConfig::new(
        cluster,
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    )
}
async fn start(root: &Path, backend: LogStorageBackend) -> Result<NodeOwner<CounterFsm>, String> {
    NodeOwner::start(
        config(root, backend),
        |_| Ok(CounterFsm::default()),
        deadline(),
    )
    .await
    .map_err(|error| format!("{error:?}"))
}
fn scratch() -> tempfile::TempDir {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/issue267/runtime");
    std::fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selecting_rocks_over_legacy_history_rejects_and_preserves_file_restart() {
    let root = scratch();
    let owner = start(root.path(), LogStorageBackend::FileLog)
        .await
        .unwrap();
    owner
        .handle()
        .propose(7, CounterFsm::encode_add(5, 1), deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    let error = match start(root.path(), LogStorageBackend::RocksDb).await {
        Ok(_) => panic!("legacy FileLog must not be silently replaced"),
        Err(error) => error,
    };
    assert!(error.contains("persisted FileLog"), "{error}");
    assert!(!root.path().join("raft-log-backend").exists());
    let owner = start(root.path(), LogStorageBackend::FileLog)
        .await
        .unwrap();
    assert_eq!(
        owner
            .handle()
            .read_linearizable(7, deadline(), |fsm| fsm.value(7))
            .await
            .unwrap(),
        5
    );
    owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backend_marker_refuses_file_switch_and_deleted_rocks_database() {
    let root = scratch();
    let owner = start(root.path(), LogStorageBackend::RocksDb)
        .await
        .unwrap();
    owner
        .handle()
        .propose(7, CounterFsm::encode_add(5, 1), deadline())
        .await
        .unwrap();
    owner.shutdown(deadline()).await.unwrap();
    assert!(start(root.path(), LogStorageBackend::FileLog)
        .await
        .is_err());
    let owner = start(root.path(), LogStorageBackend::RocksDb)
        .await
        .unwrap();
    assert_eq!(
        owner
            .handle()
            .read_linearizable(7, deadline(), |fsm| fsm.value(7))
            .await
            .unwrap(),
        5
    );
    owner.shutdown(deadline()).await.unwrap();
    std::fs::remove_dir_all(root.path().join("raft-rocksdb")).unwrap();
    let error = match start(root.path(), LogStorageBackend::RocksDb).await {
        Ok(_) => panic!("lost RocksDB must not bootstrap a pristine replacement"),
        Err(error) => error,
    };
    assert!(error.contains("database missing"), "{error}");
    assert!(!root.path().join("raft-rocksdb").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_facade_shutdown_releases_rocks_engine_while_stopped_facade_is_alive() {
    use multiraft_net::SharedFabric;
    let root = scratch();
    let mut cluster = ClusterConfig::for_test(1, &[1]);
    cluster.data_dir = root.path().to_path_buf();
    cluster.log_storage_backend = LogStorageBackend::RocksDb;
    let fabric = SharedFabric::new();
    let stopped = fabric.start_node(cluster.clone()).await.unwrap();
    stopped.create_group(7, &[1]).await.unwrap();
    multiraft_net::wait_for_leader(std::slice::from_ref(&stopped), 7, Duration::from_secs(5))
        .await
        .unwrap();
    stopped
        .propose(7, CounterFsm::encode_add(9, 1))
        .await
        .unwrap();
    stopped.shutdown().await.unwrap();
    // Keep the old facade alive throughout reopen and use of the new one.
    let restarted = fabric.start_node(cluster).await.unwrap();
    restarted.create_group(7, &[1]).await.unwrap();
    restarted
        .wait_for_recovery(7, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(restarted.with_fsm(7, |fsm| fsm.value(7)).await, Some(9));
    restarted.shutdown().await.unwrap();
    assert_eq!(stopped.node_id(), 1);
}
