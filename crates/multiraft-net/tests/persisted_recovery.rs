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
