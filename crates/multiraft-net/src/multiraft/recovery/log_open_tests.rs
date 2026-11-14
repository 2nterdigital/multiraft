//! Real opener cancellation: queued before IO and running while owning the DB.
use super::*;
use crate::{GroupConfig, NodeOwner, RuntimeConfig, RuntimeError, RuntimeTransport, SharedFabric};
use std::sync::{Condvar, Mutex};

#[derive(Default)]
pub(super) struct Probe {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: Condvar,
}
impl Probe {
    pub(super) fn pause(&self) {
        self.entered.notify_one();
        let guard = self.released.lock().unwrap();
        drop(
            self.changed
                .wait_while(guard, |released| !*released)
                .unwrap(),
        );
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Probe>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
static RUNNING_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static PROBE: Mutex<Option<(PathBuf, Arc<Probe>)>> = Mutex::new(None);
pub(super) fn probe(root: &std::path::Path) -> Option<Arc<Probe>> {
    PROBE
        .lock()
        .unwrap()
        .as_ref()
        .filter(|(path, _)| path == root)
        .map(|(_, probe)| probe.clone())
}
fn config(root: &std::path::Path) -> RuntimeConfig {
    let mut cluster = ClusterConfig::for_test(1, &[1]);
    cluster.data_dir = root.to_path_buf();
    cluster.log_storage_backend = LogStorageBackend::RocksDb;
    let mut config = RuntimeConfig::new(
        cluster,
        vec![GroupConfig {
            group_id: 7,
            voters: vec![1],
        }],
    );
    config.transport = RuntimeTransport::InProcess(SharedFabric::new());
    config
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap()
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_millis(50)
}

#[test]
fn queued_log_open_is_joined_and_fenced_after_start_deadline() {
    let runtime = runtime();
    runtime.block_on(async {
        let root = tempfile::tempdir().unwrap();
        let hold = Arc::new(Probe::default());
        let _release = Release(hold.clone());
        let blocking_hold = hold.clone();
        let blocker = tokio::task::spawn_blocking(move || blocking_hold.pause());
        hold.entered.notified().await;
        let starting = tokio::spawn(NodeOwner::start(
            config(root.path()),
            |_| Ok(CounterFsm::default()),
            deadline(),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !starting.is_finished(),
            "failed startup retains queued opener until joined"
        );
        assert!(!root.path().join("raft-rocksdb").exists());
        hold.release();
        blocker.await.unwrap();
        assert!(matches!(
            starting.await.unwrap(),
            Err(RuntimeError::Deadline {
                outcome_unknown: true,
                ..
            })
        ));
        assert!(
            !root.path().join("raft-rocksdb").exists(),
            "queued opener must observe cleanup fence before mutation"
        );
        assert!(!root.path().join("raft-log-backend").exists());
    });
}

#[test]
fn running_log_open_remains_owned_until_database_lock_is_released() {
    let runtime = runtime();
    runtime.block_on(async {
        let _serial = RUNNING_TESTS.lock().await;
        let root = tempfile::tempdir().unwrap();
        let hold = Arc::new(Probe::default());
        let _release = Release(hold.clone());
        *PROBE.lock().unwrap() = Some((root.path().to_path_buf(), hold.clone()));
        let starting = tokio::spawn(NodeOwner::start(
            config(root.path()),
            |_| Ok(CounterFsm::default()),
            deadline(),
        ));
        tokio::time::timeout(Duration::from_secs(5), hold.entered.notified())
            .await
            .unwrap();
        assert!(root.path().join("raft-rocksdb/CURRENT").is_file());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !starting.is_finished(),
            "startup cleanup must join a running opener, not detach it"
        );
        hold.release();
        assert!(matches!(
            starting.await.unwrap(),
            Err(RuntimeError::Deadline {
                outcome_unknown: true,
                ..
            })
        ));
        *PROBE.lock().unwrap() = None;
        // Reopening while the first owner were detached would fail its DB lock.
        drop(RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb")).unwrap());
        assert!(root.path().join("raft-log-backend").is_file());
    });
}

#[test]
fn dropped_start_waiter_keeps_running_opener_under_node_cleanup() {
    let runtime = runtime();
    runtime.block_on(async {
        let _serial = RUNNING_TESTS.lock().await;
        let root = tempfile::tempdir().unwrap();
        let hold = Arc::new(Probe::default());
        let _release = Release(hold.clone());
        *PROBE.lock().unwrap() = Some((root.path().to_path_buf(), hold.clone()));
        let starting = tokio::spawn(NodeOwner::start(
            config(root.path()),
            |_| Ok(CounterFsm::default()),
            tokio::time::Instant::now() + Duration::from_secs(8),
        ));
        tokio::time::timeout(Duration::from_secs(5), hold.entered.notified())
            .await
            .unwrap();
        starting.abort();
        assert!(matches!(starting.await, Err(error) if error.is_cancelled()));
        assert!(
            RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb")).is_err(),
            "running child still owns its DB while cleanup waits"
        );
        hold.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(db) = RocksLogDb::<TypeConfig>::open(root.path().join("raft-rocksdb")) {
                    drop(db);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        *PROBE.lock().unwrap() = None;
    });
}
