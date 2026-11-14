//! Local storage status reports FSM memory opportunistically: the memory sample
//! never waits for a busy FSM, and a failed FSM sample never fails the status.
use multiraft_core::{ClusterConfig, LogStorageBackend, SnapshotMode};
use multiraft_fsm::{
    AppliedState, ApplyOut, CounterFsm, DiskState, GroupId, StateMachine, StorageMemoryStats,
};
use multiraft_net::{FsmFactoryContext, MultiRaft, StateMachineFactory};
use multiraft_store::rocksdb::WriteBatch;
use multiraft_store::{DiskFsmDb, DiskFsmOptions};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::time::{timeout, Instant};

const GROUP: GroupId = 7;
const BOUND: Duration = Duration::from_secs(5);

/// Holds the FSM lock inside `apply` until released.
#[derive(Clone, Default)]
struct ApplyGate {
    released: Arc<(Mutex<bool>, Condvar)>,
    entered: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

struct GatedCounter {
    inner: CounterFsm,
    gate: ApplyGate,
}

impl StateMachine for GatedCounter {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        if let Some(entered) = self.gate.entered.lock().unwrap().take() {
            let _ = entered.send(());
            let (lock, ready) = &*self.gate.released;
            let open = lock.lock().unwrap();
            let (open, _) = ready
                .wait_timeout_while(open, BOUND, |open| !*open)
                .unwrap();
            assert!(*open, "apply gate release must be bounded");
        }
        self.inner.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.inner.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.inner.restore(group, bytes)
    }
}

/// A disk FSM whose memory sample can report the closed-database failure left
/// by an unrecoverable checkpoint install.
struct ClosableDiskFsm {
    db: DiskFsmDb,
    closed: Arc<AtomicBool>,
}

impl StateMachine for ClosableDiskFsm {
    type Error = io::Error;
    fn apply(&mut self, _: GroupId, _: u64, _: &[u8]) -> io::Result<ApplyOut> {
        Err(io::Error::other("whole-image apply in disk mode"))
    }
    fn snapshot(&self, _: GroupId) -> io::Result<Vec<u8>> {
        Err(io::Error::other("whole-image snapshot in disk mode"))
    }
    fn restore(&mut self, _: GroupId, _: &[u8]) -> io::Result<()> {
        Err(io::Error::other("whole-image restore in disk mode"))
    }
    fn disk_state(&mut self) -> Option<&mut dyn DiskState> {
        Some(self)
    }
}

impl DiskState for ClosableDiskFsm {
    fn storage_memory_stats(&self) -> io::Result<Option<StorageMemoryStats>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(io::Error::other("disk FSM database is closed"));
        }
        self.db.storage_memory_stats().map(Some)
    }
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        self.db.applied_record()
    }
    fn apply_durable(
        &mut self,
        _: GroupId,
        index: u64,
        _: Option<&[u8]>,
        applied: &AppliedState,
    ) -> io::Result<ApplyOut> {
        let mut batch = WriteBatch::default();
        batch.put_cf(self.db.cf("default")?, b"applied", index.to_be_bytes());
        self.db.commit(batch, applied)?;
        Ok(ApplyOut::default())
    }
}

async fn start<S: StateMachine>(
    root: &std::path::Path,
    factory: impl StateMachineFactory<S>,
) -> Arc<MultiRaft<S>> {
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = root.into();
    config.log_storage_backend = LogStorageBackend::RocksDb;
    config.snapshot_mode = SnapshotMode::Disabled;
    let node = MultiRaft::start_with_factory(config, factory)
        .await
        .unwrap();
    node.create_group(GROUP, &[1]).await.unwrap();
    timeout(BOUND, async {
        while !node.is_leader(GROUP) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    Arc::new(node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_fsm_lock_does_not_delay_status_or_engine_memory() {
    let root = tempfile::tempdir().unwrap();
    let gate = ApplyGate::default();
    let factory_gate = gate.clone();
    let node = start(root.path(), move |_: FsmFactoryContext| {
        Ok(GatedCounter {
            inner: CounterFsm::new(),
            gate: factory_gate.clone(),
        })
    })
    .await;

    let (entered_tx, entered_rx) = mpsc::channel();
    *gate.entered.lock().unwrap() = Some(entered_tx);
    let writer = tokio::spawn({
        let node = node.clone();
        async move { node.propose(GROUP, CounterFsm::encode_add(3, 1)).await }
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(BOUND).unwrap())
        .await
        .unwrap();

    let started = Instant::now();
    let busy = timeout(Duration::from_secs(2), node.local_storage_status(GROUP))
        .await
        .expect("status must not wait for the FSM lock")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(
        busy.fsm_memory.is_none(),
        "busy FSM is unavailable, not zero"
    );
    assert!(busy.raft_engine_memory.is_some());
    assert!(busy.retained_log_bytes.is_some());

    *gate.released.0.lock().unwrap() = true;
    gate.released.1.notify_all();
    timeout(BOUND, writer).await.unwrap().unwrap().unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_fsm_database_keeps_status_successful_without_fsm_memory() {
    let root = tempfile::tempdir().unwrap();
    let closed = Arc::new(AtomicBool::new(false));
    let fsm_root = root.path().join("fsm");
    let factory_closed = closed.clone();
    let node = start(root.path(), move |context: FsmFactoryContext| {
        Ok(ClosableDiskFsm {
            db: DiskFsmDb::open(
                DiskFsmDb::group_path(&fsm_root, context.group_id()),
                context.group_id(),
                DiskFsmOptions::new(b"closable-v1".to_vec()),
            )?,
            closed: factory_closed.clone(),
        })
    })
    .await;
    let open = node.local_storage_status(GROUP).await.unwrap();
    assert!(open.fsm_memory.is_some(), "an idle disk FSM is sampled");
    assert!(open.raft_engine_memory.is_some());

    closed.store(true, Ordering::SeqCst);
    let status = node.local_storage_status(GROUP).await.unwrap();
    assert!(status.fsm_memory.is_none());
    assert!(status.raft_engine_memory.is_some());
    assert!(status.retained_log_bytes.is_some());
    node.shutdown().await.unwrap();
}
