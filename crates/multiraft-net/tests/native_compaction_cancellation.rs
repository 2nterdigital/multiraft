//! Cancellation must not release admission owned by submitted native work.
use multiraft_core::{ClusterConfig, FileLogSyncLevel, SnapshotMode};
use multiraft_fsm::{ApplyOut, CaptureError, CounterFsm, GroupId, StateMachine};
use multiraft_net::{CompactionProgress, CompactionRejection, MultiRaft};
use std::{
    sync::{mpsc, Arc, Condvar, Mutex},
    time::Duration,
};

struct GatedFsm {
    inner: CounterFsm,
    entered: mpsc::Sender<()>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}
impl StateMachine for GatedFsm {
    type Error = <CounterFsm as StateMachine>::Error;
    fn apply(&mut self, group: GroupId, index: u64, bytes: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.inner.apply(group, index, bytes)
    }
    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        self.inner.snapshot(group)
    }
    fn restore(&mut self, group: GroupId, bytes: &[u8]) -> Result<(), Self::Error> {
        self.inner.restore(group, bytes)
    }
    fn freeze_bounded(
        &self,
        group: GroupId,
        cap: usize,
    ) -> Result<Vec<u8>, CaptureError<Self::Error>> {
        let _ = self.entered.send(());
        let (lock, ready) = &*self.gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        self.inner.freeze_bounded(group, cap)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_waiter_keeps_conflicting_operation_busy_until_native_completion() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir().unwrap();
    let (entered, receiver) = mpsc::channel();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let shared = gate.clone();
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = root.path().into();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.snapshot_mode = SnapshotMode::NativeDurable;
    config.retain_log_entries = 0;
    let node = Arc::new(
        MultiRaft::start_with_factory(config, move |_| {
            Ok(GatedFsm {
                inner: CounterFsm::new(),
                entered: entered.clone(),
                gate: shared.clone(),
            })
        })
        .await
        .unwrap(),
    );
    node.create_group(7, &[1]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_leader(7) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.propose(7, CounterFsm::encode_add(3, 1)).await.unwrap();
    let task_node = node.clone();
    let waiter = tokio::spawn(async move { task_node.request_compaction(7).await });
    tokio::task::spawn_blocking(move || receiver.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    let preparing_progress = node.local_storage_status(7).await.unwrap().progress;
    eprintln!("preparing_progress={preparing_progress:?}");
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(
        node.request_compaction(7).await.unwrap_err(),
        CompactionRejection::Busy
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    let mut last_status = None;
    let completed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = node.local_storage_status(7).await.unwrap();
            let done = status.progress == CompactionProgress::CompletedObserved;
            last_status = Some(status);
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "last storage observation: {last_status:?}"
    );
    assert_eq!(
        node.read_linearizable(7, |fsm| fsm.inner.value(7))
            .await
            .unwrap(),
        3
    );
    node.shutdown().await.unwrap();
    assert_ne!(
        preparing_progress,
        CompactionProgress::Submitted,
        "capture preparation is not native trigger submission"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_owned_capture_and_rejects_new_intake() {
    let root = tempfile::tempdir().unwrap();
    let (entered, receiver) = mpsc::channel();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let shared = gate.clone();
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = root.path().into();
    config.file_log_sync_level = FileLogSyncLevel::Data;
    config.snapshot_mode = SnapshotMode::NativeDurable;
    let node = Arc::new(
        MultiRaft::start_with_factory(config, move |_| {
            Ok(GatedFsm {
                inner: CounterFsm::new(),
                entered: entered.clone(),
                gate: shared.clone(),
            })
        })
        .await
        .unwrap(),
    );
    node.create_group(7, &[1]).await.unwrap();
    let request_node = node.clone();
    let waiter = tokio::spawn(async move { request_node.request_compaction(7).await });
    tokio::task::spawn_blocking(move || receiver.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    let shutdown_node = node.clone();
    let mut shutdown = tokio::spawn(async move { shutdown_node.shutdown().await });
    let returned_early = tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
        .await
        .is_ok();
    let rejection = node.request_compaction(7).await.unwrap_err();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    if !returned_early {
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    let _ = waiter.await;
    assert_eq!(rejection, CompactionRejection::ShuttingDown);
    assert!(
        !returned_early,
        "shutdown returned while its capture owner was still running"
    );
}
