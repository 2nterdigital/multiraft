//! Capture isolation, fences, and ownership through the real DiskState callback.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct CheckpointGate {
    entered: tokio::sync::Notify,
    started: AtomicBool,
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
    fail: AtomicBool,
    checkpoint_first: AtomicBool,
    path: std::sync::Mutex<Option<PathBuf>>,
}

impl CheckpointGate {
    pub(super) fn checkpoint(&self, db: &DiskFsmDb, dir: &Path) -> io::Result<()> {
        *self.path.lock().unwrap() = Some(dir.to_path_buf());
        if self.checkpoint_first.load(Ordering::SeqCst) {
            db.checkpoint(dir)?;
            self.wait()
        } else {
            self.wait()?;
            db.checkpoint(dir)
        }
    }

    fn wait(&self) -> io::Result<()> {
        self.started.store(true, Ordering::SeqCst);
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(io::Error::other("checkpoint IO failure"));
        }
        Ok(())
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }

    // Independent of Tokio: the old implementation cannot hang the test worker.
    fn watchdog(
        self: &Arc<Self>,
        progress: Vec<Arc<AtomicBool>>,
    ) -> std::thread::JoinHandle<Vec<bool>> {
        let gate = self.clone();
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !gate.started.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(Duration::from_millis(500));
            let observed = progress.iter().map(|p| p.load(Ordering::SeqCst)).collect();
            gate.release();
            observed
        })
    }
}

async fn gated(root: &Path, gate: Arc<CheckpointGate>) -> StateMachineStore<DiskBlobs> {
    gated_with_budget(root, gate, Arc::new(Semaphore::new(1))).await
}

async fn gated_with_budget(
    root: &Path,
    gate: Arc<CheckpointGate>,
    build_budget: Arc<Semaphore>,
) -> StateMachineStore<DiskBlobs> {
    let mut fsm = DiskBlobs::open(&path(root));
    fsm.checkpoint_gate = Some(gate);
    let mut source = StateMachineStore::with_native_options(
        GROUP,
        fsm,
        NativeSmOptions {
            catalog: Arc::new(SnapshotCatalog::new(root.join("snapshots"), 1)),
            max_snapshot_bytes: 1024 * 1024,
            build_budget,
        },
    )
    .unwrap();
    apply(&mut source, vec![membership(0), add(1, 9)]).await;
    source
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn checkpoint_allows_unrelated_group_and_timer_on_single_worker() {
    let root = scratch();
    let gate = Arc::new(CheckpointGate::default());
    let source = gated(root.path(), gate.clone()).await;
    let other_root = scratch();
    // A distinct Group and a distinct database; all probes execute on the worker.
    let other_fsm = DiskBlobs {
        db: DiskFsmDb::open(
            DiskFsmDb::group_path(other_root.path(), 8),
            8,
            options(b"disk-blobs-v1"),
        )
        .unwrap(),
        _send_only: std::cell::Cell::new(()),
        workspace_ready: None,
        inspection: None,
        checkpoint_gate: None,
    };
    let mut other = StateMachineStore::new(8, other_fsm);
    let applied = Arc::new(AtomicBool::new(false));
    let timer = Arc::new(AtomicBool::new(false));
    let watchdog = gate.watchdog(vec![applied.clone(), timer.clone()]);
    let capture = tokio::spawn(async move { source.reserve_native_compaction().await });
    gate.entered.notified().await;
    let apply_probe = tokio::spawn(async move {
        apply(&mut other, vec![membership(0), add(1, 4)]).await;
        assert_eq!(other.applied_state().await.unwrap().0, Some(log_id(1)));
        applied.store(true, Ordering::SeqCst);
    });
    let timer_probe = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        timer.store(true, Ordering::SeqCst);
    });
    capture.await.unwrap().unwrap();
    apply_probe.await.unwrap();
    timer_probe.await.unwrap();
    let observed = watchdog.join().unwrap();
    assert_eq!(
        observed,
        vec![true, true],
        "unrelated apply/read and timer must progress before checkpoint gate release"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn checkpoint_fences_same_group_and_keeps_exact_applied_membership_cut() {
    let root = scratch();
    let gate = Arc::new(CheckpointGate::default());
    let source = gated(root.path(), gate.clone()).await;
    let watchdog = gate.watchdog(vec![]);
    let capturing = source.clone();
    let capture = tokio::spawn(async move { capturing.reserve_native_compaction().await });
    gate.entered.notified().await;
    let mut advancing = source.clone();
    let mut next = Box::pin(apply(&mut advancing, vec![membership(2), add(3, 4)]));
    assert!(
        futures::poll!(next.as_mut()).is_pending(),
        "same Group apply must wait for checkpoint's atomic cut"
    );
    let mut reading = Box::pin(source.try_with_fsm(|fsm| fsm.sum()));
    assert!(
        futures::poll!(reading.as_mut()).is_pending(),
        "same Group reads retain their existing FSM fence"
    );
    gate.release();
    let reservation = capture.await.unwrap().unwrap();
    assert_eq!(reservation.target, Some(log_id(1)));
    next.await;
    assert_eq!(reading.await.unwrap(), 13);
    let mut builder_source = source.clone();
    let snapshot = builder_source
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(log_id(1)));
    assert_eq!(snapshot.meta.last_membership.log_id(), &Some(log_id(0)));
    reservation.completion.await.unwrap();
    let receiver_root = scratch();
    let mut receiver = store(
        receiver_root.path(),
        DiskBlobs::open(&path(receiver_root.path())),
    );
    let manifest = snapshot.snapshot.into_inner();
    transfer(&receiver, manifest.clone(), None).await.unwrap();
    receiver
        .verify_disk_snapshot(
            &wire_snapshot_meta(GROUP, &snapshot.meta, &manifest).unwrap(),
            &manifest,
        )
        .await
        .unwrap();
    let _install = INSTALLS.lock().await;
    receiver
        .install_snapshot(&snapshot.meta, std::io::Cursor::new(manifest))
        .await
        .unwrap();
    assert_eq!(receiver.with_fsm(|fsm| fsm.sum()).await, 9);
    assert_eq!(receiver.applied_state().await.unwrap().0, Some(log_id(1)));
    watchdog.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cancelled_checkpoint_retains_fence_permits_and_quiescence_until_io_finishes() {
    let root = scratch();
    let gate = Arc::new(CheckpointGate::default());
    gate.checkpoint_first.store(true, Ordering::SeqCst);
    let budget = Arc::new(Semaphore::new(1));
    let source = gated_with_budget(root.path(), gate.clone(), budget.clone()).await;
    let watchdog = gate.watchdog(vec![]);
    let capturing = source.clone();
    let capture = tokio::spawn(async move { capturing.reserve_native_compaction().await });
    gate.entered.notified().await;
    let checkpoint = gate.path.lock().unwrap().clone().unwrap();
    assert!(
        checkpoint.is_dir(),
        "real RocksDB checkpoint exists before cancellation"
    );
    capture.abort();
    assert!(matches!(capture.await, Err(error) if error.is_cancelled()));
    assert!(matches!(
        source.reserve_native_compaction().await,
        Err(multiraft_store::NativeCaptureError::Refused(
            multiraft_fsm::CaptureRefusal::Busy
        ))
    ));
    assert_eq!(
        budget.available_permits(),
        0,
        "cancelled IO retains node admission"
    );
    let mut reading = Box::pin(source.try_with_fsm(|fsm| fsm.sum()));
    assert!(futures::poll!(reading.as_mut()).is_pending());
    source.close_native_intake();
    let mut quiescent = Box::pin(source.wait_native_quiescent());
    assert!(
        futures::poll!(quiescent.as_mut()).is_pending(),
        "cancelled waiter cannot complete quiescence while its checkpoint owns IO"
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), quiescent)
        .await
        .unwrap();
    assert!(
        !checkpoint.exists(),
        "quiescence includes cancelled checkpoint directory cleanup"
    );
    assert_eq!(budget.available_permits(), 1);
    drop(reading);
    watchdog.join().unwrap();
}

#[tokio::test]
async fn checkpoint_error_releases_admission_and_success_retains_it_until_reservation_release() {
    let root = scratch();
    let gate = Arc::new(CheckpointGate::default());
    gate.fail.store(true, Ordering::SeqCst);
    gate.release();
    let budget = Arc::new(Semaphore::new(1));
    let mut source = gated_with_budget(root.path(), gate.clone(), budget.clone()).await;
    let error = source.reserve_native_compaction().await.err().unwrap();
    assert!(
        matches!(error, multiraft_store::NativeCaptureError::Io(error) if error.to_string() == "checkpoint IO failure")
    );
    assert!(source.native_snapshot_info().await.unwrap().is_none());
    assert_eq!(source.applied_state().await.unwrap().0, Some(log_id(1)));
    assert_eq!(budget.available_permits(), 1);
    gate.fail.store(false, Ordering::SeqCst);
    let reservation = source.reserve_native_compaction().await.unwrap();
    assert_eq!(reservation.target, Some(log_id(1)));
    assert_eq!(budget.available_permits(), 0);
    assert!(matches!(
        source.reserve_native_compaction().await,
        Err(multiraft_store::NativeCaptureError::Refused(
            multiraft_fsm::CaptureRefusal::Busy
        ))
    ));
    source.release_native_reservation();
    assert_eq!(budget.available_permits(), 1);
    assert!(reservation.completion.await.is_err());
    let next = source.reserve_native_compaction().await.unwrap();
    assert_eq!(next.target, Some(log_id(1)));
    source.release_native_reservation();
}

#[test]
fn cancelled_publisher_keeps_queued_io_inside_quiescence() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let root = scratch();
            let gate = Arc::new(CheckpointGate::default());
            gate.release();
            let mut source = gated(root.path(), gate).await;
            let reservation = source.reserve_native_compaction().await.unwrap();
            let mut builder = source.get_snapshot_builder().await;
            let entered = Arc::new(tokio::sync::Notify::new());
            let entered_child = entered.clone();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered_child.notify_one();
                // Bounded even if an assertion in the async test panics.
                let _ = blocked.recv_timeout(Duration::from_secs(5));
            });
            entered.notified().await;
            let mut publishing = Box::pin(builder.build_snapshot());
            // The real publisher now owns the captured checkpoint and is queued
            // behind our blocker, rather than an artificial publication stub.
            assert!(futures::poll!(publishing.as_mut()).is_pending());
            drop(publishing);
            source.close_native_intake();
            let mut quiescent = Box::pin(source.wait_native_quiescent());
            let retained = futures::poll!(quiescent.as_mut()).is_pending();
            release.send(()).unwrap();
            blocker.await.unwrap();
            reservation.completion.await.unwrap();
            if retained {
                tokio::time::timeout(Duration::from_secs(5), quiescent).await.unwrap();
            }
            assert!(retained, "cancelled publication cannot release its transition while actual publisher IO remains queued");
        });
}

#[test]
fn queued_capture_keeps_admission_until_closed_owner_rejection_finishes() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let root = scratch();
            let gate = Arc::new(CheckpointGate::default());
            gate.release();
            let budget = Arc::new(Semaphore::new(1));
            let source = gated_with_budget(root.path(), gate.clone(), budget.clone()).await;
            let entered = Arc::new(tokio::sync::Notify::new());
            let entered_child = entered.clone();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered_child.notify_one();
                let _ = blocked.recv_timeout(Duration::from_secs(5));
            });
            entered.notified().await;
            let mut capture = Box::pin(source.reserve_native_compaction());
            assert!(futures::poll!(capture.as_mut()).is_pending());
            drop(capture);
            source.close_native_intake();
            let mut quiescent = Box::pin(source.wait_native_quiescent());
            let retained = futures::poll!(quiescent.as_mut()).is_pending();
            let admitted = budget.available_permits();
            release.send(()).unwrap();
            blocker.await.unwrap();
            if retained {
                tokio::time::timeout(Duration::from_secs(5), quiescent)
                    .await
                    .unwrap();
            }
            assert!(
                retained,
                "queued capture owns its fences until blocking work rejects the closed owner"
            );
            assert_eq!(admitted, 0);
            assert_eq!(budget.available_permits(), 1);
            assert!(
                !gate.started.load(Ordering::SeqCst),
                "close rejects queued capture before invoking the FSM checkpoint callback"
            );
            assert!(gate.path.lock().unwrap().is_none());
        });
}
