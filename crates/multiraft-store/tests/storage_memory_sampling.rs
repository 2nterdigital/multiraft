//! Storage sampling through `try_with_storage_memory_stats` never waits for the
//! FSM lock, and keeps its captured admission permit until the blocking work
//! really ends, even when the awaiting caller is cancelled.
use std::io;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use multiraft_fsm::{
    AppliedState, ApplyOut, CounterFsm, DiskState, GroupId, StateMachine, StorageMemoryStats,
};
use multiraft_store::StateMachineStore;
use tokio::sync::{oneshot, Semaphore};
use tokio::time::{sleep, timeout, Instant};

const GROUP: u64 = 7;
const BOUND: Duration = Duration::from_secs(5);

/// A disk FSM whose memory sample blocks under the FSM lock until released.
struct SlowSampleFsm {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl StateMachine for SlowSampleFsm {
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

impl DiskState for SlowSampleFsm {
    fn storage_memory_stats(&self) -> io::Result<Option<StorageMemoryStats>> {
        if let Some(entered) = self.entered.lock().unwrap().take() {
            entered.send(()).unwrap();
        }
        self.release.lock().unwrap().recv().unwrap();
        Ok(Some(StorageMemoryStats::default()))
    }
    fn durable_applied(&self) -> io::Result<Option<Vec<u8>>> {
        Ok(None)
    }
    fn apply_durable(
        &mut self,
        _: GroupId,
        _: u64,
        _: Option<&[u8]>,
        _: &AppliedState,
    ) -> io::Result<ApplyOut> {
        Err(io::Error::other("not applied in this test"))
    }
}

#[tokio::test]
async fn cancelled_sample_holds_permit_until_blocking_work_ends() {
    let budget = Arc::new(Semaphore::new(1));
    let permit = budget.clone().acquire_owned().await.unwrap();
    let store = StateMachineStore::new(GROUP, CounterFsm::default());
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();

    let sample = store.try_with_storage_memory_stats(move |_| {
        let _permit = permit;
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    assert!(
        timeout(Duration::from_millis(50), sample).await.is_err(),
        "the blocked sample must outlive the caller's timeout"
    );
    timeout(BOUND, entered_rx).await.unwrap().unwrap();
    assert_eq!(
        budget.available_permits(),
        0,
        "dropping the caller must not release the permit while the worker runs"
    );

    // Admitted work after the FSM sample does not keep the FSM lock.
    let next = timeout(BOUND, store.try_with_storage_memory_stats(|sample| sample))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(next, Ok(None)), "{next:?}");

    release_tx.send(()).unwrap();
    let deadline = Instant::now() + BOUND;
    while budget.available_permits() != 1 {
        assert!(Instant::now() < deadline, "permit was not returned in time");
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn busy_fsm_lock_skips_memory_sample_without_waiting() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let store = StateMachineStore::new(
        GROUP,
        SlowSampleFsm {
            entered: Mutex::new(Some(entered_tx)),
            release: Mutex::new(release_rx),
        },
    );
    let holder = tokio::spawn({
        let store = store.clone();
        async move { store.try_with_storage_memory_stats(|sample| sample).await }
    });
    timeout(BOUND, entered_rx).await.unwrap().unwrap();

    let started = Instant::now();
    let skipped = timeout(BOUND, store.try_with_storage_memory_stats(|sample| sample))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(skipped.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert!(started.elapsed() < Duration::from_secs(1));

    release_tx.send(()).unwrap();
    let held = timeout(BOUND, holder).await.unwrap().unwrap().unwrap();
    assert!(held.unwrap().is_some());
}
